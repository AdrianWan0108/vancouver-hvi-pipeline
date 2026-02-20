# scripts/02_census_sensitivity.py
from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, Set, List

import geopandas as gpd
import pandas as pd

# --- Make project root importable (same approach as 01_prepare_da.py) ---
sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.config import DATA_INTERMEDIATE, get_inputs  # noqa: E402


def to_num(s: pd.Series) -> pd.Series:
    """Safely parse numeric values from StatCan CSV columns."""
    return pd.to_numeric(s, errors="coerce")


def normalize_01(s: pd.Series) -> pd.Series:
    """Min-max normalize to [0,1], ignoring NaNs."""
    s = s.astype(float)
    mn = s.min(skipna=True)
    mx = s.max(skipna=True)
    if pd.isna(mn) or pd.isna(mx) or mx == mn:
        return pd.Series([pd.NA] * len(s), index=s.index, dtype="Float64")
    return (s - mn) / (mx - mn)


# --- Exact characteristic name matching (FAST + stable) ---
# NOTE: These must match the CSV text after .str.strip()
EXACT_NAME_TO_KEY: Dict[str, str] = {
    "Population, 2021": "pop_total",
    "Unemployment rate": "unemployment_rate",
    "Prevalence of low income based on the Low-income measure, after tax (LIM-AT) (%)": "low_income_rate",
    "Living alone": "living_alone_count",
}

# Seniors: we will sum these COUNT rows (not % rows)
SENIORS_NAMES: Set[str] = {
    "65 to 74 years",
    "75 years and over",
}

# Names we care about (early filter for speed)
NAMES_WE_CARE: Set[str] = set(EXACT_NAME_TO_KEY.keys()) | set(SENIORS_NAMES)


def main() -> int:
    ins = get_inputs()

    da_gpkg = DATA_INTERMEDIATE / "da.gpkg"
    if not da_gpkg.exists():
        print(f"ERROR: Missing {da_gpkg}. Run scripts/01_prepare_da.py first.")
        return 1

    census_csv = Path(ins.census_csv)
    if not census_csv.exists():
        print(f"ERROR: Census CSV not found: {census_csv}")
        return 1

    print("=== 02_census_sensitivity.py ===")
    print("DA base:", da_gpkg)
    print("Census CSV:", census_csv)

    # Load DA layer → get valid DGUIDs
    da = gpd.read_file(da_gpkg, layer="da")
    if "DGUID" not in da.columns:
        print("ERROR: DA layer missing DGUID. Check 01_prepare_da output.")
        return 1

    valid_dguids = set(da["DGUID"].astype(str).tolist())
    print(f"Loaded DGUIDs: {len(valid_dguids):,}")

    # Read StatCan CSV in chunks
    usecols = [
        "DGUID",
        "GEO_LEVEL",
        "GEO_NAME",
        "CHARACTERISTIC_ID",
        "CHARACTERISTIC_NAME",
        "C1_COUNT_TOTAL",
        "C10_RATE_TOTAL",
    ]

    chunk_size = 250_000
    selected_chunks: List[pd.DataFrame] = []

    found_counts = {
        "pop_total": 0,
        "unemployment_rate": 0,
        "low_income_rate": 0,
        "living_alone_count": 0,
        "seniors_65plus_count": 0,
    }

    print("Streaming census CSV in chunks...")
    print("  (Tip: this may take a few minutes on a 3.38GB file — no need to stop it)")

    # Use dtype=str (faster + avoids pandas StringArray overhead on huge files)
    reader = pd.read_csv(
        census_csv,
        usecols=usecols,
        chunksize=chunk_size,
        low_memory=False,
        encoding="cp1252",
        dtype=str,
    )

    for i, chunk in enumerate(reader, start=1):
        # 1) Keep only DA-level rows (cheap)
        geo_level = chunk["GEO_LEVEL"].fillna("")
        chunk = chunk.loc[geo_level.str.contains("Dissemination", case=False, na=False)]
        if chunk.empty:
            continue

        # Make a copy so we can safely assign without SettingWithCopyWarning
        chunk = chunk.copy()

        # 2) Early filter: keep only the characteristic names we care about (BIG speed win)
        cname = chunk["CHARACTERISTIC_NAME"].fillna("").str.strip()
        chunk = chunk.loc[cname.isin(NAMES_WE_CARE)]
        if chunk.empty:
            continue

        # 3) Now filter to DGUIDs we have geometry for (do this late)
        chunk.loc[:, "DGUID"] = chunk["DGUID"].astype(str)
        chunk = chunk.loc[chunk["DGUID"].isin(valid_dguids)]
        if chunk.empty:
            continue

        # Recompute stripped cname after filtering
        cname = chunk["CHARACTERISTIC_NAME"].fillna("").str.strip()

        # Map to indicator_key
        indicator_key = cname.map(EXACT_NAME_TO_KEY)

        # Seniors rows (sum multiple)
        is_seniors = cname.isin(SENIORS_NAMES)
        indicator_key = indicator_key.where(~is_seniors, "seniors_65plus_count")

        # Keep only matched rows
        keep_mask = indicator_key.notna()
        if not keep_mask.any():
            continue

        out = chunk.loc[keep_mask].copy()
        out.loc[:, "indicator_key"] = indicator_key.loc[keep_mask].astype(str)

        # Update counts
        vc = out["indicator_key"].value_counts()
        for k, v in vc.items():
            if k in found_counts:
                found_counts[k] += int(v)

        selected_chunks.append(out)

        if i % 20 == 0:
            print(f"  processed chunks: {i}")

    if not selected_chunks:
        print("ERROR: No matching DA-level rows found.")
        print("Check EXACT_NAME_TO_KEY strings match the Census CSV exactly.")
        return 1

    df = pd.concat(selected_chunks, ignore_index=True)
    print("Selected rows:", len(df))

    # Convert numeric columns (after selection only, to keep it fast)
    df["C1_COUNT_TOTAL"] = to_num(df["C1_COUNT_TOTAL"])
    df["C10_RATE_TOTAL"] = to_num(df["C10_RATE_TOTAL"])
    
    # ---- Clean seniors rows: keep only true count-like rows ----
    is_seniors = df["indicator_key"] == "seniors_65plus_count"

    # drop rows where count is missing
    df.loc[is_seniors & df["C1_COUNT_TOTAL"].isna(), "C1_COUNT_TOTAL"] = pd.NA

    # drop suspicious rows where "count" equals "rate" (usually not real counts)
    df.loc[
        is_seniors
        & df["C1_COUNT_TOTAL"].notna()
        & df["C10_RATE_TOTAL"].notna()
        & (df["C1_COUNT_TOTAL"] == df["C10_RATE_TOTAL"]),
        "C1_COUNT_TOTAL"
    ] = pd.NA

    # Debug: write extracted long slice
    out_long = DATA_INTERMEDIATE / "census_selected_long.csv"
    df.to_csv(out_long, index=False)
    print("Wrote:", out_long)

    # Build wide DA table
    wide = pd.DataFrame({"DGUID": sorted(valid_dguids)}).set_index("DGUID")

    # Population total
    pop = df[df["indicator_key"] == "pop_total"].groupby("DGUID")["C1_COUNT_TOTAL"].first()
    wide["pop_total"] = pop

    # Unemployment rate
    unemp = df[df["indicator_key"] == "unemployment_rate"].groupby("DGUID")["C10_RATE_TOTAL"].first()
    wide["unemployment_rate"] = unemp

    # Low income rate (LIM-AT prevalence %)
    lowinc = df[df["indicator_key"] == "low_income_rate"].groupby("DGUID")["C10_RATE_TOTAL"].first()
    wide["low_income_rate"] = lowinc

    # Seniors (sum of matched age bins)
    seniors = df[df["indicator_key"] == "seniors_65plus_count"].groupby("DGUID")["C1_COUNT_TOTAL"].sum()
    wide["seniors_65plus_count"] = seniors

    # Seniors sanity: cannot exceed population
    wide.loc[wide["seniors_65plus_count"] > wide["pop_total"], "seniors_65plus_count"] = pd.NA
    wide.loc[wide["seniors_65plus_count"] < 0, "seniors_65plus_count"] = pd.NA

    # Living alone count
    alone = df[df["indicator_key"] == "living_alone_count"].groupby("DGUID")["C1_COUNT_TOTAL"].first()
    wide["living_alone_count"] = alone

    # Derived percents
    wide["pct_seniors_65plus"] = (wide["seniors_65plus_count"] / wide["pop_total"]) * 100.0
    wide["pct_living_alone"] = (wide["living_alone_count"] / wide["pop_total"]) * 100.0

    # Normalize components (0–1)
    components = {
        "unemployment_rate": wide["unemployment_rate"],
        "low_income_rate": wide["low_income_rate"],
        "pct_seniors_65plus": wide["pct_seniors_65plus"],
        "pct_living_alone": wide["pct_living_alone"],
    }

    for k, s in components.items():
        wide[f"{k}_n01"] = normalize_01(s)

    wide["sensitivity_index"] = wide[[f"{k}_n01" for k in components]].mean(axis=1, skipna=True)

    # Write output
    out_csv = DATA_INTERMEDIATE / "sensitivity.csv"
    wide.reset_index().to_csv(out_csv, index=False)
    print("Wrote:", out_csv)

    # Debug report
    report_path = DATA_INTERMEDIATE / "02_census_debug_report.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("02_census_sensitivity debug report\n")
        f.write(f"Census CSV: {census_csv}\n")
        f.write(f"DA count: {len(valid_dguids):,}\n")
        f.write(f"Selected rows: {len(df):,}\n\n")

        f.write("Matched row counts by indicator:\n")
        for k, v in found_counts.items():
            f.write(f"  {k}: {v:,}\n")

        f.write("\nMissingness in sensitivity output:\n")
        miss = wide.isna().sum().sort_values(ascending=False)
        for col, cnt in miss.items():
            f.write(f"  {col}: {int(cnt):,}\n")

    print("Wrote:", report_path)
    print("Matched rows by indicator:", found_counts)
    print("Done. Next: join sensitivity.csv to da.gpkg in QGIS and visualize sensitivity_index.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
