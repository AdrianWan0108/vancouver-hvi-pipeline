from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List, Set

import geopandas as gpd
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.config import OUTPUTS_DIR, get_inputs  # noqa: E402


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


EXACT_NAME_TO_KEY: Dict[str, str] = {
    "Population, 2021": "pop_total",
    "Unemployment rate": "unemployment_rate",
    "Prevalence of low income based on the Low-income measure, after tax (LIM-AT) (%)": "low_income_rate",
    "Living alone": "living_alone_count",
}

AGE_CHARACTERISTIC_ID_TO_KEY: Dict[str, str] = {
    "1462": "seniors_65to74_count",
    "1463": "seniors_75to84_count",
    "1464": "seniors_85plus_count",
}

NAMES_WE_CARE: Set[str] = set(EXACT_NAME_TO_KEY.keys())
AGE_IDS_WE_CARE: Set[str] = set(AGE_CHARACTERISTIC_ID_TO_KEY.keys())


def main() -> int:
    ins = get_inputs()

    da_gpkg = OUTPUTS_DIR / "da.gpkg"
    capacity_csv = OUTPUTS_DIR / "landcover_housing_capacity.csv"
    if not da_gpkg.exists():
        print(f"ERROR: Missing {da_gpkg}. Run scripts/01_prepare_da.py first.")
        return 1
    if not capacity_csv.exists():
        print(f"ERROR: Missing {capacity_csv}. Run scripts/02_landcover_housing_capacity.py first.")
        return 1

    census_csv = Path(ins.census_csv)
    if not census_csv.exists():
        print(f"ERROR: Census CSV not found: {census_csv}")
        return 1

    print("=== 03_census_social.py ===")
    print("DA base:", da_gpkg)
    print("Landcover/capacity input:", capacity_csv)
    print("Census CSV:", census_csv)

    da = gpd.read_file(da_gpkg, layer="da")
    if "DGUID" not in da.columns:
        print("ERROR: DA layer missing DGUID. Check 01_prepare_da output.")
        return 1

    adapt = pd.read_csv(capacity_csv, low_memory=False)
    required_cols = {"DGUID", "da_eligible"}
    missing = required_cols - set(adapt.columns)
    if missing:
        print(f"ERROR: landcover_housing_capacity.csv missing required columns: {sorted(missing)}")
        return 1

    adapt["DGUID"] = adapt["DGUID"].astype(str)
    eligible_dguids = set(adapt.loc[adapt["da_eligible"].fillna(False).astype(bool), "DGUID"].tolist())
    valid_dguids = set(da["DGUID"].astype(str).tolist()) & eligible_dguids
    print(f"Loaded eligible DGUIDs: {len(valid_dguids):,}")

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
        "seniors_65to74_count": 0,
        "seniors_75to84_count": 0,
        "seniors_85plus_count": 0,
    }

    print("Streaming census CSV in chunks...")
    print("  (Tip: this may take a few minutes on a 3.38GB file - no need to stop it)")

    reader = pd.read_csv(
        census_csv,
        usecols=usecols,
        chunksize=chunk_size,
        low_memory=False,
        encoding="cp1252",
        dtype=str,
    )

    for i, chunk in enumerate(reader, start=1):
        geo_level = chunk["GEO_LEVEL"].fillna("")
        chunk = chunk.loc[geo_level.str.contains("Dissemination", case=False, na=False)]
        if chunk.empty:
            continue

        chunk = chunk.copy()

        chunk["CHARACTERISTIC_NAME"] = chunk["CHARACTERISTIC_NAME"].fillna("").str.strip()
        chunk["CHARACTERISTIC_ID"] = chunk["CHARACTERISTIC_ID"].fillna("").astype(str).str.strip()

        keep_row = chunk["CHARACTERISTIC_NAME"].isin(NAMES_WE_CARE) | chunk["CHARACTERISTIC_ID"].isin(AGE_IDS_WE_CARE)
        chunk = chunk.loc[keep_row]
        if chunk.empty:
            continue

        chunk.loc[:, "DGUID"] = chunk["DGUID"].astype(str)
        chunk = chunk.loc[chunk["DGUID"].isin(valid_dguids)]
        if chunk.empty:
            continue

        indicator_key = chunk["CHARACTERISTIC_NAME"].map(EXACT_NAME_TO_KEY)
        indicator_key = indicator_key.combine_first(chunk["CHARACTERISTIC_ID"].map(AGE_CHARACTERISTIC_ID_TO_KEY))

        keep_mask = indicator_key.notna()
        if not keep_mask.any():
            continue

        out = chunk.loc[keep_mask].copy()
        out.loc[:, "indicator_key"] = indicator_key.loc[keep_mask].astype(str)

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

    df["C1_COUNT_TOTAL"] = to_num(df["C1_COUNT_TOTAL"])
    df["C10_RATE_TOTAL"] = to_num(df["C10_RATE_TOTAL"])

    age_count_keys = {"seniors_65to74_count", "seniors_75to84_count", "seniors_85plus_count"}
    is_age_count = df["indicator_key"].isin(age_count_keys)
    df.loc[is_age_count & df["C1_COUNT_TOTAL"].isna(), "C1_COUNT_TOTAL"] = pd.NA
    df.loc[
        is_age_count
        & df["C1_COUNT_TOTAL"].notna()
        & df["C10_RATE_TOTAL"].notna()
        & (df["C1_COUNT_TOTAL"] == df["C10_RATE_TOTAL"]),
        "C1_COUNT_TOTAL",
    ] = pd.NA

    out_long = OUTPUTS_DIR / "census_social_selected_long.csv"
    df.to_csv(out_long, index=False)
    print("Wrote:", out_long)

    wide = pd.DataFrame({"DGUID": sorted(valid_dguids)}).set_index("DGUID")

    pop = df[df["indicator_key"] == "pop_total"].groupby("DGUID")["C1_COUNT_TOTAL"].first()
    wide["pop_total"] = pop

    unemp = df[df["indicator_key"] == "unemployment_rate"].groupby("DGUID")["C10_RATE_TOTAL"].first()
    wide["unemployment_rate"] = unemp

    lowinc = df[df["indicator_key"] == "low_income_rate"].groupby("DGUID")["C10_RATE_TOTAL"].first()
    wide["low_income_rate"] = lowinc

    seniors_65to74 = df[df["indicator_key"] == "seniors_65to74_count"].groupby("DGUID")["C1_COUNT_TOTAL"].first()
    seniors_75to84 = df[df["indicator_key"] == "seniors_75to84_count"].groupby("DGUID")["C1_COUNT_TOTAL"].first()
    seniors_85plus = df[df["indicator_key"] == "seniors_85plus_count"].groupby("DGUID")["C1_COUNT_TOTAL"].first()
    wide["seniors_65to74_count"] = seniors_65to74
    wide["seniors_75to84_count"] = seniors_75to84
    wide["seniors_85plus_count"] = seniors_85plus
    wide["seniors_75plus_count"] = wide[["seniors_75to84_count", "seniors_85plus_count"]].sum(axis=1, min_count=1)
    wide["seniors_65plus_count"] = wide[
        ["seniors_65to74_count", "seniors_75to84_count", "seniors_85plus_count"]
    ].sum(axis=1, min_count=1)

    for col in [
        "seniors_65to74_count",
        "seniors_75to84_count",
        "seniors_85plus_count",
        "seniors_75plus_count",
        "seniors_65plus_count",
    ]:
        wide.loc[wide[col] > wide["pop_total"], col] = pd.NA
        wide.loc[wide[col] < 0, col] = pd.NA

    alone = df[df["indicator_key"] == "living_alone_count"].groupby("DGUID")["C1_COUNT_TOTAL"].first()
    wide["living_alone_count"] = alone

    wide["pct_seniors_65plus"] = (wide["seniors_65plus_count"] / wide["pop_total"]) * 100.0
    wide["pct_seniors_75plus"] = (wide["seniors_75plus_count"] / wide["pop_total"]) * 100.0
    wide["pct_living_alone"] = (wide["living_alone_count"] / wide["pop_total"]) * 100.0

    normalized_components = {
        "unemployment_rate": wide["unemployment_rate"],
        "low_income_rate": wide["low_income_rate"],
        "pct_seniors_65plus": wide["pct_seniors_65plus"],
        "pct_seniors_75plus": wide["pct_seniors_75plus"],
        "pct_living_alone": wide["pct_living_alone"],
    }

    for k, s in normalized_components.items():
        wide[f"{k}_n01"] = normalize_01(s)

    final_component_cols = [
        "unemployment_rate_n01",
        "low_income_rate_n01",
        "pct_seniors_65plus_n01",
        "pct_living_alone_n01",
    ]
    comparison_component_cols = [
        "unemployment_rate_n01",
        "low_income_rate_n01",
        "pct_seniors_75plus_n01",
        "pct_living_alone_n01",
    ]
    wide["sensitivity_index"] = wide[final_component_cols].mean(axis=1, skipna=True)
    wide["sensitivity_index_75plus_comparison"] = wide[comparison_component_cols].mean(axis=1, skipna=True)

    out_csv = OUTPUTS_DIR / "census_sensitivity.csv"
    wide.reset_index().to_csv(out_csv, index=False)
    print("Wrote:", out_csv)

    report_path = OUTPUTS_DIR / "03_census_social_debug_report.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("03_census_social debug report\n")
        f.write(f"Census CSV: {census_csv}\n")
        f.write(f"Landcover/capacity input: {capacity_csv}\n")
        f.write(f"Eligible DA count: {len(valid_dguids):,}\n")
        f.write(f"Selected rows: {len(df):,}\n\n")

        f.write("Matched row counts by indicator:\n")
        for k, v in found_counts.items():
            f.write(f"  {k}: {v:,}\n")

        f.write("\nMissingness in sensitivity output:\n")
        miss = wide.isna().sum().sort_values(ascending=False)
        for col, cnt in miss.items():
            f.write(f"  {col}: {int(cnt):,}\n")

        f.write("\nSensitivity comparison summaries:\n")
        for col in [
            "pct_seniors_65plus",
            "pct_seniors_75plus",
            "sensitivity_index",
            "sensitivity_index_75plus_comparison",
        ]:
            f.write(f"\n{col}:\n")
            summary = pd.to_numeric(wide[col], errors="coerce").describe(
                percentiles=[0.05, 0.25, 0.5, 0.75, 0.95]
            )
            f.write(str(summary) + "\n")

    print("Wrote:", report_path)
    print("Matched rows by indicator:", found_counts)
    print("Done. Next: run scripts/04_canue_exposure.py and visualize sensitivity_index if needed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
