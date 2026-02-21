# scripts/05_hvi_composite.py
from __future__ import annotations

import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd

# --- Make project root importable (same approach as 01_prepare_da.py) ---
sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.config import DATA_INTERMEDIATE, CRS_WGS84  # noqa: E402


def print_bbox(gdf: gpd.GeoDataFrame, label: str) -> None:
    """Print bbox in WGS84 to sanity-check geometry extent."""
    b = gdf.total_bounds  # [minx, miny, maxx, maxy]
    print(
        f"{label} bbox (WGS84): "
        f"minx={b[0]:.4f}, miny={b[1]:.4f}, maxx={b[2]:.4f}, maxy={b[3]:.4f}"
    )


def normalize_01(s: pd.Series) -> pd.Series:
    """Min-max normalize to [0,1], ignoring NaNs."""
    s = pd.to_numeric(s, errors="coerce")
    mn = s.min(skipna=True)
    mx = s.max(skipna=True)
    if pd.isna(mn) or pd.isna(mx) or mx == mn:
        return pd.Series([pd.NA] * len(s), index=s.index, dtype="Float64")
    return (s - mn) / (mx - mn)


def main() -> int:
    da_gpkg = DATA_INTERMEDIATE / "da.gpkg"
    sens_csv = DATA_INTERMEDIATE / "sensitivity.csv"
    adapt_csv = DATA_INTERMEDIATE / "adaptive_capacity.csv"
    expo_csv = DATA_INTERMEDIATE / "exposure.csv"

    if not da_gpkg.exists():
        print(f"ERROR: Missing {da_gpkg}. Run 01_prepare_da.py first.")
        return 1
    if not sens_csv.exists():
        print(f"ERROR: Missing {sens_csv}. Run 02_census_sensitivity.py first.")
        return 1
    if not adapt_csv.exists():
        print(f"ERROR: Missing {adapt_csv}. Run 03_adaptive_capacity.py first.")
        return 1
    if not expo_csv.exists():
        print(f"ERROR: Missing {expo_csv}. Run 04_exposure_lst.py first.")
        return 1

    print("=== 05_hvi_composite.py ===")
    print("DA:", da_gpkg)
    print("Sensitivity:", sens_csv)
    print("Adaptive:", adapt_csv)
    print("Exposure:", expo_csv)

    # --- Base DA geometry (already filtered to Metro Vancouver in script 01) ---
    da = gpd.read_file(da_gpkg, layer="da")
    if "DGUID" not in da.columns:
        print("ERROR: DA layer missing DGUID.")
        return 1

    da = da.to_crs(CRS_WGS84)
    print(f"DA features: {len(da):,}")
    print_bbox(da, "DA layer (WGS84)")

    da["DGUID"] = da["DGUID"].astype(str)
    da = da[["DGUID", "geometry"]].copy()

    # --- Load component tables ---
    sens = pd.read_csv(sens_csv, low_memory=False)
    adapt = pd.read_csv(adapt_csv, low_memory=False)
    expo = pd.read_csv(expo_csv, low_memory=False)

    for df, name in [(sens, "sensitivity"), (adapt, "adaptive_capacity"), (expo, "exposure")]:
        if "DGUID" not in df.columns:
            print(f"ERROR: {name} missing DGUID column.")
            return 1
        df["DGUID"] = df["DGUID"].astype(str)

    # --- Columns we WANT for the frontend ---
    # Sensitivity: include ALL the indicators you listed
    sens_frontend_cols = [
        "DGUID",
        "pop_total",
        "unemployment_rate",
        "low_income_rate",
        "seniors_65plus_count",
        "living_alone_count",
        "pct_seniors_65plus",
        "pct_living_alone",
        "unemployment_rate_n01",
        "low_income_rate_n01",
        "pct_seniors_65plus_n01",
        "pct_living_alone_n01",
        "sensitivity_index",
    ]
    # Keep only those that exist (so script won’t break if you tweak earlier scripts)
    sens_keep = [c for c in sens_frontend_cols if c in sens.columns]

    if "sensitivity_index" not in sens_keep:
        print("ERROR: sensitivity.csv missing sensitivity_index.")
        print("Columns:", list(sens.columns))
        return 1

    # Adaptive: include green_frac + adaptive index + class-specific fractions (optional but you want them)
    adapt_frontend_cols = [
        "DGUID",
        "adaptive_capacity_index",
        "green_frac",
        "frac_coniferous",
        "frac_deciduous",
        "frac_shrub",
        "frac_modified_herb",
        "frac_natural_herb",
    ]
    adapt_keep = [c for c in adapt_frontend_cols if c in adapt.columns]

    if "adaptive_capacity_index" not in adapt_keep:
        print("ERROR: adaptive_capacity.csv missing adaptive_capacity_index.")
        print("Columns:", list(adapt.columns))
        return 1

    # Exposure: keep exposure_mean for dropdown; keep exposure_index for HVI math
    expo_frontend_cols = [
        "DGUID",
        "exposure_mean",
        "exposure_index",
    ]
    expo_keep = [c for c in expo_frontend_cols if c in expo.columns]

    if "exposure_index" not in expo_keep:
        print("ERROR: exposure.csv missing exposure_index (needed for HVI).")
        print("Columns:", list(expo.columns))
        return 1

    # Reduce tables
    sens = sens[sens_keep].copy()
    adapt = adapt[adapt_keep].copy()
    expo = expo[expo_keep].copy()

    # --- Left joins anchored to DA universe ---
    out = da.merge(sens, on="DGUID", how="left")
    out = out.merge(adapt, on="DGUID", how="left")
    out = out.merge(expo, on="DGUID", how="left")

    # Coverage flags
    out["has_sensitivity"] = out["sensitivity_index"].notna()
    out["has_adaptive"] = out["adaptive_capacity_index"].notna()
    out["has_exposure"] = out["exposure_index"].notna()
    out["hvi_complete"] = out["has_sensitivity"] & out["has_adaptive"] & out["has_exposure"]

    # --- Compute HVI in normalized component space ---
    # H = E * (S - A)
    S = pd.to_numeric(out["sensitivity_index"], errors="coerce")
    A = pd.to_numeric(out["adaptive_capacity_index"], errors="coerce")
    E = pd.to_numeric(out["exposure_index"], errors="coerce")

    out["hvi_raw"] = E * (S - A)

    complete_mask = out["hvi_complete"] & out["hvi_raw"].notna()
    out.loc[~complete_mask, "hvi_raw"] = pd.NA

    out["hvi_index_n01"] = pd.NA
    if complete_mask.any():
        out.loc[complete_mask, "hvi_index_n01"] = normalize_01(out.loc[complete_mask, "hvi_raw"])

    # --- Write component table (no geometry) ---
    out_table = out.drop(columns=["geometry"]).copy()
    out_csv = DATA_INTERMEDIATE / "hvi_components.csv"
    out_table.to_csv(out_csv, index=False)
    print("Wrote:", out_csv)

    # --- Export GeoJSON for MapLibre ---
    # Keep: everything you want in dropdown + HVI fields + flags
    keep_props = (
        [c for c in sens_frontend_cols if c != "DGUID"]
        + [c for c in adapt_frontend_cols if c != "DGUID"]
        + ["exposure_mean"]  # only this one exposed for dropdown
        + ["hvi_raw", "hvi_index_n01", "has_sensitivity", "has_adaptive", "has_exposure", "hvi_complete"]
    )

    # Also keep exposure_index internally? (optional)
    # If you don't want it visible in frontend dropdown, you can still keep it for debugging.
    if "exposure_index" in out.columns:
        keep_props.append("exposure_index")

    # Ensure unique + existing
    keep_props = [c for c in dict.fromkeys(keep_props) if c in out.columns]

    gdf = out[["DGUID"] + keep_props + ["geometry"]].copy()

    # Ensure CRS is WGS84 for web maps / tiling
    if gdf.crs is None:
        gdf = gdf.set_crs(CRS_WGS84)
    else:
        gdf = gdf.to_crs(CRS_WGS84)

    print_bbox(gdf, "HVI output (WGS84)")

    # Optional simplification to reduce GeoJSON size
    # (Safe to keep as-is; you can tune later if you see artifacts)
    gdf["geometry"] = gdf["geometry"].simplify(tolerance=0.0002, preserve_topology=True)

    # Fix numeric columns so QGIS reads them correctly
    numeric_like = [
        # sensitivity
        "pop_total",
        "unemployment_rate",
        "low_income_rate",
        "seniors_65plus_count",
        "living_alone_count",
        "pct_seniors_65plus",
        "pct_living_alone",
        "unemployment_rate_n01",
        "low_income_rate_n01",
        "pct_seniors_65plus_n01",
        "pct_living_alone_n01",
        "sensitivity_index",
        # adaptive
        "adaptive_capacity_index",
        "green_frac",
        "frac_coniferous",
        "frac_deciduous",
        "frac_shrub",
        "frac_modified_herb",
        "frac_natural_herb",
        # exposure
        "exposure_mean",
        "exposure_index",
        # hvi
        "hvi_raw",
        "hvi_index_n01",
    ]
    for col in numeric_like:
        if col in gdf.columns:
            gdf[col] = pd.to_numeric(gdf[col], errors="coerce")

    out_geojson = DATA_INTERMEDIATE / "hvi.geojson"
    gdf.to_file(out_geojson, driver="GeoJSON")
    print("Wrote:", out_geojson)

    # --- Debug report ---
    report = DATA_INTERMEDIATE / "05_hvi_debug_report.txt"
    with open(report, "w", encoding="utf-8") as f:
        f.write("05_hvi_composite debug report\n\n")
        f.write(f"DA count: {len(out):,}\n")
        f.write(f"has_sensitivity: {int(out['has_sensitivity'].sum()):,}\n")
        f.write(f"has_adaptive: {int(out['has_adaptive'].sum()):,}\n")
        f.write(f"has_exposure: {int(out['has_exposure'].sum()):,}\n")
        f.write(f"hvi_complete: {int(out['hvi_complete'].sum()):,}\n\n")

        if complete_mask.any():
            f.write("hvi_raw summary (complete only):\n")
            f.write(str(out.loc[complete_mask, "hvi_raw"].describe()) + "\n\n")
            f.write("hvi_index_n01 summary (complete only):\n")
            f.write(str(pd.to_numeric(out.loc[complete_mask, "hvi_index_n01"], errors="coerce").describe()) + "\n")

        # Quick missingness snapshot for your dropdown fields
        f.write("\nMissingness (selected output fields):\n")
        miss_cols = ["sensitivity_index", "adaptive_capacity_index", "exposure_mean", "hvi_index_n01"]
        for c in miss_cols:
            if c in out.columns:
                f.write(f"  {c}: {int(out[c].isna().sum()):,}\n")

    print("Wrote:", report)
    print("Done. Next: load hvi.geojson in QGIS or MapLibre.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())