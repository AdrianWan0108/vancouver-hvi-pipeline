# scripts/05_hvi_composite.py
from __future__ import annotations
import json

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
    print(f"{label} bbox (WGS84): minx={b[0]:.4f}, miny={b[1]:.4f}, maxx={b[2]:.4f}, maxy={b[3]:.4f}")


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

    # --- Base DA geometry ---
    da = gpd.read_file(da_gpkg, layer="da")

    # --- Sanity check & optional geographic filter ---
    # If your da.gpkg contains more than Metro Vancouver, Tippecanoe will be extremely slow.
    # A robust filter is to clip by a bbox around Metro Vancouver in WGS84.
    # (You can adjust these bounds if needed.)
    da = da.to_crs(CRS_WGS84)
    print_bbox(da, "DA layer (raw)")

    METRO_VAN_BBOX = (-123.6, 49.0, -122.2, 49.6)  # (minx, miny, maxx, maxy)
    minx, miny, maxx, maxy = METRO_VAN_BBOX
    da = da.cx[minx:maxx, miny:maxy].copy()
    print(f"DA count after Metro Vancouver bbox filter: {len(da):,}")
    print_bbox(da, "DA layer (filtered)")



    if "DGUID" not in da.columns:
        print("ERROR: DA layer missing DGUID.")
        return 1
    da["DGUID"] = da["DGUID"].astype(str)

    base_cols = ["DGUID", "geometry"]
    da = da[base_cols].copy()

    # --- Load component tables ---
    sens = pd.read_csv(sens_csv, low_memory=False)
    adapt = pd.read_csv(adapt_csv, low_memory=False)
    expo = pd.read_csv(expo_csv, low_memory=False)

    for df, name in [(sens, "sensitivity"), (adapt, "adaptive_capacity"), (expo, "exposure")]:
        if "DGUID" not in df.columns:
            print(f"ERROR: {name} missing DGUID column.")
            return 1
        df["DGUID"] = df["DGUID"].astype(str)

    # Pick the canonical columns we expect
    # sensitivity.csv should contain sensitivity_index
    if "sensitivity_index" not in sens.columns:
        print("ERROR: sensitivity.csv missing sensitivity_index.")
        print("Columns:", list(sens.columns))
        return 1

    # adaptive_capacity.csv should contain adaptive_capacity_index and green_frac
    if "adaptive_capacity_index" not in adapt.columns:
        print("ERROR: adaptive_capacity.csv missing adaptive_capacity_index.")
        print("Columns:", list(adapt.columns))
        return 1

    # exposure.csv should contain exposure_index
    if "exposure_index" not in expo.columns:
        print("ERROR: exposure.csv missing exposure_index.")
        print("Columns:", list(expo.columns))
        return 1

    sens_keep = ["DGUID", "sensitivity_index"]
    adapt_keep = ["DGUID", "adaptive_capacity_index", "green_frac"]
    expo_keep = ["DGUID", "exposure_mean", "exposure_median", "n_postalcodes", "exposure_index"]

    sens = sens[[c for c in sens_keep if c in sens.columns]].copy()
    adapt = adapt[[c for c in adapt_keep if c in adapt.columns]].copy()
    expo = expo[[c for c in expo_keep if c in expo.columns]].copy()

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

    # Useful derived versions:
    # - "shifted" so negatives are allowed but later rescaled
    # - normalize for mapping (0–1), using only rows with complete inputs
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
    # Keep only columns we actually want in the frontend (small)
    keep_props = [
        "DGUID",
        "sensitivity_index",
        "adaptive_capacity_index",
        "green_frac",
        "exposure_mean",
        "n_postalcodes",
        "exposure_index",
        "hvi_raw",
        "hvi_index_n01",
        "has_sensitivity",
        "has_adaptive",
        "has_exposure",
        "hvi_complete",
    ]
    keep_props = [c for c in keep_props if c in out.columns]

    gdf = out[keep_props + ["geometry"]].copy()

    # Ensure CRS is WGS84 for web maps / tiling
    if gdf.crs is None:
        # da.gpkg should already have CRS, but safety fallback
        gdf = gdf.set_crs(CRS_WGS84)
    else:
        gdf = gdf.to_crs(CRS_WGS84)

    print_bbox(gdf, "HVI output (WGS84)")


    # Optional simplification to reduce GeoJSON size
    # Adjust tolerance if needed: higher = smaller file, less detail
    gdf["geometry"] = gdf["geometry"].simplify(tolerance=0.0002, preserve_topology=True)

    out_geojson = DATA_INTERMEDIATE / "hvi.geojson"

    # --- Fix NA values so QGIS reads numeric fields correctly ---
    num_cols = [
        "sensitivity_index",
        "adaptive_capacity_index",
        "exposure_index",
        "hvi_raw",
        "hvi_index_n01",
    ]

    for col in num_cols:
        if col in gdf.columns:
            gdf[col] = pd.to_numeric(gdf[col], errors="coerce")

    # Write GeoJSON
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
            f.write(str(pd.to_numeric(out.loc[complete_mask, "hvi_index_n01"], errors='coerce').describe()) + "\n")

    print("Wrote:", report)
    print("Done. Next: load hvi.geojson in QGIS or MapLibre.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
