# scripts/05_hvi_composite.py
from __future__ import annotations

import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd

# --- Make project root importable (same approach as 01_prepare_da.py) ---
sys.path.append(str(Path(__file__).resolve().parents[1]))


from scripts.config import DATA_INTERMEDIATE, CRS_WGS84, CRS_CANADA_ALBERS  # noqa: E402


def print_bbox(gdf: gpd.GeoDataFrame, label: str) -> None:
    b = gdf.total_bounds
    print(
        f"{label} bbox (WGS84): "
        f"minx={b[0]:.4f}, miny={b[1]:.4f}, maxx={b[2]:.4f}, maxy={b[3]:.4f}"
    )


def normalize_01(s: pd.Series) -> pd.Series:
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

    # --- Option B admin boundaries shapefile path ---
    project_root = Path(__file__).resolve().parents[1]
    admin_path = (
        project_root
        / "data_raw"
        / "Administrative_Boundaries_-6445306865161621642"
        / "Administrative_Boundaries.shp"
    )
    if not admin_path.exists():
        print(
            "ERROR: Metro admin boundary shapefile not found.\n"
            f"Expected at: {admin_path}",
            file=sys.stderr,
        )
        return 1

    print("=== 05_hvi_composite.py ===")
    print("DA:", da_gpkg)
    print("Sensitivity:", sens_csv)
    print("Adaptive:", adapt_csv)
    print("Exposure:", expo_csv)
    print("Admin boundaries:", admin_path)

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

    # --- Columns we WANT for the frontend (DA-level) ---
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
    sens_keep = [c for c in sens_frontend_cols if c in sens.columns]
    if "sensitivity_index" not in sens_keep:
        print("ERROR: sensitivity.csv missing sensitivity_index.")
        print("Columns:", list(sens.columns))
        return 1

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

    expo_frontend_cols = ["DGUID", "exposure_mean", "exposure_index"]
    expo_keep = [c for c in expo_frontend_cols if c in expo.columns]
    if "exposure_index" not in expo_keep:
        print("ERROR: exposure.csv missing exposure_index (needed for HVI).")
        print("Columns:", list(expo.columns))
        return 1

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
    S = pd.to_numeric(out["sensitivity_index"], errors="coerce")
    A = pd.to_numeric(out["adaptive_capacity_index"], errors="coerce")
    E = pd.to_numeric(out["exposure_index"], errors="coerce")
    out["hvi_raw"] = E * (S - A)

    complete_mask = out["hvi_complete"] & out["hvi_raw"].notna()
    out.loc[~complete_mask, "hvi_raw"] = pd.NA

    out["hvi_index_n01"] = pd.NA
    if complete_mask.any():
        out.loc[complete_mask, "hvi_index_n01"] = normalize_01(out.loc[complete_mask, "hvi_raw"])

    # --- Write DA component table (no geometry) ---
    out_table = out.drop(columns=["geometry"]).copy()
    out_csv = DATA_INTERMEDIATE / "hvi_components.csv"
    out_table.to_csv(out_csv, index=False)
    print("Wrote:", out_csv)

    # --- Export DA GeoJSON for MapLibre ---
    keep_props = (
        [c for c in sens_frontend_cols if c != "DGUID"]
        + [c for c in adapt_frontend_cols if c != "DGUID"]
        + ["exposure_mean"]
        + ["hvi_raw", "hvi_index_n01", "has_sensitivity", "has_adaptive", "has_exposure", "hvi_complete"]
    )
    # Keep exposure_index for debugging (optional)
    if "exposure_index" in out.columns:
        keep_props.append("exposure_index")

    keep_props = [c for c in dict.fromkeys(keep_props) if c in out.columns]
    gdf_da = out[["DGUID"] + keep_props + ["geometry"]].copy()

    if gdf_da.crs is None:
        gdf_da = gdf_da.set_crs(CRS_WGS84)
    else:
        gdf_da = gdf_da.to_crs(CRS_WGS84)

    print_bbox(gdf_da, "HVI DA output (WGS84)")

    # Geometry simplify (tune later if needed)
    gdf_da["geometry"] = gdf_da["geometry"].simplify(tolerance=0.0002, preserve_topology=True)

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
        if col in gdf_da.columns:
            gdf_da[col] = pd.to_numeric(gdf_da[col], errors="coerce")

    out_geojson_da = DATA_INTERMEDIATE / "hvi.geojson"
    gdf_da.to_file(out_geojson_da, driver="GeoJSON")
    print("Wrote:", out_geojson_da)

    # ==========================================================
    # REGION LEVEL OUTPUT (Admin boundaries + composite HVI)
    # ==========================================================
    admin = gpd.read_file(admin_path)
    if admin.empty:
        print("ERROR: admin boundaries layer is empty.")
        return 1

    # Keep only useful fields + geometry
    keep_admin_fields = []
    for c in ["FullName", "ShortName", "MunNum"]:
        if c in admin.columns:
            keep_admin_fields.append(c)
    if "FullName" not in keep_admin_fields:
        print("ERROR: admin boundaries missing FullName field.")
        print("Columns:", list(admin.columns))
        return 1

    admin = admin[keep_admin_fields + ["geometry"]].copy()

    # Strict DA -> Region assignment by max intersection area
    # Do this in an equal-area CRS
    da_area = gdf_da[["DGUID", "pop_total", "hvi_raw", "hvi_complete", "geometry"]].copy()
    da_area = da_area.to_crs(CRS_CANADA_ALBERS)

    admin_area = admin.to_crs(CRS_CANADA_ALBERS)

    # Overlay intersection polygons
    inter = gpd.overlay(
        da_area[["DGUID", "pop_total", "hvi_raw", "hvi_complete", "geometry"]],
        admin_area[["FullName", "ShortName", "MunNum", "geometry"]],
        how="intersection",
        keep_geom_type=True,
    )
    if inter.empty:
        print("ERROR: DA/admin overlay produced no intersections (CRS mismatch?).")
        return 1

    inter["inter_area_m2"] = inter.geometry.area

    # Pick the municipality with max intersection per DA
    inter = inter.sort_values(["DGUID", "inter_area_m2"], ascending=[True, False])
    da_to_region = inter.drop_duplicates(subset=["DGUID"], keep="first").copy()

    # Join region name back to DA table (for debugging / optional frontend usage)
    da_region = gdf_da.merge(
        da_to_region[["DGUID", "FullName", "ShortName", "MunNum"]],
        on="DGUID",
        how="left",
    )

    # Region aggregation (Option 1): pop-weighted mean of hvi_raw, then normalize
    da_region["pop_total"] = pd.to_numeric(da_region["pop_total"], errors="coerce")
    da_region["hvi_raw"] = pd.to_numeric(da_region["hvi_raw"], errors="coerce")

    # Use only DAs with valid pop + hvi_raw + assignment
    agg_src = da_region.dropna(subset=["FullName", "pop_total", "hvi_raw"]).copy()
    agg_src = agg_src[agg_src["pop_total"] > 0].copy()

    # Weighted mean helper
    def _wmean(group: pd.DataFrame) -> float:
        w = group["pop_total"].astype(float)
        x = group["hvi_raw"].astype(float)
        return float((w * x).sum() / w.sum())

    region_stats = (
        agg_src.groupby("FullName", as_index=False)
        .apply(lambda g: pd.Series({
            "region_hvi_raw_pw": _wmean(g),
            "region_pop_total": float(g["pop_total"].sum()),
            "da_count_used": int(g["DGUID"].nunique()),
        }))
        .reset_index(drop=True)
    )

    region_stats["region_hvi_n01"] = normalize_01(region_stats["region_hvi_raw_pw"])

    # Build region GeoDataFrame by merging stats onto admin polygons
    gdf_region = admin.merge(region_stats, on="FullName", how="left")

    # CRS to WGS84 for web
    gdf_region = gdf_region.to_crs(CRS_WGS84)

    # Simplify region boundaries a bit (they’re bigger, so tolerance can be larger)
    gdf_region["geometry"] = gdf_region["geometry"].simplify(tolerance=0.0005, preserve_topology=True)

    out_region_csv = DATA_INTERMEDIATE / "hvi_regions_components.csv"
    region_stats.to_csv(out_region_csv, index=False)
    print("Wrote:", out_region_csv)

    out_region_geojson = DATA_INTERMEDIATE / "hvi_regions.geojson"
    gdf_region.to_file(out_region_geojson, driver="GeoJSON")
    print("Wrote:", out_region_geojson)

    # --- Debug report ---
    report = DATA_INTERMEDIATE / "05_hvi_debug_report.txt"
    with open(report, "w", encoding="utf-8") as f:
        f.write("05_hvi_composite debug report\n\n")
        f.write(f"DA count: {len(gdf_da):,}\n")
        f.write(f"hvi_complete (DA): {int(pd.to_numeric(gdf_da['hvi_complete'], errors='coerce').sum()):,}\n")
        f.write(f"Regions (admin polygons): {len(admin):,}\n")
        f.write(f"Regions with region_hvi_raw_pw: {int(gdf_region['region_hvi_raw_pw'].notna().sum()):,}\n\n")
        f.write("DA hvi_raw summary (complete only):\n")
        da_complete = gdf_da.dropna(subset=["hvi_raw"]).copy()
        f.write(str(pd.to_numeric(da_complete["hvi_raw"], errors="coerce").describe()) + "\n\n")
        f.write("Region region_hvi_raw_pw summary:\n")
        f.write(str(pd.to_numeric(region_stats["region_hvi_raw_pw"], errors="coerce").describe()) + "\n\n")

    print("Wrote:", report)
    print("Done.")
    print("Outputs:")
    print("  - DA GeoJSON:", out_geojson_da)
    print("  - Region GeoJSON:", out_region_geojson)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())