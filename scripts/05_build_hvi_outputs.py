from __future__ import annotations

import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.config import CRS_CANADA_ALBERS, CRS_WGS84, DATA_INTERMEDIATE  # noqa: E402


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
    sens_csv = DATA_INTERMEDIATE / "census_sensitivity.csv"
    capacity_csv = DATA_INTERMEDIATE / "landcover_housing_capacity.csv"
    expo_csv = DATA_INTERMEDIATE / "canue_exposure.csv"

    if not da_gpkg.exists():
        print(f"ERROR: Missing {da_gpkg}. Run 01_prepare_da.py first.")
        return 1
    if not capacity_csv.exists():
        print(f"ERROR: Missing {capacity_csv}. Run 02_landcover_housing_capacity.py first.")
        return 1
    if not sens_csv.exists():
        print(f"ERROR: Missing {sens_csv}. Run 03_census_social.py after 02_landcover_housing_capacity.py.")
        return 1
    if not expo_csv.exists():
        print(f"ERROR: Missing {expo_csv}. Run 04_canue_exposure.py after 02_landcover_housing_capacity.py.")
        return 1

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

    print("=== 05_build_hvi_outputs.py ===")
    print("DA:", da_gpkg)
    print("Sensitivity:", sens_csv)
    print("Landcover/capacity:", capacity_csv)
    print("Exposure:", expo_csv)
    print("Admin boundaries:", admin_path)

    da = gpd.read_file(da_gpkg, layer="da")
    if "DGUID" not in da.columns:
        print("ERROR: DA layer missing DGUID.")
        return 1

    da["DGUID"] = da["DGUID"].astype(str)
    da = da.to_crs(CRS_WGS84)

    adapt = pd.read_csv(capacity_csv, low_memory=False)
    sens = pd.read_csv(sens_csv, low_memory=False)
    expo = pd.read_csv(expo_csv, low_memory=False)

    for df, name in [(adapt, "adaptive_capacity"), (sens, "sensitivity"), (expo, "exposure")]:
        if "DGUID" not in df.columns:
            print(f"ERROR: {name} missing DGUID column.")
            return 1
        df["DGUID"] = df["DGUID"].astype(str)

    required_adapt_cols = {"DGUID", "da_eligible", "adaptive_capacity_index"}
    missing = required_adapt_cols - set(adapt.columns)
    if missing:
        print(f"ERROR: landcover_housing_capacity.csv missing required columns: {sorted(missing)}")
        return 1

    eligible_dguids = set(adapt.loc[adapt["da_eligible"].fillna(False).astype(bool), "DGUID"].tolist())
    da = da[da["DGUID"].isin(eligible_dguids)].copy()
    print(f"Eligible DA features: {len(da):,}")
    print_bbox(da, "Eligible DA layer (WGS84)")

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
    sens_review_cols = [
        "seniors_65to74_count",
        "seniors_75to84_count",
        "seniors_85plus_count",
        "seniors_75plus_count",
        "pct_seniors_75plus",
        "pct_seniors_75plus_n01",
        "sensitivity_index_75plus_comparison",
    ]
    sens_keep = [c for c in sens_frontend_cols + sens_review_cols if c in sens.columns]
    if "sensitivity_index" not in sens_keep:
        print("ERROR: census_sensitivity.csv missing sensitivity_index.")
        print("Columns:", list(sens.columns))
        return 1

    adapt_frontend_cols = [
        "DGUID",
        "adaptive_capacity_index",
        "green_frac",
        "pct_renter",
        "pct_major_repairs",
        "pct_core_need",
        "hardscape_frac",
        "frac_buildings",
        "frac_paved",
        "frac_other_built",
        "frac_coniferous",
        "frac_deciduous",
        "frac_shrub",
    ]
    adapt_audit_cols = ["water_frac", "exclude_water_da", "da_eligible"]
    adapt_keep = [c for c in adapt_frontend_cols + adapt_audit_cols if c in adapt.columns]
    if "adaptive_capacity_index" not in adapt_keep:
        print("ERROR: landcover_housing_capacity.csv missing adaptive_capacity_index.")
        print("Columns:", list(adapt.columns))
        return 1

    expo_frontend_cols = ["DGUID", "exposure_mean", "exposure_index"]
    expo_review_cols = [
        "exposure_mean_n01",
        "exposure_median",
        "exposure_median_n01",
        "hardscape_frac_n01",
        "exposure_index_lst_only",
        "n_postalcodes",
    ]
    expo_keep = [c for c in expo_frontend_cols + expo_review_cols if c in expo.columns]
    if "exposure_index" not in expo_keep:
        print("ERROR: canue_exposure.csv missing exposure_index (needed for HVI).")
        print("Columns:", list(expo.columns))
        return 1

    sens = sens[sens_keep].copy()
    adapt = adapt[adapt_keep].copy()
    expo = expo[expo_keep].copy()

    out = da[["DGUID", "geometry"]].copy()
    out = out.merge(sens, on="DGUID", how="left")
    out = out.merge(adapt, on="DGUID", how="left")
    out = out.merge(expo, on="DGUID", how="left")

    out["has_sensitivity"] = out["sensitivity_index"].notna()
    out["has_adaptive"] = out["adaptive_capacity_index"].notna()
    out["has_exposure"] = out["exposure_index"].notna()
    out["hvi_complete"] = out["has_sensitivity"] & out["has_adaptive"] & out["has_exposure"]

    s = pd.to_numeric(out["sensitivity_index"], errors="coerce")
    a = pd.to_numeric(out["adaptive_capacity_index"], errors="coerce")
    e = pd.to_numeric(out["exposure_index"], errors="coerce")
    out["hvi_raw"] = (e + s + (1 - a)) / 3.0

    complete_mask = out["hvi_complete"] & out["hvi_raw"].notna()
    out.loc[~complete_mask, "hvi_raw"] = pd.NA
    out["hvi_index_n01"] = pd.NA
    if complete_mask.any():
        out.loc[complete_mask, "hvi_index_n01"] = out.loc[complete_mask, "hvi_raw"]

    out_table = out.drop(columns=["geometry"]).copy()
    out_csv = DATA_INTERMEDIATE / "hvi_da_components.csv"
    out_table.to_csv(out_csv, index=False)
    print("Wrote:", out_csv)

    keep_props = (
        [c for c in sens_frontend_cols if c != "DGUID"]
        + [c for c in adapt_frontend_cols if c != "DGUID"]
        + ["exposure_mean"]
        + ["hvi_raw", "hvi_index_n01", "has_sensitivity", "has_adaptive", "has_exposure", "hvi_complete"]
    )
    if "exposure_index" in out.columns:
        keep_props.append("exposure_index")
    keep_props = [c for c in dict.fromkeys(keep_props) if c in out.columns]

    gdf_da = out[["DGUID"] + keep_props + ["geometry"]].copy()
    if gdf_da.crs is None:
        gdf_da = gdf_da.set_crs(CRS_WGS84)
    else:
        gdf_da = gdf_da.to_crs(CRS_WGS84)

    print_bbox(gdf_da, "HVI DA output (WGS84)")
    gdf_da["geometry"] = gdf_da["geometry"].simplify(tolerance=0.0002, preserve_topology=True)

    numeric_like = [
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
        "adaptive_capacity_index",
        "green_frac",
        "pct_renter",
        "pct_major_repairs",
        "pct_core_need",
        "hardscape_frac",
        "frac_buildings",
        "frac_paved",
        "frac_other_built",
        "frac_coniferous",
        "frac_deciduous",
        "frac_shrub",
        "exposure_mean",
        "exposure_index",
        "hvi_raw",
        "hvi_index_n01",
    ]
    for col in numeric_like:
        if col in gdf_da.columns:
            gdf_da[col] = pd.to_numeric(gdf_da[col], errors="coerce")

    out_geojson_da = DATA_INTERMEDIATE / "hvi_da.geojson"
    gdf_da.to_file(out_geojson_da, driver="GeoJSON")
    print("Wrote:", out_geojson_da)

    admin = gpd.read_file(admin_path)
    if admin.empty:
        print("ERROR: admin boundaries layer is empty.")
        return 1

    keep_admin_fields = [c for c in ["FullName", "ShortName", "MunNum"] if c in admin.columns]
    if "FullName" not in keep_admin_fields:
        print("ERROR: admin boundaries missing FullName field.")
        print("Columns:", list(admin.columns))
        return 1

    admin = admin[keep_admin_fields + ["geometry"]].copy()

    da_area = out[["DGUID", "pop_total", "hvi_raw", "hvi_complete", "geometry"]].copy().to_crs(CRS_CANADA_ALBERS)
    admin_area = admin.to_crs(CRS_CANADA_ALBERS)

    inter = gpd.overlay(
        da_area[["DGUID", "pop_total", "hvi_raw", "hvi_complete", "geometry"]],
        admin_area[keep_admin_fields + ["geometry"]],
        how="intersection",
        keep_geom_type=True,
    )
    if inter.empty:
        print("ERROR: DA/admin overlay produced no intersections (CRS mismatch?).")
        return 1

    inter["inter_area_m2"] = inter.geometry.area
    inter = inter.sort_values(["DGUID", "inter_area_m2"], ascending=[True, False])
    da_to_region = inter.drop_duplicates(subset=["DGUID"], keep="first").copy()

    region_join_cols = ["DGUID"] + keep_admin_fields
    da_region = out.merge(da_to_region[region_join_cols], on="DGUID", how="left")

    da_region["pop_total"] = pd.to_numeric(da_region["pop_total"], errors="coerce")
    da_region["hvi_raw"] = pd.to_numeric(da_region["hvi_raw"], errors="coerce")

    agg_src = da_region.dropna(subset=["FullName", "pop_total", "hvi_raw"]).copy()
    agg_src = agg_src[agg_src["pop_total"] > 0].copy()

    agg_src["weighted_hvi"] = agg_src["pop_total"].astype(float) * agg_src["hvi_raw"].astype(float)
    grouped = agg_src.groupby(keep_admin_fields, dropna=False)
    region_stats = (
        grouped.agg(
            weighted_hvi_sum=("weighted_hvi", "sum"),
            region_pop_total=("pop_total", "sum"),
            da_count_used=("DGUID", "nunique"),
        )
        .reset_index()
    )
    region_stats["region_hvi_raw_pw"] = region_stats["weighted_hvi_sum"] / region_stats["region_pop_total"]
    region_stats["region_pop_total"] = region_stats["region_pop_total"].astype(float)
    region_stats["da_count_used"] = region_stats["da_count_used"].astype(int)
    region_stats = region_stats.drop(columns=["weighted_hvi_sum"])
    region_stats["region_hvi_n01"] = region_stats["region_hvi_raw_pw"]

    region_geom = da_region.dropna(subset=["FullName"]).copy()
    region_geom = region_geom[keep_admin_fields + ["geometry"]]
    region_geom = region_geom.dissolve(by=keep_admin_fields, as_index=False)
    region_geom = region_geom.merge(region_stats, on=keep_admin_fields, how="left")
    region_geom = region_geom.to_crs(CRS_WGS84)
    region_geom["geometry"] = region_geom["geometry"].simplify(tolerance=0.0005, preserve_topology=True)

    out_region_csv = DATA_INTERMEDIATE / "hvi_regions_components.csv"
    region_stats.to_csv(out_region_csv, index=False)
    print("Wrote:", out_region_csv)

    out_region_geojson = DATA_INTERMEDIATE / "hvi_regions.geojson"
    region_geom.to_file(out_region_geojson, driver="GeoJSON")
    print("Wrote:", out_region_geojson)

    report = DATA_INTERMEDIATE / "05_build_hvi_outputs_debug_report.txt"
    with open(report, "w", encoding="utf-8") as f:
        f.write("05_build_hvi_outputs debug report\n\n")
        f.write("Production HVI formula: (E + S + (1 - A)) / 3\n")
        f.write(
            "Note: hvi_raw/hvi_index_n01 and region_hvi_raw_pw/region_hvi_n01 are identical because "
            "the production HVI is already bounded to 0-1.\n\n"
        )
        f.write(f"Eligible DA count: {len(gdf_da):,}\n")
        f.write(f"hvi_complete (DA): {int(pd.to_numeric(gdf_da['hvi_complete'], errors='coerce').sum()):,}\n")
        f.write(f"Regions with retained DA geometry: {len(region_geom):,}\n")
        f.write(f"Regions with region_hvi_raw_pw: {int(region_geom['region_hvi_raw_pw'].notna().sum()):,}\n\n")

        f.write("Final component summaries (DA-level):\n")
        for col in ["exposure_index", "sensitivity_index", "adaptive_capacity_index"]:
            f.write(f"\n{col}:\n")
            summary = pd.to_numeric(gdf_da[col], errors="coerce").describe(
                percentiles=[0.05, 0.25, 0.5, 0.75, 0.95]
            )
            f.write(str(summary) + "\n")

        f.write("\n")
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
