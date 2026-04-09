from __future__ import annotations

import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd

BAD_SENTINELS = {-9999, -9999.0, -999, -999.0}
VALID_RANGE = (-50, 80)
EXPOSURE_LST_WEIGHT = 0.67
EXPOSURE_HARDSCAPE_WEIGHT = 0.33

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.config import (  # noqa: E402
    CRS_CANADA_ALBERS,
    CRS_WGS84,
    OUTPUTS_DIR,
    EXPOSURE_FIELD,
    get_inputs,
)


def normalize_01(s: pd.Series) -> pd.Series:
    """Min-max normalize to [0,1], ignoring NaNs."""
    s = pd.to_numeric(s, errors="coerce")
    mn = s.min(skipna=True)
    mx = s.max(skipna=True)
    if pd.isna(mn) or pd.isna(mx) or mx == mn:
        return pd.Series([pd.NA] * len(s), index=s.index, dtype="Float64")
    return (s - mn) / (mx - mn)


def clean_postalcode(s: pd.Series) -> pd.Series:
    """Normalize postal codes for joins."""
    return s.astype(str).str.upper().str.replace(" ", "", regex=False).str.strip()


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

    wtlst_csv = Path(ins.canue_wtlst_csv)
    dmti_csv = Path(ins.canue_dmti_csv)

    if not wtlst_csv.exists():
        print(f"ERROR: Missing WTLST CSV: {wtlst_csv}")
        return 1
    if not dmti_csv.exists():
        print(f"ERROR: Missing DMTI CSV: {dmti_csv}")
        return 1

    print("=== 04_canue_exposure.py ===")
    print("DA base:", da_gpkg)
    print("Landcover/capacity input:", capacity_csv)
    print("WTLST CSV:", wtlst_csv)
    print("DMTI CSV:", dmti_csv)
    print("Exposure field:", EXPOSURE_FIELD)

    da = gpd.read_file(da_gpkg, layer="da")
    if "DGUID" not in da.columns:
        print("ERROR: DA layer missing DGUID. Check 01_prepare_da output.")
        return 1
    if da.crs is None:
        print("ERROR: DA CRS missing.")
        return 1

    adapt = pd.read_csv(capacity_csv, low_memory=False)
    required_cols = {"DGUID", "da_eligible", "hardscape_frac"}
    missing = required_cols - set(adapt.columns)
    if missing:
        print(f"ERROR: landcover_housing_capacity.csv missing required columns: {sorted(missing)}")
        return 1

    adapt["DGUID"] = adapt["DGUID"].astype(str)
    eligible_dguids = set(adapt.loc[adapt["da_eligible"].fillna(False).astype(bool), "DGUID"].tolist())

    da["DGUID"] = da["DGUID"].astype(str)
    da = da[da["DGUID"].isin(eligible_dguids)].copy()
    da = da.to_crs(CRS_CANADA_ALBERS)
    da_bbox = da.total_bounds

    w = pd.read_csv(wtlst_csv, low_memory=False)
    w_cols = {c.lower(): c for c in w.columns}

    if "postalcode21" not in w_cols:
        print("ERROR: WTLST CSV missing postalcode21 column. Columns:", list(w.columns))
        return 1
    if EXPOSURE_FIELD.lower() not in w_cols:
        print(f"ERROR: WTLST CSV missing {EXPOSURE_FIELD} column. Columns:", list(w.columns))
        return 1

    w_pc_col = w_cols["postalcode21"]
    w_val_col = w_cols[EXPOSURE_FIELD.lower()]

    w = w[[w_pc_col, w_val_col]].copy()
    w.rename(columns={w_pc_col: "postalcode", w_val_col: "wtlst"}, inplace=True)

    w["postalcode"] = clean_postalcode(w["postalcode"])
    w["wtlst"] = pd.to_numeric(w["wtlst"], errors="coerce")

    n_w_total = len(w)
    n_w_numeric = w["wtlst"].notna().sum()

    n_w_sentinel = w["wtlst"].isin(BAD_SENTINELS).sum()
    w.loc[w["wtlst"].isin(BAD_SENTINELS), "wtlst"] = pd.NA

    n_w_out_of_range = ((w["wtlst"] < VALID_RANGE[0]) | (w["wtlst"] > VALID_RANGE[1])).sum(skipna=True)
    w.loc[(w["wtlst"] < VALID_RANGE[0]) | (w["wtlst"] > VALID_RANGE[1]), "wtlst"] = pd.NA

    n_w_valid_final = w["wtlst"].notna().sum()

    w = w.sort_values(["postalcode", "wtlst"], ascending=[True, False], na_position="last")
    w = w.drop_duplicates(subset=["postalcode"], keep="first").copy()

    d = pd.read_csv(dmti_csv, low_memory=False)
    d_cols = {c.upper(): c for c in d.columns}

    for required in ["POSTALCODE21", "LATITUDE_21", "LONGITUDE_21"]:
        if required not in d_cols:
            print("ERROR: DMTI CSV missing", required, "Columns:", list(d.columns))
            return 1

    d_pc_col = d_cols["POSTALCODE21"]
    d_lat_col = d_cols["LATITUDE_21"]
    d_lon_col = d_cols["LONGITUDE_21"]

    d = d[[d_pc_col, d_lat_col, d_lon_col]].copy()
    d.rename(columns={d_pc_col: "postalcode", d_lat_col: "lat", d_lon_col: "lon"}, inplace=True)

    d["postalcode"] = clean_postalcode(d["postalcode"])
    d["lat"] = pd.to_numeric(d["lat"], errors="coerce")
    d["lon"] = pd.to_numeric(d["lon"], errors="coerce")

    d = d.dropna(subset=["lat", "lon"])
    d = d[(d["lat"].between(-90, 90)) & (d["lon"].between(-180, 180))]
    total_dmti = len(d)

    merged = d.merge(w, on="postalcode", how="left", validate="m:1")
    matched_wtlst = merged["wtlst"].notna().sum()

    merged = merged.dropna(subset=["wtlst"]).copy()
    merged_after_drop = len(merged)

    pts = gpd.GeoDataFrame(
        merged,
        geometry=gpd.points_from_xy(merged["lon"], merged["lat"]),
        crs=CRS_WGS84,
    )
    pts = pts.to_crs(da.crs)

    minx, miny, maxx, maxy = da_bbox
    pts = pts.cx[minx:maxx, miny:maxy]
    pts_after_bbox = len(pts)

    joined = gpd.sjoin(
        pts[["postalcode", "wtlst", "geometry"]],
        da[["DGUID", "geometry"]],
        how="inner",
        predicate="within",
    )

    agg = (
        joined.groupby("DGUID")["wtlst"]
        .agg(exposure_mean="mean", exposure_median="median", n_postalcodes="count")
        .reset_index()
    )

    hardscape = adapt[["DGUID", "hardscape_frac"]].copy()
    hardscape["hardscape_frac"] = pd.to_numeric(hardscape["hardscape_frac"], errors="coerce")
    agg = agg.merge(hardscape, on="DGUID", how="left")

    agg["exposure_mean_n01"] = normalize_01(agg["exposure_mean"])
    agg["exposure_median_n01"] = normalize_01(agg["exposure_median"])
    agg["hardscape_frac_n01"] = normalize_01(agg["hardscape_frac"])
    agg["exposure_index_lst_only"] = agg["exposure_mean_n01"]
    agg["exposure_index"] = (
        (EXPOSURE_LST_WEIGHT * pd.to_numeric(agg["exposure_mean_n01"], errors="coerce"))
        + (EXPOSURE_HARDSCAPE_WEIGHT * pd.to_numeric(agg["hardscape_frac_n01"], errors="coerce"))
    )

    out_csv = OUTPUTS_DIR / "canue_exposure.csv"
    agg.to_csv(out_csv, index=False)
    print("Wrote:", out_csv)

    out_pts = OUTPUTS_DIR / "canue_exposure_points_preview.geojson"
    try:
        pts_wgs = pts.to_crs("EPSG:4326")
        pts_wgs.to_file(out_pts, driver="GeoJSON")
        print("Wrote:", out_pts)
    except Exception as e:
        print("NOTE: could not write point preview geojson:", repr(e))

    report = OUTPUTS_DIR / "04_canue_exposure_debug_report.txt"
    with open(report, "w", encoding="utf-8") as f:
        f.write("04_canue_exposure debug report\n")
        f.write(f"Landcover/capacity input: {capacity_csv}\n")
        f.write(f"WTLST CSV: {wtlst_csv}\n")
        f.write(f"DMTI CSV: {dmti_csv}\n")
        f.write(f"Exposure field: {EXPOSURE_FIELD}\n")
        f.write(f"BAD_SENTINELS: {sorted(list(BAD_SENTINELS))}\n")
        f.write(f"VALID_RANGE: {VALID_RANGE}\n\n")
        f.write(f"EXPOSURE_LST_WEIGHT: {EXPOSURE_LST_WEIGHT}\n")
        f.write(f"EXPOSURE_HARDSCAPE_WEIGHT: {EXPOSURE_HARDSCAPE_WEIGHT}\n\n")

        f.write("WTLST cleaning:\n")
        f.write(f"  WTLST rows total: {n_w_total:,}\n")
        f.write(f"  numeric before cleaning: {n_w_numeric:,}\n")
        f.write(f"  sentinel removed: {int(n_w_sentinel):,}\n")
        f.write(f"  out-of-range removed: {int(n_w_out_of_range):,}\n")
        f.write(f"  valid after cleaning: {n_w_valid_final:,}\n\n")

        f.write(f"Eligible DAs: {len(da):,}\n")
        f.write(f"DMTI rows (valid coords): {total_dmti:,}\n")
        f.write(f"WTLST matched (by postalcode): {matched_wtlst:,}\n")
        f.write(f"Rows kept after dropping missing WTLST: {merged_after_drop:,}\n")
        f.write(f"Points after DA bbox filter: {pts_after_bbox:,}\n")
        f.write(f"Joined points-in-DA: {len(joined):,}\n")
        f.write(f"DAs with exposure: {agg['DGUID'].nunique():,}\n\n")

        f.write("Exposure mean summary:\n")
        f.write(str(agg["exposure_mean"].describe()) + "\n")

        for col in ["hardscape_frac", "exposure_index_lst_only", "exposure_index"]:
            f.write(f"\n{col} summary:\n")
            summary = pd.to_numeric(agg[col], errors="coerce").describe(
                percentiles=[0.05, 0.25, 0.5, 0.75, 0.95]
            )
            f.write(str(summary) + "\n")

    print("Wrote:", report)
    print("Done. Next: run scripts/05_build_hvi_outputs.py and visualize exposure_index if needed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
