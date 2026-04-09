from __future__ import annotations

import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from scripts.config import OUTPUTS_DIR, get_inputs  # noqa: E402


HARDSCAPE_CLASSES = {1, 2, 3}
GREEN_CLASSES = {6, 7, 8}
CLASS_LABELS = {
    1: "buildings",
    2: "paved",
    3: "other_built",
    6: "coniferous",
    7: "deciduous",
    8: "shrub",
}
WATER_CLASS = 12
WATER_LABEL = "water"
WATER_EXCLUDE_THRESHOLD = 0.80
NODATA_VALUE = 0

CENSUS_CHARACTERISTIC_ID_TO_KEY = {
    "1416": "renter_count",
    "1451": "major_repairs_count",
    "1480": "core_need_count",
    "1414": "households_total",
    "1449": "dwellings_condition_total",
    "1479": "core_need_total",
}


def normalize_01(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    mn = s.min(skipna=True)
    mx = s.max(skipna=True)
    if pd.isna(mn) or pd.isna(mx) or mx == mn:
        return pd.Series([pd.NA] * len(s), index=s.index, dtype="Float64")
    return (s - mn) / (mx - mn)


def reverse_normalized_capacity(s: pd.Series) -> pd.Series:
    base = normalize_01(s)
    return 1 - base


def load_census_capacity_inputs(census_csv: Path, valid_dguids: set[str]) -> pd.DataFrame:
    usecols = [
        "DGUID",
        "GEO_LEVEL",
        "CHARACTERISTIC_ID",
        "C1_COUNT_TOTAL",
    ]

    selected_chunks: list[pd.DataFrame] = []
    chunk_size = 250_000
    ids_we_care = set(CENSUS_CHARACTERISTIC_ID_TO_KEY.keys())

    reader = pd.read_csv(
        census_csv,
        usecols=usecols,
        chunksize=chunk_size,
        low_memory=False,
        encoding="cp1252",
        dtype=str,
    )

    for chunk in reader:
        geo_level = chunk["GEO_LEVEL"].fillna("")
        chunk = chunk.loc[geo_level.str.contains("Dissemination", case=False, na=False)]
        if chunk.empty:
            continue

        chunk = chunk.copy()
        chunk["CHARACTERISTIC_ID"] = chunk["CHARACTERISTIC_ID"].fillna("").astype(str).str.strip()
        chunk = chunk.loc[chunk["CHARACTERISTIC_ID"].isin(ids_we_care)]
        if chunk.empty:
            continue

        chunk["DGUID"] = chunk["DGUID"].astype(str)
        chunk = chunk.loc[chunk["DGUID"].isin(valid_dguids)]
        if chunk.empty:
            continue

        chunk["indicator_key"] = chunk["CHARACTERISTIC_ID"].map(CENSUS_CHARACTERISTIC_ID_TO_KEY)
        selected_chunks.append(chunk[["DGUID", "indicator_key", "C1_COUNT_TOTAL"]].copy())

    if not selected_chunks:
        raise RuntimeError("No matching Census Profile rows found for adaptive-capacity housing indicators.")

    df = pd.concat(selected_chunks, ignore_index=True)
    df["C1_COUNT_TOTAL"] = pd.to_numeric(df["C1_COUNT_TOTAL"], errors="coerce")

    wide = pd.DataFrame({"DGUID": sorted(valid_dguids)}).set_index("DGUID")
    for key in CENSUS_CHARACTERISTIC_ID_TO_KEY.values():
        series = df[df["indicator_key"] == key].groupby("DGUID")["C1_COUNT_TOTAL"].first()
        wide[key] = series

    wide["pct_renter"] = (wide["renter_count"] / wide["households_total"]) * 100.0
    wide["pct_major_repairs"] = (wide["major_repairs_count"] / wide["dwellings_condition_total"]) * 100.0
    wide["pct_core_need"] = (wide["core_need_count"] / wide["core_need_total"]) * 100.0

    return wide.reset_index()


def main() -> int:
    ins = get_inputs()

    da_gpkg = OUTPUTS_DIR / "da.gpkg"
    if not da_gpkg.exists():
        print(f"ERROR: Missing {da_gpkg}. Run scripts/01_prepare_da.py first.")
        return 1

    land_tif = Path(ins.landcover_raster)
    if not land_tif.exists():
        print(f"ERROR: Landcover raster not found: {land_tif}")
        return 1

    census_csv = Path(ins.census_csv)
    if not census_csv.exists():
        print(f"ERROR: Census CSV not found: {census_csv}")
        return 1

    print("=== 02_landcover_housing_capacity.py ===")
    print("DA base:", da_gpkg)
    print("Landcover raster:", land_tif)
    print("Census CSV:", census_csv)
    print("HARDSCAPE_CLASSES:", sorted(HARDSCAPE_CLASSES))
    print("GREEN_CLASSES:", sorted(GREEN_CLASSES))
    print("CLASS_LABELS:", CLASS_LABELS)
    print("WATER_CLASS:", WATER_CLASS)
    print("WATER_EXCLUDE_THRESHOLD:", WATER_EXCLUDE_THRESHOLD)
    print("Assumed NODATA_VALUE:", NODATA_VALUE)

    try:
        import rasterio
    except Exception as e:
        print("ERROR: rasterio not available.")
        print("Install with: conda install -c conda-forge rasterio")
        print("Details:", repr(e))
        return 1

    try:
        from rasterstats import zonal_stats  # type: ignore
    except Exception as e:
        print("ERROR: rasterstats not available.")
        print("Install with: pip install rasterstats")
        print("Details:", repr(e))
        return 1

    from shapely.geometry import box

    da = gpd.read_file(da_gpkg, layer="da")
    if "DGUID" not in da.columns:
        print("ERROR: DA layer missing DGUID.")
        return 1
    if da.crs is None:
        print("ERROR: DA CRS missing in da.gpkg.")
        return 1

    with rasterio.open(land_tif) as src:
        raster_crs = src.crs
        if raster_crs is None:
            print("ERROR: Raster CRS missing. Re-export from QGIS with CRS.")
            return 1

        px_w = abs(src.transform.a)
        px_h = abs(src.transform.e)
        pixel_area = px_w * px_h
        bounds = src.bounds
        raster_bbox = box(bounds.left, bounds.bottom, bounds.right, bounds.top)

    print("Raster CRS:", raster_crs)
    print(f"Raster pixel: {px_w} x {px_h} => pixel_area={pixel_area}")
    print("Raster bounds:", bounds)

    da = da.to_crs(raster_crs)
    before = len(da)
    da = da[da.geometry.intersects(raster_bbox)].copy()
    after = len(da)
    print(f"Filtered DAs by raster extent: {before:,} -> {after:,}")

    if after == 0:
        print("ERROR: No DA polygons intersect the raster extent. CRS mismatch or wrong raster.")
        return 1

    da["DGUID"] = da["DGUID"].astype(str)
    da["da_area_m2"] = da.geometry.area

    print("Computing zonal categorical counts (after filtering)...")
    zs = zonal_stats(
        da.geometry,
        str(land_tif),
        categorical=True,
        nodata=NODATA_VALUE,
        all_touched=False,
    )

    out = pd.DataFrame({"DGUID": da["DGUID"].values})
    out["da_area_m2"] = da["da_area_m2"].astype(float).values

    tracked_codes = sorted(set(CLASS_LABELS.keys()) | {WATER_CLASS})
    total_pixels_list: list = []
    per_class_pixels = {code: [] for code in tracked_codes}

    for d in zs:
        if not isinstance(d, dict) or len(d) == 0:
            total_pixels_list.append(pd.NA)
            for code in tracked_codes:
                per_class_pixels[code].append(pd.NA)
            continue

        total_pixels = 0
        for k, v in d.items():
            try:
                kk = int(k)
            except Exception:
                continue
            if kk == NODATA_VALUE:
                continue
            total_pixels += int(v)

        total_pixels_list.append(total_pixels)
        for code in tracked_codes:
            per_class_pixels[code].append(int(d.get(code, 0)))

    out["total_pixels"] = pd.Series(total_pixels_list, dtype="Float64")

    for code, label in CLASS_LABELS.items():
        out[f"pixels_{label}"] = pd.Series(per_class_pixels[code], dtype="Float64")
        out[f"frac_{label}"] = out[f"pixels_{label}"] / out["total_pixels"]

    out[f"pixels_{WATER_LABEL}"] = pd.Series(per_class_pixels[WATER_CLASS], dtype="Float64")
    out["water_frac"] = out[f"pixels_{WATER_LABEL}"] / out["total_pixels"]

    hardscape_pixel_cols = [f"pixels_{CLASS_LABELS[code]}" for code in sorted(HARDSCAPE_CLASSES)]
    hardscape_frac_cols = [f"frac_{CLASS_LABELS[code]}" for code in sorted(HARDSCAPE_CLASSES)]
    out["hardscape_pixels"] = out[hardscape_pixel_cols].sum(axis=1, skipna=False)
    out["hardscape_frac"] = out[hardscape_frac_cols].sum(axis=1, skipna=False)

    green_pixel_cols = [f"pixels_{CLASS_LABELS[code]}" for code in sorted(GREEN_CLASSES)]
    green_frac_cols = [f"frac_{CLASS_LABELS[code]}" for code in sorted(GREEN_CLASSES)]
    out["green_pixels"] = out[green_pixel_cols].sum(axis=1, skipna=False)
    out["green_frac"] = out[green_frac_cols].sum(axis=1, skipna=False)
    out["green_area_m2"] = out["green_pixels"] * float(pixel_area)

    out["exclude_water_da"] = out["water_frac"] >= WATER_EXCLUDE_THRESHOLD
    out.loc[out["total_pixels"].isna() | (out["total_pixels"] <= 0), "exclude_water_da"] = True
    out["exclude_water_da"] = out["exclude_water_da"].fillna(True).astype(bool)
    out["da_eligible"] = ~out["exclude_water_da"]

    print("Loading Census Profile housing indicators for adaptive capacity...")
    census_capacity = load_census_capacity_inputs(census_csv, set(out["DGUID"].tolist()))
    out = out.merge(census_capacity, on="DGUID", how="left")

    out["green_capacity_n01"] = normalize_01(out["green_frac"])
    out["renter_capacity_n01"] = reverse_normalized_capacity(out["pct_renter"])
    out["major_repairs_capacity_n01"] = reverse_normalized_capacity(out["pct_major_repairs"])
    out["core_need_capacity_n01"] = reverse_normalized_capacity(out["pct_core_need"])

    capacity_components = [
        "green_capacity_n01",
        "renter_capacity_n01",
        "major_repairs_capacity_n01",
        "core_need_capacity_n01",
    ]
    out["adaptive_capacity_index"] = out[capacity_components].mean(axis=1, skipna=True)
    out.loc[
        ~out["da_eligible"],
        [
            "green_frac",
            "green_area_m2",
            "green_capacity_n01",
            "renter_capacity_n01",
            "major_repairs_capacity_n01",
            "core_need_capacity_n01",
            "adaptive_capacity_index",
        ],
    ] = pd.NA

    out_csv = OUTPUTS_DIR / "landcover_housing_capacity.csv"
    out.to_csv(out_csv, index=False)
    print("Wrote:", out_csv)

    report_path = OUTPUTS_DIR / "02_landcover_housing_capacity_debug_report.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("02_landcover_housing_capacity debug report\n")
        f.write(f"Landcover raster: {land_tif}\n")
        f.write(f"Census CSV: {census_csv}\n")
        f.write(f"Raster CRS: {raster_crs}\n")
        f.write(f"Pixel area: {pixel_area}\n")
        f.write(f"NODATA_VALUE: {NODATA_VALUE}\n")
        f.write(f"HARDSCAPE_CLASSES: {sorted(HARDSCAPE_CLASSES)}\n")
        f.write(f"GREEN_CLASSES: {sorted(GREEN_CLASSES)}\n")
        f.write(f"CLASS_LABELS: {CLASS_LABELS}\n")
        f.write(f"WATER_CLASS: {WATER_CLASS}\n")
        f.write(f"WATER_EXCLUDE_THRESHOLD: {WATER_EXCLUDE_THRESHOLD}\n")
        f.write(f"DAs used (after bbox filter): {len(out):,}\n")
        f.write(f"DAs excluded as water-dominated: {int(out['exclude_water_da'].sum()):,}\n\n")

        f.write("Missingness:\n")
        miss = out.isna().sum().sort_values(ascending=False)
        for col, cnt in miss.items():
            f.write(f"  {col}: {int(cnt):,}\n")

        f.write("\nAdaptive-capacity summaries:\n")
        for col in [
            "hardscape_frac",
            "green_frac",
            "water_frac",
            "pct_renter",
            "pct_major_repairs",
            "pct_core_need",
            "green_capacity_n01",
            "renter_capacity_n01",
            "major_repairs_capacity_n01",
            "core_need_capacity_n01",
            "adaptive_capacity_index",
        ]:
            if col in out.columns:
                f.write(f"\n{col}:\n")
                summary = pd.to_numeric(out[col], errors="coerce").describe(
                    percentiles=[0.05, 0.25, 0.5, 0.75, 0.95]
                )
                f.write(str(summary) + "\n")

    print("Wrote:", report_path)
    print("Done. Next: run scripts/03_census_social.py and scripts/04_canue_exposure.py.")
    print("Landcover/capacity output now includes greenness, housing capacity, hardscape, and DA eligibility.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
