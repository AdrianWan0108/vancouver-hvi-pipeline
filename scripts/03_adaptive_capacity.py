# scripts/03_adaptive_capacity.py
from __future__ import annotations

import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from scripts.config import DATA_INTERMEDIATE, get_inputs  # noqa: E402


# Vegetation classes (your “green” definition)
GREEN_CLASSES = {6, 7, 8, 9, 10}

# Optional: expose each vegetation class as its own fraction for frontend layers
CLASS_LABELS = {
    6: "coniferous",
    7: "deciduous",
    8: "shrub",
    9: "modified_herb",
    10: "natural_herb",
}

# In your raster, code 0 is very likely background/outside (treat as nodata)
NODATA_VALUE = 0


def normalize_01(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    mn = s.min(skipna=True)
    mx = s.max(skipna=True)
    if pd.isna(mn) or pd.isna(mx) or mx == mn:
        return pd.Series([pd.NA] * len(s), index=s.index, dtype="Float64")
    return (s - mn) / (mx - mn)


def main() -> int:
    ins = get_inputs()

    da_gpkg = DATA_INTERMEDIATE / "da.gpkg"
    if not da_gpkg.exists():
        print(f"ERROR: Missing {da_gpkg}. Run scripts/01_prepare_da.py first.")
        return 1

    land_tif = Path(ins.landcover_raster)
    if not land_tif.exists():
        print(f"ERROR: Landcover raster not found: {land_tif}")
        return 1

    print("=== 03_adaptive_capacity.py (raster landcover) ===")
    print("DA base:", da_gpkg)
    print("Landcover raster:", land_tif)
    print("GREEN_CLASSES:", sorted(GREEN_CLASSES))
    print("CLASS_LABELS:", CLASS_LABELS)
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

    # --- Load DAs (already Metro Vancouver only, from script 01) ---
    da = gpd.read_file(da_gpkg, layer="da")
    if "DGUID" not in da.columns:
        print("ERROR: DA layer missing DGUID.")
        return 1
    if da.crs is None:
        print("ERROR: DA CRS missing in da.gpkg.")
        return 1

    # --- Open raster + get bounds/CRS/pixel area ---
    with rasterio.open(land_tif) as src:
        raster_crs = src.crs
        if raster_crs is None:
            print("ERROR: Raster CRS missing. Re-export from QGIS with CRS.")
            return 1

        px_w = abs(src.transform.a)
        px_h = abs(src.transform.e)
        pixel_area = px_w * px_h

        bounds = src.bounds  # left, bottom, right, top
        raster_bbox = box(bounds.left, bounds.bottom, bounds.right, bounds.top)

    print("Raster CRS:", raster_crs)
    print(f"Raster pixel: {px_w} x {px_h} => pixel_area={pixel_area}")
    print("Raster bounds:", bounds)

    # Reproject DA polygons to raster CRS (critical)
    da = da.to_crs(raster_crs)

    # Filter: keep only DAs that intersect raster bbox (safety)
    before = len(da)
    da = da[da.geometry.intersects(raster_bbox)].copy()
    after = len(da)
    print(f"Filtered DAs by raster extent: {before:,} -> {after:,}")

    if after == 0:
        print("ERROR: No DA polygons intersect the raster extent. CRS mismatch or wrong raster.")
        return 1

    da["da_area_m2"] = da.geometry.area

    print("Computing zonal categorical counts (after filtering)...")

    zs = zonal_stats(
        da.geometry,
        str(land_tif),
        categorical=True,
        nodata=NODATA_VALUE,
        all_touched=False,
    )

    out = pd.DataFrame({"DGUID": da["DGUID"].astype(str).values})
    out["da_area_m2"] = da["da_area_m2"].astype(float).values

    # We’ll collect:
    # - total_pixels (excluding nodata)
    # - per-class pixels for 6/7/8/9/10
    total_pixels_list: list = []
    per_class_pixels = {code: [] for code in CLASS_LABELS.keys()}

    for d in zs:
        if not isinstance(d, dict) or len(d) == 0:
            total_pixels_list.append(pd.NA)
            for code in per_class_pixels:
                per_class_pixels[code].append(pd.NA)
            continue

        # total pixels excluding nodata
        tot = 0
        for k, v in d.items():
            try:
                kk = int(k)
            except Exception:
                continue
            if kk == NODATA_VALUE:
                continue
            tot += int(v)

        total_pixels_list.append(tot)

        # per-class pixels (6/7/8/9/10)
        for code in per_class_pixels:
            per_class_pixels[code].append(int(d.get(code, 0)))

    out["total_pixels"] = pd.Series(total_pixels_list, dtype="Float64")

    # Add per-class pixels + fractions (optional but useful for frontend layers)
    for code, label in CLASS_LABELS.items():
        out[f"pixels_{label}"] = pd.Series(per_class_pixels[code], dtype="Float64")
        out[f"frac_{label}"] = out[f"pixels_{label}"] / out["total_pixels"]

    # Define green_frac as the sum of vegetation class fractions
    frac_cols = [f"frac_{label}" for label in CLASS_LABELS.values()]
    out["green_frac"] = out[frac_cols].sum(axis=1, skipna=False)

    # Optional: green_area (not needed, but sometimes nice for QA)
    out["green_pixels"] = out[[f"pixels_{label}" for label in CLASS_LABELS.values()]].sum(axis=1, skipna=False)
    out["green_area_m2"] = out["green_pixels"] * float(pixel_area)

    # Adaptive capacity index (0–1)
    out["adaptive_capacity_index"] = normalize_01(out["green_frac"])

    out_csv = DATA_INTERMEDIATE / "adaptive_capacity.csv"
    out.to_csv(out_csv, index=False)
    print("Wrote:", out_csv)

    report_path = DATA_INTERMEDIATE / "03_adaptive_capacity_debug_report.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("03_adaptive_capacity debug report\n")
        f.write(f"Landcover raster: {land_tif}\n")
        f.write(f"Raster CRS: {raster_crs}\n")
        f.write(f"Pixel area: {pixel_area}\n")
        f.write(f"NODATA_VALUE: {NODATA_VALUE}\n")
        f.write(f"GREEN_CLASSES: {sorted(GREEN_CLASSES)}\n")
        f.write(f"CLASS_LABELS: {CLASS_LABELS}\n")
        f.write(f"DAs used (after bbox filter): {len(out):,}\n\n")

        f.write("Missingness:\n")
        miss = out.isna().sum().sort_values(ascending=False)
        for col, cnt in miss.items():
            f.write(f"  {col}: {int(cnt):,}\n")

        f.write("\nFraction summaries:\n")
        for col in ["green_frac"] + frac_cols:
            if col in out.columns:
                f.write(f"\n{col}:\n")
                f.write(str(pd.to_numeric(out[col], errors="coerce").describe(percentiles=[0.05, 0.25, 0.5, 0.75, 0.95])) + "\n")

    print("Wrote:", report_path)
    print("Done. Next: join adaptive_capacity.csv to da.gpkg (by DGUID) and visualize green_frac / adaptive_capacity_index.")
    print("Optional layers now available: frac_coniferous, frac_deciduous, frac_shrub, frac_modified_herb, frac_natural_herb.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())