# scripts/01_prepare_da.py
from __future__ import annotations

import sys
import geopandas as gpd

from scripts.config import (
    CRS_CANADA_ALBERS,
    DATA_INTERMEDIATE,
    PREFERRED_DA_KEYS,
    get_inputs,
)


def pick_da_key(gdf: gpd.GeoDataFrame) -> str:
    """Pick a join key column from the DA boundaries file."""
    cols = set(gdf.columns)
    for k in PREFERRED_DA_KEYS:
        if k in cols:
            return k
    # If none found, show columns so user can choose later
    raise KeyError(
        "Could not find a preferred DA join key column.\n"
        f"Looked for: {PREFERRED_DA_KEYS}\n"
        f"Available columns: {sorted(list(cols))}\n"
        "Update PREFERRED_DA_KEYS in scripts/00_config.py accordingly."
    )


def main() -> int:
    ins = get_inputs()
    da_path = ins.da_shp

    print("=== 01_prepare_da.py ===")
    print("Reading DA shapefile:", da_path)

    # Load
    gdf = gpd.read_file(da_path)
    print(f"Loaded features: {len(gdf):,}")
    print("CRS:", gdf.crs)
    print("Columns:", list(gdf.columns))

    if gdf.empty:
        print("ERROR: DA GeoDataFrame is empty. Check input shapefile.", file=sys.stderr)
        return 1

    # Basic geometry sanity checks
    invalid_count = (~gdf.is_valid).sum()
    print(f"Invalid geometries: {invalid_count:,}")

    # Pick join key
    da_key = pick_da_key(gdf)
    print("Selected DA join key:", da_key)

    # Check uniqueness of join key (should be unique per DA polygon)
    null_keys = gdf[da_key].isna().sum()
    dup_keys = gdf[da_key].duplicated().sum()
    print(f"Null {da_key}: {null_keys:,}")
    print(f"Duplicate {da_key}: {dup_keys:,}")
    if dup_keys > 0:
        print(
            f"WARNING: {da_key} has duplicates. This is unusual for DA boundaries.\n"
            "We will continue, but joins later may be ambiguous."
        )

    # Keep minimal columns (join key + any name fields if exist)
    keep_cols = [da_key, "geometry"]
    for name_col in ["DA_NAME", "NAME", "GEO_NAME", "LANDAREA", "PRNAME", "PRUID"]:
        if name_col in gdf.columns and name_col not in keep_cols:
            keep_cols.insert(1, name_col)

    gdf = gdf[keep_cols].copy()

    # Reproject to Canada Albers for area/intersection operations
    if gdf.crs is None:
        print(
            "WARNING: Input CRS is missing. Attempting to proceed without reprojection.\n"
            "If results look wrong in QGIS, we may need to set CRS manually."
        )
    else:
        gdf = gdf.to_crs(CRS_CANADA_ALBERS)

    print("Reprojected CRS:", gdf.crs)

    # Create output paths
    out_gpkg = DATA_INTERMEDIATE / "da.gpkg"
    out_layer = "da"
    out_preview_geojson = DATA_INTERMEDIATE / "da_preview.geojson"

    # Save as GeoPackage (preferred for intermediate)
    print("Writing:", out_gpkg)
    gdf.to_file(out_gpkg, layer=out_layer, driver="GPKG")

    # Optional quick preview as GeoJSON (handy for quick look)
    try:
        gdf_wgs84 = gdf.to_crs("EPSG:4326")
        print("Writing preview:", out_preview_geojson)
        gdf_wgs84.to_file(out_preview_geojson, driver="GeoJSON")
    except Exception as e:
        print("NOTE: Could not write preview GeoJSON:", repr(e))

    print("Done.")
    print("Open in QGIS:", out_gpkg, "layer:", out_layer)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
