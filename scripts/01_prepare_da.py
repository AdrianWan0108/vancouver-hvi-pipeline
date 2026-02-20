# scripts/01_prepare_da.py
from __future__ import annotations

import sys
from pathlib import Path
import geopandas as gpd

sys.path.append(str(Path(__file__).resolve().parents[1]))  # Add project root to sys.path

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
    raise KeyError(
        "Could not find a preferred DA join key column.\n"
        f"Looked for: {PREFERRED_DA_KEYS}\n"
        f"Available columns: {sorted(list(cols))}\n"
        "Update PREFERRED_DA_KEYS in scripts/00_config.py accordingly."
    )


def _try_fix_geometries(gdf: gpd.GeoDataFrame, label: str) -> gpd.GeoDataFrame:
    """Lightweight geometry fix for common invalid polygons."""
    if gdf.empty:
        return gdf

    invalid = (~gdf.is_valid).sum()
    print(f"{label} invalid geometries: {invalid:,}")

    if invalid > 0:
        print(f"Attempting to fix invalid geometries for {label} with buffer(0)...")
        gdf = gdf.copy()
        gdf.loc[~gdf.is_valid, "geometry"] = gdf.loc[~gdf.is_valid, "geometry"].buffer(0)
        invalid2 = (~gdf.is_valid).sum()
        print(f"{label} invalid geometries after fix: {invalid2:,}")

    return gdf


def main() -> int:
    ins = get_inputs()
    da_path = Path(ins.da_shp)

    # Option B path (Metro Vancouver Admin Boundaries shapefile)
    project_root = Path(__file__).resolve().parents[1]
    admin_path = (
        project_root
        / "data_raw"
        / "Administrative_Boundaries_-6445306865161621642"
        / "Administrative_Boundaries.shp"
    )

    print("=== 01_prepare_da.py (Metro Vancouver filter: Option B) ===")
    print("Reading DA shapefile:", da_path)
    print("Reading Metro admin boundaries:", admin_path)

    if not da_path.exists():
        print(f"ERROR: DA shapefile not found: {da_path}", file=sys.stderr)
        return 1

    if not admin_path.exists():
        print(
            "ERROR: Metro admin boundary shapefile not found.\n"
            f"Expected at: {admin_path}\n"
            "If your folder name differs, update admin_path in scripts/01_prepare_da.py.",
            file=sys.stderr,
        )
        return 1

    # ---------------------------
    # 1) Load DA (Canada-wide)
    # ---------------------------
    da = gpd.read_file(da_path)
    print(f"Loaded DA features: {len(da):,}")
    print("DA CRS:", da.crs)
    print("DA Columns:", list(da.columns))

    if da.empty:
        print("ERROR: DA GeoDataFrame is empty. Check input shapefile.", file=sys.stderr)
        return 1

    da = _try_fix_geometries(da, "DA")

    da_key = pick_da_key(da)
    print("Selected DA join key:", da_key)

    null_keys = da[da_key].isna().sum()
    dup_keys = da[da_key].duplicated().sum()
    print(f"Null {da_key}: {null_keys:,}")
    print(f"Duplicate {da_key}: {dup_keys:,}")

    # Keep minimal columns
    keep_cols = [da_key, "geometry"]
    for name_col in ["DAUID", "DGUID", "LANDAREA", "PRUID", "PRNAME", "DA_NAME", "NAME", "GEO_NAME"]:
        if name_col in da.columns and name_col not in keep_cols:
            keep_cols.insert(1, name_col)
    da = da[keep_cols].copy()

    # Reproject DA -> Canada Albers
    if da.crs is None:
        print("ERROR: DA CRS missing. Please set CRS correctly before running.", file=sys.stderr)
        return 1

    da = da.to_crs(CRS_CANADA_ALBERS)
    print("DA reprojected CRS:", da.crs)

    # ---------------------------
    # 2) Load Metro admin boundaries (Option B)
    # ---------------------------
    admin = gpd.read_file(admin_path)
    print(f"Loaded Admin features: {len(admin):,}")
    print("Admin CRS:", admin.crs)
    print("Admin Columns:", list(admin.columns))

    if admin.empty:
        print("ERROR: Admin boundaries GeoDataFrame is empty.", file=sys.stderr)
        return 1

    admin = _try_fix_geometries(admin, "Admin boundaries")

    if admin.crs is None:
        print("ERROR: Admin boundaries CRS missing.", file=sys.stderr)
        return 1

    admin = admin.to_crs(CRS_CANADA_ALBERS)
    print("Admin reprojected CRS:", admin.crs)

    # Keep only useful fields if present
    admin_keep = ["geometry"]
    for c in ["FullName", "ShortName", "MunNum"]:
        if c in admin.columns:
            admin_keep.insert(0, c)
    admin = admin[admin_keep].copy()

    # Dissolve -> single Metro Vancouver union polygon
    metro_union = admin.dissolve().reset_index(drop=True)
    metro_union["name"] = "Metro Vancouver"
    metro_geom = metro_union.geometry.iloc[0]

    # ---------------------------
    # 3) Strict filter DA to Metro Vancouver
    # ---------------------------
    before = len(da)

    # STRICT RULE:
    # Use representative_point (always within polygon) instead of centroid (can fall outside for weird shapes).
    # Keep DA only if its representative point is within the Metro union polygon.
    da_rep = da.geometry.representative_point()
    da_metro = da[da_rep.within(metro_geom)].copy()

    after = len(da_metro)
    print(f"DA filtered by Metro union (representative_point within): {before:,} -> {after:,}")

    if after == 0:
        print(
            "ERROR: After filtering, no DA remains. Likely CRS mismatch or wrong boundary file.",
            file=sys.stderr,
        )
        return 1

    # OPTIONAL (leave off for now):
    # If you ever need it even stricter/cleaner at edges, uncomment this overlap filter.
    # It keeps DAs where at least 50% of the DA area overlaps Metro.
    #
    # overlap_ratio = da_metro.geometry.intersection(metro_geom).area / da_metro.geometry.area
    # da_metro = da_metro[overlap_ratio >= 0.5].copy()
    # print(f"DA after overlap>=0.5 filter: {after:,} -> {len(da_metro):,}")

    # ---------------------------
    # 4) Write outputs
    # ---------------------------
    out_gpkg = DATA_INTERMEDIATE / "da.gpkg"

    print("Writing filtered DA layer to:", out_gpkg, "layer: da")
    da_metro.to_file(out_gpkg, layer="da", driver="GPKG")

    print("Writing admin boundaries to:", out_gpkg, "layer: metro_admin_boundaries")
    admin.to_file(out_gpkg, layer="metro_admin_boundaries", driver="GPKG")

    print("Writing dissolved union boundary to:", out_gpkg, "layer: metro_union")
    metro_union.to_file(out_gpkg, layer="metro_union", driver="GPKG")

    # Preview GeoJSON (WGS84)
    out_preview_geojson = DATA_INTERMEDIATE / "da_preview.geojson"
    try:
        da_wgs84 = da_metro.to_crs("EPSG:4326")
        print("Writing preview:", out_preview_geojson)
        da_wgs84.to_file(out_preview_geojson, driver="GeoJSON")
    except Exception as e:
        print("NOTE: Could not write preview GeoJSON:", repr(e))

    print("Done.")
    print("Open in QGIS:", out_gpkg)
    print("  Layers:")
    print("   - da (Metro Vancouver DAs only)")
    print("   - metro_admin_boundaries (30 regions)")
    print("   - metro_union (dissolved mask)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())