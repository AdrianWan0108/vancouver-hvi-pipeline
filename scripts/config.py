# scripts/00_config.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# -------------------------
# Project paths
# -------------------------

# This assumes scripts/00_config.py lives under <repo_root>/scripts/
REPO_ROOT = Path(__file__).resolve().parents[1]

DATA_RAW = REPO_ROOT / "data_raw"
DATA_INTERMEDIATE = REPO_ROOT / "data_intermediate"
DATA_OUT = REPO_ROOT / "data_out"

DA_DIR = DATA_RAW / "da_boundaries"
CENSUS_DIR = DATA_RAW / "census_profile"
CANUE_DIR = DATA_RAW / "canue_lst"
LANDCOVER_DIR = DATA_RAW / "landcover"

# Ensure output folders exist (safe even if already present)
DATA_INTERMEDIATE.mkdir(parents=True, exist_ok=True)
DATA_OUT.mkdir(parents=True, exist_ok=True)

# -------------------------
# Locked design decisions
# -------------------------

# Exposure
EXPOSURE_YEAR = 2021
EXPOSURE_FIELD = "wtlst21_06"  # CANUE warm-season max of mean LST (1km), year 2021

# CRS standards
CRS_WGS84 = "EPSG:4326"  # lat/lon
CRS_CANADA_ALBERS = "EPSG:3347"  # good for Canada-wide area/distance calculations

# Join keys (we will confirm exact field names during script 01/02)
# DA boundary files often have something like "DGUID" and/or "DAUID"
PREFERRED_DA_KEYS = ["DGUID", "DAUID", "DAUID_2021", "DAUID21"]

# -------------------------
# Helper functions to locate files
# -------------------------

def _first_match(folder: Path, patterns: list[str]) -> Optional[Path]:
    """Return first file matching any pattern (sorted for stability)."""
    for pat in patterns:
        hits = sorted(folder.glob(pat))
        if hits:
            return hits[0]
    return None


def find_da_shapefile() -> Path:
    """
    Find the DA boundary shapefile (.shp) under data_raw/da_boundaries/.
    Example expected: lda_000a21a_e.shp
    """
    shp = _first_match(DA_DIR, ["*.shp", "*.SHP"])
    if not shp:
        raise FileNotFoundError(
            f"Could not find a .shp file in {DA_DIR}. "
            "Make sure you extracted the DA boundary zip and copied ALL shapefile parts into data_raw/da_boundaries/."
        )
    return shp


def find_census_profile_csv() -> Path:
    """
    Find the StatCan Census Profile CSV under data_raw/census_profile/.
    Example expected: 98-401-X2021006_English_CSV_data_BritishColumbia.csv
    """
    csv = _first_match(CENSUS_DIR, ["*.csv", "*.CSV"])
    if not csv:
        raise FileNotFoundError(
            f"Could not find a Census Profile CSV in {CENSUS_DIR}. "
            "Copy 98-401-X2021006_English_CSV_data_BritishColumbia.csv into data_raw/census_profile/."
        )
    return csv


def find_canue_wtlst_csv(year: int = 2021) -> Path:
    """
    Find CANUE WTLST values file for the chosen year.
    Expected: wtlst_ava_21.csv for year 2021
    """
    suffix = str(year)[-2:]  # 2021 -> "21"
    target_patterns = [f"wtlst_ava_{suffix}.csv", f"wtlst_ava_{suffix}.CSV", "wtlst_ava_*.csv", "wtlst_ava_*.CSV"]
    csv = _first_match(CANUE_DIR, target_patterns)
    if not csv:
        raise FileNotFoundError(
            f"Could not find CANUE wtlst file in {CANUE_DIR}. "
            f"Copy wtlst_ava_{suffix}.csv into data_raw/canue_lst/."
        )
    return csv


def find_canue_dmti_sli_csv(year: int = 2021) -> Path:
    """
    Find DMTI postal code location file for the chosen year.
    Expected: DMTI_SLI_21.csv for year 2021
    """
    suffix = str(year)[-2:]  # 2021 -> "21"
    target_patterns = [f"DMTI_SLI_{suffix}.csv", f"DMTI_SLI_{suffix}.CSV", "DMTI_SLI_*.csv", "DMTI_SLI_*.CSV"]
    csv = _first_match(CANUE_DIR, target_patterns)
    if not csv:
        raise FileNotFoundError(
            f"Could not find DMTI_SLI file in {CANUE_DIR}. "
            f"Copy DMTI_SLI_{suffix}.csv into data_raw/canue_lst/."
        )
    return csv


def find_landcover_raster() -> Path:
    land_dir = DATA_RAW / "landcover"
    tifs = list(land_dir.glob("*.tif"))

    if not tifs:
        raise FileNotFoundError(
            f"No landcover GeoTIFF found in {land_dir}. "
            "Export LCC2020 as GeoTIFF (.tif) from QGIS."
        )

    return tifs[0]




# -------------------------
# Centralized accessors (nice for scripts)
# -------------------------

@dataclass(frozen=True)
class Inputs:
    da_shp: Path
    census_csv: Path
    canue_wtlst_csv: Path
    canue_dmti_csv: Path
    landcover_raster: Path

def get_inputs() -> Inputs:
    """Resolve and return all required input paths."""
    return Inputs(
        da_shp=find_da_shapefile(),
        census_csv=find_census_profile_csv(),
        canue_wtlst_csv=find_canue_wtlst_csv(EXPOSURE_YEAR),
        canue_dmti_csv=find_canue_dmti_sli_csv(EXPOSURE_YEAR),
        landcover_raster=find_landcover_raster(),
    )


# -------------------------
# Quick self-check (optional)
# -------------------------

if __name__ == "__main__":
    ins = get_inputs()
    print("Repo root:", REPO_ROOT)
    print("DA shapefile:", ins.da_shp)
    print("Census CSV:", ins.census_csv)
    print("CANUE WTLST:", ins.canue_wtlst_csv)
    print("CANUE DMTI:", ins.canue_dmti_csv)
    print("Landcover raster:", ins.landcover_raster)
    print("Exposure:", EXPOSURE_YEAR, EXPOSURE_FIELD)
