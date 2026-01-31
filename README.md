# Vancouver Heat Vulnerability (HVI) – Data Pipeline

This repository contains the **completed data processing pipeline** for the Vancouver Heat Vulnerability Index (HVI) project.

The purpose of this pipeline is to transform multiple raw spatial and tabular datasets into a **Dissemination Area (DA)–level geospatial dataset** suitable for interactive web mapping and analysis.

## Project Goal

The goal of this project is to construct a **composite Heat Vulnerability Index (HVI)** for the Metro Vancouver region by integrating indicators related to:

  - Heat Exposure
  - Sensitivity Adaptive Capacity
  - Adaptive Capacity

The resulting dataset is intended for use in an interactive MapLibre GL JS frontend as part of a broader web-based visualization tool.

## Current Output

After executing the full pipeline, the primary output is:

`data_out/da_hvi.geojson`

This DA-level GeoJSON contains:

  - Composite Heat Vulnerability Index (HVI)
  - Component indices:
    - Exposure
    - Sensitivity
    - Adaptive Capacity
  - Selected indicator-level attributes used in index construction
  - DA identifiers and geometries suitable for spatial joins and visualization

⚠️ Output files are not committed to GitHub and are excluded via .gitignore.

## Pipeline Overview

The data pipeline is implemented as a sequence of Python scripts located in the scripts/ directory and executed in order.

### Execution order:

  ```
  python scripts/01_prepare_da.py
  python scripts/02_census_sensitivity.py
  python scripts/03_exposure_lst.py
  python scripts/04_greenness_landcover.py
  python scripts/05_build_hvi.py
  ```

### Script Responsibilities

  - 00_config.py
    - Centralized configuration for file paths, constants, and shared parameters.
  - 01_prepare_da.py
    - Loads and prepares DA boundary geometries and establishes the spatial base for all subsequent joins.
  - 02_census_sensitivity.py
    - Processes DA-level census variables and computes the Sensitivity Index.
  - 03_exposure_lst.py
    - Aggregates CANUE land surface temperature (LST) data and computes the Exposure Index.
  - 04_greenness_landcover.py
    - Processes land cover classification data and computes greenness-related indicators used for Adaptive Capacity.
  - 05_hvi_composite.py
    - Normalizes component indices, constructs the composite Heat Vulnerability Index, and exports the final DA-level GeoJSON.

## Local Data (not committed)

Raw input datasets should be placed locally in the following directory structure:

```
data_raw/
├─ da_boundaries/        # Dissemination Area boundaries (shapefile)
├─ census_profile/       # Census variables at DA level (CSV)
├─ canue_lst/            # CANUE land surface temperature datasets
└─ landcover/            # Land Cover Classification 2020 (ESRI .gdb)
```

These datasets are excluded from version control via `.gitignore`.

## Environment Setup

This project uses Conda for environment management.

Create the environment:
```bash
conda env create -f environment.yml
```

Activate the environment:
```bash
conda activate vancouver-hvi
```

## Notes

This repository is designed to support:
- Academic reproducibility (script-based workflow)
- Separation of concerns (data processing vs frontend rendering)
- Incremental development and validation
