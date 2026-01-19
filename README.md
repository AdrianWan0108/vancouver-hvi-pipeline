# Vancouver Heat Vulnerability (HVI) – Data Pipeline

This repository contains the data processing pipeline for the Vancouver Heat Vulnerability Index (HVI) project.

The goal of this project is to process multiple raw datasets into a Dissemination Area (DA)–level GeoJSON that can be used directly by a MapLibre GL JS frontend.

At this stage, the repository contains project structure and environment setup only.
Data processing scripts will be added incrementally.

## Planned Output

After the pipeline is implemented, the main output will be:

- data_out/da_hvi.geojson  
  A DA-level GeoJSON containing:
  - Heat Vulnerability Index (HVI)
  - Exposure, Sensitivity, and Adaptive Capacity scores
  - Indicator-level attributes for frontend display

Output files are not committed to GitHub.

## Local Data (not committed)

Raw datasets should be placed locally in the following folders:

data_raw/
- da_boundaries/        (Dissemination Area boundaries – shapefile)
- census_profile/       (Census variables at DA level – CSV)
- canue_lst/             (CANUE postal-code LST datasets)
- landcover/             (LCC2020 land cover – ESRI .gdb)

These datasets are excluded from version control via .gitignore.

## Environment Setup

This project uses Conda for environment management.

Create the environment:
conda env create -f environment.yml

Activate the environment:
conda activate vancouver-hvi

## Pipeline Design (planned)

The data pipeline will be implemented as a sequence of Python scripts located in the scripts folder.

Planned execution order (not yet implemented):

python scripts/01_prepare_da.py
python scripts/02_census_sensitivity.py
python scripts/03_exposure_lst.py
python scripts/04_greenness_landcover.py
python scripts/05_build_hvi.py

These scripts do not yet exist and are listed to document the intended workflow.

## Repository Status

- Project structure created
- Conda environment configured
- Data processing scripts pending
- Final GeoJSON output pending

## Notes

This repository is designed to support:
- Academic reproducibility (script-based workflow)
- Separation of concerns (data processing vs frontend rendering)
- Incremental development and validation
