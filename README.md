# Vancouver Heat Vulnerability Index Data Pipeline

This repository contains the data-processing pipeline for a Metro Vancouver Heat Vulnerability Index (HVI) map.

The pipeline transforms raw spatial and tabular inputs into map-ready geospatial outputs at two levels:
- Dissemination Area (DA), which is the primary analysis unit
- Municipality/region, which is a zoomed-out display layer derived from retained DAs

## Project Goal

The goal is to construct a composite Heat Vulnerability Index for Metro Vancouver by combining:
- Exposure
- Sensitivity
- Adaptive Capacity

The final outputs are intended for a web map frontend, with layers for:
- final HVI
- component indices
- indicator-level attributes

## Current Outputs

After running the full pipeline, the main outputs are written to `data_intermediate/`:
- `hvi.geojson`: DA-level HVI and component attributes
- `hvi_regions.geojson`: municipality-level HVI derived from dissolved retained DAs
- `hvi_components.csv`: DA-level component table without geometry
- `hvi_regions_components.csv`: municipality-level component table without geometry

Water-dominated DAs are excluded from final outputs using landcover class `12` and the rule:
- `water_frac >= 0.80`

## Pipeline Overview

The scripts live in `scripts/` and should be run in this order:

```bash
python scripts/01_prepare_da.py
python scripts/03_adaptive_capacity.py
python scripts/02_census_sensitivity.py
python scripts/04_exposure_lst.py
python scripts/05_hvi_composite.py
```

## Script Responsibilities

- `scripts/config.py`
  Centralized configuration for file paths, CRS settings, and shared constants.

- `scripts/01_prepare_da.py`
  Loads DA boundaries, filters them to Metro Vancouver, and writes the base DA geometry layer.

- `scripts/03_adaptive_capacity.py`
  Processes landcover, computes adaptive capacity from classes `6`, `7`, and `8`, and creates the DA eligibility mask used to exclude water-dominated DAs.

- `scripts/02_census_sensitivity.py`
  Processes DA-level census variables for eligible DAs and computes the sensitivity index.

- `scripts/04_exposure_lst.py`
  Aggregates CANUE land surface temperature values for eligible DAs and computes the exposure index.

- `scripts/05_hvi_composite.py`
  Joins component tables, computes the final HVI, exports the DA GeoJSON, and builds the municipality layer by dissolving retained DAs.

## Adaptive Capacity Method

Adaptive capacity is currently derived from Metro Vancouver landcover classes:
- `6` Coniferous
- `7` Deciduous
- `8` Shrub

The resulting `green_frac` is a woody-vegetation proxy, not a direct tree-canopy measurement.

The landcover stage also computes:
- `water_frac`
- `exclude_water_da`
- `da_eligible`

These fields are used to remove ocean-dominated DAs from downstream analytics and map outputs.

## Local Data

Raw input datasets should be placed locally under:

```text
data_raw/
|-- da_boundaries/      # StatCan DA boundaries
|-- census_profile/     # StatCan census profile CSV
|-- canue_lst/          # CANUE WTLST + DMTI postal code inputs
`-- landcover/          # Metro Vancouver landcover raster (.tif)
```

These datasets are excluded from version control.

## Environment Setup

Create the Conda environment:

```bash
conda env create -f environment.yml
```

Activate it:

```bash
conda activate vancouver-hvi
```

Note:
`scripts/03_adaptive_capacity.py` also requires `rasterstats`.

## Notes

This repository is designed to support:
- reproducible script-based geospatial processing
- separation between data preparation and frontend rendering
- iterative refinement of HVI methodology and indicators
