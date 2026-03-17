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

The current production HVI formula is:

```text
HVI = (E + S + (1 - A)) / 3
```

where:
- `E = exposure_index`
- `S = sensitivity_index`
- `A = adaptive_capacity_index`

This production score is already bounded to `0-1`, so the exported HVI fields do not require an additional min-max normalization step.

The final outputs are intended for a web map frontend, with layers for:
- final HVI
- component indices
- indicator-level attributes

## Current Outputs

After running the full pipeline, the main outputs are written to `data_intermediate/`:
- `hvi_da.geojson`: DA-level HVI and component attributes
- `hvi_regions.geojson`: municipality-level HVI derived from dissolved retained DAs
- `hvi_da_components.csv`: DA-level component table without geometry
- `hvi_regions_components.csv`: municipality-level component table without geometry
- `census_sensitivity.csv`: census-derived sensitivity component table
- `landcover_housing_capacity.csv`: landcover, housing-capacity, hardscape, and DA eligibility table
- `canue_exposure.csv`: CANUE/DMTI exposure component table

Water-dominated DAs are excluded from final outputs using landcover class `12` and the rule:
- `water_frac >= 0.80`

## Pipeline Overview

The scripts live in `scripts/` and should be run in this order:

```bash
python scripts/01_prepare_da.py
python scripts/02_landcover_housing_capacity.py
python scripts/03_census_social.py
python scripts/04_canue_exposure.py
python scripts/05_build_hvi_outputs.py
python scripts/06_formula_review.py  # optional formula comparison stage
```

## Script Responsibilities

- `scripts/config.py`
  Centralized configuration for file paths, CRS settings, and shared constants.

- `scripts/01_prepare_da.py`
  Loads DA boundaries, filters them to Metro Vancouver, and writes the base DA geometry layer.

- `scripts/02_landcover_housing_capacity.py`
  Processes landcover plus housing-capacity census inputs, computes adaptive capacity, emits hardscape fractions, and creates the DA eligibility mask used to exclude water-dominated DAs.

- `scripts/03_census_social.py`
  Processes DA-level census social variables for eligible DAs and computes the sensitivity index.

- `scripts/04_canue_exposure.py`
  Aggregates CANUE land surface temperature values for eligible DAs, combines them with hardscape fractions, and computes the exposure index.

- `scripts/05_build_hvi_outputs.py`
  Joins component tables, computes the production HVI using the additive protective formula, exports the DA GeoJSON, and builds the municipality layer by dissolving retained DAs.

- `scripts/06_formula_review.py`
  Compares multiple HVI formulas on the current normalized `Exposure`, `Sensitivity`, and `Adaptive Capacity` outputs without changing the production HVI files.

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
`scripts/02_landcover_housing_capacity.py` also requires `rasterstats`.

## Notes

This repository is designed to support:
- reproducible script-based geospatial processing
- separation between data preparation and frontend rendering
- iterative refinement of HVI methodology and indicators
