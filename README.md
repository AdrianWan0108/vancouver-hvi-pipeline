# Vancouver HVI Pipeline

This repository contains the code for building Metro Vancouver Heat Vulnerability Index (HVI) datasets from raw spatial and tabular inputs.

The pipeline produces:
- DA-level component and HVI outputs
- Region-level aggregated outputs
- Intermediate tables for debugging and inspection
- Optional formula-comparison outputs

## Repository Layout

```text
scripts/            Pipeline scripts
data_raw/           Local raw inputs (not versioned)
data_intermediate/  Intermediate and final pipeline outputs
notebooks/          Ad hoc exploration
environment.yml     Conda environment definition
package.json        Node dependency for PMTiles conversion
```

## Requirements

- Conda
- Python environment from `environment.yml`
- Raw source data placed under `data_raw/`
- Optional for vector-tile packaging:
  - `tippecanoe`
  - `npx pmtiles`

## Environment Setup

Create the Conda environment:

```bash
conda env create -f environment.yml
```

Activate it:

```bash
conda activate vancouver-hvi
```

## Raw Data Layout

Place raw inputs under `data_raw/` using this layout:

```text
data_raw/
|-- da_boundaries/      Statistics Canada DA boundaries
|-- census_profile/     Statistics Canada census profile CSV
|-- canue_lst/          CANUE WTLST and DMTI postal-code inputs
|-- landcover/          Landcover raster
`-- ...                Other local reference data used by config.py
```

Paths and file names are controlled in `scripts/config.py`.

## Run Order

Run the scripts in this order:

```bash
python scripts/01_prepare_da.py
python scripts/02_landcover_housing_capacity.py
python scripts/03_census_social.py
python scripts/04_canue_exposure.py
python scripts/05_build_hvi_outputs.py
python scripts/06_formula_review.py
```

`06_formula_review.py` is optional. It does not modify the production HVI outputs.

## Script Responsibilities

### `scripts/config.py`
Shared configuration for paths, CRS settings, constants, and output file locations.

### `scripts/01_prepare_da.py`
Prepares the Metro Vancouver DA base layer from the source DA boundaries.

Outputs:
- `data_intermediate/da.gpkg`
- `data_intermediate/da_preview.geojson`

### `scripts/02_landcover_housing_capacity.py`
Processes landcover and housing-related inputs, computes landcover fractions, and builds adaptive-capacity inputs.

Outputs:
- `data_intermediate/landcover_housing_capacity.csv`
- `data_intermediate/02_landcover_housing_capacity_debug_report.txt`

### `scripts/03_census_social.py`
Processes DA-level census social indicators and computes the sensitivity component.

Outputs:
- `data_intermediate/census_sensitivity.csv`
- `data_intermediate/census_social_selected_long.csv`
- `data_intermediate/03_census_social_debug_report.txt`

### `scripts/04_canue_exposure.py`
Processes CANUE land surface temperature inputs, joins them to DAs, and computes the exposure component.

Outputs:
- `data_intermediate/canue_exposure.csv`
- `data_intermediate/canue_exposure_points_preview.geojson`
- `data_intermediate/04_canue_exposure_debug_report.txt`

### `scripts/05_build_hvi_outputs.py`
Joins the three components, computes the production HVI, builds region-level outputs, and writes the main final files.

Outputs:
- `data_intermediate/hvi_da_components.csv`
- `data_intermediate/hvi_da.geojson`
- `data_intermediate/hvi_regions_components.csv`
- `data_intermediate/hvi_regions.geojson`
- `data_intermediate/05_build_hvi_outputs_debug_report.txt`

### `scripts/06_formula_review.py`
Compares candidate HVI formulas using the current normalized component outputs.

Outputs:
- `data_intermediate/hvi_formula_comparison_da.csv`
- `data_intermediate/hvi_formula_comparison_regions.csv`
- `data_intermediate/06_formula_review_report.txt`

## Production HVI Implementation

The current production HVI in `scripts/05_build_hvi_outputs.py` is:

```text
HVI = (E + S + (1 - A)) / 3
```

where:
- `E = exposure_index`
- `S = sensitivity_index`
- `A = adaptive_capacity_index`

This score is already bounded to `0-1`.

## Current Component Implementation

### Exposure

```text
E = 0.67 * exposure_mean_n01 + 0.33 * hardscape_frac_n01
```

### Sensitivity

Equal-weight mean of:
- `unemployment_rate_n01`
- `low_income_rate_n01`
- `pct_seniors_65plus_n01`
- `pct_living_alone_n01`

### Adaptive Capacity

Equal-weight mean of:
- `green_capacity_n01`
- `renter_capacity_n01`
- `major_repairs_capacity_n01`
- `core_need_capacity_n01`

## Eligibility and Exclusion Rules

Water-dominated DAs are excluded downstream using:
- landcover class `12`
- `water_frac >= 0.80`

The landcover stage writes:
- `exclude_water_da`
- `da_eligible`

These fields are used by later stages.

## Main Outputs

The main production outputs written to `data_intermediate/` are:

- `hvi_da.geojson`
- `hvi_regions.geojson`
- `hvi_da_components.csv`
- `hvi_regions_components.csv`

Useful intermediate outputs include:

- `landcover_housing_capacity.csv`
- `census_sensitivity.csv`
- `canue_exposure.csv`

Each major stage also writes a debug report.

## Optional Vector-Tile Packaging

If you want tiles for the frontend, convert the final GeoJSON files after running `05_build_hvi_outputs.py`.

Example:

```bash
tippecanoe -o data_intermediate/hvi_da.mbtiles \
  -l hvi_da \
  -zg \
  --read-parallel \
  --drop-densest-as-needed \
  --extend-zooms-if-still-dropping \
  data_intermediate/hvi_da.geojson

tippecanoe -o data_intermediate/hvi_regions.mbtiles \
  -l hvi_regions \
  -zg \
  --read-parallel \
  --drop-densest-as-needed \
  --extend-zooms-if-still-dropping \
  data_intermediate/hvi_regions.geojson

npx pmtiles convert data_intermediate/hvi_da.mbtiles data_intermediate/hvi_da.pmtiles
npx pmtiles convert data_intermediate/hvi_regions.mbtiles data_intermediate/hvi_regions.pmtiles
```

## Notes

- Raw inputs are local and are not included in version control.
- The pipeline is designed to be rerun script-by-script as methods or inputs change.
- Debug reports in `data_intermediate/` are the first place to check when validating stage outputs.
