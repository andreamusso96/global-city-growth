# Large cities lose their growth advantage as urban systems mature -- Raw data

This repository contains the raw datasets used in [Large cities lose their growth advantage as urban systems mature](https://arxiv.org/abs/2510.12417). We include **only datasets whose licenses allow redistribution**. For datasets that do not allow redistribution, we provide scripts to download the data from the original sources using APIs (see the [GitHub](https://github.com/andreamusso96/global-city-growth) or [Zenodo](https://doi.org/10.5281/zenodo.17339403)).

## Overview

We study city growth in:

- United States: 1850–2020

- Global: 1975–2025

Data sources include satellite-derived rasters and micro-level censuses. 
This README references the primary works describing each dataset rather than explaining them in detail.

## Dataset Summary

| Dataset                                     | Status in this archive                  | License                                 | Version / Access date    | Citation / URL                                             |
| ------------------------------------------- | --------------------------------------- | --------------------------------------- | ------------------------ | ---------------------------------------------------------- |
| Census Place Project Crosswalks             | **Included**                            | CC BY 4.0                               | `1.0`                    | DOI: `10.1016/j.eeh.2022.101477`                           |
| NHGIS Place Points & Population             | **Not included** (download via scripts) | IPUMS NHGIS (restricted redistribution) |                          | [https://www.nhgis.org/](https://www.nhgis.org/)           |
| IPUMS USA Full Count (1850–1940)            | **Not included** (download via scripts) | IPUMS USA (restricted redistribution)   |                          | [https://usa.ipums.org/](https://usa.ipums.org/)           |
| USA State Geometries                        | **Included**                            | Public Domain (US Census)               |                          | Census Cartographic Boundary Files                         |
| GHSL (GHS-POP, GHS-SMOD)                    | **Included**                            | CC BY 4.0                               | `R2023A`                 | Copernicus EMS (GHSL)                                      |
| CShapes 2.0                                 | **Included**                            | CC BY-NC-SA                             | `2.0`                    | [https://cshapes.org/](https://cshapes.org/)               |
| CShapes → ISO/World Bank Crosswalk          | **Included**                            | CC BY 4.0                               |                          | This repository                                            |
| UN M49 Sub-region Mapping                   | **Included**                            | UN data terms of use                    |                          | UN Statistics Division (M49)                               |
| Our World in Data (population/urbanization) | **Included**                            | CC BY 4.0                               |                          | [https://ourworldindata.org/](https://ourworldindata.org/) |
| Our World in Data (population/urbanization) | **Included**                            | CC BY 4.0                               |                          | DOI: `https://doi.org/10.6084/m9.figshare.c.5521821.v1` |


## Dataset Manifest

Here is detailed list of all datasets used in this project, including those provided in this repository and those that must be downloaded separately. 

### USA Datasets

* **Census Place Project Crosswalks**
    * **Status**: Included in this repository.
    * **Description:** Crosswalks for historical US census place identifiers.
    * **Original Source:** [DOI: 10.1016/j.eeh.2022.101477](https://doi.org/10.1016/j.eeh.2022.101477)
    * **License:** CC-BY-4.0
    * **Files:**
        * `usa/census_place_project/histid_place_crosswalk_{year}.csv` (for years 1850-1940)
        * `usa/census_place_project/place_component_crosswalk.csv`

* **NHGIS Place Points & Population**
    * **Status:** <span style="color:red">**NOT INCLUDED**</span> - Must be downloaded from source.
    * **Original Source:** [IPUMS NHGIS](https://www.nhgis.org/)
    * **License:** IPUMS NHGIS — redistribution restricted
    * **Files:**
        * `usa/nhgis/geo/shapefile_tlgnis_us_place_point_{year}/US_place_point_{year}.shp` (for years 1900-2010)
        * `usa/nhgis/pop/ts_geog2010_place.csv`

* **IPUMS USA Full Count**
    * **Status:** <span style="color:red">**NOT INCLUDED**</span> - Must be downloaded from source.
    * **Original Source:** [IPUMS USA](https://usa.ipums.org/)
    * **License:** IPUMS USA — redistribution restricted
    * **Files:** `usa/ipums_full_count/ipums_full_count_{year}.csv` (for years 1850-1940)

* **USA State Geometries**
    * **Status**: Included in this repository.
    * **Description:** Cartographic boundary shapefile for US states.
    * **Original Source:** [US Census Bureau](https://www.census.gov/geographies/mapping-files/time-series/geo/carto-boundary-file.html)
    * **License:** Public Domain
    * **Files:** `usa/misc/States_shapefile.shp`

### World Datasets

* **Global Human Settlement Layer (GHSL)**
    * **Status**: Included in this repository.
    * **Description:** Gridded population (GHS-POP) and settlement model (GHS-SMOD) data.
    * **Original Source:** 
        - [GHS_POP](https://human-settlement.emergency.copernicus.eu/download.php?ds=smod)
        - [GHS_SMOD](https://human-settlement.emergency.copernicus.eu/download.php?ds=pop)
    * **License:** CC-BY-4.0
    * **Files:**
        * `world/ghsl/pop/GHS_POP_E{year}_GLOBE_R2023A_54009_1000_V1_0.tif` (for years 1975-2025)
        * `world/ghsl/smod/GHS_SMOD_E{year}_GLOBE_R2023A_54009_1000_V2_0.tif` (for years 1975-2025)

* **CShapes 2.0 Borders**
    * **Status**: Included in this repository.
    * **Description:** Historical country boundaries.
    * **Original Source:** [CShapes](https://cshapes.org/)
    * **License:** CC-BY-NC-SA
    * **Files:** `world/cshapes/CShapes-2.0.shp`

* **Our World in Data (OWID)**
    * **Status**: Included in this repository
    * **Description:** Population with projections
    * **Original Source:** 
        - [population-long-run-with-projections.csv](https://ourworldindata.org/grapher/population-long-run-with-projections)
    * **License:** CC-BY-4.0
    * **Files:**
        * `world/misc/population-long-run-with-projections.csv`

* **Updating Global Urbanization Projections Under the Shared Socioeconomic Pathways**
    * **Status**: Included in this repository
    * **Description:** Urbanization with projections. We use World Bank-based annual projections under SSP2 (``middle of the road''). 
    * **Original Source:** 
        - [urban-population-share-with-projections.csv](https://doi.org/10.6084/m9.figshare.c.5521821.v1)
    * **License:** CC-BY-4.0
    * **Files:**
        * `world/misc/urban-population-share-with-projections.csv`

* **UN M49 Sub-region Mapping**
    * **Status**: Included in this repository
    * **Description:** Country-to-sub-region mapping used for UN M49 robustness analyses.
    * **Original Source:**
        - [UN Statistics Division — Standard country or area codes for statistical use (M49)](https://unstats.un.org/unsd/methodology/m49/)
    * **License:** UN data terms of use
    * **Files:**
        * `world/misc/micro-regions.csv`

* **Handmade**
    * **Status**: Included in this repository
    * **Description:** Crosswalk file mapping CShapes codes to World Bank country codes and manual country-to-region grouping (Asia, Africa, Americas and Europe)
    * **Original Source:** Handmade by the author of this repository
    * **License:** CC-BY-4.0
    * **Files:**
        * `world/cshapes/c_shapes_to_world_bank_codes.csv`
        * `world/misc/countries_with_regions.csv`



## Folder Structure

The data in this repository is organized into the following folder structure:
```
├── usa/
│   ├── census_place_project/
│   ├── nhgis/              # populated by acquisition scripts
│   ├── ipums_full_count/   # populated by acquisition scripts
│   ├── misc/
│   └── tmp/                # for db files
├── world/
│   ├── ghsl/
│   ├── cshapes/
│   └── misc/

```
