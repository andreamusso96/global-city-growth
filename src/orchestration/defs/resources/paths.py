from dataclasses import dataclass
from pathlib import Path

# Paths are grouped by data source, and then by USA vs World.

@dataclass(frozen=True)
class CensusPlaceProjectPaths:
    root: Path
    
    def crosswalk_hist_id_to_hist_census_place(self, year: int) -> Path:
        return self.root / f"histid_place_crosswalk_{year}.csv"
    
    def hist_census_place_geom(self) -> Path:
        return self.root / "place_component_crosswalk.csv"


@dataclass(frozen=True)
class NhgisPaths:
    root: Path

    def census_place_geom_folder(self, year: int) -> Path:
        return self.root / "geo" / f"shapefile_tlgnis_us_place_point_{year}"

    def census_place_geom(self, year: int) -> Path:
        return self.census_place_geom_folder(year) / f"US_place_point_{year}.shp"
    
    def census_place_pop_1990_2020(self) -> Path:
        return self.root / "pop" / "nhgis_place_population_1990_2020.csv"

@dataclass(frozen=True)
class IpumsFullCountPaths:
    root: Path

    def ipums_full_count(self, year: int) -> Path:
        return self.root / f"ipums_full_count_{year}.csv"
    

@dataclass(frozen=True)
class USMiscPaths:
    root: Path

    def usa_state_geom(self) -> Path:
        return self.root / "States_shapefile.shp"
    

@dataclass(frozen=True)
class USAPaths:
    census_place_project: CensusPlaceProjectPaths
    nhgis: NhgisPaths
    ipums_full_count: IpumsFullCountPaths
    misc: USMiscPaths
    
    @classmethod
    def from_root(cls, root: Path) -> "USAPaths":
        return cls(
            census_place_project=CensusPlaceProjectPaths(root / "census_place_project"),
            nhgis=NhgisPaths(root / "nhgis"),
            ipums_full_count=IpumsFullCountPaths(root / "ipums_full_count"),
            misc=USMiscPaths(root / "misc")
        )
    

@dataclass(frozen=True)
class GHSLPaths:
    root: Path
    
    def pop(self, year: int) -> Path:
        return self.root / "pop" / f"GHS_POP_E{year}_GLOBE_R2023A_54009_1000_V1_0.tif"
    
    def smod(self, year: int) -> Path:
        return self.root / "smod" / f"GHS_SMOD_E{year}_GLOBE_R2023A_54009_1000_V2_0.tif"


@dataclass(frozen=True)
class CShapesBorderPaths:
    root: Path

    def country_borders(self) -> Path:
        return self.root / f"CShapes-2.0.shp"
    
    def crosswalk_cshapes_code_to_iso_code(self) -> Path:
        return self.root / "c_shapes_to_world_bank_codes.csv"
    

@dataclass(frozen=True)
class OWIDPaths:
    root: Path

    def urbanization(self) -> Path:
        return self.root / "urban-population-share-2050.csv"

    def population(self) -> Path:
        return self.root / "population-long-run-with-projections.csv"
    
    def countries_with_regions(self) -> Path:
        return self.root / "countries_with_regions.csv"


@dataclass(frozen=True)
class WorldPaths:
    ghsl: GHSLPaths
    cshapes_border: CShapesBorderPaths
    owid: OWIDPaths

    @classmethod
    def from_root(cls, root: Path) -> "WorldPaths":
        return cls(
            ghsl=GHSLPaths(root / "ghsl"),
            cshapes_border=CShapesBorderPaths(root / "cshapes"),
            owid=OWIDPaths(root / "owid")
        )
    

@dataclass(frozen=True)
class OtherPaths:
    root: Path

    def analysis_parameters(self) -> Path:
        return self.root / "analysis_parameters.csv"


@dataclass(frozen=True)
class DownloadPaths:
    root: Path
    
    def download(self) -> Path:
        return self.root

@dataclass(frozen=True)
class DataPaths:
    usa: USAPaths
    world: WorldPaths
    other: OtherPaths
    download: DownloadPaths

    @classmethod
    def from_root(cls, root: Path) -> "DataPaths":
        return cls(
            usa=USAPaths.from_root(root / "usa"),
            world=WorldPaths.from_root(root / "world"),
            other=OtherPaths(root / "other"),
            download=DownloadPaths(root / "download")
        )
    
