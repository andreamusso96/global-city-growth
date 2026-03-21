from dataclasses import dataclass
from pathlib import Path

# Paths are grouped by data source, and then by USA vs World.

@dataclass(frozen=True)
class IpumsFullCount:
    def ipums_full_count_raw(self, year: int) -> str:
        return f"ipums_full_count_{year}_raw" 
    
    def ipums_full_count_clean(self, year: int) -> str:
        return f"ipums_full_count_{year}_clean"
    
    def crosswalk_hist_id_to_hist_census_place_raw(self, year: int) -> str:
        return f"crosswalk_hist_id_to_hist_census_place_{year}_raw"
    
    def crosswalk_hist_id_to_hist_census_place_clean(self, year: int) -> str:
        return f"crosswalk_hist_id_to_hist_census_place_{year}_clean"
    
    def ipums_full_count_census_place_id(self, year: int) -> str:
        return f"ipums_full_count_census_place_id_{year}"
    
    def ipums_full_count_census_place_id_all_years(self) -> str:
        return f"ipums_full_count_census_place_id_all_years"
    
    def ipums_full_count_individual_migration(self) -> str:
        return f"ipums_full_count_individual_migration"
    
    def census_place_population(self) -> str:
        return f"census_place_population"
    
    def census_place_migration(self) -> str:
        return f"census_place_migration"
    

@dataclass(frozen=True)
class USASources:
    def usa_hist_census_place_population(self) -> str:
        return "usa_hist_census_place_population"
    
    def usa_hist_census_place_migration(self) -> str:
        return "usa_hist_census_place_migration"
    
    def usa_nhgis_census_place_population_raw_1990_2020(self) -> str:
        return "usa_nhgis_census_place_population_1990_2020_raw"
    
    def usa_nhgis_census_place_geom_all_years_raw(self) -> str:
        return "usa_nhgis_census_place_geom_all_years_raw"
    
    def usa_hist_census_place_geom_raw(self) -> str:
        return "usa_hist_census_place_geom_raw"
    
    def usa_states_geom_raw(self) -> str:
        return "usa_states_geom_raw"

    def usa_cbsa_geom_2010_raw(self) -> str:
        return "usa_cbsa_geom_2010_raw"

    def usa_cbsa_geom_2020_raw(self) -> str:
        return "usa_cbsa_geom_2020_raw"

    def usa_county_geom_2010_raw(self) -> str:
        return "usa_county_geom_2010_raw"

    def usa_county_population_raw(self) -> str:
        return "usa_county_population_raw"
    

@dataclass(frozen=True)
class USATransformations:
    def usa_nhgis_census_place_equivalence_network(self) -> str:
        return "usa_nhgis_census_place_equivalence_network"
    
    def usa_crosswalk_nhgis_census_place_to_connected_component(self) -> str:
        return "usa_crosswalk_nhgis_census_place_to_connected_component"
    
    def usa_raster_census_place(self) -> str:
        return "usa_raster_census_place"

    def usa_raster_census_place_convolved_year(self, year: int) -> str:
        return f"usa_raster_census_place_convolved_{year}"
    
    def usa_raster_census_place_convolved_all_years(self) -> str:
        return f"usa_raster_census_place_convolved_all_years"
    
    def usa_cluster_base_geom(self) -> str:
        return "usa_cluster_base_geom"
    
    def usa_cluster_base_matching(self) -> str:
        return "usa_cluster_base_matching"
    
    def usa_crosswalk_component_id_to_cluster_id(self) -> str:
        return "usa_crosswalk_component_id_to_cluster_id"

    def usa_cbsa_base_geom(self) -> str:
        return "usa_cbsa_base_geom"

    def usa_cbsa_base_matching(self) -> str:
        return "usa_cbsa_base_matching"

    def usa_crosswalk_component_id_to_cbsa_id(self) -> str:
        return "usa_crosswalk_component_id_to_cbsa_id"

    def usa_cbsa_growth_geom(self) -> str:
        return "usa_cbsa_growth_geom"

    def usa_crosswalk_county_to_cbsa_growth(self) -> str:
        return "usa_crosswalk_county_to_cbsa_growth"

    def usa_cbsa_growth_population(self) -> str:
        return "usa_cbsa_growth_population"
    
@dataclass(frozen=True)
class USAFigures:
    def usa_size_vs_growth(self) -> str:
        return "usa_size_vs_growth"
    
    def usa_rank_vs_size(self) -> str:
        return "usa_rank_vs_size"
    
    def usa_average_growth(self) -> str:
        return "usa_average_growth"
    
    def usa_size_vs_growth_normalized(self) -> str:
        return "usa_size_vs_growth_normalized"
    
    def usa_dataset_summary_table(self) -> str:
        return "usa_dataset_summary_table"

    def usa_year_epoch(self) -> str:
        return "usa_year_epoch"

    def usa_rank_size_slopes_change(self) -> str:
        return "usa_rank_size_slopes_change"

    def usa_suburbanization_size_vs_growth(self) -> str:
        return "usa_suburbanization_size_vs_growth"

    def usa_suburbanization_average_growth(self) -> str:
        return "usa_suburbanization_average_growth"

    def usa_suburbanization_size_vs_growth_normalized(self) -> str:
        return "usa_suburbanization_size_vs_growth_normalized"

    def usa_suburbanization_base_neighbor_flags(self) -> str:
        return "usa_suburbanization_base_neighbor_flags"

    def usa_suburbanization_cbsa_induced_labels(self) -> str:
        return "usa_suburbanization_cbsa_induced_labels"
    
@dataclass(frozen=True)
class USA:
    ipums_full_count: IpumsFullCount = IpumsFullCount()
    sources: USASources = USASources()
    transformations: USATransformations = USATransformations()
    figures: USAFigures = USAFigures()

@dataclass(frozen=True)
class WorldSources:
    def world_country_borders_raw(self) -> str:
        return "world_country_borders_raw"
    
    def world_urbanization_raw(self) -> str:
        return "world_urbanization_raw"
    
    def world_raster_ghsl_pop(self, year: int) -> str:
        return f"world_raster_ghsl_pop_{year}"

    def world_raster_ghsl_smod(self, year: int) -> str:
        return f"world_raster_ghsl_smod_{year}"
    
    def world_raster_ghsl_pop_all_years(self) -> str:
        return "world_raster_ghsl_pop_all_years"
    
    def world_raster_ghsl_smod_all_years(self) -> str:
        return "world_raster_ghsl_smod_all_years"
    
    def world_crosswalk_cshapes_code_to_iso_code(self) -> str:
        return "world_crosswalk_cshapes_code_to_iso_code"
    
    def world_country_region(self) -> str:
        return "world_country_region"

    def world_m49_region_raw(self) -> str:
        return "world_m49_region_raw"

@dataclass(frozen=True)
class WorldTransformations:
    def world_cluster_base_matching(self) -> str:
        return "world_cluster_base_matching"
    
    def world_crosswalk_component_id_to_cluster_id(self) -> str:
        return "world_crosswalk_component_id_to_cluster_id"

@dataclass(frozen=True)
class WorldFigures:
    def world_urbanization(self) -> str:
        return "world_urbanization"

    def world_average_growth(self) -> str:
        return "world_average_growth"
    
    def world_size_vs_growth(self) -> str:
        return "world_size_vs_growth"

    def world_size_vs_growth_normalized(self) -> str:
        return "world_size_vs_growth_normalized"

    def world_size_vs_growth_normalized_by_group(self) -> str:
        return "world_size_vs_growth_normalized_by_group"

    def world_size_growth_slopes_historical(self) -> str:
        return "world_size_growth_slopes_historical"

    def world_average_size_growth_slope_with_borders(self) -> str:
        return "world_average_size_growth_slope_with_borders"

    def world_size_growth_slopes_projections(self) -> str:
        return "world_size_growth_slopes_projections"

    def world_size_growth_slopes_historical_urbanization(self) -> str:
        return "world_size_growth_slopes_historical_urbanization"
    
    def world_rank_vs_size(self) -> str:
        return "world_rank_vs_size"

    def world_rank_size_slopes_historical(self) -> str:
        return "world_rank_size_slopes_historical"

    def world_rank_size_slopes(self) -> str:
        return "world_rank_size_slopes"
    
    def world_rank_size_slopes_change(self) -> str:
        return "world_rank_size_slopes_change"

    def world_rank_size_slopes_change_1975_2025(self) -> str:
        return "world_rank_size_slopes_change_1975_2025"
    
    def world_rank_size_slopes_decade_change(self) -> str:
        return "world_rank_size_slopes_decade_change"
    
    def world_dataset_summary_table(self) -> str:
        return "world_dataset_summary_table"

    def world_region_regression_with_urbanization_controls(self) -> str:
        return "world_region_regression_with_urbanization_controls"

    def world_urb_pop_share_cities_above_1m(self) -> str:
        return "world_urb_pop_share_cities_above_1m"

    def world_urb_pop_share_cities_above_1m_historical(self) -> str:
        return "world_urb_pop_share_cities_above_1m_historical"

    def world_urb_pop_share_cities_above_1m_projections(self) -> str:
        return "world_urb_pop_share_cities_above_1m_projections"

    def world_tot_pop_share_cities_above_1m_projections_extr(self) -> str:
        return "world_tot_pop_share_cities_above_1m_projections_extr"

    def world_tot_pop_share_cities_above_1m_projections_prop_growth(self) -> str:
        return "world_tot_pop_share_cities_above_1m_projections_prop_growth"

    def world_tot_pop_share_cities_above_1m_projections_inc_returns(self) -> str:
        return "world_tot_pop_share_cities_above_1m_projections_inc_returns"

    def world_tot_pop_share_cities_above_1m(self) -> str:
        return "world_tot_pop_share_cities_above_1m"



@dataclass(frozen=True)
class WorldSupplementaryInformation:
    def world_linearity_test_size_vs_growth(self) -> str:
        return "world_linearity_test_size_vs_growth"
    
    def world_linearity_test_rank_vs_size(self) -> str:
        return "world_linearity_test_rank_vs_size"

    def world_rank_size_slopes_ols_decade_change(self) -> str:
        return "world_rank_size_slopes_ols_decade_change"
    
    def world_size_growth_slopes_historical_ols(self) -> str:
        return "world_size_growth_slopes_historical_ols"

    def world_rank_size_slopes_historical_ols(self) -> str:
        return "world_rank_size_slopes_historical_ols"

    def world_urb_pop_share_cities_above_1m_projections_ols(self) -> str:
        return "world_urb_pop_share_cities_above_1m_projections_ols"

    def world_urban_population(self) -> str:
        return "world_urban_population"

    def world_m49_size_growth_slopes(self) -> str:
        return "world_m49_size_growth_slopes"

    def world_m49_urbanization(self) -> str:
        return "world_m49_urbanization"

    def world_m49_size_growth_slopes_urbanization(self) -> str:
        return "world_m49_size_growth_slopes_urbanization"

    def world_avg_size_growth_slope_m49_borders(self) -> str:
        return "world_avg_size_growth_slope_m49_borders"

    def world_suburbanization_augmented_cluster_metrics(self) -> str:
        return "world_suburbanization_augmented_cluster_metrics"

    def world_suburbanization_augmented_neighbors(self) -> str:
        return "world_suburbanization_augmented_neighbors"

    def world_suburbanization_augmented_pull(self) -> str:
        return "world_suburbanization_augmented_pull"

    def world_suburbanization_augmented_mapping(self) -> str:
        return "world_suburbanization_augmented_mapping"

    def world_suburbanization_augmented_population(self) -> str:
        return "world_suburbanization_augmented_population"

    def world_suburbanization_augmented_size_growth_slopes(self) -> str:
        return "world_suburbanization_augmented_size_growth_slopes"

@dataclass(frozen=True)
class World:
    sources: WorldSources = WorldSources()
    transformations: WorldTransformations = WorldTransformations()
    figures: WorldFigures = WorldFigures()
    si: WorldSupplementaryInformation = WorldSupplementaryInformation()


class Other:
    def analysis_parameters(self) -> str:
        return "analysis_parameters"


@dataclass(frozen=True)
class TableNames:
    usa: USA = USA()
    world: World = World()
    other: Other = Other()
