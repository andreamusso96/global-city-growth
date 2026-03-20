from re import A
from dagster import Definitions, in_process_executor, AssetSelection, define_asset_job

from .defs.assets.dbt import dbt_warehouse
from .defs.resources.resources import duckdb_resource, storage_resource, postgres_resource, dbt_resource, table_names_resource, pipes_subprocess_resource, postgres_pandas_io_manager, ipums_api_client
from .defs.assets.transformation.download import raw_data_zenodo, ipums_usa_full_count_downloaded, nhgis_place_population_1990_2020_downloaded, nhgis_place_geom_1900_2010_downloaded, nhgis_county_population_1990_2020_downloaded, nhgis_county_geom_2010_downloaded, nhgis_cbsa_geom_2010_2020_downloaded
from .defs.assets.transformation.ipums_full_count import ipums_full_count_raw, crosswalk_hist_id_to_hist_census_place_raw, ipums_full_count_clean, crosswalk_hist_id_to_hist_census_place_clean, ipums_full_count_census_place_id, ipums_full_count_census_place_id_all_years, census_place_population, ipums_full_count_individual_migration, census_place_migration
from .defs.assets.transformation.usa_sources import usa_hist_census_place_population, usa_hist_census_place_migration, usa_nhgis_census_place_population_1990_2020_raw, usa_nhgis_census_place_geom_all_years_raw, usa_hist_census_place_geom_raw, usa_states_geom_raw, usa_cbsa_geom_2010_raw, usa_cbsa_geom_2020_raw, usa_county_geom_2010_raw, usa_county_population_raw
from .defs.assets.transformation.usa import usa_crosswalk_nhgis_census_place_to_connected_component, usa_raster_census_place_convolved_year, usa_raster_census_place_convolved_all_years, usa_crosswalk_component_id_to_cluster_id, usa_crosswalk_component_id_to_cbsa_id
from .defs.assets.transformation.world_sources import world_raster_ghsl_pop, world_raster_ghsl_smod, world_country_borders_raw, world_urbanization_raw, world_crosswalk_cshapes_code_to_iso_code, world_raster_ghsl_pop_all_years, world_raster_ghsl_smod_all_years, world_country_region, world_population_raw
from .defs.assets.transformation.world import world_crosswalk_component_id_to_cluster_id
from .defs.assets.analysis.world_analysis import world_size_growth_slopes_historical, world_rank_size_slopes_historical, world_region_regression_with_urbanization_controls, world_size_growth_slopes_projections
from .defs.assets.analysis.usa_analysis import usa_rank_size_slopes
from .defs.assets.analysis.si.si_analysis import world_size_growth_slopes_historical_ols, world_rank_size_slopes_historical_ols, world_linearity_test_rank_vs_size, world_linearity_test_size_vs_growth
from .defs.assets.figures.figure_1 import figure_1_map, figure_1_plots
from .defs.assets.figures.figure_2 import figure_2
from .defs.assets.figures.figure_3 import figure_3
from .defs.assets.figures.tables import table_1, table_2
from .defs.assets.figures.si.si_projections import si_figure_projection_vs_historical_urb_pop_share_cities_above_1m
from .defs.assets.figures.si.si_linear_rigidity import si_figure_linear_rigidity
from .defs.assets.figures.si.si_robustness import si_figure_usa_robustness, si_tables_world_robustness
from .defs.assets.figures.si.si_linear_vs_spline import si_figure_linear_vs_spline
from .defs.assets.figures.si.si_country_borders import si_figure_country_borders
from .defs.assets.figures.si.si_suburbanization import si_figure_suburbanization_usa

all_job = define_asset_job("0_all_job", selection=AssetSelection.all(), description="Run the full pipeline.")

download_job = define_asset_job("1_download_job", 
                                selection=AssetSelection.groups("download"),
                                description="Download the raw data from the sources. Should run first.")
ipums_full_count_job = define_asset_job("2_ipums_full_count_job", 
                                        selection=AssetSelection.groups("ipums_full_count_staging", "ipums_full_count_intermediate", "ipums_full_count_final"),
                                        description="Extract census place population estimates from the IPUMS USA Full Count data. Should run second.")
usa_job = define_asset_job("3_usa_job", 
                           selection=AssetSelection.groups("usa_raw", "usa_staging", "usa_intermediate_normalize_hist_data", "usa_intermediate_rasterize_census_places", "usa_intermediate_create_clusters", "usa_intermediate_create_cbsa_clusters"),
                           description="Clean the census place population estimates, create US population rasters, and extract clusters. Should run third.")


world_job = define_asset_job("4_world_job", 
                             selection=AssetSelection.groups("world_raw", "world_staging", "world_intermediate"),
                             description="Create clusters from the GHSL grids. Should run fourth.")

analysis_job = define_asset_job("5_analysis_job", 
                                selection=AssetSelection.groups("analysis", "usa_analysis", "world_analysis", 
                                "world_analysis_size_vs_growth", "world_analysis_rank_vs_size","world_analysis_share_cities_above_1m", "si_analysis"),
                                description="Analyze the data for the figures and supplementary information. Should run fifth.")

figures_job = define_asset_job("6_figures_job", 
                               selection=AssetSelection.groups("paper_stats", "figures", "si_figures"),
                               description="Create the figures, tables, paper stats, and supplementary information. Should run sixth.")

defs = Definitions(
    assets=[
        # Download
        raw_data_zenodo,
        ipums_usa_full_count_downloaded,
        nhgis_place_population_1990_2020_downloaded,
        nhgis_place_geom_1900_2010_downloaded,
        nhgis_county_population_1990_2020_downloaded,
        nhgis_county_geom_2010_downloaded,
        nhgis_cbsa_geom_2010_2020_downloaded,

        # USA
        ## IPUMS Full Count
        ipums_full_count_raw,
        crosswalk_hist_id_to_hist_census_place_raw,
        ipums_full_count_clean,
        crosswalk_hist_id_to_hist_census_place_clean,
        ipums_full_count_census_place_id,
        ipums_full_count_census_place_id_all_years,
        ipums_full_count_individual_migration,
        census_place_population,
        census_place_migration,

        ## USA Raw
        usa_hist_census_place_population,
        usa_hist_census_place_migration,
        usa_nhgis_census_place_population_1990_2020_raw,
        usa_nhgis_census_place_geom_all_years_raw,
        usa_hist_census_place_geom_raw,
        usa_states_geom_raw,
        usa_cbsa_geom_2010_raw,
        usa_cbsa_geom_2020_raw,
        usa_county_geom_2010_raw,
        usa_county_population_raw,

        ## USA Intermediate
        usa_crosswalk_nhgis_census_place_to_connected_component,
        usa_raster_census_place_convolved_year,
        usa_raster_census_place_convolved_all_years,
        usa_crosswalk_component_id_to_cluster_id,
        usa_crosswalk_component_id_to_cbsa_id,
        
        # World
        ## World Raw
        world_raster_ghsl_pop,
        world_raster_ghsl_smod,
        world_raster_ghsl_pop_all_years,
        world_raster_ghsl_smod_all_years,
        world_country_borders_raw,
        world_urbanization_raw,
        world_crosswalk_cshapes_code_to_iso_code,
        world_country_region,
        world_population_raw,

        ## World Intermediate
        world_crosswalk_component_id_to_cluster_id,
        
        # Shared
        ## DBT
        dbt_warehouse,

        ## Figure Data Prep
        world_size_growth_slopes_historical,
        world_rank_size_slopes_historical,
        world_region_regression_with_urbanization_controls,
        world_size_growth_slopes_projections,
        usa_rank_size_slopes,

        ## Figures
        figure_1_map,
        figure_1_plots,
        figure_2,
        figure_3,
        table_1,
        table_2,

        ## SI Figure Data Prep
        world_size_growth_slopes_historical_ols,
        world_rank_size_slopes_historical_ols,
        world_linearity_test_rank_vs_size,
        world_linearity_test_size_vs_growth,

        ## Supplementary Information
        si_figure_linear_vs_spline,
        si_figure_projection_vs_historical_urb_pop_share_cities_above_1m,
        si_figure_linear_rigidity,
        si_figure_country_borders,
        si_figure_suburbanization_usa,
        si_figure_usa_robustness,
        si_tables_world_robustness
    ],
    resources={
        "duckdb": duckdb_resource,
        "storage": storage_resource,
        "postgres": postgres_resource,
        "dbt": dbt_resource,
        "tables": table_names_resource,
        "bash": pipes_subprocess_resource,
        "postgres_io_manager": postgres_pandas_io_manager,
        "ipums_api_client": ipums_api_client
    },
    jobs=[all_job, download_job, ipums_full_count_job, usa_job, world_job, analysis_job, figures_job],
    executor=in_process_executor
)
