import dagster as dg
import os
from numpy import tile
import pandas as pd
import geopandas as gpd

from ...resources.resources import PostgresResource, StorageResource, TableNamesResource
from ..constants import constants
from ....utils import union_year_raster_tables_into_single_table

from .download import raw_data_zenodo


def _load_ghsl_raster(context: dg.AssetExecutionContext, bash: dg.PipesSubprocessClient, raster_path: str, table_name: str):
    """
    Runs a bash script to load a raster into a table in postgis.

    Parameters:
    - context: dagster asset execution context
    - bash: dagster pipes subprocess client
    - raster_path: path to the raster file
    - table_name: name of the table to load the raster into
    """
    context.log.info(f"Loading {raster_path} into {table_name}")
    schema_name = 'public'
    args = [
        raster_path,
        schema_name,
        table_name
    ]

    current_dir = os.path.dirname(os.path.abspath(__file__))
    bash_script_path = f'{current_dir}/load_ghsl_raster.sh'
    cmd = ["bash", bash_script_path] + args
    bash.run(command=cmd, context=context).get_results()

@dg.asset(
    deps=[raw_data_zenodo],
    kinds={'postgres'},
    group_name="world_raw",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="rid", type="int", description="The id of the raster tile"),
            dg.TableColumn(name="rast", type="raster", description="The raster tiles in CRS = Mollweide (EPSG:54009)"),
        ])
    }
)
def world_raster_ghsl_pop(context: dg.AssetExecutionContext, storage: StorageResource, tables: TableNamesResource, bash: dg.PipesSubprocessClient):
    """ The population raster from GHSL"""
    for year in constants['GHSL_RASTER_YEARS']:
        context.log.info(f"Copying GHSL pop raster for year {year}")
        ghsl_pop_raster_path = storage.paths.world.ghsl.pop(year=year)
        table_name = tables.names.world.sources.world_raster_ghsl_pop(year=year)
        _load_ghsl_raster(context=context, bash=bash, raster_path=ghsl_pop_raster_path, table_name=table_name)

    return dg.Output(value=None, metadata={"years": constants['GHSL_RASTER_YEARS']})

@dg.asset(
    deps=[raw_data_zenodo],
    kinds={'postgres'},
    group_name="world_raw",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="rid", type="int", description="The id of the raster tile"),
            dg.TableColumn(name="rast", type="raster", description="The raster tiles in CRS = Mollweide (EPSG:54009)"),
        ])
    }
)
def world_raster_ghsl_smod(context: dg.AssetExecutionContext, storage: StorageResource, tables: TableNamesResource, bash: dg.PipesSubprocessClient):
    """ The degree of urbanization raster from the GHSL"""
    for year in constants['GHSL_RASTER_YEARS']:
        context.log.info(f"Copying GHSL smod raster for year {year}")
        ghsl_smod_raster_path = storage.paths.world.ghsl.smod(year=year)
        table_name = tables.names.world.sources.world_raster_ghsl_smod(year=year)
        _load_ghsl_raster(context=context, bash=bash, raster_path=ghsl_smod_raster_path, table_name=table_name)
    
    return dg.Output(value=None, metadata={"years": constants['GHSL_RASTER_YEARS']})


@dg.asset(
    deps=[world_raster_ghsl_pop],
    kinds={'postgres'},
    group_name="world_raw",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="rast", type="raster", description="The raster tiles in CRS = Mollweide (EPSG:54009)"),
            dg.TableColumn(name="year", type="int", description="The year of the raster"),
        ])
    }
)
def world_raster_ghsl_pop_all_years(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource):
    """The union of all population raster tables into one table"""
    context.log.info(f"Copying all GHSL pop rasters for all years in a single table")
    union_year_raster_tables_into_single_table(
        union_table_name=tables.names.world.sources.world_raster_ghsl_pop_all_years(),
        get_year_raster_table_name=tables.names.world.sources.world_raster_ghsl_pop,
        years=constants['GHSL_RASTER_YEARS'],
        context=context,
        postgres=postgres
    )
    
@dg.asset(
    deps=[world_raster_ghsl_smod],
    kinds={'postgres'},
    group_name="world_raw",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="rast", type="raster", description="The raster tiles in CRS = Mollweide (EPSG:54009)"),
            dg.TableColumn(name="year", type="int", description="The year of the raster"),
        ])
    }
)
def world_raster_ghsl_smod_all_years(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource):
    """The union of all degree of urbanization raster tables into one table"""
    context.log.info(f"Copying all GHSL smod rasters for all years in a single table")
    union_year_raster_tables_into_single_table(
        union_table_name=tables.names.world.sources.world_raster_ghsl_smod_all_years(),
        get_year_raster_table_name=tables.names.world.sources.world_raster_ghsl_smod,
        years=constants['GHSL_RASTER_YEARS'],
        context=context,
        postgres=postgres
    )

@dg.asset(
    deps=[raw_data_zenodo],
    kinds={'postgres'},
    group_name="world_raw",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="cntry_name", type="string", description="The name of the country"),
            dg.TableColumn(name="gwcode", type="int", description="The country code from CShapes"),
            dg.TableColumn(name="gwsyear", type="int", description="The start year of the country borders"),
            dg.TableColumn(name="gwsmonth", type="int", description="The start month of the country borders"),
            dg.TableColumn(name="gwsday", type="int", description="The start day of the country borders"),
            dg.TableColumn(name="gweyear", type="int", description="The end year of the country borders"),
            dg.TableColumn(name="gwemonth", type="int", description="The end month of the country borders"),
            dg.TableColumn(name="gweday", type="int", description="The end day of the country borders"),
            dg.TableColumn(name="geometry", type="geometry", description="The geometry of the country in CRS = EPSG:4326"),
        ])
    }
)
def world_country_borders_raw(context: dg.AssetExecutionContext, postgres: PostgresResource, storage: StorageResource, tables: TableNamesResource):
    """The country borders from CShapes"""
    country_borders_path = storage.paths.world.cshapes_border.country_borders()
    context.log.info(f"Copying country borders from {country_borders_path}")

    country_borders_df = gpd.read_file(country_borders_path, engine='pyogrio')
    country_borders_df.to_postgis(
        name=tables.names.world.sources.world_country_borders_raw(),
        con=postgres.get_engine(),
        schema='public',
        index=False,
        if_exists='replace'
    )

@dg.asset(
    deps=[raw_data_zenodo],
    kinds={'postgres'},
    group_name="world_raw",
    io_manager_key="postgres_io_manager",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="cshapes_code", type="int", description="The country code from CShapes"),
            dg.TableColumn(name="world_bank_code", type="string", description="The ISO code from the world bank"),
        ])
    }
)
def world_crosswalk_cshapes_code_to_iso_code(context: dg.AssetExecutionContext, storage: StorageResource):
    """A crosswalk between the country code from CShapes and the ISO code from the world bank"""
    crosswalk_path = storage.paths.world.cshapes_border.crosswalk_cshapes_code_to_iso_code()
    context.log.info(f"Copying crosswalk cshapes code to iso code from {crosswalk_path}")
    crosswalk_df = pd.read_csv(crosswalk_path)
    return crosswalk_df


@dg.asset(
    deps=[raw_data_zenodo],
    kinds={'postgres'},
    group_name="world_raw",
    io_manager_key="postgres_io_manager",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="country", type="string", description="The world bank code of the country"),
            dg.TableColumn(name="year", type="string"),
            dg.TableColumn(name="urban_population_share", type="float", description="The urban population share of the country"),
        ])
    }
)
def world_urbanization_raw(context: dg.AssetExecutionContext, storage: StorageResource):
    """Urbanization data with future projections from https://doi.org/10.6084/m9.figshare.c.5521821"""
    urbanization_path = storage.paths.world.misc.urbanization()
    context.log.info(f"Copying urbanization from {urbanization_path}")
    urbanization_df = pd.read_csv(urbanization_path)
    return urbanization_df


@dg.asset(
    deps=[raw_data_zenodo],
    kinds={'postgres'},
    group_name="world_raw",
    io_manager_key="postgres_io_manager",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="Entity", type="string", description="The entity name from the HYDE dataset"),
            dg.TableColumn(name="Code", type="string", description="The ISO-style country code from the HYDE dataset"),
            dg.TableColumn(name="Year", type="int", description="The observation year"),
            dg.TableColumn(name="Population share in urban areas", type="float", description="The urban population share reported as a percentage in HYDE"),
        ])
    }
)
def world_hyde_urbanization_raw(context: dg.AssetExecutionContext, storage: StorageResource):
    """Urbanization data from HYDE."""
    hyde_urbanization_path = storage.paths.world.misc.hyde_urbanization()
    context.log.info(f"Copying HYDE urbanization from {hyde_urbanization_path}")
    hyde_urbanization_df = pd.read_csv(hyde_urbanization_path)
    return hyde_urbanization_df



@dg.asset(
    deps=[raw_data_zenodo],
    kinds={'postgres'},
    group_name="world_raw",
    io_manager_key="postgres_io_manager",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="country", type="string", description="The world bank code of the country"),
            dg.TableColumn(name="region1", type="string", description="The first region of the country (large)"),
            dg.TableColumn(name="region2", type="string", description="The second region of the country (small). This second region is the one used throughout the analysis."),
        ])
    }
)
def world_country_region(context: dg.AssetExecutionContext, storage: StorageResource):
    """Countries with region1 and region2 manually assigned"""
    countries_with_regions_path = storage.paths.world.misc.countries_with_regions()
    context.log.info(f"Copying countries with region and subregion from {countries_with_regions_path}")
    countries_with_regions_df = pd.read_csv(countries_with_regions_path)
    return countries_with_regions_df


@dg.asset(
    deps=[raw_data_zenodo],
    kinds={'postgres'},
    group_name="world_raw",
    io_manager_key="postgres_io_manager",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="Sub-region Name", type="string", description="The UN M49 sub-region name"),
            dg.TableColumn(name="ISO-alpha3 Code", type="string", description="The ISO alpha-3 country code"),
        ])
    }
)
def world_m49_region_raw(context: dg.AssetExecutionContext, storage: StorageResource):
    """UN M49 sub-regions by country"""
    micro_regions_path = storage.paths.world.misc.micro_regions()
    context.log.info(f"Copying UN M49 sub-regions from {micro_regions_path}")
    micro_regions_df = pd.read_csv(micro_regions_path, sep=';')
    return micro_regions_df



@dg.asset(
    deps=[raw_data_zenodo],
    kinds={'postgres'},
    group_name="world_raw",
    io_manager_key="postgres_io_manager",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="Code", type="string", description="The world bank code of the country"),
            dg.TableColumn(name="Year", type="string", description="The year of the population"),
            dg.TableColumn(name="Population (historical)", type="float", description="The historical population of the country before 2024 (excl. 2024)"),
            dg.TableColumn(name="Population (projections)", type="float", description="The projected population of the country after 2024 (incl. 2024)"),
        ])
    }
)
def world_population_raw(context: dg.AssetExecutionContext, storage: StorageResource):
    """Population data with future projections from Our World in Data"""
    population_path = storage.paths.world.misc.population()
    context.log.info(f"Copying population from {population_path}")
    population_df = pd.read_csv(population_path)
    return population_df
