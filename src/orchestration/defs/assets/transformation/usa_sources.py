import dagster as dg
from dagster_duckdb import DuckDBResource
from dataclasses import dataclass
import pandas as pd
from sqlalchemy import text
import geopandas as gpd
from pathlib import Path

from ...resources.resources import PostgresResource, StorageResource, TableNamesResource
from .ipums_full_count import census_place_migration as census_place_migration_duckdb, census_place_population as census_place_population_duckdb
from .download import nhgis_place_population_1990_2020_downloaded, raw_data_zenodo, nhgis_place_geom_1900_2010_downloaded, nhgis_cbsa_geom_2010_2020_downloaded, nhgis_county_geom_2010_downloaded, nhgis_county_population_1990_2020_downloaded



def _copy_table_from_duckdb_to_postgres(table_duckdb: str, table_postgres: str, duckdb: DuckDBResource, postgres: PostgresResource):
    # Drop table for idempotency
    engine = postgres.get_engine()
    with engine.begin() as conn:
        query = f'DROP TABLE IF EXISTS {table_postgres}'
        conn.execute(text(query))

    # Copy table from duckdb to postgres
    with duckdb.get_connection() as con:
        con.sql(f"""
        ATTACH '{postgres.connection_string}' AS postgres_db (TYPE POSTGRES);
        CREATE TABLE postgres_db.{table_postgres} AS SELECT * FROM {table_duckdb};
        DETACH postgres_db;
        """)


def _read_nhgis_time_series_csv(path: Path) -> pd.DataFrame:
    """Read NHGIS time-series CSVs, dropping the descriptive second header row when present."""
    with path.open("r", encoding="latin1", errors="ignore") as f:
        f.readline()
        second_line = f.readline().strip()

    skiprows = [1] if second_line.startswith("GIS Join Match Code") else None
    return pd.read_csv(path, encoding="latin1", skiprows=skiprows)

@dg.asset(
    deps=[census_place_population_duckdb],
    kinds={'postgres'},
    group_name="usa_raw",
    pool="duckdb_write"
)
def usa_hist_census_place_population(context: dg.AssetExecutionContext, duckdb: DuckDBResource, postgres: PostgresResource, tables: TableNamesResource):
    """Population of historical census places from the Census Place Project. Same as the census_place_population asset in the ipums_full_count defs."""
    context.log.info(f"Copying census place population from duckdb to postgres")
    _copy_table_from_duckdb_to_postgres(
        table_duckdb=tables.names.usa.ipums_full_count.census_place_population(),
        table_postgres=tables.names.usa.sources.usa_hist_census_place_population(),
        duckdb=duckdb,
        postgres=postgres
    )


@dg.asset(
    deps=[census_place_migration_duckdb],
    kinds={'postgres'},
    group_name="usa_raw",
    pool="duckdb_write"
)
def usa_hist_census_place_migration(context: dg.AssetExecutionContext, duckdb: DuckDBResource, postgres: PostgresResource, tables: TableNamesResource):
    """Migration between historical census places from the Census Place Project. Same as the census_place_migration asset in the ipums_full_count defs."""

    context.log.info(f"Copying census place migration from duckdb to postgres")
    _copy_table_from_duckdb_to_postgres(
        table_duckdb=tables.names.usa.ipums_full_count.census_place_migration(),
        table_postgres=tables.names.usa.sources.usa_hist_census_place_migration(),
        duckdb=duckdb,
        postgres=postgres
    )


@dg.asset(
    deps=[nhgis_place_population_1990_2020_downloaded],
    kinds={'postgres'},
    group_name="usa_raw",
    io_manager_key="postgres_io_manager",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="GISJOIN", type="STR", description="Standard identifiers to join with boundary files"),
            dg.TableColumn(name="DATAYEAR", type="INT", description="Year of the data"),
            dg.TableColumn(name="CL8AA", type="INT", description="Population of the census place"),
        ])
    }
)
def usa_nhgis_census_place_population_1990_2020_raw(context: dg.AssetExecutionContext, storage: StorageResource):
    """Population of NHGIS census places between 1990 and 2020"""

    context.log.info(f"Copying census place population from nhgis to postgres")
    data = _read_nhgis_time_series_csv(storage.paths.usa.nhgis.census_place_pop_1990_2020())
    return data

@dg.asset(
    deps=[nhgis_place_geom_1900_2010_downloaded],
    kinds={'postgres'},
    group_name="usa_raw",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="NHGISPLACE", type="INT", description="The NHGIS ID of the census place"),
            dg.TableColumn(name="PLACE", type="STR", description="The name of the census place"),
            dg.TableColumn(name="geometry", type="GEOGRAPHY", description="The geometry of the census place (an point)"),
        ])
    }
)
def usa_nhgis_census_place_geom_all_years_raw(context: dg.AssetExecutionContext, postgres: PostgresResource, storage: StorageResource, tables: TableNamesResource):
    """Geometry of NHGIS census places between 1900 and 2010"""

    context.log.info(f"Copying census place geom from nhgis to postgres for year")
    census_place_geom_gdf_list = []
    for year in range(1900, 2011, 10):
        census_place_geom_gdf = gpd.read_file(storage.paths.usa.nhgis.census_place_geom(year=year))
        census_place_geom_gdf_list.append(census_place_geom_gdf)

    census_place_geom_gdf = pd.concat(census_place_geom_gdf_list)
    census_place_geom_gdf = gpd.GeoDataFrame(census_place_geom_gdf, geometry='geometry')
    census_place_geom_gdf.to_postgis(
        name=tables.names.usa.sources.usa_nhgis_census_place_geom_all_years_raw(),
        con=postgres.get_engine(),
        if_exists="replace",
        index=False,
        schema="public"
    )


@dg.asset(
    deps=[raw_data_zenodo],
    kinds={'postgres'},
    group_name="usa_raw",
    io_manager_key="postgres_io_manager",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="id", type="INT", description="see crosswalk_hist_id_to_hist_census_place_raw"),
            dg.TableColumn(name="lon", type="FLOAT", description="The longitude of the census place"),
            dg.TableColumn(name="lat", type="FLOAT", description="The latitude of the census place"),
        ])
    }
)
def usa_hist_census_place_geom_raw(context: dg.AssetExecutionContext, storage: StorageResource):
    """Geometry of historical census places from the Census Place Project"""
    context.log.info(f"Copying census place geom from hist to postgres")
    census_place_geom_df = pd.read_csv(storage.paths.usa.census_place_project.hist_census_place_geom())
    return census_place_geom_df


@dg.asset(
    deps=[raw_data_zenodo],
    kinds={'postgres'},
    group_name="usa_raw",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="State_Code", type="STR", description="The two letter code of the state (e.g. CA, NY)"),
            dg.TableColumn(name="State_Name", type="STR", description="The name of the state (e.g. California, New York)"),
            dg.TableColumn(name="geometry", type="GEOGRAPHY", description="The geometry of the state"),
        ])
    }
)
def usa_states_geom_raw(context: dg.AssetExecutionContext, postgres: PostgresResource, storage: StorageResource, tables: TableNamesResource):
    """Geometry of the states of the USA"""
    context.log.info(f"Copying states geom from hist to postgres")
    states_geom_gdf = gpd.read_file(storage.paths.usa.misc.usa_state_geom())
    states_geom_gdf.to_postgis(
        name=tables.names.usa.sources.usa_states_geom_raw(),
        con=postgres.get_engine(),
        if_exists="replace",
        index=False,
        schema="public"
    )


@dg.asset(
    deps=[nhgis_cbsa_geom_2010_2020_downloaded],
    kinds={'postgres'},
    group_name="usa_raw",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="CBSAFP10", type="STR", description="The CBSA identifier in the 2010 source shapefile"),
            dg.TableColumn(name="NAME10", type="STR", description="The CBSA name in the 2010 source shapefile"),
            dg.TableColumn(name="geometry", type="GEOGRAPHY", description="The geometry of the CBSA polygon"),
        ])
    }
)
def usa_cbsa_geom_2010_raw(context: dg.AssetExecutionContext, postgres: PostgresResource, storage: StorageResource, tables: TableNamesResource):
    """Raw 2010 CBSA geometry."""
    cbsa_geom_gdf = gpd.read_file(storage.paths.usa.suburbanization.cbsa_geom(year=2010))
    cbsa_geom_gdf.to_postgis(
        name=tables.names.usa.sources.usa_cbsa_geom_2010_raw(),
        con=postgres.get_engine(),
        if_exists="replace",
        index=False,
        schema="public"
    )


@dg.asset(
    deps=[nhgis_cbsa_geom_2010_2020_downloaded],
    kinds={'postgres'},
    group_name="usa_raw",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="CBSAFP", type="STR", description="The CBSA identifier in the 2020 source shapefile"),
            dg.TableColumn(name="NAME", type="STR", description="The CBSA name in the 2020 source shapefile"),
            dg.TableColumn(name="geometry", type="GEOGRAPHY", description="The geometry of the CBSA polygon"),
        ])
    }
)
def usa_cbsa_geom_2020_raw(context: dg.AssetExecutionContext, postgres: PostgresResource, storage: StorageResource, tables: TableNamesResource):
    """Raw 2020 CBSA geometry."""
    cbsa_geom_gdf = gpd.read_file(storage.paths.usa.suburbanization.cbsa_geom(year=2020))
    cbsa_geom_gdf.to_postgis(
        name=tables.names.usa.sources.usa_cbsa_geom_2020_raw(),
        con=postgres.get_engine(),
        if_exists="replace",
        index=False,
        schema="public"
    )


@dg.asset(
    deps=[nhgis_county_geom_2010_downloaded],
    kinds={'postgres'},
    group_name="usa_raw",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="GISJOIN", type="STR", description="County identifier in the NHGIS boundary file"),
            dg.TableColumn(name="GEOID10", type="STR", description="2010 county GEOID"),
            dg.TableColumn(name="STATEFP10", type="STR", description="2010 county state FIPS"),
            dg.TableColumn(name="geometry", type="GEOGRAPHY", description="The geometry of the county polygon"),
        ])
    }
)
def usa_county_geom_2010_raw(context: dg.AssetExecutionContext, postgres: PostgresResource, storage: StorageResource, tables: TableNamesResource):
    """Raw 2010 county geometry."""
    county_geom_gdf = gpd.read_file(storage.paths.usa.suburbanization.county_geom_2010())
    county_geom_gdf.to_postgis(
        name=tables.names.usa.sources.usa_county_geom_2010_raw(),
        con=postgres.get_engine(),
        if_exists="replace",
        index=False,
        schema="public"
    )


@dg.asset(
    deps=[nhgis_county_population_1990_2020_downloaded],
    kinds={'postgres'},
    group_name="usa_raw",
    io_manager_key="postgres_io_manager",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="GISJOIN", type="STR", description="County identifier used to join to the county boundary file"),
            dg.TableColumn(name="DATAYEAR", type="INT", description="Year of the county population observation"),
            dg.TableColumn(name="CL8AA", type="FLOAT", description="County population"),
        ])
    }
)
def usa_county_population_raw(context: dg.AssetExecutionContext, storage: StorageResource):
    """Raw county population time series."""
    return _read_nhgis_time_series_csv(storage.paths.usa.suburbanization.county_population())
