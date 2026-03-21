import dagster as dg
import pandas as pd

from ....resources.resources import PostgresResource, TableNamesResource
from ...constants import constants
from ..figure_io import materialize_table, read_pandas, save_latex_table
from ..tables import make_table_2

MAIN_ANALYSIS_ID = constants["MAIN_ANALYSIS_ID"]


def _build_augmented_suburbanization_table_data(
    postgres: PostgresResource,
    tables: TableNamesResource,
) -> pd.DataFrame:
    q = f"""
    WITH country_year_counts AS (
        SELECT  country,
                year,
                COUNT(*) AS n_clusters
        FROM {tables.names.world.si.world_suburbanization_augmented_population()}
        GROUP BY country, year
    ),
    eligible_countries AS (
        SELECT country
        FROM country_year_counts
        GROUP BY country
        HAVING MIN(n_clusters) > 50
    ),
    base_slopes AS (
        SELECT  country,
                year,
                size_growth_slope,
                urban_population_share
        FROM {tables.names.world.figures.world_size_growth_slopes_historical_urbanization()}
        WHERE analysis_id = {MAIN_ANALYSIS_ID}
    ),
    augmented_slopes AS (
        SELECT  country,
                year,
                size_growth_slope AS size_growth_slope_aug
        FROM {tables.names.world.si.world_suburbanization_augmented_size_growth_slopes()}
        WHERE analysis_id = {MAIN_ANALYSIS_ID}
        AND country IN (SELECT country FROM eligible_countries)
    ),
    combined AS (
        SELECT  b.country,
                b.year,
                CASE
                    WHEN g.urban_population_share_group = '60-100' THEN a.size_growth_slope_aug
                    ELSE b.size_growth_slope
                END AS size_growth_slope,
                b.urban_population_share
        FROM base_slopes b
        LEFT JOIN augmented_slopes a
        USING (country, year)
        LEFT JOIN world_urbanization_groups g
        USING (country)
    )
    SELECT *
    FROM combined
    WHERE size_growth_slope IS NOT NULL
    """
    return pd.read_sql(q, con=postgres.get_engine())


@dg.asset(
    deps=[
        TableNamesResource().names.other.analysis_parameters(),
        TableNamesResource().names.world.figures.world_size_growth_slopes_historical_urbanization(),
    ],
    group_name="si_figures",
)
def si_tables_world_hyperparameter_robustness(
    context: dg.AssetExecutionContext,
    postgres: PostgresResource,
    tables: TableNamesResource,
) -> dg.MaterializeResult:
    context.log.info("Creating SI tables: world hyperparameter robustness")
    params = pd.read_sql(
        f"SELECT * FROM {tables.names.other.analysis_parameters()}",
        con=postgres.get_engine(),
    )
    analysis_ids = params[params["robustness_for_dataset"] == "world"]["analysis_id"].tolist()
    generated_files = []

    for analysis_id in analysis_ids:
        table_file_name = f"table_2_robustness_{analysis_id}.txt"
        world_size_growth_slopes_urbanization = read_pandas(
            engine=postgres.get_engine(),
            table=tables.names.world.figures.world_size_growth_slopes_historical_urbanization(),
            analysis_id=analysis_id,
        )
        latex_table = make_table_2(
            df_size_growth_slopes=world_size_growth_slopes_urbanization
        )
        save_latex_table(table=latex_table, table_file_name=table_file_name, si=True)
        generated_files.append(table_file_name)

    manifest_file_name = "table_2_robustness.txt"
    save_latex_table(table="\n".join(generated_files), table_file_name=manifest_file_name, si=True)
    return materialize_table(table_file_name=manifest_file_name)


@dg.asset(
    deps=[
        TableNamesResource().names.world.figures.world_size_growth_slopes_historical_urbanization(),
        TableNamesResource().names.world.si.world_suburbanization_augmented_population(),
        TableNamesResource().names.world.si.world_suburbanization_augmented_size_growth_slopes(),
    ],
    group_name="si_figures",
)
def si_tables_world_augmented_suburbanization(
    context: dg.AssetExecutionContext,
    postgres: PostgresResource,
    tables: TableNamesResource,
) -> dg.MaterializeResult:
    context.log.info("Creating SI table: world augmented suburbanization")
    table_file_name = "table_augmented_suburbanization.txt"
    df = _build_augmented_suburbanization_table_data(postgres=postgres, tables=tables)
    latex_table = make_table_2(df_size_growth_slopes=df)
    save_latex_table(table=latex_table, table_file_name=table_file_name, si=True)
    return materialize_table(table_file_name=table_file_name)


@dg.asset(
    deps=[TableNamesResource().names.world.si.world_size_growth_slopes_historical_hyde_urbanization()],
    group_name="si_figures",
)
def si_tables_world_hyde_urbanization(
    context: dg.AssetExecutionContext,
    postgres: PostgresResource,
    tables: TableNamesResource,
) -> dg.MaterializeResult:
    context.log.info("Creating SI table: world HYDE urbanization")
    table_file_name = "table_hyde_urbanization.txt"
    slopes_urbanization = read_pandas(
        engine=postgres.get_engine(),
        table=tables.names.world.si.world_size_growth_slopes_historical_hyde_urbanization(),
        analysis_id=MAIN_ANALYSIS_ID,
    )
    latex_table = make_table_2(df_size_growth_slopes=slopes_urbanization)
    save_latex_table(table=latex_table, table_file_name=table_file_name, si=True)
    return materialize_table(table_file_name=table_file_name)
