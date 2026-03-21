# src/orchestration/defs/assets/figures/figure_data_prep.py
import dagster as dg
import pandas as pd
from pygam import LinearGAM, s
import numpy as np
import statsmodels.formula.api as smf
from scipy.stats import norm

from ....resources.resources import PostgresResource, TableNamesResource
from ...constants import constants
from ...stats_utils import get_mean_derivative_penalized_b_spline, get_ols_slope
from ...constants import constants

MAIN_ANALYSIS_ID = constants['MAIN_ANALYSIS_ID']

@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_size_vs_growth()],
    kinds={'postgres'},
    group_name="si_analysis",
    io_manager_key="postgres_io_manager",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="analysis_id", type="string", description="see world_cluster_growth_population_country_analysis"),
            dg.TableColumn(name="country", type="string", description="see world_cluster_growth_geocoding"),
            dg.TableColumn(name="year", type="int", description="The start year of the decade"),
            dg.TableColumn(name="size_growth_slope", type="float", description="The coefficient of and OLS regression of log-size vs log-growth"),
        ])
    } 
)
def world_size_growth_slopes_historical_ols(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource)  -> pd.DataFrame:
    """We take city log-size at the start of the decade against their log-growth over one decade for various countries and years. We then fit an OLS regression (instead of a penalized B-spline) and take the slope coefficient. """

    context.log.info("Calculating world size growth slopes")
    xaxis = 'log_population'
    yaxis = 'log_growth'

    world_size_vs_growth = pd.read_sql(f"SELECT * FROM {tables.names.world.figures.world_size_vs_growth()}", con=postgres.get_engine())
    slopes = world_size_vs_growth.groupby(['analysis_id', 'country', 'year']).apply(lambda x: get_ols_slope(df=x, xaxis=xaxis, yaxis=yaxis)).reset_index().rename(columns={0:'size_growth_slope'})
    return slopes


@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_rank_vs_size()],
    kinds={'postgres'},
    group_name="si_analysis",
    io_manager_key="postgres_io_manager",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="analysis_id", type="string", description="see world_cluster_growth_population_country_analysis"),
            dg.TableColumn(name="country", type="string", description="see world_cluster_growth_geocoding"),
            dg.TableColumn(name="year", type="int", description="The start year of the decade"),
            dg.TableColumn(name="rank_size_slope", type="float", description="The coefficient of and OLS regression of log-rank vs log-size"),
        ])
    }
)
def world_rank_size_slopes_historical_ols(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource)  -> pd.DataFrame:
    """We take the city log-rank and log-size in a given year for various countries and years. We then fit an OLS regression (instead of a penalized B-spline) and take the slope coefficient. """

    context.log.info("Calculating world rank size slopes")
    xaxis = 'log_rank'
    yaxis = 'log_population'

    world_rank_vs_size = pd.read_sql(f"SELECT * FROM {tables.names.world.figures.world_rank_vs_size()}", con=postgres.get_engine())
    slopes = world_rank_vs_size.groupby(['analysis_id', 'country', 'year']).apply(lambda x: get_ols_slope(df=x, xaxis=xaxis, yaxis=yaxis)).reset_index().rename(columns={0:'rank_size_slope'})
    slopes['rank_size_slope'] = slopes['rank_size_slope'].abs()
    return slopes


def _calculate_gaussian_loglik(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    n = len(y_true)
    rss = np.sum((y_true - y_pred)**2)
    sigma2_mle = rss / n
    log_likelihood = np.sum(norm.logpdf(y_true, loc=y_pred, scale=np.sqrt(sigma2_mle)))
    return log_likelihood

def _test_linearity_of_size_growth_curve(df: pd.DataFrame, x_axis: str, y_axis: str, n_boots: int, lam: float) -> bool:
    """ 
    We test the linearity of the log-size vs log-growth curve using a bootstrap test. This test is explained in the Supplementary Information of the paper.
    """

    df = df[[x_axis, y_axis]].copy()
    n = df.shape[0]
    lin = smf.ols(f'{y_axis} ~ {x_axis}', data=df).fit()

    X = df[[x_axis]].values
    y = df[y_axis].values
    spline = LinearGAM(s(0, n_splines=20), lam=[lam], fit_intercept=False).fit(X, y)

    loglik_lin = _calculate_gaussian_loglik(y_true=y, y_pred=lin.predict(df))
    loglik_spline = _calculate_gaussian_loglik(y_true=y, y_pred=spline.predict(X))
    D_true = 2 * (loglik_spline - loglik_lin)

    yhat0 = lin.params['Intercept'] + lin.params[x_axis] * df[x_axis]
    resid = df[y_axis] - yhat0

    def _weights(shape): return np.random.choice([-1, 1], size=shape)
    
    D_bootstraps = []
    for b in range(n_boots):
        y_star = yhat0 + _weights(shape=n) * resid
        df_b = df.copy()
        df_b['y_star'] = y_star

        lin_b = smf.ols(f'y_star ~ {x_axis}', data=df_b).fit()
        loglik_lin_b = _calculate_gaussian_loglik(y_true=y_star, y_pred=lin_b.predict(df_b))

        X_b = df_b[[x_axis]].values
        spline_b = LinearGAM(s(0, n_splines=20), lam=[lam], fit_intercept=False).fit(X_b, y_star)
        loglik_spline_b = _calculate_gaussian_loglik(y_true=y_star, y_pred=spline_b.predict(X_b))

        D_b = 2 * (loglik_spline_b - loglik_lin_b)
        D_bootstraps.append(D_b)

    boot_res = pd.DataFrame({
        'D_bootstrap': D_bootstraps
    })
    boot_res['D_true'] = D_true
    boot_res['reject_linear'] = boot_res['D_bootstrap'] > D_true
    return boot_res


@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_size_vs_growth()],
    group_name="si_analysis",
    io_manager_key="postgres_io_manager",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="analysis_id", type="string", description="see world_cluster_growth_population_country_analysis"),
            dg.TableColumn(name="country", type="string", description="see world_cluster_growth_geocoding"),
            dg.TableColumn(name="year", type="int", description="The start year of the decade"),
            dg.TableColumn(name="D_bootstrap", type="float", description="The bootstraped test statistic"),
            dg.TableColumn(name="D_true", type="float", description="The true test statistic"),
            dg.TableColumn(name="reject_linear", type="bool", description="Whether the null hypothesis of linearity is rejected using a threshold of 0.05"),
        ])
    }
    )
def world_linearity_test_size_vs_growth(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    """We test the linearity of the log-size vs log-growth curve using a bootstrap test. We do this only for the main analysis id because it takes too long to run for all analysis ids."""

    context.log.info("Running linearity test for size vs growth")
    df = pd.read_sql(f"SELECT * FROM {tables.names.world.figures.world_size_vs_growth()} WHERE analysis_id = {MAIN_ANALYSIS_ID}", con=postgres.get_engine())
    n_boots = 100
    lam = constants['PENALTY_SIZE_GROWTH_CURVE']
    test_results = df.groupby(['analysis_id', 'country', 'year']).apply(lambda x: _test_linearity_of_size_growth_curve(df=x, x_axis='log_population', y_axis='log_growth', n_boots=n_boots, lam=lam)).reset_index()
    test_results = test_results.rename(columns={'level_3': 'n_boot'})
    return test_results


def _measure_distortion_rank_size_curve(df: pd.DataFrame, x_axis: str, y_axis: str):
    df = df[[x_axis, y_axis]].copy()
    lin = smf.ols(f'{y_axis} ~ {x_axis}', data=df).fit()
    return np.max(np.abs(lin.resid))


@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_rank_vs_size()],
    group_name="si_analysis",
    io_manager_key="postgres_io_manager",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="analysis_id", type="string", description="see world_cluster_growth_population_country_analysis"),
            dg.TableColumn(name="country", type="string", description="see world_cluster_growth_geocoding"),
            dg.TableColumn(name="year", type="int", description="The start year of the decade"),
            dg.TableColumn(name="max_resid", type="float", description="The maximum residual from the line fit through an OLS regression"),
        ])
    }
)
def world_linearity_test_rank_vs_size(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    """We measure the distortion of the log-rank vs log-size by computing the maximum residual from the line fit through an OLS regression. Large residuals indicate non-linearity."""

    context.log.info("Measuring distortion for rank size curve")
    df = pd.read_sql(f"SELECT * FROM {tables.names.world.figures.world_rank_vs_size()}", con=postgres.get_engine())
    test_results = df.groupby(['analysis_id', 'country', 'year']).apply(lambda x: _measure_distortion_rank_size_curve(df=x, x_axis='log_rank', y_axis='log_population')).reset_index().rename(columns={0: 'max_resid'})
    return test_results


@dg.asset(
    deps=[
        TableNamesResource().names.world.figures.world_size_vs_growth(),
        dg.AssetKey("world_m49_region"),
    ],
    kinds={'postgres'},
    group_name="si_analysis",
    io_manager_key="postgres_io_manager",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="analysis_id", type="int", description="The analysis id, fixed to the main analysis"),
            dg.TableColumn(name="micro_region", type="string", description="The UN M49 sub-region name"),
            dg.TableColumn(name="year", type="int", description="The start year of the decade"),
            dg.TableColumn(name="size_growth_slope", type="float", description="The average spline derivative of the micro-region size-growth curve"),
            dg.TableColumn(name="n_cities", type="int", description="The number of cities pooled in the micro-region-year curve"),
        ])
    }
)
def world_m49_size_growth_slopes(
    context: dg.AssetExecutionContext,
    postgres: PostgresResource,
    tables: TableNamesResource,
) -> pd.DataFrame:
    """Size-growth slopes pooled at the UN M49 sub-region-year level for the main analysis."""

    context.log.info("Calculating world M49 size-growth slopes")
    xaxis = 'log_population'
    yaxis = 'log_growth'
    lam = constants['PENALTY_SIZE_GROWTH_CURVE']

    q = f"""
    SELECT  country,
            year,
            log_population,
            log_growth,
            analysis_id
    FROM {tables.names.world.figures.world_size_vs_growth()}
    WHERE analysis_id = {MAIN_ANALYSIS_ID}
    """
    world_size_vs_growth = pd.read_sql(q, con=postgres.get_engine())
    world_m49_region = pd.read_sql(
        "SELECT country, micro_region FROM world_m49_region",
        con=postgres.get_engine(),
    )

    df = world_size_vs_growth.merge(world_m49_region, on='country', how='inner')
    slopes = (
        df.groupby(['micro_region', 'year'], sort=True)
        .apply(
            lambda x: pd.Series(
                {
                    'size_growth_slope': get_mean_derivative_penalized_b_spline(
                        df=x,
                        xaxis=xaxis,
                        yaxis=yaxis,
                        lam=lam,
                    ),
                    'n_cities': x.shape[0],
                }
            )
        )
        .reset_index()
    )
    slopes['analysis_id'] = MAIN_ANALYSIS_ID
    return slopes[['analysis_id', 'micro_region', 'year', 'size_growth_slope', 'n_cities']]


@dg.asset(
    deps=[TableNamesResource().names.world.si.world_suburbanization_augmented_population()],
    kinds={'postgres'},
    group_name="si_analysis",
    io_manager_key="postgres_io_manager",
    metadata={
        "dagster/column_schema": dg.TableSchema([
            dg.TableColumn(name="analysis_id", type="int", description="The analysis id, fixed to the main analysis"),
            dg.TableColumn(name="country", type="string", description="see world_cluster_growth_geocoding"),
            dg.TableColumn(name="year", type="int", description="The start year of the decade"),
            dg.TableColumn(name="size_growth_slope", type="float", description="The average spline derivative of the augmented-cluster size-growth curve"),
        ])
    }
)
def world_suburbanization_augmented_size_growth_slopes(
    context: dg.AssetExecutionContext,
    postgres: PostgresResource,
    tables: TableNamesResource,
) -> pd.DataFrame:
    """Size-growth slopes computed from gravity-augmented clusters for the main analysis."""

    context.log.info("Calculating world suburbanization augmented size-growth slopes")
    xaxis = 'log_population'
    yaxis = 'log_growth'
    lam = constants['PENALTY_SIZE_GROWTH_CURVE']

    q = f"""
    SELECT  country,
            year,
            log_population,
            log_growth
    FROM {tables.names.world.si.world_suburbanization_augmented_population()}
    """
    augmented_size_vs_growth = pd.read_sql(q, con=postgres.get_engine())
    slopes = (
        augmented_size_vs_growth.groupby(['country', 'year'], sort=True)[[xaxis, yaxis]]
        .apply(
            lambda x: get_mean_derivative_penalized_b_spline(
                df=x,
                xaxis=xaxis,
                yaxis=yaxis,
                lam=lam,
            )
        )
        .reset_index()
        .rename(columns={0: 'size_growth_slope'})
    )
    slopes['analysis_id'] = MAIN_ANALYSIS_ID
    return slopes[['analysis_id', 'country', 'year', 'size_growth_slope']]
