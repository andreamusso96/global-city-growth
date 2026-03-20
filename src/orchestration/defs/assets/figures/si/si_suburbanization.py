from typing import Dict, Tuple

import dagster as dg
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
import seaborn as sns
from matplotlib import gridspec
from matplotlib.lines import Line2D
from matplotlib.ticker import MaxNLocator

from ....resources.resources import PostgresResource, TableNamesResource
from ...constants import constants
from ...stats_utils import (
    bootstrap_ci,
    fit_penalized_b_spline,
    get_mean_derivative_penalized_b_spline,
    size_growth_slope_by_year_with_cis,
)
from ..figure_io import materialize_image, read_pandas, save_figure
from ..figure_style import (
    annotate_letter_label,
    apply_figure_theme,
    format_population_ticks,
    plot_spline_with_ci,
    style_axes,
    style_config,
)

MAIN_ANALYSIS_ID = constants["MAIN_ANALYSIS_ID"]
PENALTY_SIZE_GROWTH_CURVE = constants["PENALTY_SIZE_GROWTH_CURVE"]
FIGURE_FILE_NAME = "si_figure_suburbanization_usa.png"
N_BOOTS = 1000

DEFINITION_COLORS = {
    "base": px.colors.qualitative.Plotly[2],
    "density": px.colors.qualitative.Plotly[0],
    "cbsa": px.colors.qualitative.Plotly[1],
}

DEFINITION_LABELS = {
    "base": "Base clusters",
    "density": "Density-based clusters",
    "cbsa": "CBSAs",
}


def _read_table(engine, table: str, cols: str = "*", where: str = "") -> pd.DataFrame:
    q = f"SELECT {cols} FROM {table}"
    if where:
        q += f" WHERE {where}"
    return pd.read_sql(q, con=engine)


def _get_suburbanization_inputs(
    postgres: PostgresResource, tables: TableNamesResource
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    engine = postgres.get_engine()

    size_vs_growth = _read_table(
        engine=engine,
        table=tables.names.usa.figures.usa_suburbanization_size_vs_growth(),
    )
    normalized = _read_table(
        engine=engine,
        table=tables.names.usa.figures.usa_suburbanization_size_vs_growth_normalized(),
    )
    neighbor_flags = _read_table(
        engine=engine,
        table=tables.names.usa.figures.usa_suburbanization_base_neighbor_flags(),
    )
    world_slopes_2010 = read_pandas(
        engine=engine,
        table=tables.names.world.figures.world_size_growth_slopes_historical(),
        analysis_id=MAIN_ANALYSIS_ID,
        where="year = 2010",
    )
    return size_vs_growth, normalized, neighbor_flags, world_slopes_2010


def _plot_size_growth_curves(
    ax: plt.Axes,
    df: pd.DataFrame,
    xaxis: str,
    yaxis: str,
) -> plt.Axes:
    for definition in ["base", "density", "cbsa"]:
        df_definition = df[df["definition"] == definition].copy()
        x, y, ci_low, ci_high = fit_penalized_b_spline(
            df=df_definition,
            xaxis=xaxis,
            yaxis=yaxis,
            lam=PENALTY_SIZE_GROWTH_CURVE,
        )
        plot_spline_with_ci(
            ax=ax,
            x=x,
            y=y,
            ci_low=ci_low,
            ci_high=ci_high,
            color=DEFINITION_COLORS[definition],
            label=DEFINITION_LABELS[definition],
        )
    return ax


def _get_base_residuals(
    size_vs_growth: pd.DataFrame, neighbor_flags: pd.DataFrame
) -> pd.DataFrame:
    base_df = size_vs_growth[size_vs_growth["definition"] == "base"].copy()
    x, y, _, _ = fit_penalized_b_spline(
        df=base_df,
        xaxis="log_population",
        yaxis="log_growth",
        lam=PENALTY_SIZE_GROWTH_CURVE,
    )
    base_df["log_growth_pred"] = np.interp(base_df["log_population"], x, y)
    base_df["log_growth_diff"] = base_df["log_growth"] - base_df["log_growth_pred"]

    residuals = neighbor_flags.merge(
        base_df[["cluster_id", "year", "log_population", "log_growth_diff"]],
        on=["cluster_id", "year"],
        how="left",
    )
    return residuals[residuals["log_population"] < 6].copy()


def _plot_panel_c(ax: plt.Axes, df: pd.DataFrame) -> plt.Axes:
    df_plot = df.copy()

    np.random.seed(42)
    for has_large_nbr, y in [(False, 0), (True, 1)]:
        df_group = df_plot[df_plot["has_large_nbr"] == has_large_nbr].copy()
        mean, ci_low, ci_high = bootstrap_ci(
            estimator=lambda x: x["log_growth_diff"].mean(),
            df=df_group,
            nboots=N_BOOTS,
        )
        ax.errorbar(
            mean,
            y,
            xerr=[[mean - ci_low], [ci_high - mean]],
            fmt="o",
            color="0.25",
            markersize=5,
            linewidth=1.5,
            capsize=0,
            zorder=3,
        )

    ax.axvline(0, color="0.6", linestyle=":", linewidth=1)
    ax.xaxis.set_major_locator(MaxNLocator(5))
    ax.set_yticks([0, 1])
    ax.set_yticklabels(["False", "True"])
    style_axes(
        ax=ax,
        xlabel=r"$\mathbf{Deviation \ of \ growth \ rate \ from \ expectation}$"
        + "\n"
        + r"$(\log_{10}S_{t+10} \ / \ S_t - E[\log_{10}S_{t+10} \ / \ S_t \mid S_t])$",
        ylabel=r"$\mathbf{Has \ 1M+ \ cluster}$"
        + "\n"
        + r"$\mathbf{within \ 100km}$",
    )
    return ax


def _bootstrap_slope(df: pd.DataFrame) -> Tuple[float, float, float]:
    estimator = lambda x: get_mean_derivative_penalized_b_spline(
        df=x,
        xaxis="log_population",
        yaxis="log_growth",
        lam=PENALTY_SIZE_GROWTH_CURVE,
    )
    return bootstrap_ci(estimator=estimator, df=df, nboots=N_BOOTS)


def _get_usa_slope_estimates(
    size_vs_growth: pd.DataFrame,
    usa_density_historical: pd.DataFrame,
) -> Tuple[Dict[str, Dict[str, float]], pd.DataFrame]:
    np.random.seed(42)
    density_slopes = size_growth_slope_by_year_with_cis(
        df=usa_density_historical,
        xaxis="log_population",
        yaxis="log_growth",
        lam=PENALTY_SIZE_GROWTH_CURVE,
        n_boots=N_BOOTS,
    )

    base_df = size_vs_growth[size_vs_growth["definition"] == "base"].copy()
    cbsa_df = size_vs_growth[size_vs_growth["definition"] == "cbsa"].copy()

    base_slope, base_ci_low, base_ci_high = _bootstrap_slope(base_df)
    cbsa_slope, cbsa_ci_low, cbsa_ci_high = _bootstrap_slope(cbsa_df)

    density_2010 = density_slopes[density_slopes["year"] == 2010].iloc[0]
    estimates = {
        "base": {
            "slope": float(base_slope),
            "ci_low": float(base_ci_low),
            "ci_high": float(base_ci_high),
        },
        "density": {
            "slope": float(density_2010["size_growth_slope"]),
            "ci_low": float(density_2010["ci_low"]),
            "ci_high": float(density_2010["ci_high"]),
        },
        "cbsa": {
            "slope": float(cbsa_slope),
            "ci_low": float(cbsa_ci_low),
            "ci_high": float(cbsa_ci_high),
        },
    }
    return estimates, density_slopes


def _plot_panel_d(
    ax: plt.Axes,
    world_slopes_2010: pd.DataFrame,
    estimates: Dict[str, Dict[str, float]],
) -> plt.Axes:
    sns.kdeplot(
        data=world_slopes_2010,
        x="size_growth_slope",
        ax=ax,
        fill=True,
        color="lightgray",
        linewidth=1,
    )
    for definition in ["base", "density", "cbsa"]:
        ax.axvline(
            estimates[definition]["slope"],
            color=DEFINITION_COLORS[definition],
            linestyle="--",
            linewidth=1.5,
        )
    style_axes(
        ax=ax,
        xlabel=r"$\mathbf{Size\!-\!growth \ slope \ \beta}$",
        ylabel=r"$\mathbf{Density}$",
    )
    ax.xaxis.set_major_locator(MaxNLocator(5))
    return ax


def _plot_panel_e(
    ax: plt.Axes,
    density_slopes: pd.DataFrame,
    estimates: Dict[str, Dict[str, float]],
) -> plt.Axes:
    density_color = DEFINITION_COLORS["density"]
    ax.plot(
        density_slopes["year"],
        density_slopes["size_growth_slope"],
        color=density_color,
        linewidth=1.5,
        marker="o",
        markersize=4,
    )
    ax.fill_between(
        density_slopes["year"],
        density_slopes["ci_low"],
        density_slopes["ci_high"],
        color=density_color,
        alpha=0.2,
    )

    point_x = {"base": 2008, "cbsa": 2012}
    for definition in ["base", "cbsa"]:
        slope = estimates[definition]["slope"]
        ci_low = estimates[definition]["ci_low"]
        ci_high = estimates[definition]["ci_high"]
        ax.errorbar(
            point_x[definition],
            slope,
            yerr=[[slope - ci_low], [ci_high - slope]],
            fmt="o",
            color=DEFINITION_COLORS[definition],
            markersize=5,
            linewidth=1.5,
        )

    style_axes(
        ax=ax,
        xlabel=r"$\mathbf{Year}$",
        ylabel=r"$\mathbf{Size\!-\!growth \ slope \ \beta}$",
    )
    ax.set_xlim(density_slopes["year"].min() - 5, 2022)
    return ax


@dg.asset(
    deps=[
        TableNamesResource().names.usa.figures.usa_suburbanization_size_vs_growth(),
        TableNamesResource().names.usa.figures.usa_suburbanization_size_vs_growth_normalized(),
        TableNamesResource().names.usa.figures.usa_suburbanization_base_neighbor_flags(),
        TableNamesResource().names.usa.figures.usa_size_vs_growth(),
        TableNamesResource().names.world.figures.world_size_growth_slopes_historical(),
    ],
    group_name="si_figures",
)
def si_figure_suburbanization_usa(
    context: dg.AssetExecutionContext,
    postgres: PostgresResource,
    tables: TableNamesResource,
) -> dg.MaterializeResult:
    context.log.info("Creating SI figure: USA suburbanization")
    apply_figure_theme()

    size_vs_growth, normalized, neighbor_flags, world_slopes_2010 = (
        _get_suburbanization_inputs(postgres=postgres, tables=tables)
    )
    usa_density_historical = read_pandas(
        engine=postgres.get_engine(),
        table=tables.names.usa.figures.usa_size_vs_growth(),
        analysis_id=MAIN_ANALYSIS_ID,
    )

    residuals = _get_base_residuals(
        size_vs_growth=size_vs_growth,
        neighbor_flags=neighbor_flags,
    )
    estimates, density_slopes = _get_usa_slope_estimates(
        size_vs_growth=size_vs_growth,
        usa_density_historical=usa_density_historical,
    )

    fig = plt.figure(figsize=(10, 10))
    outer = gridspec.GridSpec(
        2,
        2,
        figure=fig,
        width_ratios=[1, 1],
        height_ratios=[1, 1.05],
        wspace=0.30,
        hspace=0.34,
    )

    ax_a = plt.subplot(outer[0, 0])
    ax_b = plt.subplot(outer[0, 1])
    ax_e = plt.subplot(outer[1, 1])
    left_bottom = gridspec.GridSpecFromSubplotSpec(
        2,
        1,
        subplot_spec=outer[1, 0],
        height_ratios=[1, 1],
        hspace=0.95,
    )
    ax_c = plt.subplot(left_bottom[0, 0])
    ax_d = plt.subplot(left_bottom[1, 0])

    _plot_size_growth_curves(
        ax=ax_a,
        df=size_vs_growth,
        xaxis="log_population",
        yaxis="log_growth",
    )
    style_axes(
        ax=ax_a,
        xlabel=r"$\mathbf{Size}$ (log-scale)",
        ylabel=r"$\mathbf{Growth \ rate}$ " + r"$(\log_{10}S_{t+10} \ / \ S_t)$",
    )
    format_population_ticks(ax=ax_a, is_xaxis=True)

    _plot_size_growth_curves(
        ax=ax_b,
        df=normalized,
        xaxis="normalized_log_population",
        yaxis="normalized_log_growth",
    )
    style_axes(
        ax=ax_b,
        xlabel=r"$\mathbf{Normalized \ size}$ "
        + r"$(\log_{10}S_t - \min \ \log_{10}S_t)$",
        ylabel=r"$\mathbf{Normalized \ growth \ rate}$"
        + "\n"
        + r"$(\log_{10}S_{t+10} \ / \ S_t - \log_{10}\Sigma S_{t+10} / \Sigma S_t)$",
    )

    _plot_panel_c(ax=ax_c, df=residuals)
    _plot_panel_d(ax=ax_d, world_slopes_2010=world_slopes_2010, estimates=estimates)
    _plot_panel_e(ax=ax_e, density_slopes=density_slopes, estimates=estimates)

    legend_handles = [
        Line2D(
            [0],
            [0],
            color=DEFINITION_COLORS[definition],
            linewidth=2,
            marker="o",
            markersize=5,
            label=DEFINITION_LABELS[definition],
        )
        for definition in ["base", "density", "cbsa"]
    ]
    fig.legend(
        handles=legend_handles,
        loc="upper center",
        bbox_to_anchor=(0.5, 0.50),
        ncol=3,
        frameon=False,
        fontsize=style_config["label_font_size"],
    )

    annotate_letter_label(
        axes=[ax_a, ax_b, ax_c, ax_d, ax_e],
        left_side=[True, True, True, False, False],
    )
    save_figure(fig=fig, figure_file_name=FIGURE_FILE_NAME, si=True)
    return materialize_image(figure_file_name=FIGURE_FILE_NAME, si=True)
