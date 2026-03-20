from typing import Tuple

import dagster as dg
import geopandas as gpd
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
import seaborn as sns
from matplotlib.colors import to_rgba

from ....resources.resources import PostgresResource, TableNamesResource
from ...constants import constants
from ...stats_utils import fit_penalized_b_spline
from ..figure_io import materialize_image, read_pandas, read_postgis, save_figure
from ..figure_style import (
    annotate_letter_label,
    apply_figure_theme,
    create_bicolor_cmap,
    plot_spline_with_ci,
    style_inset_axes,
)

MAIN_ANALYSIS_ID = constants["MAIN_ANALYSIS_ID"]
PENALTY_SLOPE_SPLINE = constants["PENALTY_SLOPE_SPLINE"]

FIGURE_FILE_NAME = "si_figure_country_borders.png"

def _load_plot_data(
    postgres: PostgresResource,
    tables: TableNamesResource,
) -> Tuple[gpd.GeoDataFrame, pd.DataFrame]:
    subregion_map = read_postgis(
        engine=postgres.get_engine(),
        table=tables.names.world.si.world_avg_size_growth_slope_m49_borders(),
        analysis_id=MAIN_ANALYSIS_ID,
    )
    slopes_urbanization = read_pandas(
        engine=postgres.get_engine(),
        table=tables.names.world.si.world_m49_size_growth_slopes_urbanization(),
        analysis_id=MAIN_ANALYSIS_ID,
    )
    return subregion_map, slopes_urbanization


def _get_cmap_and_norm(values: pd.Series) -> Tuple[mcolors.Colormap, mcolors.Normalize]:
    vmin = float(np.nanmin(values))
    vmax = float(np.nanmax(values))
    if np.isclose(vmin, vmax):
        vmax = vmin + 1e-6

    midpoint = 0.0
    if vmax <= midpoint:
        cmap = plt.get_cmap("Greys_r")
    elif vmin >= midpoint:
        cmap = plt.get_cmap("Blues")
    else:
        midpoint_frac = (midpoint - vmin) / (vmax - vmin)
        cmap = create_bicolor_cmap(
            cmap_neg="Greys_r",
            cmap_pos="Blues",
            midpoint_frac=float(np.clip(midpoint_frac, 0.0, 1.0)),
        )
    return cmap, mcolors.Normalize(vmin=vmin, vmax=vmax)


def _plot_subregion_map(
    fig: plt.Figure,
    ax: plt.Axes,
    gdf: gpd.GeoDataFrame,
) -> Tuple[plt.Figure, plt.Axes]:
    projection_epsg = 4087
    colorbar_pos = [0.39, 0.12, 0.22, 0.025]
    colorbar_label = r"$\mathbf{Size\!-\!growth \ slope \ \beta}$"

    gdf_proj = gdf.to_crs(epsg=projection_epsg)
    cmap, norm = _get_cmap_and_norm(gdf_proj["size_growth_slope"])

    ax.set_axis_off()
    cax = fig.add_axes(colorbar_pos)
    gdf_proj.plot(
        column="size_growth_slope",
        ax=ax,
        legend=True,
        cax=cax,
        cmap=cmap,
        norm=norm,
        linewidth=0.5,
        edgecolor="0.5",
        missing_kwds={
            "color": "white",
            "edgecolor": to_rgba("#D3D3D3", 0.8),
            "hatch": "///",
        },
        legend_kwds={"orientation": "horizontal"},
    )

    for spine in cax.spines.values():
        spine.set_visible(False)
    cax.set_xlabel(colorbar_label, fontsize=12)
    cax.xaxis.set_label_position("top")
    return fig, ax


def _plot_slope_vs_urbanization_inset(
    ax: plt.Axes,
    df: pd.DataFrame,
) -> plt.Axes:
    color = px.colors.qualitative.Plotly[0]
    x, y, ci_low, ci_high = fit_penalized_b_spline(
        df=df,
        xaxis="urban_population_share",
        yaxis="size_growth_slope",
        lam=PENALTY_SLOPE_SPLINE,
    )
    plot_spline_with_ci(
        ax=ax, x=x, y=y, ci_low=ci_low, ci_high=ci_high, color=color, linewidth=1.5
    )
    ax.axhline(y=0, color="black", linestyle="--", linewidth=0.5)
    style_inset_axes(ax=ax, xlabel="Urban population share", ylabel=r"$\beta$")
    ax.set_facecolor("white")
    return ax


def _plot_slope_density_inset(ax: plt.Axes, df: pd.DataFrame) -> plt.Axes:
    sns.kdeplot(
        data=df,
        x="size_growth_slope",
        ax=ax,
        color="black",
        fill=True,
        alpha=0.2,
        linewidth=1,
    )
    ax.axvline(
        x=df["size_growth_slope"].median(),
        color="black",
        linestyle="--",
        linewidth=0.8,
    )
    style_inset_axes(ax=ax, xlabel=r"$\beta$", ylabel="Density")
    ax.set_yticks([])
    ax.set_facecolor("white")
    return ax


@dg.asset(
    deps=[
        TableNamesResource().names.world.si.world_avg_size_growth_slope_m49_borders(),
        TableNamesResource().names.world.si.world_m49_size_growth_slopes_urbanization(),
    ],
    group_name="si_figures",
)
def si_figure_country_borders(
    context: dg.AssetExecutionContext,
    postgres: PostgresResource,
    tables: TableNamesResource,
) -> dg.MaterializeResult:
    apply_figure_theme()
    context.log.info("Creating SI figure: UN M49 sub-region robustness")

    subregion_map, slopes_urbanization = _load_plot_data(
        postgres=postgres, tables=tables
    )
    regions_without_data = sorted(
        subregion_map.loc[~subregion_map["has_data"], "micro_region"].tolist()
    )
    context.log.info(f"Regions plotted without data: {regions_without_data}")

    fig, ax_map = plt.subplots(figsize=(10, 6))
    _plot_subregion_map(fig=fig, ax=ax_map, gdf=subregion_map)

    ax_inset_left = fig.add_axes([0.20, 0.20, 0.12, 0.20])
    ax_inset_right = fig.add_axes([0.64, 0.20, 0.09, 0.16])
    _plot_slope_vs_urbanization_inset(ax=ax_inset_left, df=slopes_urbanization)
    _plot_slope_density_inset(ax=ax_inset_right, df=slopes_urbanization)
    save_figure(fig=fig, figure_file_name=FIGURE_FILE_NAME, si=True)
    return materialize_image(figure_file_name=FIGURE_FILE_NAME, si=True)
