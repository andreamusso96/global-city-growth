# src/orchestration/defs/assets/figures/figure_1.py
import dagster as dg
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
import seaborn as sns
import geopandas as gpd
from typing import Tuple

import matplotlib.colors as mcolors

from matplotlib.colors import to_rgba
from mpl_toolkits.axes_grid1 import make_axes_locatable
import matplotlib.gridspec as gridspec
from sqlalchemy import true

from ...resources.resources import PostgresResource, TableNamesResource
from .figure_style import create_bicolor_cmap, style_inset_axes, style_axes, annotate_letter_label, region_colors, style_config, apply_figure_theme, plot_spline_with_ci, format_population_ticks
from ..stats_utils import fit_penalized_b_spline
from .figure_io import read_pandas, read_postgis, save_figure, materialize_image
from ..constants import constants

MAIN_ANALYSIS_ID = constants['MAIN_ANALYSIS_ID']

def _plot_world_map_with_colorbar(fig: plt.Figure, ax: plt.Axes, gdf: gpd.GeoDataFrame) -> Tuple[plt.Figure, plt.Axes]:

    # Map 
    col = 'size_growth_slope'
    midpoint = 0
    
    missing_hatch = '///'
    missing_color = 'white'
    projection_epsg = 4087

    colorbar_label_fontsize = 12
    colorbar_pos = [0.4, 0.16, 0.2, 0.025]
    colorbar_label_pos = 'top'
    colorbar_label = r'$\mathbf{Size\!-\!growth \ slope \ \beta}$'
    
    gdf_proj = gdf.to_crs(epsg=projection_epsg)
    vmin, vmax = np.nanmin(gdf_proj[col]), np.nanmax(gdf_proj[col])
    norm = mcolors.Normalize(vmin=vmin, vmax=vmax)
    midpoint_frac = (midpoint - vmin) / (vmax - vmin)
    rescaled_cmap = create_bicolor_cmap(cmap_neg='Greys_r', cmap_pos='Blues', midpoint_frac=midpoint_frac)


    ax.set_axis_off()
    divider = make_axes_locatable(ax)
    cax = fig.add_axes(colorbar_pos)

    gdf_proj.plot(column=col, ax=ax, legend=True, cax=cax, cmap=rescaled_cmap, norm=norm, linewidth=0.5, edgecolor='0.5', missing_kwds={
                        "color": missing_color,
                        "edgecolor": to_rgba('#D3D3D3', 0.8),
                        "hatch": missing_hatch,
                    }, legend_kwds={
                        'orientation': "horizontal",
                    })


    for spine in cax.spines.values(): spine.set_visible(False)
    cax.set_xlabel(colorbar_label, fontsize=colorbar_label_fontsize)
    cax.xaxis.set_label_position(colorbar_label_pos)


    # Inset distribution

    inset_distribution_pos = [0.64, 0.2, 0.09, 0.16]
    inset_distribution_ylabel = 'Density'
    inset_distribution_xlabel = r'$\beta$'

    ax_inset = fig.add_axes(inset_distribution_pos)
    sns.kdeplot(gdf_proj[col], ax=ax_inset, color='black', fill=True, alpha=0.2)
    ax_inset.axvline(x=gdf_proj[col].median(), color='black', linestyle='--', linewidth=0.5)
    style_inset_axes(ax=ax_inset, xlabel=inset_distribution_xlabel, ylabel=inset_distribution_ylabel)
    ax_inset.set_title('')
    ax_inset.set_yticks([])

    # Inset growth size
    inset_growth_size_pos = [0.2, 0.2, 0.12, 0.2]
    inset_growth_size_xlabel = r'Size ($\log_{10}S_t$)'
    inset_growth_size_ylabel = 'Growth rate\n' + r'($\log_{10}S_{t+10} \ / \ S_t$)'
    inset_label_text_font_size = 8
    inset_label_font_size = style_config['inset_label_font_size']

    ax_inset2 = fig.add_axes(inset_growth_size_pos)

    x_ = [4, 5, 6, 7]
    y_ = [0.04, 0.045, 0.07, 0.06]
    p = np.polyfit(x_, y_, 4)
    x_ = np.linspace(4, 7, 100)
    y_ = np.polyval(p, x_)
    ax_inset2.plot(x_, y_, color='black', linewidth=1)

    p_der = np.polyder(p)
    x_points = [4.5, 5.5, 6.5]
    shift_y = [-0.008, 0,  0.008]
    color_tangent = px.colors.qualitative.Plotly[1]
    for i, x_tan in enumerate(x_points):
        y_tan = np.polyval(p, x_tan)
        slope = np.polyval(p_der, x_tan)
        x_line = np.linspace(x_tan - 0.4, x_tan + 0.4, 2)
        y_line = slope * (x_line - x_tan) + y_tan
        ax_inset2.plot(x_line, y_line, color=color_tangent, linestyle='--', linewidth=1.5)
        shift_y_i = shift_y[i]
        if i == 2:
            ax_inset2.text(x_tan, y_tan + shift_y_i, f'Slope', fontsize=inset_label_text_font_size, ha='center', va='center', color=color_tangent)


    ax_inset2.annotate(
        text=r'$\beta = $ average slope' + '\nof this curve',
        xy=(5.5, 0.06),  # The point on the curve to point to
        xytext=(5.5, 0.115),    # Where the text is located
        xycoords='data',
        arrowprops=dict(facecolor='black', shrink=0.05, width=0.5, headwidth=4, headlength=8),
        ha='center',
        va='center',
        fontsize=inset_label_font_size
    )
    # ax_inset2.text(5.5, 0.12, f'Size-growth slope =\naverage slope of this curve', fontsize=inset_font_size, fontfamily=font_family, ha='center', va='center')
    style_inset_axes(ax=ax_inset2, xlabel=inset_growth_size_xlabel, ylabel=inset_growth_size_ylabel)
    ax_inset2.set_yticks([])
    ax_inset2.set_xlim(4, 7)
    ax_inset2.set_ylim(0.02, 0.09)
    return fig, ax

def _plot_growth_rates_group_barchart_by_region(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    region_col = 'region'
    x_axis = 'city_group'
    y_axis = 'normalized_growth'
    weights = 'population'

    y_axis_label = r'$\mathbf{Growth \ advantage \ over \ national \ average}$' + '\n' + r'($g_{\text{group}} \ / \ g_{\text{national}} - 1$)'

    group_to_order = {
        'above_1m': 1,
        'below_1m': 0,
        'largest_city': 2,
    }
    df['city_group_ordered'] = df['city_group'].map(group_to_order)
    df = df.sort_values(by=['city_group_ordered', 'region'])

    group_to_label = {
        'above_1m': 'Population\nabove 1M',
        'below_1m': 'Population\nbelow 1M',
        'largest_city': 'Largest city',
    }
    sns.barplot(data=df, x=x_axis, y=y_axis, ax=ax, weights=weights, errorbar=('ci', 95), dodge=True, orient='v', hue=region_col, palette=region_colors, fill=True, saturation=1, formatter=lambda x: group_to_label[x], legend=False, err_kws={'linewidth': 1.5, 'alpha': 0.8})
    style_axes(ax=ax, xlabel='', ylabel=y_axis_label)
    ax.set_xlabel('')
    return fig, ax

def _plot_growth_size_curve_by_region(fig: plt.Figure, ax: plt.Axes, df_size_vs_growth_normalized: pd.DataFrame, df_average_growth: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    x_axis = 'log_population'
    y_axis = 'normalized_log_growth'

    x_axis_label = r'$\mathbf{Size}$ (log-scale)'
    y_axis_label = r'$\mathbf{Growth \ rate} \ (\log_{10}S_{t+10} \ / \ S_t)$'
    
    lam = constants['PENALTY_SIZE_GROWTH_CURVE']

    region_col = 'region'

    regions = sorted(df_size_vs_growth_normalized[region_col].unique().tolist())
    for i, r in enumerate(regions):
        size_vs_growth_normalized_r = df_size_vs_growth_normalized[df_size_vs_growth_normalized[region_col] == r].copy()  
        x, y, ci_low, ci_high = fit_penalized_b_spline(df=size_vs_growth_normalized_r, xaxis=x_axis, yaxis=y_axis, lam=lam)

        df_average_growth_r = df_average_growth[df_average_growth[region_col] == r].copy()
        average_log_growth_r = df_average_growth_r['log_average_growth'].mean()

        color = region_colors[r]
        plot_spline_with_ci(ax=ax, x=x, y=average_log_growth_r + y, ci_low=average_log_growth_r + ci_low, ci_high=average_log_growth_r + ci_high, color=color, label=r)

    format_population_ticks(ax=ax, is_xaxis=True)
    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label)
    return fig, ax


@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_average_size_growth_slope_with_borders()],
    group_name="figures"
)
def figure_1_map(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    apply_figure_theme()
    context.log.info(f"Creating figure 1: map")
    fig, ax = plt.subplots(figsize=(10, 5))
    figure_file_name = 'figure_1_map.png'

    rank_size_slopes_and_country_borders = read_postgis(engine=postgres.get_engine(), table=tables.names.world.figures.world_average_size_growth_slope_with_borders(), analysis_id=MAIN_ANALYSIS_ID)
    _plot_world_map_with_colorbar(fig=fig, ax=ax, gdf=rank_size_slopes_and_country_borders)
    
    _, dummy_axes = plt.subplots(2, 1, figsize=(10, 5))
    dummy_ax1, dummy_ax2 = dummy_axes.flatten()
    annotate_letter_label(axes=[dummy_ax1, dummy_ax2, ax], left_side=[True, True, True])
    save_figure(fig=fig, figure_file_name=figure_file_name)
    return materialize_image(figure_file_name=figure_file_name)


@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_size_vs_growth_normalized_by_group(), TableNamesResource().names.world.figures.world_size_vs_growth_normalized(), TableNamesResource().names.world.figures.world_average_growth()],
    group_name="figures"
)
def figure_1_plots(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    apply_figure_theme()
    # Create a figure
    context.log.info(f"Creating figure 1: plots")
    figure_file_name = 'figure_1_plots.png'

    fig = plt.figure(figsize=(10, 5))
    gs = gridspec.GridSpec(1, 2, wspace=0.25)
    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])  

    engine = postgres.get_engine()
    world_size_vs_growth_normalized_by_group = read_pandas(engine=engine, table=tables.names.world.figures.world_size_vs_growth_normalized_by_group(), analysis_id=MAIN_ANALYSIS_ID)
    _plot_growth_rates_group_barchart_by_region(fig=fig, ax=ax1, df=world_size_vs_growth_normalized_by_group)

    world_size_vs_growth_normalized = read_pandas(engine=engine, table=tables.names.world.figures.world_size_vs_growth_normalized(), analysis_id=MAIN_ANALYSIS_ID)
    world_average_growth = read_pandas(engine=engine, table=tables.names.world.figures.world_average_growth(), analysis_id=MAIN_ANALYSIS_ID)
    _plot_growth_size_curve_by_region(fig=fig, ax=ax2, df_size_vs_growth_normalized=world_size_vs_growth_normalized, df_average_growth=world_average_growth)
    
    annotate_letter_label(axes=[ax1, ax2], left_side=[True, True, True])
    save_figure(fig=fig, figure_file_name=figure_file_name)
    return materialize_image(figure_file_name=figure_file_name)
    
