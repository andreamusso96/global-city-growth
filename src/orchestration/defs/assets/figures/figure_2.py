# src/orchestration/defs/assets/figures/figure_2.py
import dagster as dg
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
import seaborn as sns
from typing import Tuple
from matplotlib import gridspec

from ...resources.resources import PostgresResource, TableNamesResource
from .figure_style import style_axes, annotate_letter_label, plot_spline_with_ci, style_inset_axes, apply_figure_theme, style_config, format_population_ticks
from ..stats_utils import fit_penalized_b_spline, size_growth_slope_by_year_with_cis
from .figure_io import read_pandas, save_figure, materialize_image
from ..constants import constants

MAIN_ANALYSIS_ID = constants['MAIN_ANALYSIS_ID']

def _plot_size_growth_curve_by_urbanization_group(fig: plt.Figure, ax: plt.Axes, ax_legend: plt.Axes, df_size_growth_slopes: pd.DataFrame, df_size_vs_growth_normalized: pd.DataFrame, df_average_growth: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    lam = constants['PENALTY_SIZE_GROWTH_CURVE']
    # Main plot
    x_axis = 'log_population'
    y_axis = 'normalized_log_growth'
    title = ' Global cross-section'
    
    x_axis_label = r'$\mathbf{Size}$ (log-scale)'
    y_axis_label = r'$\mathbf{Growth \ rate} \ (\log_{10}S_{t+10} \ / \ S_t)$'
    label_font_size = style_config['label_font_size']

    colors = [px.colors.qualitative.Plotly[4], px.colors.qualitative.Plotly[6]]

    groups = sorted(df_size_vs_growth_normalized['urban_population_share_group'].unique().tolist())
    for i, g in enumerate(groups):
        pop_growth_g = df_size_vs_growth_normalized[df_size_vs_growth_normalized['urban_population_share_group'] == g].copy()
        x, y, ci_low, ci_high = fit_penalized_b_spline(df=pop_growth_g, xaxis=x_axis, yaxis=y_axis, lam=lam)
        average_log_growth_g = df_average_growth[df_average_growth['urban_population_share_group'] == g]['log_average_growth'].mean()
        color = colors[i]
        plot_spline_with_ci(ax=ax, x=x, y=average_log_growth_g + y, ci_low=average_log_growth_g + ci_low, ci_high=average_log_growth_g + ci_high, color=color, label=None)

    format_population_ticks(ax=ax, is_xaxis=True)
    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label, title=title)
    ax.set_ylim(0.01, 0.17)

    # Legend plot

    ax_legend.axis('off')
    ax_legend.plot([None], [None], linewidth=3, color=colors[0], label='Less urbanized (0-60%)')
    ax_legend.plot([None], [None], linewidth=3, color=colors[1], label='More urbanized (60-100%)')
    ax_legend.legend(loc='upper left', fontsize=label_font_size, frameon=False, title='Urban population share 1975', title_fontsize=label_font_size, ncol=1)

    start_point = (0.0, 0.0)
    end_point = (-0.2, -0.2)
    ax_legend.annotate(
        text="",
        xy=end_point,
        xytext=start_point,
        xycoords='axes fraction',
        arrowprops=dict(
            facecolor='black',
            width=0.5,
            shrink=0.05,
            headwidth=4,
            headlength=8,
        )
    )

    # Inset plot
    ax_inset = fig.add_axes([0.2, 0.77, 0.1, 0.1])
    x_axis_inset = 'urban_population_share'
    y_axis_inset = 'size_growth_slope'

    x_axis_inset_label = 'Urban population share'
    y_axis_inset_label = r'$\beta$'

    lam = constants['PENALTY_SLOPE_SPLINE']
    color = px.colors.qualitative.Plotly[0]
    x, y, ci_low, ci_high = fit_penalized_b_spline(df=df_size_growth_slopes, xaxis=x_axis_inset, yaxis=y_axis_inset, lam=lam)
    plot_spline_with_ci(ax=ax_inset, x=x, y=y, ci_low=ci_low, ci_high=ci_high, color=color)
    ax_inset.axhline(y=0, color='black', linestyle='--', linewidth=0.5)
    style_inset_axes(ax=ax_inset, xlabel=x_axis_inset_label, ylabel=y_axis_inset_label)
    return fig, ax

def _plot_size_growth_curve_by_epoch(fig: plt.Figure, ax: plt.Axes, df_size_vs_growth_normalized: pd.DataFrame, df_average_growth: pd.DataFrame, x_axis: str, y_axis: str, lam: float) -> Tuple[plt.Figure, plt.Axes]:
    colors = [px.colors.qualitative.Plotly[4], px.colors.qualitative.Plotly[6]]

    epochs = sorted(df_size_vs_growth_normalized['epoch'].dropna().unique().tolist())
    for i, e in enumerate(epochs):
        pop_growth_e = df_size_vs_growth_normalized[df_size_vs_growth_normalized['epoch'] == e].copy()
        x, y, ci_low, ci_high = fit_penalized_b_spline(df=pop_growth_e, xaxis=x_axis, yaxis=y_axis, lam=lam)

        average_log_growth_e = df_average_growth[df_average_growth['epoch'] == e]['log_average_growth'].mean()

        color = colors[i]
        plot_spline_with_ci(ax=ax, x=x, y=average_log_growth_e + y, ci_low=average_log_growth_e + ci_low, ci_high=average_log_growth_e + ci_high, color=color, label=e)

    return fig, ax

def _plot_size_growth_slope_by_year_inset(fig: plt.Figure, ax: plt.Axes, df_size_vs_growth: pd.DataFrame, x_axis: str, y_axis: str, lam: float, n_boots: int) -> Tuple[plt.Figure, plt.Axes]:
    df_slopes_with_cis = size_growth_slope_by_year_with_cis(df=df_size_vs_growth, xaxis='log_population', yaxis='log_growth', lam=lam, n_boots=n_boots)
    color_inset = px.colors.qualitative.Plotly[0]
    ax.plot(df_slopes_with_cis[x_axis], df_slopes_with_cis[y_axis], color=color_inset, linewidth=1, marker='o', markersize=2)
    ax.fill_between(df_slopes_with_cis[x_axis], df_slopes_with_cis['ci_low'], df_slopes_with_cis['ci_high'], color=color_inset, alpha=0.2)
    ax.axhline(y=0, color='black', linestyle='--', linewidth=0.5)
    return fig, ax


def _plot_size_growth_curve_kor_by_epoch(fig: plt.Figure, ax: plt.Axes, df_size_vs_growth: pd.DataFrame, df_size_vs_growth_normalized: pd.DataFrame, df_average_growth: pd.DataFrame, n_boots: int) -> Tuple[plt.Figure, plt.Axes]:
    lam = constants['PENALTY_SIZE_GROWTH_CURVE']

    def map_year_to_epoch(year: int) -> str:
        if year >= 1975 and year < 1995:
            return '1975-2000'
        elif year >= 2000 and year < 2025:
            return '2000-2025'

    df_size_vs_growth_normalized['epoch'] = df_size_vs_growth_normalized['year'].apply(map_year_to_epoch)
    df_average_growth['epoch'] = df_average_growth['year'].apply(map_year_to_epoch)

    # Main plot
    x_axis = 'log_population'
    y_axis = 'normalized_log_growth'

    title = 'South Korea'
    x_axis_label = r'$\mathbf{Size}$ (log-scale)'
    y_axis_label = r'$\mathbf{Growth \ rate} \ (\log_{10}S_{t+10} \ / \ S_t)$'

    _plot_size_growth_curve_by_epoch(fig=fig, ax=ax, df_size_vs_growth_normalized=df_size_vs_growth_normalized, df_average_growth=df_average_growth, x_axis=x_axis, y_axis=y_axis, lam=lam)
    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label, title=title, legend_loc='lower right')
    format_population_ticks(ax=ax, is_xaxis=True)

    # Inset plot
    x_axis_inset = 'year'
    y_axis_inset = 'size_growth_slope'

    x_axis_inset_label = 'Year'
    y_axis_inset_label = r'$\beta$'

    ax_inset = fig.add_axes([0.19, 0.34, 0.1, 0.1])

    _plot_size_growth_slope_by_year_inset(fig=fig, ax=ax_inset, df_size_vs_growth=df_size_vs_growth, x_axis=x_axis_inset, y_axis=y_axis_inset, lam=lam, n_boots=n_boots)
    style_inset_axes(ax=ax_inset, xlabel=x_axis_inset_label, ylabel=y_axis_inset_label)
    return fig, ax


def _plot_size_growth_curve_usa_by_epoch(fig: plt.Figure, ax: plt.Axes, df_size_vs_growth: pd.DataFrame, df_size_vs_growth_normalized: pd.DataFrame, df_average_growth: pd.DataFrame, n_boots: int) -> Tuple[plt.Figure, plt.Axes]:
    lam = constants['PENALTY_SIZE_GROWTH_CURVE']

    def map_year_to_epoch(year: int) -> str:
        if year >= 1850 and year < 1930:
            return '1850-1930'
        elif year >= 1930 and year <= 2020:
            return '1930-2020'

    df_size_vs_growth_normalized['epoch'] = df_size_vs_growth_normalized['year'].apply(map_year_to_epoch)
    df_average_growth['epoch'] = df_average_growth['year'].apply(map_year_to_epoch)
        
    # Main plot
    x_axis = 'log_population'
    y_axis = 'normalized_log_growth'

    title = 'USA'
    x_axis_label = r'$\mathbf{Size}$ (log-scale)'
    y_axis_label = r'$\mathbf{Growth \ rate} \ (\log_{10}S_{t+10} \ / \ S_t)$'

    _plot_size_growth_curve_by_epoch(fig=fig, ax=ax, df_size_vs_growth_normalized=df_size_vs_growth_normalized, df_average_growth=df_average_growth, x_axis=x_axis, y_axis=y_axis, lam=lam)
    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label, title=title, legend_loc='lower center')
    format_population_ticks(ax=ax, is_xaxis=True)


    # Inset plot
    x_axis_inset = 'year'
    y_axis_inset = 'size_growth_slope'

    x_axis_inset_label = 'Year'
    y_axis_inset_label = r'$\beta$'
    ax_inset = fig.add_axes([0.62, 0.34, 0.1, 0.1])
    _plot_size_growth_slope_by_year_inset(fig=fig, ax=ax_inset, df_size_vs_growth=df_size_vs_growth, x_axis=x_axis_inset, y_axis=y_axis_inset, lam=lam, n_boots=n_boots)
    style_inset_axes(ax=ax_inset, xlabel=x_axis_inset_label, ylabel=y_axis_inset_label)
    return fig, ax

def _plot_region_regression_with_urbanization_controls(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    df = df.set_index('region')

    bar_width = 0.35
    index = np.arange(len(df.index))
    label_font_size = style_config['label_font_size']
    inset_label_font_size = style_config['inset_label_font_size']

    x_axis_label = r'$\mathbf{Deviation \ of \ \beta \ from \ global \ average}$'
    y_axis_label = ''

    no_control_error = [df['coeff_no_control'] - df['ci_low_no_control'], df['ci_high_no_control'] - df['coeff_no_control']]
    with_control_error = [df['coeff_with_control'] - df['ci_low_with_control'], df['ci_high_with_control'] - df['coeff_with_control']]

    ax.barh(y=index - bar_width/2, width=df['coeff_no_control'], height=bar_width,
                xerr=no_control_error, capsize=0,
                label='Without', color='skyblue', error_kw={'linewidth': 1.5, 'alpha': 0.8}, ecolor='#4f4f4f')

    ax.barh(y=index + bar_width/2, width=df['coeff_with_control'], height=bar_width,
                xerr=with_control_error, capsize=0,
                label='With', color='lightcoral', error_kw={'linewidth': 1.5, 'alpha': 0.8}, ecolor='#4f4f4f')

    ax.axvline(0, color='grey', linewidth=0.8, linestyle='--')
    ax.annotate('Global\naverage', 
                xy=[0, 0.5], 
                xytext=[-0.012, 0.4], 
                arrowprops=dict(facecolor='black', shrink=0.05, width=0.5, headwidth=4, headlength=8),
                fontsize=label_font_size)

    ax.set_yticks(index)
    ax.set_yticklabels(df.index)
    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label)
    ax.legend(fontsize=inset_label_font_size, frameon=False, title='Urb. pop. share control', ncol=1, bbox_to_anchor=(0.5, 0.5)) # here we want a smaller legend font so we style it ourselves
    return fig, ax


@dg.asset(
    deps=[TableNamesResource().names.usa.figures.usa_size_vs_growth_normalized(), TableNamesResource().names.usa.figures.usa_average_growth(), TableNamesResource().names.usa.figures.usa_size_vs_growth(), TableNamesResource().names.world.figures.world_size_growth_slopes_historical_urbanization(), TableNamesResource().names.world.figures.world_size_vs_growth_normalized(), TableNamesResource().names.world.figures.world_average_growth(), TableNamesResource().names.world.figures.world_size_vs_growth(),
    TableNamesResource().names.world.figures.world_region_regression_with_urbanization_controls()],
    group_name="figures"
)
def figure_2(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    # Create a figure
    context.log.info(f"Creating figure 2")
    apply_figure_theme()
    figure_file_name = 'figure_2.png'

    fig = plt.figure(figsize=(10, 10))
    gs0 = gridspec.GridSpec(2, 2, figure=fig, wspace=0.25, hspace=0.25)
    ax1 = plt.subplot(gs0[0, 0])
    ax2 = plt.subplot(gs0[1, 0])
    ax3 = plt.subplot(gs0[1, 1])
    gs00 = gridspec.GridSpecFromSubplotSpec(2, 1, subplot_spec=gs0[0, 1], height_ratios=[1, 5], hspace=0.4)
    ax1_legend = plt.subplot(gs00[0, 0])
    ax4 = plt.subplot(gs00[1, 0])

    engine = postgres.get_engine()

    world_size_growth_slopes = read_pandas(engine=engine, table=tables.names.world.figures.world_size_growth_slopes_historical_urbanization(), analysis_id=MAIN_ANALYSIS_ID)
    world_size_vs_growth_normalized = read_pandas(engine=engine, table=tables.names.world.figures.world_size_vs_growth_normalized(), analysis_id=MAIN_ANALYSIS_ID)
    world_average_growth = read_pandas(engine=engine, table=tables.names.world.figures.world_average_growth(), analysis_id=MAIN_ANALYSIS_ID)
    _plot_size_growth_curve_by_urbanization_group(fig=fig, ax=ax1, ax_legend=ax1_legend, df_size_growth_slopes=world_size_growth_slopes, df_size_vs_growth_normalized=world_size_vs_growth_normalized, df_average_growth=world_average_growth)

    n_boots = 1000
    kor_size_vs_growth = read_pandas(engine=engine, table=tables.names.world.figures.world_size_vs_growth(), analysis_id=MAIN_ANALYSIS_ID, where="country = 'KOR'")
    kor_size_vs_growth_normalized = read_pandas(engine=engine, table=tables.names.world.figures.world_size_vs_growth_normalized(), analysis_id=MAIN_ANALYSIS_ID, where="country = 'KOR'")
    kor_average_growth = read_pandas(engine=engine, table=tables.names.world.figures.world_average_growth(), analysis_id=MAIN_ANALYSIS_ID, where="country = 'KOR'")
    _plot_size_growth_curve_kor_by_epoch(fig=fig, ax=ax2, df_size_vs_growth=kor_size_vs_growth, df_size_vs_growth_normalized=kor_size_vs_growth_normalized, df_average_growth=kor_average_growth, n_boots=n_boots)

    usa_size_vs_growth = read_pandas(engine=engine, table=tables.names.usa.figures.usa_size_vs_growth(), analysis_id=MAIN_ANALYSIS_ID)
    usa_size_vs_growth_normalized = read_pandas(engine=engine, table=tables.names.usa.figures.usa_size_vs_growth_normalized(), analysis_id=MAIN_ANALYSIS_ID)
    usa_average_growth = read_pandas(engine=engine, table=tables.names.usa.figures.usa_average_growth(), analysis_id=MAIN_ANALYSIS_ID)
    _plot_size_growth_curve_usa_by_epoch(fig=fig, ax=ax3, df_size_vs_growth=usa_size_vs_growth, df_size_vs_growth_normalized=usa_size_vs_growth_normalized, df_average_growth=usa_average_growth, n_boots=n_boots)

    world_region_regression_with_urbanization_controls = read_pandas(engine=engine, table=tables.names.world.figures.world_region_regression_with_urbanization_controls(), analysis_id=MAIN_ANALYSIS_ID)
    _plot_region_regression_with_urbanization_controls(fig=fig, ax=ax4, df=world_region_regression_with_urbanization_controls)

    annotate_letter_label(axes=[ax1, ax2, ax3, ax4], left_side=[False, False, False, False])
    save_figure(fig=fig, figure_file_name=figure_file_name)
    return materialize_image(figure_file_name=figure_file_name)