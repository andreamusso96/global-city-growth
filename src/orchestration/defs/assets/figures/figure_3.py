# src/orchestration/defs/assets/figures/figure_3.py
import dagster as dg
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import seaborn as sns
import numpy as np
from typing import Tuple
from matplotlib import ticker as mtick
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from matplotlib import gridspec

from ...resources.resources import PostgresResource, TableNamesResource
from .figure_style import style_axes, annotate_letter_label, style_config, apply_figure_theme, style_inset_axes, get_light_variant_of_hex, format_population_ticks
from .figure_io import read_pandas, save_figure, materialize_image
from ..stats_utils import fit_penalized_b_spline
from ..constants import constants

MAIN_ANALYSIS_ID = constants['MAIN_ANALYSIS_ID']



def _plot_rank_vs_size_curves(fig: plt.Figure, ax: plt.Axes, y1: int, y2: int, df: pd.DataFrame, x_axis: str, y_axis: str, linewidth: int, markersize: int) -> None:
    colors = [px.colors.qualitative.Plotly[4], px.colors.qualitative.Plotly[6]]
    lam = constants['PENALTY_RANK_SIZE_CURVE']
    years = [y1, y2]
    for i, year in enumerate(years):
        df_y = df[df['year'] == year].copy()
        x, y, ci_low, ci_high = fit_penalized_b_spline(df=df_y, xaxis=x_axis, yaxis=y_axis, lam=lam)
        ax.plot(x, y, color=colors[i], linewidth=linewidth, label=f'{year}')
        light_hex = get_light_variant_of_hex(base_hex=colors[i])
        ax.scatter(x=df_y[x_axis], y=df_y[y_axis], color=light_hex, marker='o', s=markersize, label=None)
    return fig, ax

def _plot_rank_vs_size_usa_kor(fig: plt.Figure, ax: plt.Axes, df_usa: pd.DataFrame, df_kor: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    # Main plot (USA)
    x_axis = 'log_rank'
    y_axis = 'log_population'

    title = 'USA'
    x_axis_label = r'$\mathbf{Rank}$ (log-scale)'
    y_axis_label = r'$\mathbf{Size}$ (log-scale)'
    y1, y2 = 1850, 2020

    _plot_rank_vs_size_curves(fig=fig, ax=ax, y1=y1, y2=y2, df=df_usa, x_axis=x_axis, y_axis=y_axis, linewidth=2, markersize=30)
    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label, title=title, legend_loc="lower left")
    format_population_ticks(ax=ax, is_xaxis=False)
    ax.set_xticks([0, 1, 2, 3])
    ax.set_xticklabels(['1', '10', '100', '1K'])
    ax.set_ylim(3.6, 7.9)

    # Inset plot (Korea)
    inset_title = 'South Korea'
    x_axis_inset_label = r'Rank (log-scale)'
    y_axis_inset_label = r'Size (log-scale)'
    y1, y2 = 1975, 2025

    ax_inset = fig.add_axes([0.35, 0.76, 0.1, 0.1])
    _plot_rank_vs_size_curves(fig=fig, ax=ax_inset, y1=y1, y2=y2, df=df_kor, x_axis=x_axis, y_axis=y_axis, linewidth=1.5, markersize=0)
    style_inset_axes(ax=ax_inset, xlabel=x_axis_inset_label, ylabel=y_axis_inset_label, title=inset_title)
    format_population_ticks(ax=ax_inset, is_xaxis=False)
    ax_inset.set_xticks([0, 1, 2])
    ax_inset.set_xticklabels(['1', '10', '100'])
    return fig, ax



def _plot_rank_size_slope_change_curves(fig: plt.Figure, ax: plt.Axes, year_base: int, year_middle: int, year_end: int, df: pd.DataFrame, x_axis: str, y_axis: str, linewidth: int, markersize: int) -> None:
    colors = [px.colors.qualitative.Plotly[4], px.colors.qualitative.Plotly[6]]
    df_early = df[(df['year_base'] == year_base) & (df['year'] <= year_middle) & (df['year'] >= year_base)].copy()
    df_late = df[(df['year_base'] == year_middle) & (df['year'] >= year_middle) & (df['year'] <= year_end)].copy()
    ax.plot(df_early[x_axis] - year_base, df_early[y_axis], label=f'{year_base}-{year_middle}', color=colors[0], marker='o', linewidth=linewidth, markersize=markersize)
    ax.plot(df_late[x_axis] - year_middle, df_late[y_axis], label=f'{year_middle}-{year_end}', color=colors[1], marker='o', linewidth=linewidth, markersize=markersize)
    return fig, ax

def _plot_rank_size_slope_change_usa_kor(fig: plt.Figure, ax: plt.Axes, df_usa: pd.DataFrame, df_kor: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    # Main plot (USA)
    x_axis = 'year'
    y_axis = 'rank_size_slope_change'

    title = 'USA'
    x_axis_label = r'$\mathbf{Time \ since \ t_0}$'
    y_axis_label = r'$\mathbf{Change \ in \ \alpha \ since \ t_0} \ (\alpha_{t} \ / \ \alpha_{t_0} - 1)$'
    year_base, year_middle, year_end = 1850, 1930, 2020

    _plot_rank_size_slope_change_curves(fig=fig, ax=ax, year_base=year_base, year_middle=year_middle, year_end=year_end, df=df_usa, x_axis=x_axis, y_axis=y_axis, linewidth=2, markersize=5)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label, title=title, legend_loc="lower right")

    # Inset plot (Korea)
    inset_title = 'South Korea'
    x_axis_inset_label = r'Time since $t_0$'
    y_axis_inset_label = r'$\alpha_{t} \ / \ \alpha_{t_0} - 1$'
    year_base, year_middle, year_end = 1975, 2000, 2025

    ax_inset = fig.add_axes([0.63, 0.76, 0.1, 0.1])
    _plot_rank_size_slope_change_curves(fig=fig, ax=ax_inset, year_base=year_base, year_middle=year_middle, year_end=year_end, df=df_kor, x_axis=x_axis, y_axis=y_axis, linewidth=1, markersize=2)
    ax_inset.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
    style_inset_axes(ax=ax_inset, xlabel=x_axis_inset_label, ylabel=y_axis_inset_label, title=inset_title)
    return fig, ax


def _plot_rank_size_slope_change_by_urbanization_group(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    x_axis = f'year_since_1975'
    y_axis = 'rank_size_slope_change'
    group_col = 'urban_population_share_group'

    x_axis_label = r'$\mathbf{Time \ since \ 1975}$'

    y_axis_label = r'$\mathbf{Change \ in \ \alpha \ since \ 1975} \ (\alpha_t \ / \ \alpha_{1975} - 1)$'

    colors = [px.colors.qualitative.Plotly[4], px.colors.qualitative.Plotly[6], 'grey']
    x_bins = np.arange(5, 55, 10)

    groups = df[group_col].unique().tolist()
    for i, g in enumerate(groups):
        df_g = df[df[group_col] == g]
        sns.regplot(data=df_g, x=x_axis, y=y_axis, ax=ax, ci=95, fit_reg=False, x_bins=x_bins, color=colors[i], line_kws={'linewidth': 1.5})

    group_labels = ['Less urbanized (0-60%)', 'More urbanized (60-100%)', 'All (0-100%)']
    handles, labels = ax.get_legend_handles_labels()
    for i, group_label in enumerate(group_labels):
        proxy = Patch(facecolor=colors[i], label=group_label)
        handles.append(proxy)
        labels.append(group_label)

    ax.legend(handles=handles, labels=labels, frameon=False, title='Urban population share in 1975', loc='upper left')
        
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label)
    ax.set_ylim(-0.01, 0.25)
    return fig, ax


def _plot_rank_size_slope_by_urbanization_group(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    x_axis = 'rank_size_slope'
    y_axis = 'year'
    group_col = 'urban_population_share_group'

    x_axis_label = r'$\mathbf{Rank\!-\!size \ slope \ \alpha}$'
    y_axis_label = r'$\mathbf{Year}$'

    x_bins = [1975, 2025, 2100]
    df_plot = df[df['year'].isin(x_bins)]
    df_plot_world = df_plot.copy()
    df_plot_world[group_col] = 'World'
    df_plot_all = pd.concat([df_plot, df_plot_world], axis=0)

    colors = [px.colors.qualitative.Plotly[4], px.colors.qualitative.Plotly[6], 'grey']
    sns.barplot(data=df_plot_all, x=x_axis, y=y_axis, ax=ax, errorbar=('ci', 95), dodge=True, orient='h', hue=group_col, palette=colors, fill=True, saturation=1, legend=False, err_kws={'linewidth': 1.5, 'alpha': 0.8})

    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label)
    ax.set_xlim(0.9, 1.4)
    return fig, ax


def _plot_bar_share_cities_above_1m(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame, year: int, label: str, hatch: str) -> Tuple[plt.Figure, plt.Axes]:
    df_y = df[df['year'] == year].copy()
    df_y['label'] = label

    n_before = len(ax.patches)
    sns.barplot(data=df_y, x='total_population_share_cities_above_one_million', y='label', ax=ax, color='grey', weights='population', native_scale=False, saturation=1, errorbar=('ci', 95), err_kws={'linewidth': 1.5, 'alpha': 0.8}, orient='h')
    new_bars = ax.patches[n_before:]
    for p in new_bars:
        p.set_hatch(hatch)
    return fig, ax

def _plot_world_population_share_cities_above_1m_scenarios(fig: plt.Figure, ax: plt.Axes, extr: pd.DataFrame, prop_growth: pd.DataFrame, inc_returns: pd.DataFrame, model: pd.DataFrame, data: pd.DataFrame) -> Tuple[plt.Figure, plt.Axes]:
    x_label = r'$\mathbf{Share \ of \ world \ population \ in \ 1M+ \ cities}$'
    y_label = ' '
    
    _plot_bar_share_cities_above_1m(fig=fig, ax=ax, df=data, year=1975, label='1975 (DT)', hatch=None)
    _plot_bar_share_cities_above_1m(fig=fig, ax=ax, df=extr, year=2025, label='2025 (DT)', hatch=None)
    _plot_bar_share_cities_above_1m(fig=fig, ax=ax, df=prop_growth, year=2100, label='2100 (PG)',  hatch=None)
    _plot_bar_share_cities_above_1m(fig=fig, ax=ax, df=model, year=2100, label='2100 (OM)', hatch=None)
    _plot_bar_share_cities_above_1m(fig=fig, ax=ax, df=extr, year=2100, label='2100 (EX)', hatch=None)
    _plot_bar_share_cities_above_1m(fig=fig, ax=ax, df=inc_returns, year=2100, label='2100 (IR)', hatch=None)

    style_axes(ax=ax, ylabel=y_label, xlabel=x_label)
    ax.xaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))

    plt.setp(ax.get_yticklabels(), ha='left', position=(-0.21, 0))
    return fig, ax


@dg.asset(
    deps=[TableNamesResource().names.usa.figures.usa_rank_vs_size(), TableNamesResource().names.world.figures.world_rank_vs_size(),
    TableNamesResource().names.usa.figures.usa_rank_size_slopes_change(), TableNamesResource().names.world.figures.world_rank_size_slopes_change(),TableNamesResource().names.world.figures.world_rank_size_slopes_change_1975_2025(),
    TableNamesResource().names.world.figures.world_rank_size_slopes(), TableNamesResource().names.world.figures.world_tot_pop_share_cities_above_1m_projections_extr(), TableNamesResource().names.world.figures.world_tot_pop_share_cities_above_1m_projections_prop_growth(), TableNamesResource().names.world.figures.world_tot_pop_share_cities_above_1m_projections_inc_returns(), TableNamesResource().names.world.figures.world_tot_pop_share_cities_above_1m()],
    group_name="figures"
)
def figure_3(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    # Create a figure
    context.log.info(f"Creating figure 3")
    apply_figure_theme()
    figure_file_name = 'figure_3.png'

    fig = plt.figure(figsize=(10,10))
    gs0 = gridspec.GridSpec(2, 2, wspace=0.3, hspace=0.25)
    ax1 = plt.subplot(gs0[0, 0])
    ax2 = plt.subplot(gs0[0, 1])
    ax3 = plt.subplot(gs0[1, 0])
    gs00 = gridspec.GridSpecFromSubplotSpec(2, 1, subplot_spec=gs0[1, 1], hspace=0.4, height_ratios=[2, 3])
    ax4 = plt.subplot(gs00[0, 0])
    ax5 = plt.subplot(gs00[1, 0])

    engine = postgres.get_engine()

    usa_rank_vs_size = read_pandas(engine=engine, table=tables.names.usa.figures.usa_rank_vs_size(), analysis_id=MAIN_ANALYSIS_ID)
    kor_rank_vs_size = read_pandas(engine=engine, table=tables.names.world.figures.world_rank_vs_size(), analysis_id=MAIN_ANALYSIS_ID, where="country = 'KOR'")
    _plot_rank_vs_size_usa_kor(fig=fig, ax=ax1, df_usa=usa_rank_vs_size, df_kor=kor_rank_vs_size)

    usa_rank_size_slopes_change = read_pandas(engine=engine, table=tables.names.usa.figures.usa_rank_size_slopes_change(), analysis_id=MAIN_ANALYSIS_ID)
    kor_rank_size_slopes_change = read_pandas(engine=engine, table=tables.names.world.figures.world_rank_size_slopes_change(), analysis_id=MAIN_ANALYSIS_ID, where="country = 'KOR'")
    _plot_rank_size_slope_change_usa_kor(fig=fig, ax=ax2, df_usa=usa_rank_size_slopes_change, df_kor=kor_rank_size_slopes_change)

    y_fig = 0.475
    line = Line2D([0.1, 0.9], [y_fig, y_fig],transform=fig.transFigure, color='0.6', lw=1, ls='--', zorder=1000, clip_on=False)
    fig.add_artist(line)

    world_rank_size_slopes_change_1975_2025 = read_pandas(engine=engine, table=tables.names.world.figures.world_rank_size_slopes_change_1975_2025(), analysis_id=MAIN_ANALYSIS_ID)
    _plot_rank_size_slope_change_by_urbanization_group(fig=fig, ax=ax3, df=world_rank_size_slopes_change_1975_2025)

    world_rank_size_slopes = read_pandas(engine=engine, table=tables.names.world.figures.world_rank_size_slopes(), analysis_id=MAIN_ANALYSIS_ID)
    _plot_rank_size_slope_by_urbanization_group(fig=fig, ax=ax4, df=world_rank_size_slopes)

    world_share_cities_above_1m_projections_extr = read_pandas(engine=engine, table=tables.names.world.figures.world_tot_pop_share_cities_above_1m_projections_extr(), analysis_id=MAIN_ANALYSIS_ID)
    world_share_cities_above_1m_projections_prop_growth = read_pandas(engine=engine, table=tables.names.world.figures.world_tot_pop_share_cities_above_1m_projections_prop_growth(), analysis_id=MAIN_ANALYSIS_ID)
    world_share_cities_above_1m_projections_inc_returns = read_pandas(engine=engine, table=tables.names.world.figures.world_tot_pop_share_cities_above_1m_projections_inc_returns(), analysis_id=MAIN_ANALYSIS_ID)
    world_share_cities_above_1m_projections_model = read_pandas(engine=engine, table=tables.names.world.figures.world_tot_pop_share_cities_above_1m(), analysis_id=MAIN_ANALYSIS_ID, where="year > 2025")
    world_share_cities_above_1m_data = read_pandas(engine=engine, table=tables.names.world.figures.world_tot_pop_share_cities_above_1m(), analysis_id=MAIN_ANALYSIS_ID, where="year <= 1975")
    _plot_world_population_share_cities_above_1m_scenarios(fig=fig, ax=ax5, extr=world_share_cities_above_1m_projections_extr, prop_growth=world_share_cities_above_1m_projections_prop_growth, inc_returns=world_share_cities_above_1m_projections_inc_returns, model=world_share_cities_above_1m_projections_model, data=world_share_cities_above_1m_data)

    annotate_letter_label(axes=[ax1, ax2, ax3, ax4, ax5], left_side=[True, False, False, False, False])
    save_figure(fig=fig, figure_file_name=figure_file_name)

    return materialize_image(figure_file_name=figure_file_name)