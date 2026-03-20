# src/orchestration/defs/assets/figures/si/si_linear_rigidity.py

import numpy as np
import pandas as pd
from scipy.stats import norm
import statsmodels.formula.api as smf
from pygam import LinearGAM, s
import dagster as dg
from typing import Dict, Any, Tuple, List
import plotly.express as px
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib import gridspec
from sqlalchemy.engine import Engine

from ....resources.resources import PostgresResource, TableNamesResource
from ..figure_style import style_axes, style_config, annotate_letter_label, style_inset_axes, format_population_ticks
from ..figure_io import read_pandas, save_figure, materialize_image
from ...stats_utils import fit_penalized_b_spline
from ...constants import constants

MAIN_ANALYSIS_ID = constants['MAIN_ANALYSIS_ID']

def _plot_non_linearity_figure_size_growth(fig: plt.Figure, ax: plt.Axes, colors: List[str], title: str, df_size_vs_growth: pd.DataFrame, df_test_results: pd.DataFrame, yaxlim: tuple = None):
    x_axis = 'log_population'
    y_axis = 'log_growth'

    x_axis_label = r'$\mathbf{Size}$ (log-scale)'
    y_axis_label = r'$\mathbf{Growth \ rate} \ (\log_{10}S_{t+10} \ / \ S_t)$'

    x_axis_inset = 'D_bootstrap'

    x_axis_inset_label = 'Test statistic ($D$)'
    y_axis_inset_label = 'Count'

    lam = constants['PENALTY_SIZE_GROWTH_CURVE']
    
    x, y_spline, ci_low_spline, ci_high_spline = fit_penalized_b_spline(df=df_size_vs_growth, xaxis=x_axis, yaxis=y_axis, lam=lam)
    lin_model = smf.ols(f'{y_axis} ~ {x_axis}', data=df_size_vs_growth).fit()
    y_lin = lin_model.predict(pd.DataFrame({x_axis: x}))
    ax.scatter(df_size_vs_growth[x_axis], df_size_vs_growth[y_axis], alpha=0.3, color='grey')
    ax.plot(x, y_lin, color=colors[0], linewidth=2,  label='Linear')
    ax.plot(x, y_spline, color=colors[1], linewidth=2, label='Spline')

    ax_inset = ax.inset_axes([0.65, 0.85, 0.3, 0.1])
    ax_inset.hist(df_test_results[x_axis_inset], bins=20, color='grey', alpha=0.5)
    ax_inset.axvline(df_test_results['D_true'].unique()[0], color='black', linewidth=2, linestyle='--')

    style_inset_axes(ax=ax_inset, xlabel=x_axis_inset_label, ylabel=y_axis_inset_label)
    ax_inset.set_yticks([])

    style_axes(ax=ax, title=title, xlabel=x_axis_label, ylabel=y_axis_label)
    format_population_ticks(ax=ax, is_xaxis=True)
    if yaxlim is not None:
        ax.set_ylim(yaxlim)
    return fig, ax

def _plot_non_linearity_figure_rank_size(fig: plt.Figure, ax: plt.Axes, colors: List[str], title: str, plot_legend: bool, df_rank_vs_size: pd.DataFrame):
    x_axis = 'log_rank'
    y_axis = 'log_population'

    x_axis_label = r'$\mathbf{Rank}$ (log-scale)'
    y_axis_label = r'$\mathbf{Size}$ (log-scale)'

    lam = constants['PENALTY_RANK_SIZE_CURVE']

    x, y_spline, ci_low_spline, ci_high_spline = fit_penalized_b_spline(df=df_rank_vs_size, xaxis=x_axis, yaxis=y_axis, lam=lam)
    lin_model = smf.ols(f'{y_axis} ~ {x_axis}', data=df_rank_vs_size).fit()
    y_lin = lin_model.predict(pd.DataFrame({x_axis: x}))

    ax.scatter(df_rank_vs_size[x_axis], df_rank_vs_size[y_axis], alpha=0.3, color='grey')
    ax.plot(x, y_lin, color=colors[0], linewidth=2,  label='Linear')
    ax.plot(x, y_spline, color=colors[1], linewidth=2, label='Spline')

    style_axes(ax=ax, title=title, xlabel=x_axis_label, ylabel=y_axis_label)
    format_population_ticks(ax=ax, is_xaxis=False)

    max_rank = int(np.round(df_rank_vs_size['log_rank'].max(), decimals=0))
    ax.set_xticks(np.arange(0, max_rank + 1, 1))
    tick_labels = ['1', '10', '100', '1K', '10K']
    ax.set_xticklabels(tick_labels[:len(ax.get_xticks())])

    if plot_legend:
        ax.legend(loc='upper right', frameon=False, fontsize=style_config['label_font_size'], ncol=2, bbox_to_anchor=(0.25, 1.2))
    return fig, ax


def _plot_barchart_slope_ols_vs_spline(fig: plt.Figure, ax: plt.Axes, colors: List[str], x_axis_label: str, df: pd.DataFrame):
    df = df.sort_values(by='country', ascending=False)
    sns.barplot(x='slope', y='country', hue='model', data=df, ax=ax, palette=colors, orient='h')
    style_axes(ax=ax, xlabel=x_axis_label, ylabel=' ', legend_loc='center')
    ax.legend(loc='center', frameon=False, fontsize=style_config['label_font_size'], ncol=1, bbox_to_anchor=(0.5, 1.2), ncols=2)
    return fig, ax


def _plot_size_growth_linearity_test_results(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame):
    x_axis_label = r'$\mathbf{p\!-\!value \ threshold}$'
    y_axis_label = r'$\mathbf{Share \ of \ samples \ with}$' + '\n' + r'$\mathbf{p\!-\!value < \ threshold}$'

    pvalues = df.groupby(['country', 'year']).agg({'reject_linear': 'mean'}).reset_index().rename(columns={'reject_linear': 'pvalue'})
    pval = np.linspace(0.001, 0.1, 10)
    ys = []
    for p in pval:
        ys.append(pvalues[pvalues['pvalue'] < p].shape[0] / len(pvalues))

    ax.plot(pval, ys, color='black', linewidth=2)
    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label, legend_loc='upper right')
    ax.set_ylim(0, 0.4)
    return fig, ax


def _plot_rank_size_linearity_test_results(fig: plt.Figure, ax: plt.Axes, df: pd.DataFrame):
    x_axis_label = r'$\mathbf{Max \ residual \ threshold}$'
    y_axis_label = r'$\mathbf{Share \ of \ samples \ with}$' + '\n' + r'$\mathbf{max \ residual > \ threshold}$'

    max_resid_thresholds = np.linspace(0.5, df['max_resid'].max(), 10)
    ys = []
    for mr in max_resid_thresholds:
        ys.append(df[df['max_resid'] > mr].shape[0] / len(df))


    ax.plot(max_resid_thresholds, ys, color='black', linewidth=2)
    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label, legend_loc='upper right')
    style_axes(ax=ax, xlabel=x_axis_label, ylabel=y_axis_label, legend_loc='upper right')
    ax.set_ylim(0, 0.3)
    return fig, ax


def _get_data_and_test_results(engine: Engine, table_data: str, table_test_results: str, country: str, year: int) -> pd.DataFrame:
    data = read_pandas(engine=engine, 
                                    table=table_data, 
                                    analysis_id=MAIN_ANALYSIS_ID, 
                                    where=f"country = '{country}' AND year = {year}")

    test_results = read_pandas(engine=engine, 
                                    table=table_test_results, 
                                    analysis_id=MAIN_ANALYSIS_ID, 
                                    where=f"country = '{country}' AND year = {year}")
    return data, test_results


def _get_data_linear_vs_spline_slope_for_multiple_country_year(engine: Engine, table_name: str, column_name: str, country_year_list: List[Tuple[str, int]]) -> pd.DataFrame:
    slopes = []
    for country, year in country_year_list:
        slopes.append(_get_data_linear_vs_spline_slope(engine=engine, table_name=table_name, column_name=column_name, country=country, year=year))
    return pd.concat(slopes)


def _get_data_linear_vs_spline_slope(engine: Engine, table_name: str, column_name: str, country: str, year: int) -> pd.DataFrame:
    col_name_ols = f'{column_name}_ols'
    col_name_spline = f'{column_name}_spline'
    q = f"""
    SELECT  country, 
            year, 
            s.{column_name} AS {col_name_spline}, 
            o.{column_name} AS {col_name_ols}
    FROM {table_name} s
    JOIN {table_name}_ols o
    USING (country, year, analysis_id)
    WHERE analysis_id = {MAIN_ANALYSIS_ID}
    AND country = '{country}'
    AND year = {year}
    """
    df = pd.read_sql(q, con=engine)

    slope_ols = df[col_name_ols].values[0]
    slope_spline = df[col_name_spline].values[0]
    slopes = pd.DataFrame({
        'country': country,
        'model': ['Linear', 'Spline'],
        'slope': [slope_ols, slope_spline]
    })
    return slopes
      
@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_rank_vs_size(), TableNamesResource().names.world.figures.world_size_vs_growth(), TableNamesResource().names.world.si.world_linearity_test_size_vs_growth(), TableNamesResource().names.world.si.world_linearity_test_rank_vs_size(), TableNamesResource().names.world.figures.world_rank_size_slopes(), TableNamesResource().names.world.figures.world_size_growth_slopes_historical()],
    group_name="si_figures",
    io_manager_key="postgres_io_manager"
)
def si_figure_linear_rigidity(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    context.log.info("Plotting figure SI linear rigidity")
    figure_file_name = 'si_figure_linear_rigidity.png'

    fig = plt.figure(figsize=(15, 10))
    gs1 = gridspec.GridSpec(2, 3, wspace=0.3, hspace=0.3)
    ax1 = plt.subplot(gs1[0,0])
    ax2 = plt.subplot(gs1[0,1])
    gs2 = gridspec.GridSpecFromSubplotSpec(2, 1, subplot_spec=gs1[0,2], height_ratios=[1, 2], wspace=0.3, hspace=0.4)
    ax3 = plt.subplot(gs2[0,0])
    ax4 = plt.subplot(gs2[1,0])

    ax5 = plt.subplot(gs1[1,0])
    ax6 = plt.subplot(gs1[1,1])
    gs3 = gridspec.GridSpecFromSubplotSpec(2, 1, subplot_spec=gs1[1,2], height_ratios=[1, 2], wspace=0.3, hspace=0.4)
    ax7 = plt.subplot(gs3[0,0])
    ax8 = plt.subplot(gs3[1,0])

    colors = [px.colors.qualitative.Plotly[4], px.colors.qualitative.Plotly[6]]

    # Size growth

    size_growth_country_year_1 = ('IDN', 1975)
    size_growth_country_year_2 = ('BRA', 2010)

    idn_size_vs_growth, idn_test_results = _get_data_and_test_results(engine=postgres.get_engine(), 
                                                                                table_data=tables.names.world.figures.world_size_vs_growth(), 
                                                                                table_test_results=tables.names.world.si.world_linearity_test_size_vs_growth(), 
                                                                                country=size_growth_country_year_1[0], 
                                                                                year=size_growth_country_year_1[1])

    _plot_non_linearity_figure_size_growth(fig=fig, ax=ax1, colors=colors, title='Indonesia', df_size_vs_growth=idn_size_vs_growth, df_test_results=idn_test_results)

    bra_size_vs_growth, bra_test_results = _get_data_and_test_results(engine=postgres.get_engine(), 
                                                                                table_data=tables.names.world.figures.world_size_vs_growth(), 
                                                                                table_test_results=tables.names.world.si.world_linearity_test_size_vs_growth(), 
                                                                                country=size_growth_country_year_2[0], 
                                                                                year=size_growth_country_year_2[1])

    _plot_non_linearity_figure_size_growth(fig=fig, ax=ax2, colors=colors, title='Brazil', df_size_vs_growth=bra_size_vs_growth, df_test_results=bra_test_results, yaxlim=(-0.08, 0.2))

    size_growth_slopes = _get_data_linear_vs_spline_slope_for_multiple_country_year(engine=postgres.get_engine(), 
                                                                                    table_name=tables.names.world.figures.world_size_growth_slopes_historical(), 
                                                                                    column_name='size_growth_slope', 
                                                                                    country_year_list=[size_growth_country_year_1, size_growth_country_year_2])

    _plot_barchart_slope_ols_vs_spline(fig=fig, ax=ax3, colors=colors, x_axis_label=r'$\mathbf{Size\!-\!growth \ slope \ \beta}$', df=size_growth_slopes)

    linearity_test_results = read_pandas(engine=postgres.get_engine(), 
                                        table=tables.names.world.si.world_linearity_test_size_vs_growth(), 
                                        analysis_id=MAIN_ANALYSIS_ID)
    _plot_size_growth_linearity_test_results(fig=fig, ax=ax4, df=linearity_test_results)


    # Rank size

    rank_size_country_year_1 = ('THA', 1990)
    rank_size_country_year_2 = ('CHN', 1990)

    thai_rank_vs_size = read_pandas(engine=postgres.get_engine(), 
                                    table=tables.names.world.figures.world_rank_vs_size(), 
                                    analysis_id=MAIN_ANALYSIS_ID, 
                                    where=f"country = '{rank_size_country_year_1[0]}' AND year = {rank_size_country_year_1[1]}")
    _plot_non_linearity_figure_rank_size(fig=fig, ax=ax5, colors=colors, title='Thailand', df_rank_vs_size=thai_rank_vs_size, plot_legend=False)

    chn_rank_vs_size = read_pandas(engine=postgres.get_engine(), 
                                 table=tables.names.world.figures.world_rank_vs_size(), 
                                 analysis_id=MAIN_ANALYSIS_ID, 
                                 where=f"country = '{rank_size_country_year_2[0]}' AND year = {rank_size_country_year_2[1]}")
    _plot_non_linearity_figure_rank_size(fig=fig, ax=ax6, colors=colors, title='China', df_rank_vs_size=chn_rank_vs_size, plot_legend=True)

    rank_size_slopes = _get_data_linear_vs_spline_slope_for_multiple_country_year(engine=postgres.get_engine(), 
                                                                                    table_name=tables.names.world.figures.world_rank_size_slopes_historical(), 
                                                                                    column_name='rank_size_slope', 
                                                                                    country_year_list=[rank_size_country_year_1, rank_size_country_year_2])

    _plot_barchart_slope_ols_vs_spline(fig=fig, ax=ax7, colors=colors, x_axis_label=r'$\mathbf{Rank\!-\!size \ slope \ \alpha}$', df=rank_size_slopes)


    linearity_test_results = read_pandas(engine=postgres.get_engine(), 
                                        table=tables.names.world.si.world_linearity_test_rank_vs_size(), 
                                        analysis_id=MAIN_ANALYSIS_ID)
    _plot_rank_size_linearity_test_results(fig=fig, ax=ax8, df=linearity_test_results)

    annotate_letter_label(axes=[ax1, ax2, ax3, ax4, ax5, ax6, ax7, ax8], left_side=[True, True, False, True, False, False, False, True])
    save_figure(fig=fig, figure_file_name=figure_file_name, si=True)
    return materialize_image(figure_file_name=figure_file_name, si=True)