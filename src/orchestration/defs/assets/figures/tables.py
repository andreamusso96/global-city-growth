# src/orchestration/defs/assets/figures/tables.py
import dagster as dg
import pandas as pd
import statsmodels.formula.api as smf

from ...resources.resources import PostgresResource, TableNamesResource
from .figure_io import read_pandas, save_latex_table, materialize_table
from ..constants import constants

MAIN_ANALYSIS_ID = constants['MAIN_ANALYSIS_ID']


def _make_table_slopes_by_urbanization(
    df: pd.DataFrame,
    x_axis: str,
    y_axis: str,
    fixed_effect_col: str,
) -> pd.DataFrame:
    ols_reg_no_fe = smf.ols(f'{y_axis} ~ {x_axis}', data=df).fit()
    ols_reg_with_fe = smf.ols(f'{y_axis} ~ {x_axis} + C({fixed_effect_col})', data=df).fit()

    results = []

    for model in [ols_reg_no_fe, ols_reg_with_fe]:
        coef = model.params.get(x_axis)
        bse = model.bse.get(x_axis)
        pval = model.pvalues.get(x_axis)
        r2 = model.rsquared
        nobs = model.nobs
        results.append([coef, bse, pval, r2, nobs])

    country_fe = ['no', 'yes']

    table = pd.DataFrame(results, columns=['coef', 'bse', 'pval', 'r2', 'nobs'])
    table['country_fe'] = country_fe
    return table

def _format_table_slopes_by_urbanization_for_latex(
    table: pd.DataFrame,
    x_label: str,
    fixed_effect_label: str,
) -> pd.DataFrame:
    m = len(table)  

    def stars(p): return '***' if p<0.01 else '**' if p<0.05 else '*' if p<0.10 else ''
    coef_str = [f"{table.loc[i,'coef']:.3f}{stars(table.loc[i,'pval'])}" for i in range(m)]
    bse_str   = [f"({table.loc[i,'bse']:.3f})" for i in range(m)]
    n_str    = [f"{int(table.loc[i,'nobs'])}" for i in range(m)]
    r2_str   = [f"{table.loc[i,'r2']:.3f}" for i in range(m)]
    cfe_str  = [str(table.loc[i,'country_fe']).capitalize() for i in range(m)]

    out = pd.DataFrame([
        [x_label,              *coef_str],
        ["",                   *bse_str],
        [fixed_effect_label,   *cfe_str],
        ["Observations",         *n_str],
        [r"$R^2$",               *r2_str],
    ], columns=[''] + [f"col{i}" for i in range(1, m+1)]) 

    return out

def _get_latex_from_formatted_table_slopes_by_urbanization(table: pd.DataFrame, y_label) -> str:
    colfmt = '@{\\extracolsep{\\fill}}lcc'

    latex = table.to_latex(
        index=False,
        header=False,
        escape=False,
        column_format=colfmt,
    ).replace("\r\n", "\n").replace("\r", "\n")

    # switch tabular -> tabular* without regex (avoids \l issue)
    latex = latex.replace(r"\begin{tabular}{", "\\begin{tabular*}{0.8\\linewidth}{", 1)
    latex = latex.replace(r"\end{tabular}", r"\end{tabular*}", 1)

    # header/footer
    header_line = (
        r"\\[-1.8ex]\hline"
        r"\hline \\[-1.8ex]"
        f"Independent \\textbackslash{{}} Dependent& "
        f"\\multicolumn{{2}}{{c}}{{{y_label}}} \\\\\n"
    )
    footer_line = (
        r"\hline"
        r"\hline \\[-1.8ex]"
        f"& \\multicolumn{{2}}{{r}}{{$^{{*}}$p$<$0.1; $^{{**}}$p$<$0.05; $^{{***}}$p$<$0.01}} \\\\\n"
    )

    latex = latex.replace("\\toprule\n", header_line, 1)
    latex = latex.replace("\\bottomrule\n", footer_line, 1)
    return latex



def make_table_slopes_by_urbanization(
    df_size_growth_slopes: pd.DataFrame,
    fixed_effect_col: str = 'country',
    fixed_effect_label: str = 'Country fixed effect',
    x_axis: str = 'urban_population_share',
    y_axis: str = 'size_growth_slope',
    x_label: str = 'Urban population share',
    y_label: str = 'Size-growth slope',
) -> str:
    table = _make_table_slopes_by_urbanization(
        df=df_size_growth_slopes,
        x_axis=x_axis,
        y_axis=y_axis,
        fixed_effect_col=fixed_effect_col,
    )
    table_formatted = _format_table_slopes_by_urbanization_for_latex(
        table=table,
        x_label=x_label,
        fixed_effect_label=fixed_effect_label,
    )
    latex = _get_latex_from_formatted_table_slopes_by_urbanization(
        table=table_formatted,
        y_label=y_label,
    )
    return latex


def make_table_2(df_size_growth_slopes: pd.DataFrame) -> str:
    x_axis = 'urban_population_share'
    y_axis = 'size_growth_slope'
    x_label = 'Urban population share'
    y_label = 'Size-growth slope'
    return make_table_slopes_by_urbanization(
        df_size_growth_slopes=df_size_growth_slopes,
        fixed_effect_col='country',
        fixed_effect_label='Country fixed effect',
        x_axis=x_axis,
        y_axis=y_axis,
        x_label=x_label,
        y_label=y_label,
    )

def _get_latex_from_formatted_table_dataset_summary(table: pd.DataFrame) -> str:
    colfmt = '@{\\extracolsep{\\fill}}' + ('l' + 'c' * table.shape[1])
    latex = table.to_latex(
        index=False,
        escape=False,
        column_format=colfmt
    )
    latex = latex.replace(r"\begin{tabular}{", "\\begin{tabular*}{0.8\\linewidth}{", 1)
    latex = latex.replace(r"\end{tabular}", r"\end{tabular*}", 1)

    latex = latex.replace("\r\n", "\n").replace("\r", "\n")
    header_line = (
        r"\\[-1.8ex]\hline"
        r"\hline \\[-1.8ex]"
    )   

    footer_line = (
        r"\hline"
        r"\hline \\[-1.8ex]"
    )

    latex = latex.replace("\\toprule\n", header_line, 1)
    latex = latex.replace("\\bottomrule\n", footer_line, 1)
    return latex

def _make_table_dataset_summary(df_usa_dataset_summary_table: pd.DataFrame, df_world_dataset_summary_table: pd.DataFrame) -> str:
    table = pd.concat([df_world_dataset_summary_table, df_usa_dataset_summary_table], axis=0)
    table = table.drop(columns=['analysis_id'])
    table['Dataset'] = ['Global cities', 'USA cities']
    table = table.rename(columns={'num_cities': 'Cities', 'num_countries': 'Countries', 'num_years': 'Years', 'min_year': 'Min year', 'max_year': 'Max year'})
    col_order = ['Dataset'] + [c for c in table.columns if c != 'Dataset']
    table = table[col_order]
    latex = _get_latex_from_formatted_table_dataset_summary(table=table)
    return latex

@dg.asset(
    deps=[TableNamesResource().names.usa.figures.usa_dataset_summary_table(), TableNamesResource().names.world.figures.world_dataset_summary_table()],
    group_name="figures"
)
def table_1(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    context.log.info(f"Creating table 1")
    table_file_name = 'table_1.txt'
    usa_dataset_summary_table = read_pandas(engine=postgres.get_engine(), table=tables.names.usa.figures.usa_dataset_summary_table(), analysis_id=MAIN_ANALYSIS_ID)
    world_dataset_summary_table = read_pandas(engine=postgres.get_engine(), table=tables.names.world.figures.world_dataset_summary_table(), analysis_id=MAIN_ANALYSIS_ID)
    latex_table = _make_table_dataset_summary(df_usa_dataset_summary_table=usa_dataset_summary_table, df_world_dataset_summary_table=world_dataset_summary_table)
    save_latex_table(table=latex_table, table_file_name=table_file_name)

    return materialize_table(table_file_name=table_file_name)



@dg.asset(
    deps=[TableNamesResource().names.world.figures.world_size_growth_slopes_historical_urbanization()],
    group_name="figures"
)
def table_2(context: dg.AssetExecutionContext, postgres: PostgresResource, tables: TableNamesResource) -> dg.MaterializeResult:
    # Create a figure
    context.log.info(f"Creating table 2")
    table_file_name = 'table_2.txt'

    world_size_growth_slopes_urbanization = read_pandas(engine=postgres.get_engine(), table=tables.names.world.figures.world_size_growth_slopes_historical_urbanization(), analysis_id=MAIN_ANALYSIS_ID)
    latex_table = make_table_2(df_size_growth_slopes=world_size_growth_slopes_urbanization)
    save_latex_table(table=latex_table, table_file_name=table_file_name)
    return materialize_table(table_file_name=table_file_name)
