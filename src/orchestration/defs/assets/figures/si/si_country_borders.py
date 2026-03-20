from pathlib import Path
from typing import List, Tuple

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
from ...stats_utils import fit_penalized_b_spline, get_mean_derivative_penalized_b_spline
from ..figure_io import materialize_image, read_pandas, read_postgis, save_figure
from ..figure_style import (
    annotate_letter_label,
    apply_figure_theme,
    create_bicolor_cmap,
    plot_spline_with_ci,
    style_inset_axes,
)

MAIN_ANALYSIS_ID = constants["MAIN_ANALYSIS_ID"]
PENALTY_SIZE_GROWTH_CURVE = constants["PENALTY_SIZE_GROWTH_CURVE"]
PENALTY_SLOPE_SPLINE = constants["PENALTY_SLOPE_SPLINE"]

MIN_SUBREGION_CITY_COUNT = 50
FIGURE_FILE_NAME = "si_figure_country_borders.png"
WORLD_POPULATION_TABLE = "world_population"


def _resolve_micro_regions_path() -> Path:
    repo_root = Path(__file__).resolve().parents[6]
    candidates = [
        repo_root / "revisions2" / "data" / "micro-regions.csv",
        Path.cwd() / "revisions2" / "data" / "micro-regions.csv",
    ]
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError("Could not locate revisions2/data/micro-regions.csv")


def _load_micro_regions() -> pd.DataFrame:
    path = _resolve_micro_regions_path()
    df = pd.read_csv(path, sep=";")
    df = df[["Sub-region Name", "ISO-alpha3 Code"]].rename(
        columns={"Sub-region Name": "micro_region", "ISO-alpha3 Code": "country"}
    )
    return df.dropna().drop_duplicates().reset_index(drop=True)


def _get_subregion_borders(
    postgres: PostgresResource,
    tables: TableNamesResource,
    micro_regions: pd.DataFrame,
) -> gpd.GeoDataFrame:
    country_borders = read_postgis(
        engine=postgres.get_engine(),
        table=tables.names.world.figures.world_average_size_growth_slope_with_borders(),
        analysis_id=MAIN_ANALYSIS_ID,
        cols="country, geom",
    )
    country_borders = country_borders.drop_duplicates(subset=["country"]).merge(
        micro_regions, on="country", how="left"
    )
    country_borders["micro_region"] = country_borders["micro_region"].fillna("Other")
    country_borders = gpd.GeoDataFrame(
        country_borders[["micro_region", "geom"]],
        geometry="geom",
        crs=country_borders.crs,
    )
    subregion_borders = country_borders.dissolve(by="micro_region").reset_index()
    return gpd.GeoDataFrame(subregion_borders, geometry="geom", crs=country_borders.crs)


def _get_subregion_slopes(
    postgres: PostgresResource,
    tables: TableNamesResource,
    micro_regions: pd.DataFrame,
) -> pd.DataFrame:
    size_vs_growth = read_pandas(
        engine=postgres.get_engine(),
        table=tables.names.world.figures.world_size_vs_growth(),
        analysis_id=MAIN_ANALYSIS_ID,
        cols="country, year, log_population, log_growth",
    )
    size_vs_growth = size_vs_growth.merge(micro_regions, on="country", how="inner")

    rows = []
    grouped = size_vs_growth.groupby(["micro_region", "year"], sort=True)
    for (micro_region, year), df_group in grouped:
        rows.append(
            {
                "micro_region": micro_region,
                "year": year,
                "size_growth_slope": get_mean_derivative_penalized_b_spline(
                    df=df_group,
                    xaxis="log_population",
                    yaxis="log_growth",
                    lam=PENALTY_SIZE_GROWTH_CURVE,
                ),
                "n_cities": df_group.shape[0],
            }
        )

    return pd.DataFrame(rows)


def _get_subregion_urbanization(
    postgres: PostgresResource,
    tables: TableNamesResource,
    micro_regions: pd.DataFrame,
) -> pd.DataFrame:
    q = f"""
    SELECT  p.country,
            p.year,
            p.population,
            u.urban_population_share
    FROM {WORLD_POPULATION_TABLE} p
    JOIN {tables.names.world.figures.world_urbanization()} u
    USING (country, year)
    WHERE year >= 1975 AND year <= 2025
    """
    urbanization = pd.read_sql(q, con=postgres.get_engine())
    urbanization = urbanization.merge(micro_regions, on="country", how="inner")
    urbanization["weighted_urban_population_share"] = (
        urbanization["urban_population_share"] * urbanization["population"]
    )
    urbanization = (
        urbanization.groupby(["micro_region", "year"], as_index=False)
        .agg(
            weighted_urban_population_share=("weighted_urban_population_share", "sum"),
            population=("population", "sum"),
        )
        .assign(
            urban_population_share=lambda df: (
                df["weighted_urban_population_share"] / df["population"]
            )
        )
    )
    return urbanization[["micro_region", "year", "urban_population_share"]]


def _prepare_plot_data(
    postgres: PostgresResource,
    tables: TableNamesResource,
) -> Tuple[gpd.GeoDataFrame, pd.DataFrame, List[str]]:
    micro_regions = _load_micro_regions()
    subregion_borders = _get_subregion_borders(
        postgres=postgres, tables=tables, micro_regions=micro_regions
    )
    subregion_slopes = _get_subregion_slopes(
        postgres=postgres, tables=tables, micro_regions=micro_regions
    )
    subregion_urbanization = _get_subregion_urbanization(
        postgres=postgres, tables=tables, micro_regions=micro_regions
    )

    regions_to_drop = (
        subregion_slopes.groupby("micro_region")["n_cities"].min().reset_index()
    )
    regions_to_drop = regions_to_drop[
        regions_to_drop["n_cities"] < MIN_SUBREGION_CITY_COUNT
    ]["micro_region"].tolist()
    regions_to_drop = sorted(set(regions_to_drop + ["Other"]))

    slopes_valid = subregion_slopes[
        ~subregion_slopes["micro_region"].isin(regions_to_drop)
    ].copy()
    slopes_map = (
        slopes_valid.groupby("micro_region", as_index=False)["size_growth_slope"]
        .mean()
    )

    subregion_map = subregion_borders.merge(slopes_map, on="micro_region", how="left")
    subregion_map.loc[
        subregion_map["micro_region"].isin(regions_to_drop), "size_growth_slope"
    ] = np.nan
    subregion_map = gpd.GeoDataFrame(
        subregion_map, geometry="geom", crs=subregion_borders.crs
    )

    slopes_urbanization = slopes_valid.merge(
        subregion_urbanization, on=["micro_region", "year"], how="inner"
    )
    return subregion_map, slopes_urbanization, regions_to_drop


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
        TableNamesResource().names.world.figures.world_size_vs_growth(),
        TableNamesResource().names.world.figures.world_average_size_growth_slope_with_borders(),
        TableNamesResource().names.world.figures.world_urbanization(),
        dg.AssetKey("world_population"),
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
    context.log.info(f"Loading M49 sub-regions from {_resolve_micro_regions_path()}")

    subregion_map, slopes_urbanization, regions_to_drop = _prepare_plot_data(
        postgres=postgres, tables=tables
    )
    context.log.info(f"Regions plotted without data: {regions_to_drop}")

    fig, ax_map = plt.subplots(figsize=(10, 6))
    _plot_subregion_map(fig=fig, ax=ax_map, gdf=subregion_map)

    ax_inset_left = fig.add_axes([0.20, 0.20, 0.12, 0.20])
    ax_inset_right = fig.add_axes([0.64, 0.20, 0.09, 0.16])
    _plot_slope_vs_urbanization_inset(ax=ax_inset_left, df=slopes_urbanization)
    _plot_slope_density_inset(ax=ax_inset_right, df=slopes_urbanization)

    annotate_letter_label(
        axes=[ax_map, ax_inset_left, ax_inset_right],
        left_side=[True, True, True],
    )
    save_figure(fig=fig, figure_file_name=FIGURE_FILE_NAME, si=True)
    return materialize_image(figure_file_name=FIGURE_FILE_NAME, si=True)
