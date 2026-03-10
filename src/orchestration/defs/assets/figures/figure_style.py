from typing import List, Optional

import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import colorsys
from mpl_toolkits.axes_grid1.inset_locator import inset_axes
import seaborn as sns
import plotly.express as px


style_config = {
    'font_name': 'Helvetica',
    'font_family': 'sans-serif',
    'title_font_size': 14,
    'label_font_size': 12,
    'tick_font_size': 10,
    'inset_title_font_size': 12,
    'inset_label_font_size': 10,
    'inset_tick_font_size': 8,
    'inset_text_label_font_size': 8,
    'letter_label_font_size': 16,
}

region_colors = {
    'Africa': px.colors.qualitative.Plotly[1],
    'Americas': px.colors.qualitative.Plotly[2],
    'Asia': px.colors.qualitative.Plotly[3],
    'Europe': px.colors.qualitative.Plotly[5]
}


def apply_figure_theme() -> None:
    """
    Set rcParams from a simple style_config dict.
    """
    mpl.rcParams.update({
        "font.family": style_config.get("font_family", "Helvetica")
    })


def _style_axes(ax: plt.Axes, *, title: str = "", xlabel: str = "", ylabel: str = "", title_font_size: int = None, label_font_size: int = None, tick_font_size: int = None, legend_loc: str = "") -> None:
    if title: ax.set_title(title)
    if xlabel: ax.set_xlabel(xlabel, fontsize=label_font_size)
    if ylabel: ax.set_ylabel(ylabel, fontsize=label_font_size)
    if title: ax.set_title(title, fontsize=title_font_size)
    if tick_font_size: ax.tick_params(axis='both', which='major', labelsize=tick_font_size)
    if legend_loc: ax.legend(loc=legend_loc, frameon=False, fontsize=label_font_size)
    sns.despine(ax=ax)


def style_axes(ax: plt.Axes, *, title: str = "", xlabel: str = "", ylabel: str = "", legend_loc: str = "") -> None:
    _style_axes(ax=ax, title=title, xlabel=xlabel, ylabel=ylabel, title_font_size=style_config['title_font_size'], label_font_size=style_config['label_font_size'], tick_font_size=style_config['tick_font_size'], legend_loc=legend_loc)

def style_inset_axes(ax: plt.Axes, *, xlabel: str = "", ylabel: str = "", title: str = "", legend_loc: str = "") -> None:
    _style_axes(ax=ax, xlabel=xlabel, ylabel=ylabel, title=title, title_font_size=style_config['inset_title_font_size'], label_font_size=style_config['inset_label_font_size'], tick_font_size=style_config['inset_tick_font_size'], legend_loc=legend_loc)


def annotate_letter_label(axes: List[plt.Axes], left_side: List[bool]) -> None:
    for i, ax in enumerate(axes):
        y = 0.99
        x = 0.05 if left_side[i] else 0.95
        ax.annotate(
            text=f'{chr(65 + i)}',
            xy=(x, y),
            xycoords='axes fraction',
            ha='left',
            va='top',
            fontsize=style_config['letter_label_font_size'],
            fontweight='bold'
        )


def plot_spline_with_ci(ax: plt.Axes, x: np.ndarray, y: np.ndarray, ci_low: np.ndarray, ci_high: np.ndarray, color: str, label: Optional[str] = None, linewidth: float = 2.0, alpha_fill: float = 0.2) -> None:
    ax.plot(x, y, color=color, lw=linewidth, label=label)
    ax.fill_between(x, ci_low, ci_high, color=color, alpha=alpha_fill)


def create_bicolor_cmap(cmap_neg: str, cmap_pos: str, midpoint_frac: float, name: str = 'bicolor_cmap', N: int = 256) -> mcolors.LinearSegmentedColormap:
    cmap_neg_obj = plt.get_cmap(cmap_neg)
    cmap_pos_obj = plt.get_cmap(cmap_pos)

    n_neg = int(np.round(N * midpoint_frac))
    n_pos = N - n_neg

    neg_colors = cmap_neg_obj(np.linspace(1 - midpoint_frac, 1, n_neg))
    pos_colors = cmap_pos_obj(np.linspace(0.0, 1 - midpoint_frac, n_pos))
    
    all_colors = np.vstack((neg_colors, pos_colors))
    return mcolors.LinearSegmentedColormap.from_list(name, all_colors)

def get_light_variant_of_hex(base_hex: str) -> tuple[list[str], list[str]]:
    base_rgb = mcolors.to_rgb(base_hex)
    h, l, s = colorsys.rgb_to_hls(*base_rgb)
    light_l = l + (1.0 - l) * 0.6
    light_rgb = colorsys.hls_to_rgb(h, light_l, s)
    light_hex = mcolors.to_hex(light_rgb)
    return light_hex


def format_population_ticks(ax: plt.Axes, is_xaxis: bool = True) -> None:
    ticks = [4, 5, 6, 7]
    tick_labels = ['10K', '100K', '1M', '10M']
    if is_xaxis:
        ax.set_xticks(ticks)
        ax.set_xticklabels(tick_labels)
    else:
        ax.set_yticks(ticks)
        ax.set_yticklabels(tick_labels)
