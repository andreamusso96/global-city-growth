# src/orchestration/defs/assets/figures/figure_db.py
from typing import Optional
import pandas as pd
import geopandas as gpd
from sqlalchemy.engine import Engine
import matplotlib.pyplot as plt
import os
import base64
import dagster as dg

figure_dir = '/app/figures'
figure_si_dir = '/app/figures/si'
table_dir = '/app/figures/tables'
table_si_dir = '/app/figures/si/tables'

def read_pandas(engine: Engine, table: str, analysis_id: int, where: Optional[str] = None, cols: str = "*") -> pd.DataFrame:
    q = f"SELECT {cols} FROM {table}" + (f" WHERE analysis_id = {analysis_id}" + (f" AND ({where})" if where else ""))
    return pd.read_sql(q, con=engine)


def read_postgis(engine: Engine, table: str, analysis_id: int, cols: str = "*") -> pd.DataFrame:
    q = f"SELECT {cols} FROM {table}" + (f" WHERE analysis_id = {analysis_id}")
    return gpd.read_postgis(q, con=engine)


def save_figure(fig: plt.Figure, figure_file_name: str, dpi: int = 300, si: bool = False) -> None:
    path = os.path.join(figure_dir if not si else figure_si_dir, figure_file_name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fig.savefig(path, dpi=dpi, bbox_inches="tight")
    if not si:
        eps_path = f"{os.path.splitext(path)[0]}.pdf"
        fig.savefig(eps_path, bbox_inches="tight", format="pdf")
    plt.close(fig)


def save_latex_table(table: str, table_file_name: str, si: bool = False) -> None:
    path = os.path.join(table_dir if not si else table_si_dir, table_file_name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        f.write(table)

def materialize_table(table_file_name: str) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata={
            "path": dg.MetadataValue.path(table_file_name),
        }
    )


def materialize_image(figure_file_name: str, si: bool = False) -> dg.MaterializeResult:
    path = os.path.join(figure_dir if not si else figure_si_dir, figure_file_name)
    with open(path, "rb") as f:
        data = f.read()
    ext = os.path.splitext(path)[1].lower()
    mime = "image/png" if ext in (".png", ".apng") else "image/jpeg"
    md = f"![Image](data:{mime};base64,{base64.b64encode(data).decode('utf-8')})"
    return dg.MaterializeResult(
        metadata={"preview": dg.MetadataValue.md(md), "path": dg.MetadataValue.path(path)}
    )
