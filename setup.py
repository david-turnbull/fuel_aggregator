# -*- coding: utf-8 -*-
"""
Created on Mon Aug 18 11:09:16 2025

@author: david
"""
"""Setup utilities for the fuel pipeline.

Creates a fresh SQLite database from the configured schema and returns:
- the database path
- discovered table names
- an empty registry of DataFrames (``comb_dict``) keyed by table
- core run-time parameters (cost frame, fuel frame/list, config, etc)
"""
from pathlib import Path
from typing import Dict, List, Tuple
import logging
import sqlite3
import yaml
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def load_config(path: str | Path = "input/params.yaml") -> dict:
    """Load YAML configuration.

    Parameters
    ----------
    path
        Location of ``params.yaml``.

    Returns
    -------
    dict
        Parsed configuration.
    """
    path = Path(path)
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def init_database(config: dict, output_dir: str | Path = "output", db_name: str = "CAN_fuel.sqlite") -> Tuple[Path, List[str], Dict[str, pd.DataFrame]]:
    """Create a new SQLite DB from schema and return empty table registry.

    Returns
    -------
    (Path, list[str], dict[str, DataFrame])
        ``(db_path, tables, comb_dict)``
    """
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    db_path = output / db_name

    # Read schema
    version = config['schema_version'][0]
    schema_file = Path("input") / (f"schema_{version}.sql" if version != 31 else "schema_3_1.sql")
    schema_sql = schema_file.read_text(encoding="utf-8")

    # Recreate DB
    if db_path.exists():
        db_path.unlink()

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.executescript(schema_sql)
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [r[0] for r in cur.fetchall()]

        comb_dict: Dict[str, pd.DataFrame] = {}
        for t in tables:
            cur.execute(f"PRAGMA table_info('{t}');")
            cols = [c[1] for c in cur.fetchall()]
            comb_dict[t] = pd.DataFrame(columns=cols)

    return db_path, tables, comb_dict


def build_runtime_frames(df_raw: pd.DataFrame, config: dict) -> Tuple[pd.DataFrame, pd.DataFrame, List[str], pd.DataFrame, List[str], Dict[str, str]]:
    """Reproduce your original transformations into cost_df/fuel_df/etc.

    Parameters
    ----------
    df_raw
        Raw EIA dataframe from the API/cache.
    config
        Global configuration from YAML.

    Returns
    -------
    tuple
        ``(cost_df, fuel_df, fuel_list, province_list, periods, dict_id)``
    """
    df = df_raw.copy()

    # Keep specific unit/years and remove 'average' rows (matches your original)
    df = df[df['unit'] == '2024 $/MMBtu']
    periods = list(map(str, config['periods']))
    df = df[df['period'].isin(periods)]
    df = df[~df['seriesName'].str.contains('average', case=False)]

    sector_mapping = {
        'Commercial': 'C', 'Industrial': 'I', 'Electric Power': 'E', 'Residential': 'R', 'Transportation': 'T'
    }
    fuel_mapping = {
        'Natural Gas': 'ng', 'Distillate Fuel Oil': 'dsl', 'Diesel Fuel': 'dsl', 'Residual Fuel Oil': 'hfo',
        'Propane': 'prop', 'Jet Fuel': 'jtf', 'Residual Fuel': 'oil', 'Hydrogen': 'h2',
        'Metallurgical Coal': 'coal', 'Motor Gasoline': 'gsl'
    }

    split_data = df['seriesName'].str.split(' : ', expand=True)
    df['sector_code'] = split_data[1].map(sector_mapping)
    df['fuel_code'] = split_data[2].map(fuel_mapping)

    # Convert HFO → OIL in C/R/E; ensure E_hfo → E_oil in Tech Name
    df['fuel_code'] = df.apply(
        lambda row: 'oil' if row['fuel_code'] == 'hfo' and row['sector_code'] in ['C', 'R', 'E'] else row['fuel_code'],
        axis=1
    )
    df['Tech Name'] = (df['sector_code'] + '_' + df['fuel_code']).replace({'E_hfo': 'E_oil'})

    df = df.dropna()
    cost_df = df[['period', 'sector_code', 'fuel_code', 'Tech Name', 'value', 'unit']].copy()
    cost_df = cost_df.sort_values(by='period', ascending=True).reset_index(drop=True)

    # Fuel list from CSV
    fuel_df = pd.read_csv('input/fuel_list.csv')
    fuel_list = fuel_df['Commodity'].to_list()

    province_list = ['AB', 'ON', 'BC', 'MB', 'SK', 'QC', 'CAN']
    dict_id = {pro: (f"{pro}DIST{config['version']}" if pro != 'CAN' else f"GENDIST{config['version']}") for pro in province_list}

    return cost_df, fuel_df, fuel_list, province_list, config['periods'], dict_id

# Expose constants that were previously globals in setup.py (so other modules can import)
def inflation_constants() -> dict:
    """Return deflation/currency factors and fixed prices used elsewhere."""
    return dict(
        deflation_2022=0.861446913,
        deflation_2025=0.877689699,
        currencyadjustment=1.22,
        mmbtuconvertor=1.055,
        eth_price=25.801332399,
        rdsl_price=34.286607549,
        spk_price=53.947379869,
    )