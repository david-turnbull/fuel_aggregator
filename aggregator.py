# -*- coding: utf-8 -*-
"""
Created on Mon Aug 25 07:38:47 2025

@author: david
"""
"""End‑to‑end orchestrator for the fuel pipeline."""
from pathlib import Path
import logging
import sqlite3
import os
import pandas as pd

from setup import load_config, init_database, build_runtime_frames, inflation_constants
from eia_api import load_cached, fetch_and_cache
from techcom import build_comm_and_tech
from efficiency import build_mapping, add_efficiency
from costvariable import build_costvariable
from emissionactivity import build_emission_activity
from postprocessing import add_metadata

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def _write_all(db_path: Path, comb_dict: dict) -> None:
    def _to_scalar(x):
        # Collapse weird pandas objects to simple scalars/strings
        import pandas as pd
        if isinstance(x, pd.Series):
            return x.iloc[0] if len(x) else None
        if isinstance(x, pd.DataFrame):
            return x.iloc[0, 0] if not x.empty else None
        if isinstance(x, (list, tuple, dict)):
            return str(x)
        return x

    with sqlite3.connect(db_path) as conn:
        for table, df in comb_dict.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                safe_df = df.applymap(_to_scalar)
                logging.info("Writing %-24s %6d rows", table, len(safe_df))
                safe_df.to_sql(table, conn, if_exists='append', index=False)


def run() -> None:
    cfg = load_config()
    db_path, tables, comb_dict = init_database(cfg)

    # Source (EIA)
    cache_path = Path('cache/dataframes.pkl')
    try:
        df_raw = load_cached(cache_path)
        logging.info("Loaded EIA cache: %d rows", len(df_raw))
    except FileNotFoundError:
        api_key = os.getenv('EIA_API_KEY')
        df_raw = fetch_and_cache(int(cfg['eia_year']), api_key, cache_path)
        logging.info("Fetched & cached EIA: %d rows", len(df_raw))

    # Build runtime frames
    cost_df, fuel_df, fuel_list, province_list, periods, dict_id = build_runtime_frames(df_raw, cfg)

    # Dimensions
    comb_dict, tech_list = build_comm_and_tech(
        comb_dict, cost_df=cost_df, fuel_df=fuel_df, fuel_list=fuel_list, dict_id=dict_id
    )

    # Efficiency (unit) and mapping
    comb_dict = add_efficiency(
        comb_dict, province_list=province_list, periods=periods, dict_id=dict_id, tech_list=tech_list
    )
    mapping = build_mapping(tech_list)

    # Costs
    factors = inflation_constants()
    comb_dict = build_costvariable(
        comb_dict,
        cost_df=cost_df,
        tech_list=tech_list,
        mapping=mapping,
        province_list=province_list,
        periods=periods,
        dict_id=dict_id,
        factors=factors,
        fuel_df=fuel_df.rename(columns={'Fuel_type': 'Commodity', 'Fuel_name': 'notes'}).assign(source='[F1]'),
    )

    # Emissions
    comb_dict = build_emission_activity(
        comb_dict,
        province_list=province_list,
        periods=periods,
        dict_id=dict_id,
        mapping=mapping,
    )

    # Metadata
    comb_dict = add_metadata(comb_dict, config=cfg, dict_id=dict_id, province_list=province_list)

    # Persist
    _write_all(db_path, comb_dict)
    logging.info("Done. SQLite written to: %s", db_path)


if __name__ == "__main__":
    run()