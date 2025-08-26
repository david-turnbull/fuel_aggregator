# -*- coding: utf-8 -*-
"""
Created on Wed Aug 20 14:02:36 2025

@author: david
"""

"""Create EmissionActivity rows from upstream/direct CSVs and tech mapping."""
from typing import Dict, List
import pandas as pd


def build_emission_activity(
    comb_dict: Dict[str, pd.DataFrame],
    *,
    province_list: List[str],
    periods: List[int],
    dict_id: Dict[str, str],
    mapping: Dict[str, Dict[str, str]],
    upstream_csv: str = 'input/upstream_emissions_fuels.csv',
    direct_csv: str = 'input/direct_comb_emission.csv',
) -> Dict[str, pd.DataFrame]:
    """Append EmissionActivity; drop duplicates on the uniqueness key."""
    upstream = pd.read_csv(upstream_csv)
    direct = pd.read_csv(direct_csv)
    emis_df = pd.concat([upstream, direct]).reset_index(drop=True)

    rows: List[list] = []
    for pro in province_list:
        if pro == 'CAN':
            continue
        for _, r in emis_df.iterrows():
            em, out, val, units, notes, ref = r['emission'], r['commodity'], r['value'], r['units'], r['notes'], r['source']
            for tech, tv in mapping.items():
                if tv.get('output') == out:
                    inp = tv.get('input')
                    for per in periods:
                        rows.append([pro, em, inp, tech, per, out, val, units, notes, ref, 1, 2, 2, 2, 2, dict_id[pro]])

    em_df = pd.DataFrame(rows, columns=comb_dict['EmissionActivity'].columns)
    em_df = em_df.drop_duplicates(subset=['region','emis_comm','input_comm','tech','vintage','output_comm','data_id'])
    comb_dict['EmissionActivity'] = pd.concat([comb_dict['EmissionActivity'], em_df], ignore_index=True)
    return comb_dict