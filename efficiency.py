# -*- coding: utf-8 -*-
"""
Created on Wed Aug 20 10:08:08 2025

@author: david
"""
"""Generate Efficiency rows from technology naming rules (unit efficiency)."""
from typing import Dict, List
import pandas as pd


def build_mapping(tech_list: List[str]) -> Dict[str, Dict[str, str]]:
    """Create input/output commodity mapping using name conventions."""
    mapping: Dict[str, Dict[str, str]] = {}
    for tech in tech_list:
        parts = tech.split("_")
        if tech.startswith("F_IMP_"):
            fuel = "_".join(parts[2:]).lower()
            mapping[tech] = {"input": "F_ethos", "output": f"F_{fuel}"}
        elif tech.startswith("E_"):
            sector = parts[1].upper()
            mapping[tech] = {"input": "E_elc_dx", "output": f"{sector}_elc"}
        elif tech.startswith("F_"):
            sector = parts[1].upper()
            fuel = "_".join(parts[2:]).lower()
            mapping[tech] = {"input": f"F_{fuel}", "output": f"{sector}_{fuel}"}
    return mapping


def add_efficiency(
    comb_dict: Dict[str, pd.DataFrame],
    *,
    province_list: List[str],
    periods: List[int],
    dict_id: Dict[str, str],
    tech_list: List[str],
) -> Dict[str, pd.DataFrame]:
    """Append Efficiency rows with value 1.0 for each tech/period/province."""
    mapping = build_mapping(tech_list)
    rows = []
    for pro in province_list:
        if pro == 'CAN':
            continue
        for vint in periods:
            for tech in tech_list:
                i = mapping.get(tech, {}).get('input', '')
                o = mapping.get(tech, {}).get('output', '')
                rows.append([pro, i, tech, vint, o, 1.0, "Arbitrary value for transfer technology", '', '', '', '', '', '', dict_id[pro]])
    eff_df = pd.DataFrame(rows, columns=comb_dict['Efficiency'].columns)
    if not eff_df.empty:
        comb_dict['Efficiency'] = pd.concat([comb_dict['Efficiency'], eff_df], ignore_index=True)
    return comb_dict