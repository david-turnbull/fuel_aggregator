# -*- coding: utf-8 -*-
"""
Created on Mon Aug 18 13:41:05 2025

@author: david
"""
"""Build Commodity and Technology tables from a fuel list.

Fixes in this version:
- Correctly **uses** the description from ``generate_fuel_flow_description`` (bug fix)
- Removes stray pattern checks like ``'E_C_.'`` → ``'E_C_'`` (typos)
- Avoids side effects at import
"""
from typing import Dict, List, Tuple
import pandas as pd


def _sectors_map() -> Dict[str, str]:
    return {
        "E": "Electric power sector", "R": "Residential sector", "C": "Commercial sector",
        "I": "Industrial sector", "T": "Transportation sector", "A": "Agriculture sector",
        "F": "Fuel production sector",
    }


def build_comm_and_tech(
    comb_dict: Dict[str, pd.DataFrame],
    *,
    cost_df: pd.DataFrame,
    fuel_df: pd.DataFrame,
    fuel_list: List[str],
    dict_id: Dict[str, str],
) -> Tuple[Dict[str, pd.DataFrame], List[str]]:
    """Populate ``Commodity`` and ``Technology``; return the tech list."""
    sectors = _sectors_map()
    fuels = fuel_df.set_index('Fuel_type')['Fuel_name'].to_dict()

    def generate_description(code: str) -> str:
        parts = code.split('_')
        prefix = parts[0]
        sector = sectors.get(prefix, "Unknown sector")
        if code in fuels:
            fuel = fuels[code]
        else:
            key = '_'.join(parts[1:]) if len(parts) > 1 else parts[0]
            fuel = 'electricity' if key.upper() == 'ELC' else ('electricity (direct use)' if key.upper() == 'ELC_DX' else fuels.get(key, key))
        return f"{fuel.capitalize()} for the {sector.lower()}"

    com_rows = []
    for code in fuel_list:
        flag = 's' if code == 'F_ethos' else 'p'
        com_rows.append([code, flag, generate_description(code), dict_id['CAN']])

    # Identify general F_<fuel> codes actually used (skip elc/elc_dx)
    used_fuels = set()
    for code in fuel_list:
        parts = code.split('_')
        fuel_key = '_'.join(parts[1:]) if len(parts) > 1 else parts[0]
        if fuel_key.upper() not in {"ELC", "ELC_DX"} and fuel_key in fuels:
            used_fuels.add(fuel_key)

    for fuel_code in sorted(used_fuels):
        com_rows.append([f"F_{fuel_code}", 'p', f"{fuels[fuel_code].capitalize()} for Fuel sector", dict_id['CAN']])

    # Build techs from commodity codes
    def generate_fuel_flow_description(code: str) -> str:
        parts = code.split('_')
        if parts[0] == 'F' and len(parts) > 1 and parts[1] == 'IMP':
            fuel_key = '_'.join(parts[2:]).lower()
            fuel_name = fuels.get(fuel_key, fuel_key)
            return f"{fuel_name.capitalize()} import into fuel sector"
        if parts[0] == 'F' and len(parts) > 2:
            to_sector = parts[1]
            fuel_key = '_'.join(parts[2:]).lower()
            to_sector_name = sectors.get(to_sector, to_sector)
            fuel_name = fuels.get(fuel_key, fuel_key)
            return f"{fuel_name.capitalize()} distribution from fuel sector to {to_sector_name.lower()}"
        if parts[0] == 'E' and len(parts) > 2:
            to_sector = parts[1]
            fuel_key = '_'.join(parts[2:]).lower()
            to_sector_name = sectors.get(to_sector, to_sector)
            fuel_name = fuels.get(fuel_key, fuel_key)
            return f"{fuel_name.capitalize()} distribution to {to_sector_name.lower()}"
        return f"Fuel flow for {code}"

    fuel_flow_list: List[str] = []
    for code, _, _, _ in com_rows:
        original = code.lower()
        if original in {'e_elc_dx', 'e_elc', 'f_ethos'}:
            continue
        parts = original.split('_')
        prefix = parts[0].upper()
        fuel_part = '_'.join(parts[1:]).upper() if len(parts) > 1 else parts[0].upper()
        if fuel_part.startswith('ELC') and prefix != 'E':
            tech_code = f"E_{prefix}_{fuel_part}"
        elif prefix == 'F':
            tech_code = f"F_IMP_{fuel_part}"
        else:
            tech_code = f"F_{prefix}_{fuel_part}"
        fuel_flow_list.append(tech_code)

    tech_rows = []
    for code in fuel_flow_list:
        flag = 'r' if code.startswith('F_IMP_') else 'p'
        if code.startswith('F_C_') or code.startswith('E_C_'):
            sector = 'commercial'
        elif code.startswith('F_I_') or code.startswith('E_I_'):
            sector = 'industrial'
        elif code.startswith('F_R_') or code.startswith('E_R_'):
            sector = 'residential'
        elif code.startswith('F_A_') or code.startswith('E_A_'):
            sector = 'agriculture'
        elif code.startswith('F_T_') or code.startswith('E_T_'):
            sector = 'transportation'
        elif code.startswith('F_E_'):
            sector = 'electricity'
        else:
            sector = 'fuel'
        desc = generate_fuel_flow_description(code)
        tech_rows.append([code, flag, sector, '', '', 1, 0, 0, 0, 0, 0, 0, 0, desc, dict_id['CAN']])

    # Write out
    comm_df = pd.DataFrame(com_rows, columns=comb_dict['Commodity'].columns).drop_duplicates(subset=['name'])
    comb_dict['Commodity'] = pd.concat([comb_dict['Commodity'], comm_df], ignore_index=True)

    tech_df = pd.DataFrame(tech_rows, columns=comb_dict['Technology'].columns)
    comb_dict['Technology'] = pd.concat([comb_dict['Technology'], tech_df], ignore_index=True)

    tech_list = tech_df['tech'].tolist()
    return comb_dict, tech_list