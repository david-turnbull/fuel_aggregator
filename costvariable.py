# -*- coding: utf-8 -*-
"""
Created on Wed Aug 20 14:02:26 2025

@author: david
"""

"""Compute CostVariable values from EIA price frame and config factors."""
from typing import Dict, List
import pandas as pd


def _calc_value(
    tech: str,
    tech_name: str,
    period_val: int,
    *,
    cost_df: pd.DataFrame,
    cfg: dict,
    mmbtuconvertor: float,
    currencyadjustment: float,
    defl22: float,
    defl25: float,
    eth_price: float,
    rdsl_price: float,
    spk_price: float,
) -> float:
    """Encapsulate the branchy pricing logic from your script."""
    if 'BIO' in tech or 'WOOD' in tech:
        return ((cfg['b_price'] * mmbtuconvertor) * currencyadjustment) * defl22
    if 'U_NAT' in tech or 'U_ENR' in tech:
        return ((cfg['u_price'] * mmbtuconvertor) * currencyadjustment) * defl22
    if 'ETH' in tech:
        return eth_price
    if 'RDSL' in tech:
        return rdsl_price
    if 'SPK' in tech:
        return spk_price

    if any(x in tech for x in ['LNG', 'CNG', 'NGL']):
        lookup = 'T_ng' if tech in ['F_T_LNG', 'F_T_CNG'] else 'I_prop'
        base = cost_df.loc[
            (cost_df['period'] == period_val) & (cost_df['Tech Name'] == lookup), 'value'
        ].squeeze()
        return ((base * mmbtuconvertor) * currencyadjustment) * defl25 * 0.89

    if 'LPG' in tech:
        lookup = 'R_prop' if tech == 'F_R_LPG' else 'T_prop'
        base = cost_df.loc[
            (cost_df['period'] == period_val) & (cost_df['Tech Name'] == lookup), 'value'
        ].squeeze()
        return ((base * mmbtuconvertor) * currencyadjustment) * defl25

    if 'E_coal' in tech_name:
        base = cost_df.loc[
            (cost_df['period'] == period_val) & (cost_df['Tech Name'] == 'I_coal'), 'value'
        ].squeeze()
        return ((base * mmbtuconvertor) * currencyadjustment) * defl25
    if 'E_gsl' in tech_name:
        base = cost_df.loc[
            (cost_df['period'] == period_val) & (cost_df['Tech Name'] == 'T_gsl'), 'value'
        ].squeeze()
        return ((base * mmbtuconvertor) * currencyadjustment) * defl25
    if 'R_oil' in tech_name:
        base = cost_df.loc[
            (cost_df['period'] == period_val) & (cost_df['Tech Name'] == 'C_oil'), 'value'
        ].squeeze()
        return ((base * mmbtuconvertor) * currencyadjustment) * defl25
    if 'C_h2' in tech_name or 'R_h2' in tech_name:
        base = cost_df.loc[
            (cost_df['period'] == period_val) & (cost_df['Tech Name'] == 'I_h2'), 'value'
        ].squeeze()
        return ((base * mmbtuconvertor) * currencyadjustment) * defl25
    if 'I_pcoke' in tech_name or 'I_coke' in tech_name:
        base = cost_df.loc[
            (cost_df['period'] == period_val) & (cost_df['Tech Name'] == 'I_coal'), 'value'
        ].squeeze()
        return ((base * mmbtuconvertor) * currencyadjustment) * defl25
    if any(x in tech_name for x in ['A_ng', 'A_dsl', 'A_prop']):
        if 'A_ng' in tech_name:
            base = cost_df.loc[
                (cost_df['period'] == period_val) & (cost_df['Tech Name'] == 'I_ng'), 'value'
            ].squeeze()
        elif 'A_dsl' in tech_name:
            base = cost_df.loc[
                (cost_df['period'] == period_val) & (cost_df['Tech Name'] == 'T_dsl'), 'value'
            ].squeeze()
        else:
            base = cost_df.loc[
                (cost_df['period'] == period_val) & (cost_df['Tech Name'] == 'T_prop'), 'value'
            ].squeeze()
        return ((base * mmbtuconvertor) * currencyadjustment) * defl25

    # Default lookup straight from name
    base = cost_df.loc[
        (cost_df['period'] == period_val) & (cost_df['Tech Name'] == tech_name), 'value'
    ].squeeze()
    return ((base * mmbtuconvertor) * currencyadjustment) * defl25


def build_costvariable(
    comb_dict: Dict[str, pd.DataFrame],
    *,
    cost_df: pd.DataFrame,
    tech_list: List[str],
    mapping: Dict[str, Dict[str, str]],
    province_list: List[str],
    periods: List[int],
    dict_id: Dict[str, str],
    factors: dict,
    fuel_df: pd.DataFrame,
) -> Dict[str, pd.DataFrame]:
    """Append CostVariable rows across provinces, vintages, and periods."""
    cdf = cost_df.copy()
    cdf['period'] = cdf['period'].astype(int)
    cdf['Tech Name'] = cdf['Tech Name'].astype(str)
    cdf['value'] = cdf['value'].astype(float)

    # --- fix: drop duplicate column names in fuel_df ---
    fuel_df = fuel_df.loc[:, ~fuel_df.columns.duplicated()].copy()
    for col in ("Commodity", "notes", "source"):
        if col not in fuel_df.columns:
            fuel_df[col] = ""

    rows = []
    for pro in province_list:
        if pro == 'CAN':
            continue
        for vint in periods:
            for per in periods:
                if per < vint:
                    continue
                for tech in tech_list:
                    if any(x in tech for x in ['F_IMP', 'ELC', 'OTH']):
                        continue
                    tech_name = mapping[tech]['output'].strip()
                    val = _calc_value(
                        tech,
                        tech_name,
                        int(per),
                        cost_df=cdf,
                        cfg={'b_price': 0, 'u_price': 0},
                        mmbtuconvertor=factors['mmbtuconvertor'],
                        currencyadjustment=factors['currencyadjustment'],
                        defl22=factors['deflation_2022'],
                        defl25=factors['deflation_2025'],
                        eth_price=factors['eth_price'],
                        rdsl_price=factors['rdsl_price'],
                        spk_price=factors['spk_price'],
                    )
                    unit = "2020 M$/PJ"

                    # Safe scalar extraction
                    match = fuel_df.loc[fuel_df['Commodity'] == tech_name]
                    if not match.empty:
                        notes = match['notes'].iloc[0] if not isinstance(match['notes'], pd.DataFrame) else match['notes'].iloc[0, 0]
                        ref   = match['source'].iloc[0] if not isinstance(match['source'], pd.DataFrame) else match['source'].iloc[0, 0]
                    else:
                        notes, ref = '', ''

                    rows.append(
                        [pro, per, tech, vint, val, unit, notes, ref, 2, 3, 2, 1, 1, dict_id[pro]]
                    )

    out = pd.DataFrame(rows, columns=comb_dict['CostVariable'].columns)
    if not out.empty:
        comb_dict['CostVariable'] = pd.concat(
            [comb_dict['CostVariable'], out], ignore_index=True
        )
    return comb_dict