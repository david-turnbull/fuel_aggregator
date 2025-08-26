# -*- coding: utf-8 -*-
"""
Created on Wed Aug 20 14:02:44 2025

@author: david
"""

"""Append DataSet, DataSource, and SectorLabel tables (fuel pipeline)."""
from typing import Dict, List
import pandas as pd


def add_metadata(
    comb_dict: Dict[str, pd.DataFrame],
    *,
    config: dict,
    dict_id: Dict[str, str],
    province_list: List[str],
) -> Dict[str, pd.DataFrame]:
    """Populate DataSet/DataSource/SectorLabel; returns updated registry."""
    # DataSet
    ds_rows = []
    for pro in province_list:
        ds_rows.append([dict_id[pro], f'{pro} - fuel ', f"v{config['version']}", '2025 annual update', 'active',
                        'David Turnbull - david.turnbull1@ucalgary.ca', '08-2025', '', 'Original sector design', ''])
    ds_df = pd.DataFrame(ds_rows, columns=comb_dict['DataSet'].columns)
    comb_dict['DataSet'] = pd.concat([comb_dict['DataSet'], ds_df], ignore_index=True)

    # DataSource (kept from your original list)
    src_rows = [
        ['[F1]','EIA AEO 2025','Using table 3 via the API to get access to the costs of the different fuels for different sectors',dict_id['CAN']],
        ['[F2]','NREL ATB electricity sector 2022','Taking the fuel costs from the appropriate places in the excel workbook',dict_id['CAN']],
        ['[F3]', 'Biofuels in Canada 2023', 'Michael Wolinetz & Sam Harrison. (2023). Biofuels in Canada 2023: Tracking biofuel consumption, feedstocks and avoided greenhouse gas emissions. Navius Research.', dict_id['CAN']],
        ['[F4]','Government of Canada, Emission factors and reference values','The appropriate emission factors for sector and fuel are converted to tonnes or ktonnes per PJ',dict_id['CAN']],
        ['[F5]', 'IPCC AR6', 'Used for the GWP100 values for methane, carbon dioxide and nitrous oxide for calculating CO2eq', dict_id['CAN']],
        ['[F6]', 'NS Dept. of Environment & Climate Change', 'QRV standards (wood/ethanol/biodiesel factors)', dict_id['CAN']],
        ['[F7]', 'Argonne National Laboratory, GREET model', 'Upstream fuel emissions factors', dict_id['CAN']],
    ]
    src_df = pd.DataFrame(src_rows, columns=comb_dict['DataSource'].columns)
    comb_dict['DataSource'] = pd.concat([comb_dict['DataSource'], src_df], ignore_index=True)

    # SectorLabel (fixed bug: assign columns on the DataFrame, not the name 'sec')
    sectors = {
        "electricity": "Electric power sector",
        "residential": "Residential sector",
        "commercial": "Commercial sector",
        "industrial": "Industrial sector",
        "transportation": "Transportation sector",
        "agriculture": "Agriculture sector",
        "fuel": "Fuel production sector",
    }
    sec_df = pd.DataFrame([[k, v] for k, v in sectors.items()], columns=comb_dict['SectorLabel'].columns)
    comb_dict['SectorLabel'] = pd.concat([comb_dict['SectorLabel'], sec_df], ignore_index=True)
    return comb_dict