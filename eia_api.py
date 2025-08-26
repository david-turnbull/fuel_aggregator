# -*- coding: utf-8 -*-
"""
Created on Wed Aug 13 12:44:12 2025

@author: david
"""
"""EIA API utilities (fetch/cache raw table to a pickle)."""
from pathlib import Path
import logging
import os
import pickle
import requests
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

CACHE_PATH = Path('cache/dataframes.pkl')


def load_cached(path: Path = CACHE_PATH) -> pd.DataFrame:
    """Load cached DataFrame or raise ``FileNotFoundError``."""
    if not path.is_file():
        raise FileNotFoundError(path)
    with path.open('rb') as fh:
        return pickle.load(fh)


def fetch_and_cache(year: int, api_key: str | None, path: Path = CACHE_PATH) -> pd.DataFrame:
    """Fetch EIA AEO table 3 and cache as a pickle at ``path``.

    Parameters
    ----------
    year
        EIA AEO year to query (e.g., 2025).
    api_key
        Optional API key (falls back to unauthenticated if None).
    path
        Where to store the pickle.
    """
    base_url = (
        f"https://api.eia.gov/v2/aeo/{year}/data/?frequency=annual&data[0]=value"
        "&facets[regionId][]=1-0&facets[scenario][]=ref2025&facets[tableId][]=3"
        "&start=2023&end=2050&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"
    )
    params = {'api_key': api_key} if api_key else None
    logging.info("Requesting EIA data: %s", base_url)
    resp = requests.get(base_url, params=params, timeout=60)
    resp.raise_for_status()

    df = pd.DataFrame(resp.json()['response']['data'])
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('wb') as fh:
        pickle.dump(df, fh)
    return df