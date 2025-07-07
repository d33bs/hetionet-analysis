"""
Creates UDFs for various Het.io API calls.
"""

import json

import requests


def get_paths_json(source: int, target: int, metapath: str) -> str:
    """
    Fetch the full Het.io “paths” JSON blob for the given triple
    and return it as a raw JSON string.
    """
    url = (
        f"https://search-api.het.io/v1/paths/"
        f"source/{source}/target/{target}/metapath/{metapath}/"
        "?format=json"
    )
    resp = requests.get(url)
    resp.raise_for_status()
    raw_paths = resp.json()["paths"]

    EXPECTED_KEYS = [
        "metapath",
        "node_ids",
        "rel_ids",
        "percent_of_DWPC",
        "PC",
        "DWPC",
        "score",
    ]

    return json.dumps([{k: p.get(k) for k in EXPECTED_KEYS} for p in raw_paths])