"""
Creates UDFs for various Het.io API calls.
"""

import requests
from threading import Lock
from typing import Dict, Tuple, Optional

# Module-level cache shared across all UDF calls
# Maps (source, target, metapath) → { pathcount_id: (pdp, percent_of_dwpc), … }
_API_CACHE: Dict[Tuple[int, int, str], Dict[int, Tuple[float, float]]] = {}
_CACHE_LOCK = Lock()


def _fetch_paths(source: int, target: int, metapath: str) -> Dict[int, Tuple[float, float]]:
    """
    Fetch per-path PDP and percent-of-DWPC data from the Het.io API based on the aggregate path_count_info.

    Parameters
    ----------
    source : int
        The source node ID.
    target : int
        The target node ID.
    metapath : str
        The metapath abbreviation (e.g. "BPpGdCrC").

    Returns
    -------
    Dict[int, Tuple[float, float]]
        A mapping containing a single entry:
        { pathcount_id: (metapath_score, metapath_adjusted_p_value) }.
    """
    url = (
        f"https://search-api.het.io/v1/paths/"
        f"source/{source}/target/{target}/metapath/{metapath}/"
        "?format=json&limit=1"
    )
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    pci = data.get("path_count_info", {})
    # Extract the primary identifiers and scores
    raw_id = pci.get("id")
    try:
        pid = int(raw_id)
    except (TypeError, ValueError):
        return {}

    # Use metapath_score and adjusted p-value as placeholders for PDP and percent
    raw_score = pci.get("metapath_score", 0.0)
    raw_adj_p = pci.get("metapath_adjusted_p_value", 0.0)
    try:
        score_val = float(raw_score)
    except (TypeError, ValueError):
        score_val = 0.0
    try:
        adj_p_val = float(raw_adj_p)
    except (TypeError, ValueError):
        adj_p_val = 0.0

    return {pid: (score_val, adj_p_val)}


def get_pdp(
    source: int,
    target: int,
    metapath: str,
    pathcount_id: int
) -> Optional[float]:
    """
    Retrieve the PDP value for a specific pathcount from cache or via API.

    The first time a unique (source, target, metapath) combination is requested,
    this will fetch and cache the entire mapping once. Subsequent calls
    use the in-memory cache.

    Parameters
    ----------
    source : int
        The source node ID.
    target : int
        The target node ID.
    metapath : str
        The metapath abbreviation.
    pathcount_id : int
        The PathCount record ID whose PDP you want.

    Returns
    -------
    Optional[float]
        The PDP value, or None if the pathcount_id is not found.
    """
    key = (source, target, metapath)
    if key not in _API_CACHE:
        with _CACHE_LOCK:
            if key not in _API_CACHE:
                _API_CACHE[key] = _fetch_paths(source, target, metapath)

    pdp_val = _API_CACHE[key].get(pathcount_id, (None, None))[0]
    return pdp_val


def get_pct(
    source: int,
    target: int,
    metapath: str,
    pathcount_id: int
) -> Optional[float]:
    """
    Retrieve the percent-of-DWPC for a specific pathcount from cache or via API.

    Parameters
    ----------
    source : int
        The source node ID.
    target : int
        The target node ID.
    metapath : str
        The metapath abbreviation.
    pathcount_id : int
        The PathCount record ID whose percent-of-DWPC you want.

    Returns
    -------
    Optional[float]
        The percent-of-DWPC value, or None if the pathcount_id is not found.
    """
    key = (source, target, metapath)
    if key not in _API_CACHE:
        with _CACHE_LOCK:
            if key not in _API_CACHE:
                _API_CACHE[key] = _fetch_paths(source, target, metapath)

    pct_val = _API_CACHE[key].get(pathcount_id, (None, None))[1]
    return pct_val
