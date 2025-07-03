"""
Tests for udf.py
"""
import pytest

import hetionet_utils.udf as hetio_udf
from hetionet_utils.udf import _fetch_paths, get_pdp, get_pct

# Use real Het.io API endpoint parameters
LIVE_SOURCE = 42494
LIVE_TARGET = 39906
LIVE_METAPATH = 'BPpGdCrC'

@pytest.fixture(autouse=True)
def clear_cache():
    """
    Clear the module-level cache before each test.
    """
    hetio_udf._API_CACHE.clear()
    yield
    hetio_udf._API_CACHE.clear()


def test_fetch_paths_live():
    """
    Fetch real data from the Het.io API and verify structure.
    """
    result = _fetch_paths(LIVE_SOURCE, LIVE_TARGET, LIVE_METAPATH)
    # The live API should return at least one path
    assert isinstance(result, dict)
    assert result, "Expected non-empty dict from live fetch"
    # Keys should be integers and values tuples of two floats
    for path_id, (pdp, pct) in result.items():
        assert isinstance(path_id, int)
        assert isinstance(pdp, float)
        assert isinstance(pct, float)
        # PDP and percent should be non-negative
        assert pdp >= 0.0
        assert pct >= 0.0
        break  # only check the first entry


def test_get_pdp_and_pct_live():
    """
    Test get_pdp and get_pct against the live API data.
    """
    # Fetch mapping once
    mapping = _fetch_paths(LIVE_SOURCE, LIVE_TARGET, LIVE_METAPATH)
    # Choose an arbitrary existing pathcount_id
    pathcount_id, (expected_pdp, expected_pct) = next(iter(mapping.items()))

    # First calls should populate the cache and return correct values
    pdp_val = get_pdp(LIVE_SOURCE, LIVE_TARGET, LIVE_METAPATH, pathcount_id)
    pct_val = get_pct(LIVE_SOURCE, LIVE_TARGET, LIVE_METAPATH, pathcount_id)
    assert pdp_val == pytest.approx(expected_pdp)
    assert pct_val == pytest.approx(expected_pct)

    # Second calls should use the cache and return the same values without error
    pdp_again = get_pdp(LIVE_SOURCE, LIVE_TARGET, LIVE_METAPATH, pathcount_id)
    pct_again = get_pct(LIVE_SOURCE, LIVE_TARGET, LIVE_METAPATH, pathcount_id)
    assert pdp_again == pytest.approx(expected_pdp)
    assert pct_again == pytest.approx(expected_pct)


def test_missing_pathcount_id():
    """
    Request a non-existent pathcount_id and expect None results.
    """
    # Use an ID very unlikely to exist
    missing_id = 0
    pdp = get_pdp(LIVE_SOURCE, LIVE_TARGET, LIVE_METAPATH, missing_id)
    pct = get_pct(LIVE_SOURCE, LIVE_TARGET, LIVE_METAPATH, missing_id)
    assert pdp is None
    assert pct is None

