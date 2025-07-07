"""
Tests for udf.py
"""
import pytest

import duckdb
import hetionet_utils.udf as hetio_udf
from hetionet_utils.udf import get_paths_json

@pytest.mark.parametrize(
    "source, target, metapath",
    [
        # Case 1: Successful extraction
        (
            42494,
            39906,
            "BPpGdCrC"
        ),
    ])
def test_get_paths_json(source: int, target: int, metapath: str):
    """
    Fetch real data from the Het.io API and verify structure.
    """
    result = get_paths_json(source, target, metapath)
    # The live API should return at least one path
    assert isinstance(result, str)
