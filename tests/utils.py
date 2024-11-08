"""
Utilities for testing
"""

from typing import Any


def sample_generator(data: Any):
    """
    Helper function for creating a generator from any iterable data.

    Args:
        data (Any): 
            An iterable data structure (e.g., list, tuple, set) containing 
            elements to be yielded one by one.

    Yields:
        Any: Each element of the input data, yielded in sequence.
    """
    for item in data:
        yield item

