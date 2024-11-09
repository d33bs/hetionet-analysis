"""
Focuses on generating combinations from input data.
"""

from itertools import product
from typing import Generator, Iterator, Tuple

import pyarrow as pa


def generate_combinations_for_bioprocs_genes_and_metapaths(
    table_bioprocesses: pa.Table, table_genes: pa.Table, table_metapaths: pa.Table
) -> Generator[Tuple[str, str, str], None, None]:
    """
    Generates all possible combinations of IDs from three Arrow tables.

    Args:
        table_bioprocesses (pa.Table):
            Arrow Table containing bioprocess IDs in an 'id' column.
        table_genes (pa.Table):
            Arrow Table containing gene IDs in an 'id' column.
        table_metapaths (pa.Table):
            Arrow Table containing metapath values in a 'metapath' column.

    Yields:
        Tuple[str, str, str]:
            A tuple with a bioprocess ID, a gene ID, and a metapath value.
    """
    for combo in product(
        table_bioprocesses["id"].to_pylist(),
        table_genes["id"].to_pylist(),
        table_metapaths["metapath"].to_pylist(),
    ):
        yield combo


def process_in_chunks_for_bioprocs_genes_and_metapaths(
    generator: Iterator[Tuple[str, str, str]], chunk_size: int = 1000
) -> Iterator[pa.Table]:
    """
    Processes combinations from a generator in smaller chunks
    as Arrow Tables.

    Args:
        generator (Iterator[Tuple[str, str, str]]):
            A generator that yields tuples of combinations.
        chunk_size (int, optional):
            The number of rows per chunk. Defaults to 1000.

    Yields:
        pa.Table:
            An Arrow Table containing a chunk of combinations
            with columns ['source_id', 'target_id', 'metapath'].
    """
    chunk = []
    for i, combo in enumerate(generator):
        chunk.append(combo)

        if (i + 1) % chunk_size == 0:
            # Create Arrow Table from the chunk
            yield pa.table(
                {
                    "source_id": [row[0] for row in chunk],
                    "target_id": [row[1] for row in chunk],
                    "metapath": [row[2] for row in chunk],
                }
            )
            chunk = []

    # Yield any remaining combinations as an Arrow Table
    if chunk:
        yield pa.table(
            {
                "source_id": [row[0] for row in chunk],
                "target_id": [row[1] for row in chunk],
                "metapath": [row[2] for row in chunk],
            }
        )
