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
        # Convert each combination to a tuple of strings to avoid None values
        combo_tuple = tuple(str(x) if x is not None else "" for x in combo)
        chunk.append(combo_tuple)

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


@pytest.mark.parametrize(
    "data, chunk_size, expected_num_chunks, expected_chunk_sizes",
    [
        # Test case 1: Data exactly matches one chunk
        (
            [("bio1", "gene1", "meta1")] * 5,
            5,
            1,
            [5],
        ),
        
        # Test case 2: Data smaller than chunk size
        (
            [("bio1", "gene1", "meta1"), ("bio2", "gene2", "meta2")],
            5,
            1,
            [2],
        ),
        
        # Test case 3: Data larger than chunk size, even division
        (
            [("bio1", "gene1", "meta1")] * 6,
            3,
            2,
            [3, 3],
        ),
        
        # Test case 4: Data larger than chunk size, uneven division
        (
            [("bio1", "gene1", "meta1")] * 7,
            3,
            3,
            [3, 3, 1],
        ),
        
        # Test case 5: Very large chunk size compared to data
        (
            [("bio1", "gene1", "meta1"), ("bio2", "gene2", "meta2"), ("bio3", "gene3", "meta3")],
            100,
            1,
            [3],
        ),
    ],
)
def test_process_in_chunks_for_bioprocs_genes_and_metapaths(data, chunk_size, expected_num_chunks, expected_chunk_sizes):
    # Create generator for test data
    generator = sample_generator(data)
    
    # Process in chunks and collect the result
    result_chunks = list(process_in_chunks_for_bioprocs_genes_and_metapaths(generator, chunk_size=chunk_size))
    
    # Check number of chunks generated
    assert len(result_chunks) == expected_num_chunks
    
    # Check sizes of each chunk
    for result_chunk, expected_size in zip(result_chunks, expected_chunk_sizes):
        assert result_chunk.num_rows == expected_size
    
    # Check column names and content of the first chunk
    if data:
        first_chunk = result_chunks[0]
        assert first_chunk.column_names == ["source_id", "target_id", "metapath"]
        assert first_chunk["source_id"].to_pylist() == [row[0] for row in data[:expected_chunk_sizes[0]]]
        assert first_chunk["target_id"].to_pylist() == [row[1] for row in data[:expected_chunk_sizes[0]]]
        assert first_chunk["metapath"].to_pylist() == [row[2] for row in data[:expected_chunk_sizes[0]]]
