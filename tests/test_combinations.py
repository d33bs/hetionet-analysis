"""
Tests for combination.py
"""

import pyarrow as pa
import pytest
from hetionet_utils.combination import (
    generate_combinations_for_bioprocs_genes_and_metapaths,
)


@pytest.mark.parametrize(
    "bioprocesses, genes, metapaths, expected_combinations",
    [
        # Test case 1: All tables have one element each
        (["bio1"], ["gene1"], ["meta1"], [("bio1", "gene1", "meta1")]),
        # Test case 2: One table is empty, no combinations should be generated
        (["bio1"], [], ["meta1"], []),
        # Test case 3: Two elements in each table
        (
            ["bio1", "bio2"],
            ["gene1", "gene2"],
            ["meta1", "meta2"],
            [
                ("bio1", "gene1", "meta1"),
                ("bio1", "gene1", "meta2"),
                ("bio1", "gene2", "meta1"),
                ("bio1", "gene2", "meta2"),
                ("bio2", "gene1", "meta1"),
                ("bio2", "gene1", "meta2"),
                ("bio2", "gene2", "meta1"),
                ("bio2", "gene2", "meta2"),
            ],
        ),
        # Test case 4: Only one non-empty table (other two are empty)
        (["bio1"], [], [], []),
        # Test case 5: Larger tables with three elements each to check combination generation
        (
            ["bio1", "bio2", "bio3"],
            ["gene1", "gene2", "gene3"],
            ["meta1", "meta2", "meta3"],
            [
                ("bio1", "gene1", "meta1"),
                ("bio1", "gene1", "meta2"),
                ("bio1", "gene1", "meta3"),
                ("bio1", "gene2", "meta1"),
                ("bio1", "gene2", "meta2"),
                ("bio1", "gene2", "meta3"),
                ("bio1", "gene3", "meta1"),
                ("bio1", "gene3", "meta2"),
                ("bio1", "gene3", "meta3"),
                ("bio2", "gene1", "meta1"),
                ("bio2", "gene1", "meta2"),
                ("bio2", "gene1", "meta3"),
                ("bio2", "gene2", "meta1"),
                ("bio2", "gene2", "meta2"),
                ("bio2", "gene2", "meta3"),
                ("bio2", "gene3", "meta1"),
                ("bio2", "gene3", "meta2"),
                ("bio2", "gene3", "meta3"),
                ("bio3", "gene1", "meta1"),
                ("bio3", "gene1", "meta2"),
                ("bio3", "gene1", "meta3"),
                ("bio3", "gene2", "meta1"),
                ("bio3", "gene2", "meta2"),
                ("bio3", "gene2", "meta3"),
                ("bio3", "gene3", "meta1"),
                ("bio3", "gene3", "meta2"),
                ("bio3", "gene3", "meta3"),
            ],
        ),
    ],
)
def test_generate_combinations_for_bioprocs_genes_and_metapaths(
    bioprocesses, genes, metapaths, expected_combinations
):
    # Create Arrow tables for the test case
    table_bioprocesses = pa.table({"id": bioprocesses})
    table_genes = pa.table({"id": genes})
    table_metapaths = pa.table({"metapath": metapaths})

    # Generate combinations and compare with expected output
    result = list(
        generate_combinations_for_bioprocs_genes_and_metapaths(
            table_bioprocesses, table_genes, table_metapaths
        )
    )
    assert result == expected_combinations


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
