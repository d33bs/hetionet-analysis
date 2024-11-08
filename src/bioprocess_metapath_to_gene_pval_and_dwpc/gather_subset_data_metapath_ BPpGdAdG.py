# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.16.4
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# # Biological Process and Gene Metapath Data Gathering - Subset - Metapath BPpGdAdG
#
# This notebook focuses on gathering **a subset** of data related to the following requirements and **focusing on metapath BPpGdAdG**:
#
# - Each value from `BP.csv` is a source and each value
# from `Gene.csv` is a target.
# - Each source + target pairing may have a metapath which
# is found within `metapaths.csv`.
# - For each pair metapath we need the DWPC and p-value
# stored in a table for reference.
# - Ignore metapaths found within
# `metapaths_ignore.csv`.

# +
import pathlib
from itertools import product
from typing import Generator, Iterator, Tuple

import lancedb
import pandas as pd
import pyarrow as pa
from pyarrow import csv

from hetionet_utils.database import HetionetNeo4j


# +
def generate_combinations(
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


def process_in_chunks(
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


# -

# gather metapaths which are not in the metapaths_ignore.csv
df_metapaths = pd.read_csv("data/sources/metapaths.csv")
df_metapaths_ignore = pd.read_csv("data/sources/metapaths_ignore.csv")
df_metapaths = df_metapaths[
    ~df_metapaths["metapath"].isin(df_metapaths_ignore["metapath"])
]
df_metapaths.head()

# +
# Load input CSV files into Arrow Tables
table_bioprocesses = csv.read_csv("data/sources/BP.csv").select(["id"])
table_genes = csv.read_csv("data/sources/Gene.csv").select(["id"])
table_metapaths = pa.Table.from_pandas(df_metapaths)

print(
    "Expected number of queries: ",
    (
        expected_queries := table_bioprocesses.num_rows
        * table_genes.num_rows
        * table_metapaths.num_rows
    ),
)
# -

hetiocli = HetionetNeo4j()
sample_result = hetiocli.get_metapath_data(
    source_id=str(table_bioprocesses[0][0]),
    target_id=int(str(table_genes[0][0])),
    metapath=str(table_metapaths[0][0]),
)
sample_result

# export to file and measure the size
sample_result.to_parquet((filepath := "example_output.parquet"))
print(
    "Expected storage: ",
    (
        # bytes
        pathlib.Path(filepath).stat().st_size
        *
        # multiplied by the number of expected queries we need to make
        expected_queries
    )
    /
    # kilobytes
    1024 /
    # megabytes
    1024 /
    # gigabytes
    1024,
    "GB",
)

# +
# create results folder
pathlib.Path("data/results").mkdir(exist_ok=True)

# Initialize your LanceDB database and table
db = lancedb.connect("data/results/bioprocess_and_gene_metapaths")
table_name = "bioprocess_gene_metapath_scores"

# create table, overwriting previous results
db.create_table(
    table_name,
    schema=pa.Table.from_pandas(sample_result).schema,
    mode="overwrite",
)

table = db.open_table(table_name)

# +
# Generate combinations
generator = generate_combinations(table_bioprocesses, table_genes, table_metapaths)

count = 1
# Process and print chunks
for chunk_table in process_in_chunks(generator, chunk_size=5000000):
    # add the chunk to the table
    print(f"Adding chunk {count}")
    table.add(chunk_table)
    count += 1
    break

# +
# After inserting all chunks, show the shape of the table
num_rows = table.count()
num_columns = len(table.schema().names)

print(f"Table shape: ({num_rows}, {num_columns})")
