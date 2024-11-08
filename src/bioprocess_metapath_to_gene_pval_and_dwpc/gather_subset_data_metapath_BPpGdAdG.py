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
# This notebook focuses on gathering **a subset** of data related
# to the following requirements and **focusing on metapath BPpGdAdG**:
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
from functools import partial

import lancedb
import pandas as pd
import pyarrow as pa
from joblib import Parallel, delayed
from pyarrow import csv

from hetionet_utils.combination import (
    generate_combinations_for_bioprocs_genes_and_metapaths,
    process_in_chunks_for_bioprocs_genes_and_metapaths,
)
from hetionet_utils.database import HetionetNeo4j

# -

# gather metapaths which are not in the metapaths_ignore.csv
df_metapaths = pd.read_csv("data/sources/metapaths.csv")
df_metapaths_ignore = pd.read_csv("data/sources/metapaths_ignore.csv")
df_metapaths = df_metapaths[
    ~df_metapaths["metapath"].isin(df_metapaths_ignore["metapath"])
]
# filter to a single metapath (instead of all)
df_metapaths = df_metapaths[df_metapaths["metapath"] == "BPpGdAdG"]
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

# build a sample result from HetionetNeo4j
hetiocli = HetionetNeo4j()
sample_result = hetiocli.get_metapath_data(
    source_id=str(table_bioprocesses[0][0]),
    target_id=int(str(table_genes[0][0])),
    metapath=str(table_metapaths[0][0]),
    columns=["source_id", "target_id", "PDP", "DWPC"],
)
sample_result

# export to file and measure the size
sample_result.to_parquet((filepath := "example_output.parquet"))
print(
    "Expected storage: ",
    (  # bytes
        pathlib.Path(filepath).stat().st_size
        /
        # kilobytes
        1024
        /
        # megabytes
        1024
        /
        # gigabytes
        1024
    )
    * expected_queries,
    "GB",
)
# remove the example file
pathlib.Path(filepath).unlink()

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
generator = generate_combinations_for_bioprocs_genes_and_metapaths(
    table_bioprocesses, table_genes, table_metapaths
)
# set a class method reference for use with the requests
get_metapath_data_with_pdp_and_dwpc = partial(
    hetiocli.get_metapath_data, columns=["source_id", "target_id", "PDP", "DWPC"]
)

# create a counter for gathering data and running iterations
count = 1

# Process and print chunks
for chunk_table in process_in_chunks_for_bioprocs_genes_and_metapaths(
    generator, chunk_size=3
):
    # add the chunk to the table
    print(f"Processing chunk {count}")

    # process threaded results
    results = Parallel(n_jobs=3, backend="threading")(
        delayed(get_metapath_data_with_pdp_and_dwpc)(source_id, target_id, metapath)
        for source_id, target_id, metapath in zip(
            chunk_table["source_id"].to_pylist(),
            chunk_table["target_id"].to_pylist(),
            chunk_table["metapath"].to_pylist(),
        )
    )

    # add a concatted dataframe of the results to the lancedb table
    table.add(pd.concat(results))

    # increment the counter
    count += 1

    # temporary break for feedback / testing
    break

# +
# After inserting all chunks, show the shape of the table
num_rows = table.count_rows()
num_columns = len(table.schema.names)

print(f"Table shape: ({num_rows}, {num_columns})")
