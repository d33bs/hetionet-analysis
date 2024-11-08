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

# # Biological Process and Gene Metapath Data Gathering - All
#
# This notebook focuses on gathering **all** data related to the following requirements:
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

import pandas as pd
import pyarrow as pa
from pyarrow import csv

from hetionet_utils.database import HetionetNeo4j

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

# build a sample result from HetionetNeo4j
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
    1024
    /
    # megabytes
    1024
    /
    # gigabytes
    1024,
    "GB",
)
