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

# # Gather Connectivity Search `PathCount` Table
#
# Negar mentioned needing data from a PostgreSQL database archive,
# `connectivity-search-pg_dump.sql.gz`, which was created as part of
# https://github.com/greenelab/connectivity-search-backend/blob/main/README.md .
# The archive is available under https://zenodo.org/records/3978766 .
# Only the `PathCount` Table is needed in order to extract single metapaths
# at a time (needed for other work).
#
# Additionally, we extract a `Node` table to help associate `identifier`
# with `id` (internal versus external labels for data).

# +
import gzip
import os
import pathlib

import duckdb
import requests
from duckdb import DuckDBPyConnection

from hetionet_utils.udf import get_paths_json
from hetionet_utils.sql import (
    extract_and_write_sql_block,
    remove_first_and_last_line_of_file,
)

# create the data dir
pathlib.Path("data").mkdir(exist_ok=True)

# url for source data
url = (
    "https://zenodo.org/records/3978766/files/"
    "connectivity-search-pg_dump.sql.gz?download=1"
)

# local archive file location
sql_file = "data/connectivity-search-pg_dump.sql.gz"

# expected number of tables within dump
expected_table_count = 15

# table which is targeted within the sql archive above
target_pathcount_table_name = "public.dj_hetmech_app_pathcount"
target_identifier_table_name = "public.dj_hetmech_app_node"

# duckdb filename
duckdb_filename = "data/connectivity-search.duckdb"

# +
# gather postgresql database archive

# if the file doesn't exist, download it
if not pathlib.Path(sql_file).exists():
    # Download the file in streaming mode
    response = requests.get(url, stream=True)

    # Check if the request was successful
    response.raise_for_status()

    # Write the response content to a file in chunks
    with open(sql_file, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file.write(chunk)

pathlib.Path(sql_file).exists()
# -

# show the tables
count = 0
create_table_names = []
with gzip.open(sql_file, "rt") as f:
    for line in f:
        # seek table creation lines
        if "CREATE TABLE" in line:
            # append a cleaned up line from the table creation statement
            # so we may gather the table name.
            create_table_names.append(
                line.strip().replace(" (", "").replace("CREATE TABLE ", "")
            )
            count += 1
            # there are roughly 15 tables
            # so we break here to avoid further processing
            if count == expected_table_count:
                break
create_table_names

# gather the create table statements for each table
for table_name in create_table_names:
    extract_and_write_sql_block(
        sql_file=sql_file,
        sql_start=f"CREATE TABLE {table_name}",
        sql_end=";",
        output_file=(create_pathcount_table_file := f"create_table.{table_name}.sql"),
    )

# show the create table statements
for table_name in create_table_names:
    with open(f"create_table.{table_name}.sql", "r") as table_create_sql:
        table_sql = "".join(table_create_sql.readlines())

    print(table_sql)

# gather the data for populating the tables
# note: this can take a while!
# (we're extracting large portions of TSV data
# from a single file.)
for table_name in create_table_names:
    copy_data_file = f"copy_data.{table_name}.tsv"

    # only create the file if we don't already have it.
    if not pathlib.Path(copy_data_file).is_file():
        extract_and_write_sql_block(
            sql_file=sql_file,
            sql_start=f"COPY {table_name}",
            sql_end="\\.",
            output_file=copy_data_file,
        )
        # replace the first and last lines of the copy files
        # as these are the header and data termination lines
        # which have no actual values.
        remove_first_and_last_line_of_file(target_file=copy_data_file)

# create the tables within a duckdb database
if not pathlib.Path(duckdb_filename).is_file():
    with duckdb.connect(duckdb_filename) as ddb:
        for table_name in create_table_names:
            with open(f"create_table.{table_name}.sql", "r") as table_create_sql:
                # read the table creation sql into duckdb execution
                # replace "public." for table naming, and "jsonb" to
                # align data typing from postrgres to duckdb (duckdb
                # includes no "jsonb" type but is compatible with the
                # insertion data in the form "json").
                ddb.execute(
                    "".join(table_create_sql.readlines())
                    .replace("public.", "")
                    .replace("jsonb", "json")
                )

# # copy the data from the files to duckdb database
# using tab-delimited files.
# note: this can take a while!
# (we're ingesting data from TSV format into DuckDB)
with duckdb.connect(duckdb_filename) as ddb:
    for table_name in create_table_names:
        # only copy data if we have data to copy
        if os.path.getsize(copy_data_file := f"copy_data.{table_name}.tsv") > 0:
            table_name = table_name.replace("public.", "")  # noqa: PLW2901

            # only populate the table if it hasn't already been
            # populated.
            row_count = ddb.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            if row_count == 0:
                ddb.execute(
                    f"""
                    COPY {table_name}
                    FROM '{copy_data_file}'
                    (DELIMITER '\t', HEADER false);
                    """
                )

# +
# read and export data to parquet for simpler use
metapath_file = "./data/connectivity-search-precalculated-metapath-data.parquet"

# if we don't already have a file, create it
if not pathlib.Path(metapath_file).is_file():
    with duckdb.connect(duckdb_filename) as ddb:
        # copy data directly to Parquet from DuckDB
        ddb.execute(
            f"""
            COPY (
                SELECT
                    pathcount.id,
                    source.identifier AS source_identifier,
                    target.identifier AS target_identifier,
                    pathcount.metapath_id,
                    pathcount.path_count,
                    /* we build an adjusted p_value based on the implementation
                    found here:
                    https://github.com/greenelab/connectivity-search-backend/blob/main/dj_hetmech_app/models.py#L94
                    */
                    CASE
                        WHEN pathcount.p_value * metapath.n_similar > 1.0 THEN 1.0
                        ELSE pathcount.p_value * metapath.n_similar
                    END AS adjusted_p_value,
                    pathcount.p_value,
                    pathcount.dwpc,
                    degree.source_degree,
                    degree.target_degree,
                    degree.n_dwpcs,
                    degree.n_nonzero_dwpcs,
                    degree.nonzero_mean,
                    degree.nonzero_sd,
                    pathcount.source_id,
                    pathcount.target_id,
                    pathcount.dgp_id
                FROM
                    dj_hetmech_app_pathcount as pathcount
                LEFT JOIN dj_hetmech_app_node AS source ON
                    pathcount.source_id = source.id
                LEFT JOIN dj_hetmech_app_node AS target ON
                    pathcount.target_id = target.id
                LEFT JOIN dj_hetmech_app_degreegroupedpermutation as degree ON
                    pathcount.dgp_id = degree.id
                    AND pathcount.metapath_id = degree.metapath_id
                LEFT JOIN dj_hetmech_app_metapath as metapath ON
                    pathcount.metapath_id = metapath.abbreviation
            )
            TO '{metapath_file}'
            (FORMAT parquet, COMPRESSION zstd);
            """
        )
# confirm that we have the file
pathlib.Path(metapath_file).is_file()
# -

# show an row count using the parquet file output
with duckdb.connect() as ddb:
    count = ddb.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet('{target_file}')
        """
    ).df()
count

# show an example of using the parquet file output
with duckdb.connect() as ddb:
    sample = ddb.execute(
        f"""
        SELECT *
        FROM read_parquet('{target_file}')
        LIMIT 5;
        """
    ).df()
sample

# show results in alignment with:
# https://het.io/search/?source=34901&target=4145
with duckdb.connect() as ddb:
    sample = ddb.execute(
        f"""
        SELECT *
        FROM read_parquet('{target_file}')
        WHERE source_id = 34901
        AND target_id = 4145;
        """
    ).df()
sample

# double check our count is distinct
with duckdb.connect() as ddb:
    sample = ddb.execute(
        f"""
        WITH example AS (
            SELECT 
                DISTINCT
                source_id,
                target_id,
                id
            FROM read_parquet('{target_file}')
        )
        SELECT count(*) FROM example;
        """
    ).df()
sample

# %%time
# create a paths table
path_file = "./data/connectivity-search-precalculated-path-data.parquet"
if not pathlib.Path(target_file).is_file():
    with duckdb.connect() as ddb:
        ddb.create_function(
            "get_paths_json",
            get_paths_json,
            parameters=["INTEGER", "INTEGER", "VARCHAR"],
            return_type="VARCHAR",
            null_handling="special",
        )
        sample = ddb.execute(
            f"""
            COPY (
                WITH metapaths AS (
                    SELECT 
                        source_id,
                        target_id,
                        metapath_id
                    FROM read_parquet('{metapath_file}')
                    LIMIT 2
                ),
                paths_json AS (
                    SELECT
                        source_id,
                        target_id,
                        metapath_id,
                        CAST(
                            get_paths_json(
                                source_id,
                                target_id,
                                metapath_id
                            ) 
                        AS JSON
                        ) AS paths
                      FROM metapaths
                    )
                    SELECT 
                        source_id,
                        target_id,
                        metapath_id,
                        /* we unnest the json data from each record
                        so as to show it in a more flattened representation */
                        unnest(json_extract(paths, '$[*].node_ids')) as node_ids,
                        unnest(json_extract(paths, '$[*].rel_ids')) as rel_ids,
                        unnest(json_extract(paths, '$[*].PDP')) as PDP,
                        unnest(json_extract(paths, '$[*].PC')) as PC,
                        unnest(json_extract(paths, '$[*].DWPC')) as DWPC,
                        unnest(json_extract(paths, '$[*].percent_of_DWPC')) as percent_of_DWPC,
                        unnest(json_extract(paths, '$[*].score')) as score
                    FROM paths_json
                )
                TO '{path_file}'
                (FORMAT parquet, COMPRESSION zstd);
            """
        )
pathlib.Path(path_file).is_file()


