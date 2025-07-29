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
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Any, Callable, Iterator, List, Tuple

import duckdb
import requests
from duckdb import DuckDBPyConnection

from hetionet_utils.sql import (
    extract_and_write_sql_block,
    remove_first_and_last_line_of_file,
)
from hetionet_utils.udf import get_paths_json

# get number of physical/logical CPUs
n_cpus = os.cpu_count() or 1

# heuristic factor: 4× cores for I/O bound work
DEFAULT_THREADS = min(256, n_cpus * 4)

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

# setup paths and filenames for data extraction
metapath_file = "./data/connectivity-search-precalculated-metapath-data.parquet"
PATHS_DB_PATH = "./data/hetio-connectivity-paths.duckdb"
PATHS_TABLE_NAME = "connectivity_paths"

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

with duckdb.connect(duckdb_filename) as ddb:
    # copy data directly to Parquet from DuckDB
    ddb.execute(
        f"""
        COPY (
            SELECT * 
            FROM dj_hetmech_app_node
        )
        TO './data/node_lookup_table.parquet'
        (FORMAT parquet, COMPRESSION zstd);
        """
    )

# +
# %%time
# create a paths table
# file holding all the per‐combination Parquet outputs
output_dir = "./data/connectivity_paths_by_metapath/"

if not pathlib.Path(output_dir).is_dir():
    pathlib.Path(output_dir).mkdir(parents=True)

    with duckdb.connect() as ddb:
        ddb.create_function(
            "get_paths_json",
            get_paths_json,
            parameters=["INTEGER", "INTEGER", "VARCHAR"],
            return_type="VARCHAR",
            null_handling="special",
        )
        ddb.execute(
            f"""
            COPY (
                WITH metapaths AS (
                    SELECT 
                        /* gather only combinations
                        we require for path APIpath_file queries */
                        source_id,
                        target_id,
                        metapath_id
                    FROM read_parquet('{metapath_file}')
                ),
                paths_json AS (
                    SELECT
                        source_id,
                        target_id,
                        metapath_id,
                        /* we cast data from an API
                        as JSON records */
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
            TO '{output_dir}'
            (FORMAT parquet, 
                COMPRESSION zstd, 
                PARTITION_BY (source_id, target_id, metapath_id
                )
            );
            """
        )
# show the number of parquet files we generated
len(list(pathlib.Path(output_dir).rglob("**/*.parquet")))
# +
def metapath_generator(metapath_file: str) -> Iterator[Tuple[int, int, str]]:
    """Stream (source_id, target_id, metapath_id) one row at a time."""
    conn = duckdb.connect()
    cur = conn.cursor()
    cur.execute(
        f"SELECT source_id, target_id, metapath_id "
        f"FROM read_parquet('{metapath_file}')"
    )
    while True:
        row = cur.fetchone()
        if row is None:
            break
        yield row
    conn.close()


def _export_one(
    source_id: int,
    target_id: int,
    metapath_id: str,
    output_dir: str,
    get_paths_json: Callable[[int, int, str], Any],
) -> None:
    """Export one triplet to its own Parquet file."""
    conn = duckdb.connect()
    conn.create_function(
        "get_paths_json",
        get_paths_json,
        parameters=["INTEGER", "INTEGER", "VARCHAR"],
        return_type="VARCHAR",
        null_handling="special",
    )
    out_file = (
        f"{output_dir}/paths_s{source_id}_" f"t{target_id}_m{metapath_id}.parquet"
    )
    conn.execute(
        f"""
        COPY (
          WITH paths_json AS (
            SELECT
              {source_id} AS source_id, 
              {target_id} AS target_id,
              '{metapath_id}' AS metapath_id,
              CAST(get_paths_json({source_id}, {target_id}, '{metapath_id}')
                   AS JSON) AS paths
          )
          SELECT
            source_id, target_id, metapath_id,
            unnest(json_extract(paths,'$[*].node_ids'))      AS node_ids,
            unnest(json_extract(paths,'$[*].rel_ids'))       AS rel_ids,
            unnest(json_extract(paths,'$[*].PDP'))           AS PDP,
            unnest(json_extract(paths,'$[*].PC'))            AS PC,
            unnest(json_extract(paths,'$[*].DWPC'))          AS DWPC,
            unnest(json_extract(paths,
                  '$[*].percent_of_DWPC'))                AS percent_of_DWPC,
            unnest(json_extract(paths,'$[*].score'))         AS score
          FROM paths_json
        )
        TO '{out_file}'
        (FORMAT parquet, COMPRESSION zstd)
        """
    )
    conn.close()


def parallel_export_threads_streaming(
    metapath_file: str,
    output_dir: str,
    get_paths_json: Callable[[int, int, str], Any],
    max_workers: int = 8,
) -> None:
    """
    Stream and thread-parallelize one‐file‐per‐triplet exports
    without ever building a huge in-memory list.
    """
    pathlib.Path(output_dir).mkdir(exist_ok=True, parents=True)
    gen = metapath_generator(metapath_file)

    pending = set()
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        for source_id, target_id, metapath_id in gen:
            # submit one more job
            fut = exe.submit(
                _export_one,
                source_id,
                target_id,
                metapath_id,
                output_dir,
                get_paths_json,
            )
            pending.add(fut)

            # if we have too many in flight, wait for some to finish
            if len(pending) >= max_workers * 2:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                # re-raise any errors
                for d in done:
                    d.result()

        # finally, wait for all remaining to finish
        for d in pending:
            d.result()


# -

parallel_export_threads_streaming(
        metapath_file=metapath_file,
        output_dir="./data/connectivity-paths-parquet-dataset",
        get_paths_json=get_paths_json,
        max_workers=DEFAULT_THREADS,
)

# +
import pathlib
import duckdb
from typing import Any, Callable, Iterator, List, Tuple
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

Triplet = Tuple[int, int, str]

def metapath_generator(
    metapath_file: str
) -> Iterator[Triplet]:
    """Stream (source_id, target_id, metapath_id) one row at a time."""
    conn = duckdb.connect()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT source_id, target_id, metapath_id
        FROM read_parquet('{metapath_file}') LIMIT 8000
    """)
    while True:
        row = cur.fetchone()
        if row is None:
            break
        yield row  # type: ignore
    conn.close()

def batch_generator(
    gen: Iterator[Triplet],
    batch_size: int
) -> Iterator[List[Triplet]]:
    """Group up to batch_size items from gen into lists."""
    batch: List[Triplet] = []
    for trip in gen:
        batch.append(trip)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

def _export_batch(
    batch: List[Tuple[int,int,str]],
    output_file: str,
    get_paths_json: Callable[[int, int, str], Any]
) -> None:
    """Export one batch of triplets into a single Parquet file."""
    conn = duckdb.connect()
    try:
        conn.create_function(
            "get_paths_json",
            get_paths_json,
            parameters=["INTEGER","INTEGER","VARCHAR"],
            return_type="VARCHAR",
            null_handling="special",
        )
    except duckdb.CatalogException:
        pass

    # build VALUES rows
    vals = ",\n".join(
        f"({s}, {t}, '{m}')"
        for s, t, m in batch
    )

    sql = f"""
        COPY (
          WITH metapaths(source_id, target_id, metapath_id) AS (
            VALUES
              {vals}
          ),
          paths_json AS (
            SELECT
              source_id,
              target_id,
              metapath_id,
              CAST(get_paths_json(source_id, target_id, metapath_id)
                   AS JSON) AS paths
            FROM metapaths
          )
          SELECT
            source_id,
            target_id,
            metapath_id,
            unnest(json_extract(paths,'$[*].node_ids'))      AS node_ids,
            unnest(json_extract(paths,'$[*].rel_ids'))       AS rel_ids,
            unnest(json_extract(paths,'$[*].PDP'))           AS PDP,
            unnest(json_extract(paths,'$[*].PC'))            AS PC,
            unnest(json_extract(paths,'$[*].DWPC'))          AS DWPC,
            unnest(json_extract(paths,
                  '$[*].percent_of_DWPC'))                AS percent_of_DWPC,
            unnest(json_extract(paths,'$[*].score'))         AS score
          FROM paths_json
        )
        TO '{output_file}'
        (FORMAT parquet, COMPRESSION zstd)
        """
    conn.execute(sql)
    conn.close()

def parallel_export_batched(
    metapath_file: str,
    output_dir: str,
    get_paths_json: Callable[[int, int, str], Any],
    batch_size: int = 10,
    max_workers: int = 4
) -> None:
    """
    Threaded, streaming export in batches of triplets per Parquet.

    Args:
      metapath_file:   Parquet with source_id,target_id,metapath_id.
      output_dir:      Directory for output files.
      get_paths_json:  UDF to fetch the JSON paths.
      batch_size:      How many triplets to pack into each Parquet.
      max_workers:     Number of threads to use.
    """
    pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)
    gen = metapath_generator(metapath_file)
    batches = batch_generator(gen, batch_size)

    pending = set()
    file_idx = 0

    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        for batch in batches:
            file_idx += 1
            out_file = f"{output_dir}/paths_batch_{file_idx:05d}.parquet"
            fut = exe.submit(_export_batch, batch, out_file, get_paths_json)
            pending.add(fut)

            if len(pending) >= max_workers * 2:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for d in done:
                    d.result()

        # drain remaining
        for d in pending:
            d.result()



# -

parallel_export_batched(
        metapath_file=metapath_file,
        output_dir="./data/connectivity-paths-parquet-dataset",
        get_paths_json=get_paths_json,
        batch_size=100,
        max_workers=8,
)

with duckdb.connect() as ddb:
    result = ddb.execute("""
    SELECT DISTINCT
        source_id,
        target_id,
        metapath_id
    FROM read_parquet('./data/connectivity-paths-parquet-dataset/paths_batch_00001.parquet')
    """).df()
result


# +
def metapath_generator(metapath_file: str) -> Iterator[Tuple[int, int, str]]:
    """Yield (source_id, target_id, metapath_id) one row at a time."""
    conn = duckdb.connect()
    cur = conn.cursor()
    cur.execute(
        f"SELECT source_id, target_id, metapath_id "
        f"FROM read_parquet('{metapath_file}')"
    )
    while True:
        row = cur.fetchone()
        if row is None:
            break
        yield row  # type: ignore
    conn.close()


def init_db(db_path: str) -> None:
    """Create the DuckDB file and the target table if missing."""
    conn = duckdb.connect(db_path)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {PATHS_TABLE_NAME} (
          source_id        INTEGER,
          target_id        INTEGER,
          metapath_id      VARCHAR,
          node_ids         INTEGER[],
          rel_ids          INTEGER[],
          PDP              DOUBLE,
          PC               DOUBLE,
          DWPC             DOUBLE,
          percent_of_DWPC  DOUBLE,
          score            DOUBLE
        )
    """
    )
    conn.commit()
    conn.close()


def _insert_one(
    source_id: int,
    target_id: int,
    metapath_id: str,
    db_path: str,
    get_paths_json: Callable[[int, int, str], Any],
) -> None:
    """Worker: fetch one triplet and INSERT into the DuckDB table."""
    conn = duckdb.connect(db_path)
    # register UDF, but ignore “already exists”
    try:
        conn.create_function(
            "get_paths_json",
            get_paths_json,
            parameters=["INTEGER", "INTEGER", "VARCHAR"],
            return_type="VARCHAR",
            null_handling="special",
        )
    except duckdb.CatalogException:
        pass

    conn.execute(
        f"""
      INSERT INTO {PATHS_TABLE_NAME}
      WITH paths_json AS (
        SELECT
          {source_id}   AS source_id,
          {target_id}   AS target_id,
          '{metapath_id}' AS metapath_id,
          CAST(get_paths_json(
            {source_id}, {target_id}, '{metapath_id}'
          ) AS JSON) AS paths
      )
      SELECT
        source_id,
        target_id,
        metapath_id,
        unnest(json_extract(paths,'$[*].node_ids'))     AS node_ids,
        unnest(json_extract(paths,'$[*].rel_ids'))      AS rel_ids,
        unnest(json_extract(paths,'$[*].PDP'))          AS PDP,
        unnest(json_extract(paths,'$[*].PC'))           AS PC,
        unnest(json_extract(paths,'$[*].DWPC'))         AS DWPC,
        unnest(json_extract(paths,
              '$[*].percent_of_DWPC'))                 AS percent_of_DWPC,
        unnest(json_extract(paths,'$[*].score'))        AS score
      FROM paths_json
    """
    )
    conn.close()


def parallel_insert_to_db(
    metapath_file: str,
    db_path: str,
    get_paths_json: Callable[[int, int, str], Any],
    max_workers: int = 8,
) -> None:
    """
    Stream triplets and use threads to INSERT into one DuckDB table.

    Args:
      metapath_file: Parquet file with source/target/metapath.
      db_path:       Path to the .duckdb file.
      get_paths_json: UDF for pulling the JSON path.
      max_workers:   Thread count.
    """
    # ensure folder & table are ready
    pathlib.Path(db_path).parent.mkdir(exist_ok=True, parents=True)
    init_db(db_path)
    print("created db")

    pending = set()
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        for src, tgt, mp in metapath_generator(metapath_file):
            fut = exe.submit(_insert_one, src, tgt, mp, db_path, get_paths_json)
            pending.add(fut)

            if len(pending) >= max_workers * 2:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for d in done:
                    d.result()

        # drain remaining
        for d in pending:
            d.result()


# -


# if we don't already have our paths database, create it
if not pathlib.Path(PATHS_DB_PATH).is_file():
    parallel_insert_to_db(
        metapath_file=metapath_file,
        db_path=PATHS_DB_PATH,
        get_paths_json=get_paths_json,
        max_workers=DEFAULT_THREADS,
    )
pathlib.Path(PATHS_DB_PATH).is_file()

# show the count of the database
with duckdb.connect(PATHS_DB_PATH) as ddb:
    result = ddb.execute(
        """
        SELECT count(*)
        FROM connectivity_paths
        """
    ).df()
result

# show the count of the database
with duckdb.connect(PATHS_DB_PATH) as ddb:
    result = ddb.execute(
        """
        WITH paths AS (
            SELECT DISTINCT
                source_id,
                target_id,
                metapath_id
            FROM connectivity_paths
        )
        SELECT count(*)
        FROM paths
        """
    ).df()
result

# show an example of using the parquet file output
with duckdb.connect() as ddb:
    sample = ddb.execute(
        f"""
        SELECT *
        FROM read_parquet('{"./data/connectivity-search-precalculated-metapath-data.parquet"}')
        WHERE metapath_id=''
        LIMIT 5;
        """
    ).df()
sample


