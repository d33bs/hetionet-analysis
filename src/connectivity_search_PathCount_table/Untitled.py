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

# +
# %%time

import builtins
import json
import pathlib
from typing import Iterator, Tuple

import duckdb
from hetnetpy.hetnet import MetaGraph
from hetnetpy.neo4j import construct_pdp_query
from py2neo import Graph as Neo4jGraph

Triplet = Tuple[int, int, str]

TABLE_NAME = "paths"
DUCKDB_PATH = "./data/connectivity-path-data.duckdb"

def convert(type_id, value):
    """
    Convert a value by type id.
    """
    return type_id(value)


def type_lookup_and_convert(identifier: str):
    """
    Convert the node identifier type so it may be properly
    used within neo4j context.
    """
    
    with duckdb.connect("./data/connectivity-search.duckdb") as ddb:
        type_result = ddb.execute(
            f"""
            SELECT identifier_type
            FROM dj_hetmech_app_node
            WHERE identifier = '{identifier}'
            """
        ).fetchone()
        # use the type from builtins to convert the identifier properly
        return convert(getattr(builtins, type_result[0]), identifier)


def metapath_generator(parquet: str) -> Iterator[Triplet]:
    conn = duckdb.connect()
    cur = conn.cursor()
    cur.execute(
        f"SELECT source_identifier, target_identifier, metapath_id FROM read_parquet('{parquet}') LIMIT 1000"
    )
    while True:
        row = cur.fetchone()
        if row is None:
            break
        yield row  # type: ignore
    conn.close()


def load_metagraph(schema_json: str) -> MetaGraph:
    """Load your schema into a MetaGraph using your JSON’s field names."""
    with open(schema_json) as f:
        jd = json.load(f)

    mg = MetaGraph()

    # 1) add node kinds
    for kind in jd["metanode_kinds"]:
        mg.add_node(kind)

    # 2) add metaedge kinds
    for src, tgt, edge_kind, direction in jd["metaedge_tuples"]:
        mg.add_edge((src, tgt, edge_kind, direction))

    # 3) build the kind->abbrev map exactly as set_abbreviations expects
    kind_to_abbrev = {}
    for k, v in jd["kind_to_abbrev"].items():
        # k may be a string like 'Anatomy' or a tuple‐string like 'Anatomy,Compound,...'
        if isinstance(k, str) and "," in k:
            key = tuple(k.split(","))
        elif isinstance(k, list):
            key = tuple(k)
        else:
            key = k
        kind_to_abbrev[key] = v

    mg.set_abbreviations(kind_to_abbrev)
    return mg


def run_all_pdp_to_duckdb(
    neo4j_uri: str,
    schema_json: str,
    metapath_file: str,
    w: float = 0.5
) -> None:
    """
    For each (src, tgt, mp_id) in metapath_file:
      - look up the MetaPath
      - build the Cypher via construct_pdp_query
      - execute it in Neo4j
      - append the result rows into a DuckDB table
    """
    # 1) ensure our DuckDB file & table exist
    pathlib.Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(DUCKDB_PATH)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            source_identifier VARCHAR,
            target_identifier VARCHAR,
            metapath_id VARCHAR,
            path VARCHAR,
            PDP DOUBLE,
            percent_of_DWPC DOUBLE
        )
    """)
    conn.close()

    # 2) load schema and Neo4j client
    mg    = load_metagraph(schema_json)
    neo4j = Neo4jGraph(neo4j_uri)

    # 3) stream triples and append each batch
    for src, tgt, mp_abbrev in metapath_generator(metapath_file):
        # build and run the PDP query
        mp     = mg.metapath_from_abbrev(mp_abbrev)
        # follow structure from:
        # https://github.com/greenelab/connectivity-search-backend/blob/0a9a5a694c1df6a3a62d75d8b1122218f8bfb60f/dj_hetmech_app/serializers.py#L109
        cypher = construct_pdp_query(mp, property="identifier", path_style="string") + '\nLIMIT 10'
        
        # convert our types for use within neo4j context
        src = type_lookup_and_convert(identifier=src)
        tgt = type_lookup_and_convert(identifier=tgt)
        
        df     = neo4j.run(cypher, source=src, target=tgt, w=w).to_data_frame()

        # annotate with your identifiers
        df["source_identifier"] = str(src)
        df["target_identifier"] = str(tgt)
        df["metapath_id"]       = mp_abbrev
        df = df[["source_identifier","target_identifier", "metapath_id", "path", "PDP", "percent_of_DWPC"]]

        # 4) append into DuckDB
        conn = duckdb.connect(DUCKDB_PATH)
        
        # insert into persistent table
        conn.execute(f"""
            INSERT INTO {TABLE_NAME}
            SELECT
              source_identifier,
              target_identifier,
              metapath_id,
              path,
              PDP,
              percent_of_DWPC
            FROM df
        """)
        conn.close()

import pathlib
from typing import Tuple, Any
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

import duckdb
from py2neo import Graph as Neo4jGraph

Triplet = Tuple[int, int, str]

def _pdp_worker(
    task: Triplet,
    neo4j: Neo4jGraph,
    mg: Any,
    w: float,
    master_con: duckdb.DuckDBPyConnection
) -> None:
    """Fetch one PDP result and INSERT it into DuckDB via a thread-local cursor."""
    src, tgt, mp_abbrev = task

    # 1) build & run Cypher
    mp     = mg.metapath_from_abbrev(mp_abbrev)
    cypher = construct_pdp_query(
        mp, property="identifier", path_style="string"
    ) + "\nLIMIT 10"
    src2 = type_lookup_and_convert(src)
    tgt2 = type_lookup_and_convert(tgt)

    df = neo4j.run(cypher, source=src2, target=tgt2, w=w).to_data_frame()
    if df.empty:
        return

    # 2) annotate & select columns
    df["source_identifier"] = str(src2)
    df["target_identifier"] = str(tgt2)
    df["metapath_id"]       = mp_abbrev
    df = df[
        ["source_identifier","target_identifier",
         "metapath_id","path","PDP","percent_of_DWPC"]
    ]

    # 3) insert via a thread-local cursor
    cur = master_con.cursor()
    cur.register("tmp_df", df)
    cur.execute(f"""
        INSERT INTO {TABLE_NAME}
        SELECT * FROM tmp_df
    """)
    cur.unregister("tmp_df")
    cur.close()


def run_all_pdp_to_duckdb_parallel(
    neo4j_uri: str,
    schema_json: str,
    metapath_file: str,
    w: float = 0.5,
    max_workers: int = 12
) -> None:
    """Parallelized PDP → DuckDB using one master connection + cursors."""
    # 0) ensure DB & table exist
    pathlib.Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
    master_con = duckdb.connect(DUCKDB_PATH)
    master_con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            source_identifier  VARCHAR,
            target_identifier  VARCHAR,
            metapath_id        VARCHAR,
            path               VARCHAR,
            PDP                DOUBLE,
            percent_of_DWPC    DOUBLE
        )
    """)

    # 1) load schema & Neo4j client
    mg    = load_metagraph(schema_json)
    neo4j = Neo4jGraph(neo4j_uri)

    # 2) dispatch threaded work
    pending = set()
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        for triplet in metapath_generator(metapath_file):
            fut = exe.submit(
                _pdp_worker,
                triplet, neo4j, mg, w, master_con
            )
            pending.add(fut)

            if len(pending) >= max_workers * 2:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for d in done:
                    d.result()

        # drain remaining
        for d in pending:
            d.result()

    # 3) close master connection (this also checkpoints)
    master_con.close()

def _pdp_parquet_worker(
    task: Triplet,
    neo4j: Neo4jGraph,
    mg: Any,
    w: float,
    output_dir: str
) -> None:
    """Fetch one PDP result from Neo4j and write it to a Parquet file."""
    src, tgt, mp_abbrev = task

    # 1) build & run the Cypher
    mp     = mg.metapath_from_abbrev(mp_abbrev)
    cypher = (
        construct_pdp_query(mp, property="identifier", path_style="string")
        + "\nLIMIT 10"
    )
    src2 = type_lookup_and_convert(src)
    tgt2 = type_lookup_and_convert(tgt)

    df = neo4j.run(cypher, source=src2, target=tgt2, w=w).to_data_frame()
    if df.empty:
        return

    # 2) annotate & keep only the wanted columns
    df["source_identifier"] = str(src2)
    df["target_identifier"] = str(tgt2)
    df["metapath_id"]       = mp_abbrev
    df = df[
        ["source_identifier",
         "target_identifier",
         "metapath_id",
         "path",
         "PDP",
         "percent_of_DWPC"]
    ]

    # 3) write to Parquet
    out_file = (
        f"{output_dir}/pdp_s{src2}_t{tgt2}_{mp_abbrev}.parquet"
    )
    df.to_parquet(out_file, compression="zstd")


def run_all_pdp_to_parquet_parallel(
    neo4j_uri: str,
    schema_json: str,
    metapath_file: str,
    output_dir: str,
    w: float = 0.5,
    max_workers: int = 12
) -> None:
    """
    Parallelize PDP queries over (src, tgt, mp) triples and write each
    result to its own Parquet file.

    Args:
        neo4j_uri:     Bolt URI for your Neo4j instance.
        schema_json:   Path to the hetnet metagraph JSON.
        metapath_file: Parquet of (source_id, target_id, metapath_id).
        output_dir:    Directory to emit per-triplet Parquet files.
        w:             Window size parameter for PDP queries.
        max_workers:   Number of concurrent threads.
    """
    # prepare output folder
    pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)
    print(output_dir)

    # load schema & Neo4j client
    mg    = load_metagraph(schema_json)
    neo4j = Neo4jGraph(neo4j_uri)

    # stream and dispatch
    pending = set()
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        for triplet in metapath_generator(metapath_file):
            fut = exe.submit(
                _pdp_parquet_worker,
                triplet, neo4j, mg, w, output_dir
            )
            pending.add(fut)

            # throttle to max_workers*2 in-flight tasks
            if len(pending) >= max_workers * 2:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for d in done:
                    d.result()

        # finish any remaining
        for d in pending:
            d.result()

run_all_pdp_to_parquet_parallel(
    neo4j_uri="bolt://neo4j.het.io:7687",
    schema_json="./data/hetionet-v1.0-metagraph.json",
    metapath_file="./data/connectivity-search-precalculated-metapath-data.parquet",
    output_dir="./example_parquet_paths_output"
)

# +
"""
duckdb:
20 workers and 12 workers
CPU times: user 18 s, sys: 3.7 s, total: 21.7 s
Wall time: 2min 42s
"""
import duckdb

with duckdb.connect("./data/connectivity-path-data.duckdb") as ddb:
    result = ddb.execute("""
    SELECT *
    FROM paths
    """).arrow()

result
# -

getattr(builtins, "int")

# +
import json
from pprint import pprint

schema_path = "./data/hetionet-v1.0-metagraph.json"  # adjust as needed
with open(schema_path) as f:
    jd = json.load(f)

print("Top-level keys:", list(jd.keys()))

# +
import pandas as pd
from py2neo import Graph

# 1) Create the client (adjust URI/auth as needed)
neo4j = Graph("bolt://neo4j.het.io:7687", auth=None)

# 2) Simple “ping” query
try:
    df = neo4j.run("RETURN 1 AS test").to_data_frame()
    print("Connectivity test passed:", df)
except Exception as e:
    print("Connectivity test failed:", e)

# 3) (Optional) More realistic smoke test: count of all nodes
try:
    df2 = neo4j.run(
        """
MATCH path = (n0:CellularComponent)-[:PARTICIPATES_GpCC]-(n1)-[:EXPRESSES_AeG]-(n2)-[:DOWNREGULATES_AdG]-(n3:Gene)
USING JOIN ON n1
WHERE n0.identifier = 'GO:0015030'
AND n3.identifier = 80761
AND n1 <> n3
WITH
[
size((n0)-[:PARTICIPATES_GpCC]-()),
size(()-[:PARTICIPATES_GpCC]-(n1)),
size((n1)-[:EXPRESSES_AeG]-()),
size(()-[:EXPRESSES_AeG]-(n2)),
size((n2)-[:DOWNREGULATES_AdG]-()),
size(()-[:DOWNREGULATES_AdG]-(n3))
] AS degrees, path
WITH path, reduce(pdp = 1.0, d in degrees| pdp * d ^ -0.5) AS PDP
WITH collect({paths: path, PDPs: PDP}) AS data_maps, count(path) AS PC, sum(PDP) AS DWPC
UNWIND data_maps AS data_map
WITH data_map.paths AS path, data_map.PDPs AS PDP, PC, DWPC
RETURN
  path AS neo4j_path,
  substring(reduce(s = '', node IN nodes(path)| s + '–' + node.name), 1) AS path,
  PDP,
  100 * (PDP / DWPC) AS percent_of_DWPC
ORDER BY percent_of_DWPC DESC
LIMIT 10
    """,
    ).to_data_frame()
    print(df2)
except Exception as e:
    print("Count query failed:", e)

df2["neo4j_path"].iloc[0]

# +
import pandas as pd

pd.read_parquet("./data/example-path-data/pdp_s38441_t20413_CCpGeAdG.parquet")
# -

print(
    "MATCH path = (n0:CellularComponent)-[:PARTICIPATES_GpCC]-(n1)-[:EXPRESSES_AeG]-(n2)-[:DOWNREGULATES_AdG]-(n3:Gene)\nUSING JOIN ON n1\nWHERE n0.identifier = 'GO:0015030' // Cajal body\nAND n3.identifier = 80761 // UPK3B\nAND n1 \u003C\u003E n3\nWITH\n[\nsize((n0)-[:PARTICIPATES_GpCC]-()),\nsize(()-[:PARTICIPATES_GpCC]-(n1)),\nsize((n1)-[:EXPRESSES_AeG]-()),\nsize(()-[:EXPRESSES_AeG]-(n2)),\nsize((n2)-[:DOWNREGULATES_AdG]-()),\nsize(()-[:DOWNREGULATES_AdG]-(n3))\n] AS degrees, path\nWITH path, reduce(pdp = 1.0, d in degrees| pdp * d ^ -0.5) AS PDP\nWITH collect({paths: path, PDPs: PDP}) AS data_maps, count(path) AS PC, sum(PDP) AS DWPC\nUNWIND data_maps AS data_map\nWITH data_map.paths AS path, data_map.PDPs AS PDP, PC, DWPC\nRETURN\n  path AS neo4j_path,\n  substring(reduce(s = '', node IN nodes(path)| s + '–' + node.name), 1) AS path,\n  PDP,\n  100 * (PDP / DWPC) AS percent_of_DWPC\nORDER BY percent_of_DWPC DESC\nLIMIT 10"
)

int("GO:0015030")


