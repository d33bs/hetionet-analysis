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
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Any, Iterator, Tuple

import duckdb
import pandas as pd
from hetnetpy.hetnet import MetaGraph
from hetnetpy.neo4j import construct_pdp_query
from neo4j import GraphDatabase, Driver

# Now each task is (index, source_id, target_id, metapath_id)
IndexedTask = Tuple[int, int, int, str]

TABLE_NAME = "paths"
DUCKDB_PATH = "./data/connectivity-path-data.duckdb"


def convert(type_id, value):
    return type_id(value)


def type_lookup_and_convert(identifier: str) -> Any:
    with duckdb.connect("./data/connectivity-search.duckdb") as ddb:
        type_result = ddb.execute(
            f"""
            SELECT identifier_type
            FROM dj_hetmech_app_node
            WHERE identifier = '{identifier}'
            """
        ).fetchone()
    return convert(getattr(builtins, type_result[0]), identifier)


def metapath_generator(parquet: str) -> Iterator[IndexedTask]:
    conn = duckdb.connect()
    cur = conn.execute(
        f"SELECT source_identifier, target_identifier, metapath_id "
        f"FROM read_parquet('{parquet}')"
    )
    idx = 0
    while True:
        row = cur.fetchone()
        if row is None:
            break
        idx += 1
        yield idx, row[0], row[1], row[2]
    conn.close()


def load_metagraph(schema_json: str) -> MetaGraph:
    with open(schema_json) as f:
        jd = json.load(f)

    mg = MetaGraph()
    for kind in jd["metanode_kinds"]:
        mg.add_node(kind)
    for src, tgt, edge_kind, direction in jd["metaedge_tuples"]:
        mg.add_edge((src, tgt, edge_kind, direction))

    kind_to_abbrev = {}
    for k, v in jd["kind_to_abbrev"].items():
        if isinstance(k, str) and "," in k:
            key = tuple(k.split(","))
        elif isinstance(k, list):
            key = tuple(k)
        else:
            key = k
        kind_to_abbrev[key] = v
    mg.set_abbreviations(kind_to_abbrev)
    return mg


def _pdp_worker(
    task: IndexedTask,
    driver: Driver,
    mg: MetaGraph,
    w: float,
    master_con: duckdb.DuckDBPyConnection,
) -> None:
    idx, src0, tgt0, mp_abbrev = task
    print(f"[Worker {idx}] START  {src0} → {tgt0} via {mp_abbrev}")

    mp = mg.metapath_from_abbrev(mp_abbrev)
    cypher = construct_pdp_query(mp, property="identifier", path_style="string") + "\nLIMIT 10"

    src = type_lookup_and_convert(src0)
    tgt = type_lookup_and_convert(tgt0)

    with driver.session() as session:
        res = session.run(cypher, source=src, target=tgt, w=w)
        records = [r.data() for r in res]

    if not records:
        print(f"[Worker {idx}] SKIP   {src0} → {tgt0} via {mp_abbrev} (no paths)")
        return

    df = pd.DataFrame(records)
    df["source_identifier"] = str(src)
    df["target_identifier"] = str(tgt)
    df["metapath_id"] = mp_abbrev
    df = df[[
        "source_identifier",
        "target_identifier",
        "metapath_id",
        "path",
        "PDP",
        "percent_of_DWPC",
    ]]

    cur = master_con.cursor()
    cur.register("tmp_df", df)
    cur.execute(f"""
        INSERT INTO {TABLE_NAME}
        SELECT * FROM tmp_df
    """)
    cur.unregister("tmp_df")
    cur.close()

    print(f"[Worker {idx}] FINISH {src0} → {tgt0} via {mp_abbrev}")


def run_all_pdp_to_duckdb(
    neo4j_uri: str,
    schema_json: str,
    metapath_file: str,
    w: float = 0.5
) -> None:
    # ensure DuckDB table
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

    mg = load_metagraph(schema_json)
    driver: Driver = GraphDatabase.driver(neo4j_uri, auth=None)

    for idx, src0, tgt0, mp_abbrev in metapath_generator(metapath_file):
        print(f"[Serial {idx}] START  {src0} → {tgt0} via {mp_abbrev}")

        mp = mg.metapath_from_abbrev(mp_abbrev)
        cypher = construct_pdp_query(mp, property="identifier", path_style="string") + "\nLIMIT 10"

        src = type_lookup_and_convert(src0)
        tgt = type_lookup_and_convert(tgt0)

        with driver.session() as session:
            result = session.run(cypher, source=src, target=tgt, w=w)
            records = [r.data() for r in result]
        df = pd.DataFrame(records)

        if df.empty:
            print(f"[Serial {idx}] SKIP   {src0} → {tgt0} via {mp_abbrev} (no paths)")
            continue

        df["source_identifier"] = str(src)
        df["target_identifier"] = str(tgt)
        df["metapath_id"] = mp_abbrev
        df = df[[
            "source_identifier",
            "target_identifier",
            "metapath_id",
            "path",
            "PDP",
            "percent_of_DWPC",
        ]]

        conn = duckdb.connect(DUCKDB_PATH)
        conn.register("tmp_df", df)
        conn.execute(f"""
            INSERT INTO {TABLE_NAME}
            SELECT * FROM tmp_df
        """)
        conn.unregister("tmp_df")
        conn.close()

        print(f"[Serial {idx}] FINISH {src0} → {tgt0} via {mp_abbrev}")

    driver.close()


def run_all_pdp_to_duckdb_parallel(
    neo4j_uri: str,
    schema_json: str,
    metapath_file: str,
    w: float = 0.5,
    max_workers: int = 12,
) -> None:
    # prepare DuckDB
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

    mg = load_metagraph(schema_json)
    driver: Driver = GraphDatabase.driver(neo4j_uri, auth=None)

    pending = set()
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        for task in metapath_generator(metapath_file):
            pending.add(exe.submit(_pdp_worker, task, driver, mg, w, master_con))
            if len(pending) >= max_workers * 2:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for d in done:
                    d.result()
        for d in pending:
            d.result()

    master_con.close()
    driver.close()


# example invocation
run_all_pdp_to_duckdb_parallel(
    neo4j_uri="bolt://localhost:7687",
    schema_json="./data/hetionet-v1.0-metagraph.json",
    metapath_file="./data/connectivity-search-precalculated-metapath-data.parquet",
    w=0.5,
    max_workers=8,
)

# -


