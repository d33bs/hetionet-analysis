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
from pathlib import Path
from typing import Any, Iterator, Tuple

import duckdb
import pandas as pd
import pyarrow.parquet as pq
from hetnetpy.hetnet import MetaGraph
from hetnetpy.neo4j import construct_pdp_query
from neo4j import GraphDatabase

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


def _pdp_parquet_worker(
    task: Tuple[int, int, str],
    driver: GraphDatabase.driver,
    mg: Any,
    w: float,
    output_dir: str,
) -> None:
    src, tgt, mp_abbrev = task

    # 0) if we've already done this, skip it
    out_file = Path(output_dir) / f"pdp_s{src}_t{tgt}_{mp_abbrev}.parquet"
    if out_file.exists():
        return

    # 1) build & run the Cypher
    mp = mg.metapath_from_abbrev(mp_abbrev)
    cypher = (
        construct_pdp_query(mp, property="identifier", path_style="string")
        + "\nLIMIT 10"
    )
    src2 = type_lookup_and_convert(src)
    tgt2 = type_lookup_and_convert(tgt)

    # run via official driver
    with driver.session() as session:
        result = session.run(cypher, source=src2, target=tgt2, w=w)
        records = list(result)
    if not records:
        return

    # 2) convert to DataFrame and annotate
    df = pd.DataFrame([r.data() for r in records])
    df["source_identifier"] = str(src2)
    df["target_identifier"] = str(tgt2)
    df["metapath_id"] = mp_abbrev
    df = df[
        [
            "source_identifier",
            "target_identifier",
            "metapath_id",
            "path",
            "PDP",
            "percent_of_DWPC",
        ]
    ]

    # 3) write to Parquet
    df.to_parquet(str(out_file), compression="zstd")


def run_all_pdp_to_parquet_parallel(
    neo4j_uri: str,
    schema_json: str,
    metapath_file: str,
    output_dir: str,
    w: float = 0.5,
    max_workers: int = 12,
) -> None:
    """
    Parallelize PDP queries over (src, tgt, mp) triples and write each
    result to its own Parquet file.
    """
    # prepare output folder
    pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)

    # load schema & Neo4j driver
    mg = load_metagraph(schema_json)
    driver = GraphDatabase.driver(neo4j_uri, auth=None)

    pending = set()
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        for triplet in metapath_generator(metapath_file):
            fut = exe.submit(_pdp_parquet_worker, triplet, driver, mg, w, output_dir)
            pending.add(fut)

            # throttle to max_workers*2 in-flight tasks
            if len(pending) >= max_workers * 2:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for d in done:
                    d.result()

        # finish any remaining
        for d in pending:
            d.result()

    driver.close()


run_all_pdp_to_parquet_parallel(
    neo4j_uri="bolt://localhost:7687",
    schema_json="./data/hetionet-v1.0-metagraph.json",
    metapath_file="./data/connectivity-search-precalculated-metapath-data.parquet",
    output_dir="./example_parquet_paths_output",
)

# +
input_dir = Path("./example_parquet_paths_output")
output_pq = Path("./combined_paths.parquet")

# grab a ParquetFile and pull out its Arrow schema
first_file = next(input_dir.glob("*.parquet"))
pf = pq.ParquetFile(first_file)
arrow_schema = pf.schema_arrow

writer = pq.ParquetWriter(output_pq, arrow_schema)

for path in input_dir.glob("*.parquet"):
    tbl = pq.read_table(path)
    writer.write_table(tbl)

writer.close()
