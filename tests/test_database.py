"""
Tests for database.py
"""

from hetionet_utils.database import HetionetNeo4j


def test_get_id_from_gene_ontology_identifer(fixture_HetionetNeo4j: HetionetNeo4j):
    """
    Tests HetionetNeo4j.get_id_from_gene_ontology_identifer
    """

    # test Neo4j ID from Gene ontology identifier
    assert fixture_HetionetNeo4j.get_id_from_identifer("UBERON:0001135") == 18472

    # test Neo4j ID from Uber-anatomy ontology identifier
    assert fixture_HetionetNeo4j.get_id_from_identifer("GO:0000002") == 40731

    # test Neo4j ID from Entrez identifier
    assert fixture_HetionetNeo4j.get_id_from_identifer(1) == 16764


def test_get_metapath_data(fixture_HetionetNeo4j: HetionetNeo4j):
    """
    Tests HetionetNeo4j.get_metapath_data
    """

    # check a metapath result from hetionet
    assert fixture_HetionetNeo4j.get_metapath_data(
        source_id="UBERON:0001135", target_id="DOID:13223", metapath="AeGiGaD"
    ).iloc[1].to_dict() == {
        "metapath": "AeGiGaD",
        "node_ids": [18472, 34788, 13320, 17256],
        "rel_ids": [1722767, 1555684, 94499],
        "PDP": 6.413883958245207e-05,
        "percent_of_DWPC": 5.967299638241672,
        "score": -0.0,
        "PC": 447.0,
        "DWPC": 0.0010748385948547952,
    }


def test_run_query(fixture_HetionetNeo4j: HetionetNeo4j):
    """
    Tests HetionetNeo4j.run_query
    """

    assert dict(
        fixture_HetionetNeo4j.run_query(
            query="""
            MATCH (node)
            WHERE
              node.identifier = 1
            RETURN
              id(node) AS neo4j_id,
              node.identifier AS identifier,
              node.url as node_url
            ORDER BY neo4j_id
            """
        )[0]
    ) == {
        "neo4j_id": 16764,
        "identifier": 1,
        "node_url": "http://identifiers.org/ncbigene/1",
    }
