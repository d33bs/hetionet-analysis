"""
Modules for interacting with various databases.
"""

from typing import List, Optional, Self

import pandas as pd
import requests
from neo4j import GraphDatabase


class HetionetNeo4j:
    """
    A class to interact with the Hetionet Neo4j database.

    Attributes:
        driver (neo4j.Driver):
            The Neo4j driver for database connection.
        query_node_identifier_to_neo4j_id (str):
            The Cypher query to get Neo4j ID from a node identifier.
    """

    def __init__(
        self: Self,
        uri: str = "bolt://neo4j.het.io:7687",
    ) -> None:
        """
        Initialize the HetionetNeo4j class with a connection
        to the Neo4j database.

        Args:
            uri (str):
                The URI of the Neo4j database.
        """
        self.driver = GraphDatabase.driver(uri, auth=None)
        self.query_node_identifier_to_neo4j_id = """
            MATCH (node)
            WHERE
              node.identifier = $identifier
            RETURN
              id(node) AS neo4j_id,
              node.identifier AS identifier
            ORDER BY neo4j_id
            """
        self.api_base_path = "https://search-api.het.io/v1"

    def close(self: Self) -> None:
        """
        Close the connection to the Neo4j database.
        """
        self.driver.close()

    def run_query(
        self: Self, query: str, parameters: Optional[dict] = None
    ) -> List[dict]:
        """
        Run a Cypher query against the Neo4j database.

        Args:
            query (str):
                The Cypher query to run.
            parameters (dict, optional):
                The parameters for the Cypher query.
                Default is None.

        Returns:
            List[dict]:
                A list of dictionaries containing the
                query results with keys "neo4j_id" and "identifier".
        """
        with self.driver.session() as session:
            result = session.run(query, parameters)
            return list(result)

    def get_id_from_identifer(self: Self, identifier: str) -> int:
        """
        Get the Neo4j ID of a node from its identifier.

        Args:
            identifier (str):
                The identifier of the node.

        Returns:
            int:
                The Neo4j ID of the node.
        """

        return self.run_query(
            query=self.query_node_identifier_to_neo4j_id,
            parameters={"identifier": identifier},
        )[0]["neo4j_id"]

    def get_metapath_data(
        self: Self,
        source_id: str,
        target_id: str,
        metapath: str,
        columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Retrieves metapath data between a source and target node from the
        Hetionet database via a REST API.

        Args:
            source_id (str):
                The identifier for the source node.
            target_id (str):
                The identifier for the target node.
            metapath (str):
                The metapath pattern to query, representing a specific path
                through the network.
            columns (Optional[List[str]], optional):
                A list of specific columns to include in the result DataFrame.
                If None, all columns are included. Defaults to None.

        Returns:
            pd.DataFrame:
                A DataFrame containing paths between the source and
                target based on the specified metapath. The DataFrame includes a
                'source_id' and 'target_id' column with the identifiers for context.
        """

        url = (
            f"{self.api_base_path}/paths/source/{self.get_id_from_identifer(source_id)}"
            f"/target/{self.get_id_from_identifer(target_id)}/metapath/{metapath}"
        )

        # gather response paths as dataframe
        df_result = pd.DataFrame(requests.get(url).json()["paths"])

        # add the source and target ids
        df_result["source_id"] = source_id
        df_result["target_id"] = target_id

        return df_result if columns is None else df_result[columns]
