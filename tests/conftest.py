"""
Fixtures for testing with pytest.
See here for more information:
https://docs.pytest.org/en/stable/explanation/fixtures.html
"""

import pytest
from hetionet_utils.database import HetionetNeo4j

@pytest.fixture
def fixture_HetionetNeo4j() -> HetionetNeo4j:
    """
    Creates a HetionetNeo4j object for interacting with Hetionet Neo4j database.
    Closes the connection after work is completed.
    """

    # Initialize HetionetNeo4j
    yield (hetionet := HetionetNeo4j())

    # close the connection
    hetionet.close()




