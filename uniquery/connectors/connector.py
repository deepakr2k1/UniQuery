from enum import Enum
from uniquery.connectors import MongoDBConnector, MySQLConnector, Neo4jConnector

def get_connection(connection_details):
    connector = None

    print(connection_details['type'])

    if connection_details['type'] == "mysql":
        connector = MySQLConnector(
            connection_details['host'],
            connection_details['port'],
            connection_details['username'],
            connection_details['password'],
            connection_details['database']
        )
    elif connection_details['type'] == "mongodb":
        connector = MongoDBConnector(
            connection_details['host'],
            connection_details['port'],
            connection_details['username'],
            connection_details['password'],
            connection_details['database']
        )
    elif connection_details['type'] == "neo4j":
        connector = Neo4jConnector(
            connection_details['host'],
            connection_details['port'],
            connection_details['uri']
        )
    else:
        raise ValueError(f"Unsupported connector type: {connection_details['type']}")

    return connector
