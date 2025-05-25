from .mysql_connector import MySQLConnector
from .mongodb_connector import MongoDBConnector
from .neo4j_connector import Neo4jConnector
from ..utils.constants import DatabaseType

def get_connection(connection_details):
    try:
        connector = None
        database_type = DatabaseType(connection_details['type'])

        if database_type == DatabaseType.MYSQL:
            connector = MySQLConnector(
                connection_details['host'],
                connection_details['port'],
                connection_details['username'],
                connection_details['password'],
                connection_details['database']
            )
        elif database_type == DatabaseType.MONGO_DB:
            connector = MongoDBConnector(
                connection_details['host'],
                connection_details['port'],
                connection_details['username'],
                connection_details['password'],
                connection_details['database']
            )
        elif database_type == DatabaseType.NEO4J:
            connector = Neo4jConnector(
                connection_details['uri'],
                connection_details['username'],
                connection_details['password']
            )
        return connector

    except Exception as err:
            raise ValueError(f"{err}")
