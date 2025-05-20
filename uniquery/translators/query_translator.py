from .query_generator import QueryGenerator
from uniquery.translators.sql_parser import SqlParser
from uniquery.connectors.mongodb_connector import MongoDBConnector
from uniquery.connectors.mysql_connector import MySQLConnector
from uniquery.connectors.neo4j_connector import Neo4jConnector


class QueryTranslator:
    def __init__(self):
        self.sql_parser = SqlParser()
        self.query_generator = QueryGenerator()

    def translate(self, sql_query: str, target_format: str):
        parsed_data = self.sql_parser.parse(sql_query)
        match target_format:
            case "SQL":
                return sql_query
            case "CYPHER":
                return self.query_generator.get_cypher_query(parsed_data)
            case "MQL":
                return self.query_generator.get_mongodb_query(parsed_data)
            case _:
                raise ValueError(f"Unsupported target format: {target_format}")
