from .query_generator import QueryGenerator
from uniquery.query_engine.translators.sql_parser import SqlParser


class QueryTranslator:
    def __init__(self, target_format):
        self.sql_parser = SqlParser()
        self.query_generator = QueryGenerator()
        self.target_format = target_format

    def translate(self, sql_query: str):
        parsed_data = self.sql_parser.parse(sql_query)
        match self.target_format:
            case "mysql":
                return sql_query
            case "neo4j":
                return self.query_generator.get_cypher_query(parsed_data)
            case "mongodb":
                return self.query_generator.get_mongodb_query(parsed_data)
            case _:
                raise ValueError(f"Unsupported target format: {self.target_format}")
