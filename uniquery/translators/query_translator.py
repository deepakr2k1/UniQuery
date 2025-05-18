from .query_generator import QueryGenerator
from .sql_parser import SqlParser


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
            case _:
                raise ValueError(f"Unsupported target format: {target_format}")
