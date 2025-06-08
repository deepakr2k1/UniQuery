from uniquery.src.utils import DatabaseType
from uniquery.src.query_engine.translators.sql_parser import SqlParser
from uniquery.src.query_engine.translators.query_generator import get_mongodb_query, get_cypher_query

class QueryTranslator:
    def __init__(self, database_type: DatabaseType):
        self.sql_parser = SqlParser()
        self.database_type = database_type

    def translate(self, sql_query: str):
        try:
            parsed_data = self.sql_parser.parse(sql_query)

            if self.database_type.is_sql():
                return sql_query
            elif self.database_type.is_mql():
                return get_mongodb_query(parsed_data)
            elif self.database_type.is_cypher():
                return get_cypher_query(parsed_data)

            raise Exception(f"Translation is not supported for database type: {self.database_type.value}")

        except Exception as err:
            raise Exception(f"Error Translating SQL query: {err}")
