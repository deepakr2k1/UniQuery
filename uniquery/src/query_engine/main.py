import json
from typing import Any

from uniquery.src.utils import DatabaseType
from uniquery.src.query_engine.translators import QueryTranslator

class QueryEngine:

    def __init__(self, database_type: DatabaseType, connector, is_native_mode = False, output_format = "TABULAR"):
        self.database_type = database_type
        self.connector = connector
        self.is_native_mode = is_native_mode
        self.output_format = output_format
        self.translator = QueryTranslator(database_type)

    def set_is_native_mode(self, is_native_mode: bool) -> None:
        self.is_native_mode = is_native_mode

    def set_output_format(self, output_format: str) -> None:
        # Handle valid op formats
        self.output_format = output_format

    def format_result(self, result: Any) -> str:
        return result

    def execute_query(self, query: str) -> Any:
        try:
            if not self.connector:
                raise Exception("No active connection available")

            if not self.is_native_mode:
                query = self.translator.translate(query)
            else:
                if self.database_type.is_mql():
                    query = json.loads(query)

            print(f"Translated MQL query: {query}")

            result = self.connector.run_query(query)

            return self.format_result(result)

        except Exception as err:
            raise Exception(f"Error executing query: {err}")