from typing import Any

from uniquery.query_engine.translators import QueryTranslator


class QueryEngine:

    def __init__(self, db_type, connector, is_native_mode = False, output_format = "TABULAR"):
        self.db_type = db_type
        self.connector = connector
        self.is_native_mode = is_native_mode
        self.output_format = output_format
        self.translator = QueryTranslator(db_type)

    def set_is_native_mode(self, is_native_mode: bool) -> None:
        self.is_native_mode = is_native_mode

    def set_output_format(self, output_format: str) -> None:
        # Handle valid op formats
        self.output_format = output_format

    def execute_query(self, query: str) -> Any:
        if not self.connector:
            raise ValueError("No active connection available")

        if not self.is_native_mode:
            query = self.translator.translate(query)

        result = self.connector.run_query(query)

        return result