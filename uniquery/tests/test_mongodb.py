import unittest

from uniquery.src.connection_details_manager import ConnectionDetailsManager
from uniquery.src.connectors import get_connection
from uniquery.src.query_engine.translators import QueryTranslator
from uniquery.src.query_engine import QueryEngine
from uniquery.src.utils import DatabaseType

test_alias = "mongo"

class TestMongoDBWithEngine(unittest.TestCase):
    def setUp(self):
        db_type = DatabaseType("mongodb")
        manager = ConnectionDetailsManager()
        details = manager.get_connection(test_alias)
        connector = get_connection(details)
        engine = QueryEngine(db_type, connector)
        self.translator = QueryTranslator()

    def test_run(self):
        sql = "SELECT p.name FROM students"
        expected = "{}"
        self.assertEqual(self.translator.translate(sql), expected)

if __name__ == '__main__':
    unittest.main()