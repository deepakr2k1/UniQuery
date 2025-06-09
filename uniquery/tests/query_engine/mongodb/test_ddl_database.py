import unittest

from uniquery.src.query_engine import QueryEngine
from uniquery.src.connectors.mongodb_connector import MongoDBConnector
from uniquery.src.utils import DatabaseType

class TestEngineMongo(unittest.TestCase):
    def setUp(self):
        mongodb = DatabaseType.MONGO_DB
        self.connector = MongoDBConnector(
            "127.0.0.1",
            27017,
            "admin",
            "admin123",
            "student"
        )
        self.query_engine = QueryEngine(mongodb, self.connector, False)

    def tearDown(self):
        self.connector.close()

    # DATABASE OPERATIONS
    def test_create_database(self):
        sql = "CREATE DATABASE employee"
        expected_output = True
        self.assertEqual(expected_output, self.query_engine.execute_query(sql))

    def test_create_existing_database(self):
        sql = "CREATE DATABASE employee"
        with self.assertRaises(Exception) as context:
            self.query_engine.execute_query(sql)
        expected_exception = "Database `employee` already exist"
        self.assertIn(expected_exception, str(context.exception))

    def test_use_database(self):
        sql = "USE DATABASE employee"
        expected_output = True
        self.assertEqual(expected_output, self.query_engine.execute_query(sql))

    def test_use_database_not_found(self):
        sql = "USE DATABASE order"
        with self.assertRaises(Exception) as context:
            self.query_engine.execute_query(sql)
        expected_exception = "Database `order` does not exist"
        self.assertIn(expected_exception, str(context.exception))

    def test_drop_database(self):
        sql = "DROP DATABASE employee"
        expected_output = True
        self.assertEqual(expected_output, self.query_engine.execute_query(sql))

    def test_drop_database_not_found(self):
        sql = "DROP DATABASE order"
        with self.assertRaises(Exception) as context:
            self.query_engine.execute_query(sql)
        expected_exception = "Database `order` does not exist"
        self.assertIn(expected_exception, str(context.exception))

    def test_show_databases(self):
        sql = "SHOW DATABASES"
        expected_output = ['admin', 'config', 'local', 'student', 'test']
        self.assertEqual(expected_output, self.query_engine.execute_query(sql))

if __name__ == '__main__':
    unittest.main()
