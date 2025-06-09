import unittest

from pymongo.results import *

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
            "test_database"
        )
        self.query_engine = QueryEngine(mongodb, self.connector, False)

    def tearDown(self):
        self.connector.close()

    def test_insert_into_table(self):
        """MongoDB: INSERT_DATA from SQL INSERT INTO"""
        sql = "INSERT INTO employees (id, name, salary) VALUES (1, 'Alice', 5000), (2, 'Bob', 6000)"
        result = self.query_engine.execute_query(sql)
        self.assertIsInstance(result, InsertManyResult)
        self.assertTrue(result.acknowledged)
        self.assertEqual(len(result.inserted_ids), 2)

    def test_update_table(self):
        """MongoDB: UPDATE_DATA from SQL UPDATE"""
        sql = "UPDATE employees SET salary = 7500 WHERE id = 1"
        result = self.query_engine.execute_query(sql)
        self.assertIsInstance(result, UpdateResult)
        self.assertTrue(result.acknowledged)
        self.assertEqual(result.modified_count, 1)

    def test_delete_from_table(self):
        sql = "DELETE FROM employees WHERE id = 2"
        result = self.query_engine.execute_query(sql)
        self.assertIsInstance(result, DeleteResult)
        self.assertTrue(result.acknowledged)
        self.assertEqual(result.deleted_count, 1)

if __name__ == '__main__':
    unittest.main()
