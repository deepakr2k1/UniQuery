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
            "test_database"
        )
        self.query_engine = QueryEngine(mongodb, self.connector, False)

    def tearDown(self):
        self.connector.close()

    def test_create_table(self):
        sql = """CREATE TABLE employees (
            id INT PRIMARY KEY,
            name VARCHAR(100),
            department VARCHAR(50),
            salary DECIMAL(10,2)
        )"""
        expected_output = True
        self.assertEqual(expected_output, self.query_engine.execute_query(sql))

    def test_create_table_exist(self):
        sql = """CREATE TABLE employees (
            id INT PRIMARY KEY,
            name VARCHAR(100)
        )"""
        with self.assertRaises(Exception) as context:
            self.query_engine.execute_query(sql)
        self.assertIn("Collection `employees` already exist", str(context.exception))

    def test_drop_table(self):
        sql = "DROP TABLE employees"
        expected_output = True
        self.assertEqual(expected_output, self.query_engine.execute_query(sql))

    def test_drop_table_not_found(self):
        sql = "DROP TABLE nonexistent"
        with self.assertRaises(Exception) as context:
            self.query_engine.execute_query(sql)
        self.assertIn("Collection `nonexistent` does not exist", str(context.exception))

    def test_rename_table(self):
        sql = "ALTER TABLE employees RENAME TO staff"
        expected_output = True
        self.assertEqual(expected_output, self.query_engine.execute_query(sql))

    def test_rename_table_not_found(self):
        sql = "ALTER TABLE nonexistent RENAME TO archive"
        with self.assertRaises(Exception) as context:
            self.query_engine.execute_query(sql)
        self.assertIn("Collection `nonexistent` does not exist", str(context.exception))

    def test_show_tables(self):
        sql = "SHOW TABLES"
        expected_output = ['__init__', 'staff']
        self.assertEqual(expected_output, self.query_engine.execute_query(sql))

    def test_show_table(self):
        sql = "SHOW TABLE staff"
        print(self.query_engine.execute_query(sql))
        expected_output = {
            'collection': 'staff',
            'indexes': [
                {'name': '_id_', 'keys': [('_id', 1)]}
            ],
            'stats': {
                'count': 0,
                'size': 0,
                'storageSize': 4096
            },
            'sample_fields': []
        }
        self.assertEqual(expected_output, self.query_engine.execute_query(sql))

    def test_show_table_not_found(self):
        sql = "SHOW TABLE unknown"
        with self.assertRaises(Exception) as context:
            self.query_engine.execute_query(sql)
        self.assertIn("Collection `unknown` does not exist", str(context.exception))

    def test_alter_table_unsupported(self):
        sql = "ALTER TABLE employees ADD COLUMN location VARCHAR(100)"
        with self.assertRaises(Exception) as context:
            self.query_engine.execute_query(sql)
        self.assertIn('This operation is not supported for MQL translation', str(context.exception))

    def test_create_index(self):
        sql = "CREATE INDEX idx_salary ON staff(salary)"
        expected_output = True
        self.assertEqual(expected_output, self.query_engine.execute_query(sql))

    def test_create_index_exist(self):
        sql = "CREATE INDEX idx_salary ON staff(salary)"
        with self.assertRaises(Exception) as context:
            self.query_engine.execute_query(sql)
        self.assertIn("Index `idx_salary` already exist", str(context.exception))

    def test_drop_index(self):
        sql = "DROP INDEX idx_salary ON staff"
        expected_output = True
        self.assertEqual(expected_output, self.query_engine.execute_query(sql))

    def test_drop_index_not_found(self):
        sql = "DROP INDEX idx_salary ON staff"
        with self.assertRaises(Exception) as context:
            self.query_engine.execute_query(sql)
        self.assertIn("Index `idx_salary` does not exist", str(context.exception))

if __name__ == '__main__':
    unittest.main()
