import unittest

from uniquery.src.query_engine.translators import QueryTranslator
from uniquery.src.utils import DatabaseType

class TestSqlToMqlTranslation(unittest.TestCase):
    def setUp(self):
        db_type = DatabaseType("mongodb")
        self.translator = QueryTranslator(db_type)

    # DATABASE OPERATIONS
    def test_create_database(self):
        sql = "create database employee"
        expected = {'operation': 'createDatabase', 'name': 'employee'}
        self.assertEqual(self.translator.translate(sql), expected)

    def test_use_database(self):
        sql = "use employee"
        expected = {'operation': 'useDatabase', 'name': 'employee'}
        self.assertEqual(self.translator.translate(sql), expected)

    def test_drop_database(self):
        sql = "drop database employee"
        expected = {'operation': 'dropDatabase', 'name': 'employee'}
        self.assertEqual(self.translator.translate(sql), expected)


    def test_create_table(self):
        sql = """CREATE TABLE employees (
            id INT PRIMARY KEY,
            name VARCHAR(100),
            department VARCHAR(50),
            salary DECIMAL(10,2)
        )"""
        expected = {'operation': 'createCollection', 'name': 'employees'}
        self.assertEqual(self.translator.translate(sql), expected)

    def test_alter_table_add_column(self):
        sql = ("ALTER TABLE employees "
               "ADD COLUMN building VARCHAR(10) DEFAULT 'BER-12', "
               "ADD COLUMN building_addr VARCHAR(100)")
        expected = {'operation': 'updateMany', 'name': 'employees', 'filter': {}, 'documents': {'$set': {'building': 'BER-12', 'building_addr': None}}}
        self.assertEqual(self.translator.translate(sql), expected)

    def test_alter_table_drop_column(self):
        sql = ("ALTER TABLE employees DROP COLUMN building")
        expected = {'operation': 'updateMany', 'name': 'employees', 'filter': {}, 'documents': {'$unset': {'building': ''}}}
        self.assertEqual(self.translator.translate(sql), expected)

    def test_alter_table_rename_column(self):
        sql = "ALTER TABLE employees RENAME COLUMN building TO location"
        expected = {
            'operation': 'updateMany',
            'name': 'employees',
            'filter': {},
            'documents': {'$rename': {'building': 'location'}}
        }
        self.assertEqual(self.translator.translate(sql), expected)

    def test_alter_table_modify_column(self):
        sql = "ALTER TABLE employees MODIFY COLUMN salary FLOAT DEFAULT 0.0"
        expected = {
            'operation': 'updateMany',
            'name': 'employees',
            'filter': {},
            'documents': {'$set': {'salary': 0.0}}  # MongoDB is schemaless, so only default setting is represented
        }
        self.assertEqual(self.translator.translate(sql), expected)

    def test_delete_table(self):
        sql = "DROP TABLE employees"
        expected = {
            'operation': 'dropCollection',
            'name': 'employees'
        }
        self.assertEqual(self.translator.translate(sql), expected)

    def test_create_index(self):
        sql = "CREATE INDEX idx_name ON employees(name)"
        expected = {
            'operation': 'createIndex',
            'name': 'employees',
            'index': {'name': 1},
            'index_name': 'idx_name'
        }
        self.assertEqual(self.translator.translate(sql), expected)

    def test_delete_index(self):
        sql = "DROP INDEX idx_name ON employees"
        expected = {
            'operation': 'dropIndex',
            'name': 'employees',
            'index_name': 'idx_name'
        }
        self.assertEqual(self.translator.translate(sql), expected)

if __name__ == '__main__':
    unittest.main()
