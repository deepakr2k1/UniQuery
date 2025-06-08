import unittest

from uniquery.src.query_engine.translators.query_generator import get_mongodb_query

class TestTableOperationsMqlGenerator(unittest.TestCase):

    def test_create_collection(self):
        """MongoDB: CREATE_COLLECTION from SQL CREATE TABLE"""
        parsed_sql = {
            'operation': 'CREATE_TABLE',
            'table_name': 'employees',
            'columns': [
                {'name': 'id', 'type': 'INT', 'constraints': ['PRIMARY KEY']},
                {'name': 'name', 'type': 'VARCHAR(100)', 'constraints': []},
            ],
            "constraints": []
        }
        expected_mql = {
            'operation': 'CREATE_COLLECTION',
            'table': 'employees'
        }
        self.assertEqual(expected_mql, get_mongodb_query(parsed_sql))

    def test_drop_collection(self):
        """MongoDB: DROP_COLLECTION from SQL DROP TABLE"""
        parsed_sql = {
            'operation': 'DROP_TABLE',
            'table_name': 'employees'
        }
        expected_mql = {
            'operation': 'DROP_COLLECTION',
            'table': 'employees'
        }
        self.assertEqual(expected_mql, get_mongodb_query(parsed_sql))

    def test_rename_collection(self):
        """MongoDB: RENAME_COLLECTION from SQL RENAME TABLE"""
        parsed_sql = {
            'operation': 'RENAME_TABLE',
            'old_name': 'employees',
            'new_name': 'staff'
        }
        expected_mql = {
            'operation': 'RENAME_COLLECTION',
            'old_name': 'employees',
            'new_name': 'staff'
        }
        self.assertEqual(expected_mql, get_mongodb_query(parsed_sql))

    def test_show_collections(self):
        """MongoDB: SHOW_COLLECTIONS from SQL SHOW TABLES"""
        parsed_sql = {
            'operation': 'SHOW_TABLES'
        }
        expected_mql = {
            'operation': 'SHOW_COLLECTIONS'
        }
        self.assertEqual(expected_mql, get_mongodb_query(parsed_sql))

    def test_show_collection(self):
        """MongoDB: SHOW_COLLECTION from SQL SHOW TABLE"""
        parsed_sql = {
            'operation': 'SHOW_TABLE',
            'table_name': 'employees'
        }
        expected_mql = {
            'operation': 'SHOW_COLLECTION',
            'table_name': 'employees'
        }
        self.assertEqual(expected_mql, get_mongodb_query(parsed_sql))

    def test_alter_table(self):
        """MongoDB: ALTER TABLE translation raises not supported exception"""
        parsed_sql = {
            'operation': 'ALTER_TABLE',
            'table_name': 'employees',
            'actions': [
                {'action_type': 'ADD_COLUMN', 'column': 'building', 'type': 'VARCHAR', 'default_value': 'BER-12'},
                {'action_type': 'ADD_COLUMN', 'column': 'building_addr', 'type': 'VARCHAR', 'default_value': None}
            ]
        }
        with self.assertRaises(Exception) as context:
            get_mongodb_query(parsed_sql)
        self.assertEqual('This operation is not supported for MQL translation', str(context.exception))

    def test_create_index(self):
        """MongoDB: CREATE_INDEX from SQL CREATE INDEX"""
        parsed_sql = {
            'operation': 'CREATE_INDEX',
            'index_name': 'idx_email',
            'table': 'employees',
            'columns': ['email']
        }
        expected_mql = {
            'operation': 'CREATE_INDEX',
            'index_name': 'idx_email',
            'table': 'employees',
            'columns': ['email']
        }
        self.assertEqual(expected_mql, get_mongodb_query(parsed_sql))

    def test_drop_index(self):
        """MongoDB: DROP_INDEX from SQL DROP INDEX"""
        parsed_sql = {
            'operation': 'DROP_INDEX',
            'index_name': 'idx_email',
            'table': 'employees',
            'columns': ['email']
        }
        expected_mql = {
            'operation': 'DROP_INDEX',
            'index_name': 'idx_email',
            'table': 'employees',
            'columns': ['email']
        }
        self.assertEqual(expected_mql, get_mongodb_query(parsed_sql))

if __name__ == '__main__':
    unittest.main()