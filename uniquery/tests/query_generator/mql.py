import unittest

from uniquery.src.query_engine.translators.query_generator import get_mongodb_query

class TestMqlGenerator(unittest.TestCase):
    # DATABASE OPERATIONS
    def test_create_database(self):
        parsed_sql = {'operation': 'CREATE_DATABASE', 'database_name': 'employee'}
        expected_mql = {'operation': 'CREATE_DATABASE', 'database_name': 'employee'}
        self.assertEqual(expected_mql, get_mongodb_query(parsed_sql))

    def test_use_database(self):
        parsed_sql = {'operation': 'USE_DATABASE', 'database_name': 'employee'}
        expected_mql = {'operation': 'USE_DATABASE', 'database_name': 'employee'}
        self.assertEqual(expected_mql, get_mongodb_query(parsed_sql))

    def test_drop_database(self):
        parsed_sql = {'operation': 'DROP_DATABASE', 'database_name': 'employee'}
        expected_mql = {'operation': 'DROP_DATABASE', 'database_name': 'employee'}
        self.assertEqual(expected_mql, get_mongodb_query(parsed_sql))

    def test_show_databases(self):
        parsed_sql = {'operation': 'SHOW_DATABASES'}
        expected_mql = {'operation': 'SHOW_DATABASES'}
        self.assertEqual(expected_mql, get_mongodb_query(parsed_sql))

if __name__ == '__main__':
    unittest.main()
