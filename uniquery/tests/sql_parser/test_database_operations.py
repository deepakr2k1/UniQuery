import unittest

from uniquery.src.query_engine.translators.sql_parser import SqlParser

class TestSqlParserDatabaseQueries(unittest.TestCase):
    def setUp(self):
        self.sql_parser = SqlParser()

    # DATABASE OPERATIONS
    def test_create_database(self):
        sql = "CREATE DATABASE employee"
        expected = {'operation': 'CREATE_DATABASE', 'database_name': 'employee'}
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_use_database(self):
        sql = "USE employee"
        expected = {'operation': 'USE_DATABASE', 'database_name': 'employee'}
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_drop_database(self):
        sql = "DROP DATABASE employee"
        expected = {'operation': 'DROP_DATABASE', 'database_name': 'employee'}
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_show_databases(self):
        sql = "SHOW DATABASES"
        expected = {
            'operation': 'SHOW_DATABASES'
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

if __name__ == '__main__':
    unittest.main()
