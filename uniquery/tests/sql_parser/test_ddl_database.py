import unittest

from uniquery.src.query_engine.translators.sql_parser import SqlParser

class TestSqlParserDatabaseQueries(unittest.TestCase):
    def setUp(self):
        self.sql_parser = SqlParser()

    # DATABASE OPERATIONS
    def test_create_database(self):
        sql = "CREATE DATABASE employee"
        parsed_sql = {'operation': 'CREATE_DATABASE', 'database_name': 'employee'}
        self.assertEqual(parsed_sql, self.sql_parser.parse(sql))

    def test_use_database(self):
        sql = "USE employee"
        parsed_sql = {'operation': 'USE_DATABASE', 'database_name': 'employee'}
        self.assertEqual(parsed_sql, self.sql_parser.parse(sql))

    def test_drop_database(self):
        sql = "DROP DATABASE employee"
        parsed_sql = {'operation': 'DROP_DATABASE', 'database_name': 'employee'}
        self.assertEqual(parsed_sql, self.sql_parser.parse(sql))

    def test_show_databases(self):
        sql = "SHOW DATABASES"
        parsed_sql = {'operation': 'SHOW_DATABASES'}
        self.assertEqual(parsed_sql, self.sql_parser.parse(sql))

if __name__ == '__main__':
    unittest.main()
