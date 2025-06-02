import unittest

from uniquery.src.query_engine.translators.sql_parser import SqlParser

class TestSqlParserDataManipulationQueries(unittest.TestCase):
    def setUp(self):
        self.sql_parser = SqlParser()

    def test_insert_into_table(self):
        sql = "INSERT INTO employees (id, name, salary) VALUES (1, 'Alice', 5000)"
        expected = {
            'operation': 'INSERT_DATA',
            'table_name': 'employees',
            'columns': ['id', 'name', 'salary'],
            'values': [1, 'Alice', 5000]
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_update_table(self):
        sql = "UPDATE employees SET salary = 5000"
        expected = {
            'operation': 'UPDATE_DATA',
            'table_name': 'employees',
            'columns': ['salary'],
            'values': [5000],
            'condition': {}
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_delete_from_table(self):
        sql = "DELETE FROM employees"
        expected = {
            'operation': 'DELETE_DATA',
            'table_name': 'employees',
            'condition': {},
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

if __name__ == '__main__':
    unittest.main()
