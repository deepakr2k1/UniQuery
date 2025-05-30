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
        sql = "UPDATE employees SET salary = 6000 WHERE id = 1"
        expected = {
            'operation': 'UPDATE_DATA',
            'table_name': 'employees',
            'set': {'salary': 6000},
            'condition': {'id': 1}
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_delete_from_table(self):
        sql = "DELETE FROM employees WHERE id = 1"
        expected = {
            'operation': 'DELETE_DATA',
            'table_name': 'employees',
            'condition': {'id': 1}
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_delete_with_complex_condition(self):
        sql = "DELETE FROM employees WHERE salary > 5000 AND department_id = 2"
        expected = {
            'operation': 'DELETE_DATA',
            'table_name': 'employees',
            'condition': {
                'AND': [
                    {'salary': {'>': 5000}},
                    {'department_id': 2}
                ]
            }
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

if __name__ == '__main__':
    unittest.main()
