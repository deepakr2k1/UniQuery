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
            'values': [[1, 'Alice', 5000]]
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_insert_into_table_multiple_records(self):
        sql = "INSERT INTO employees (id, name, salary) VALUES (1, 'Alice', 5000), (2, 'Bob', 6000)"
        expected = {
            'operation': 'INSERT_DATA',
            'table_name': 'employees',
            'columns': ['id', 'name', 'salary'],
            'values': [[1, 'Alice', 5000], [2, 'Bob', 6000]]
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_update_table(self):
        sql = "UPDATE employees SET salary = 5000 WHERE (department = 'Marketing' OR department = 'Sales') AND salary > 5000"
        expected = {
            'operation': 'UPDATE_DATA',
            'table_name': 'employees',
            'columns': ['salary'],
            'values': [5000],
            'filter': {
                'operator': 'AND',
                'operands': [
                    {
                        'operator': 'OR',
                        'operands': [
                            {
                                'operator': '=',
                                'column': 'department',
                                'value': 'Marketing'
                            },
                            {
                                'operator': '=',
                                'column': 'department',
                                'value': 'Sales'
                            }
                        ]

                    },
                    {
                        'operator': '>',
                        'column': 'salary',
                        'value': 5000
                    },
                ]
            }
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_delete_from_table(self):
        sql = "DELETE FROM employees WHERE (department = 'Marketing' OR department = 'Sales') AND salary < 5000"
        expected = {
            'operation': 'DELETE_DATA',
            'table_name': 'employees',
            'filter': {
                'operator': 'AND',
                'operands': [
                    {
                        'operator': 'OR',
                        'operands': [
                            {
                                'operator': '=',
                                'column': 'department',
                                'value': 'Marketing'
                            },
                            {
                                'operator': '=',
                                'column': 'department',
                                'value': 'Sales'
                            }
                        ]

                    },
                    {
                        'operator': '<',
                        'column': 'salary',
                        'value': 5000
                    },
                ]
            }
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

if __name__ == '__main__':
    unittest.main()
