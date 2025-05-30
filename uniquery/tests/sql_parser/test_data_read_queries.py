import unittest

from uniquery.src.query_engine.translators.sql_parser import SqlParser

class TestSqlParserDataReadQueries(unittest.TestCase):
    def setUp(self):
        self.sql_parser = SqlParser()

    def test_select_all_columns(self):
        sql = "SELECT * FROM employees"
        expected = {
            'operation': 'SELECT',
            'columns': [{'name': '*'}],
            'table': {'name': 'employees'}
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_specific_columns(self):
        sql = "SELECT id, name FROM employees"
        expected = {
            'operation': 'SELECT',
            'columns': [{'name': 'id'}, {'name': 'name'}],
            'table': {'name': 'employees'}
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_table_alias(self):
        sql = "SELECT e.id, e.name FROM employees e"
        expected = {
            'operation': 'SELECT',
            'columns': [
                {'name': 'e.id', 'alias': 'Id'},
                {'name': 'e.name', 'alias': 'Name'}
            ],
            'table': {'name': 'employees','alias': 'e'}
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_where_clause(self):
        sql = "SELECT * FROM employees WHERE department = 'Sales'"
        expected = {
            'operation': 'SELECT',
            'columns': [{'name': '*'}],
            'table': {'name': 'employees'},
            'where': {
                'column': 'department',
                'operator': '=',
                'value': 'Sales'
            }
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_order_by(self):
        sql = "SELECT name FROM employees ORDER BY salary DESC"
        expected = {
            'operation': 'SELECT',
            'columns': [{'name': 'name'}],
            'table': {'name': 'employees'},
            'order_by': {
                'column': 'salary',
                'order': 'DESC'
            }
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_limit(self):
        sql = "SELECT * FROM employees LIMIT 5"
        expected = {
            'operation': 'SELECT',
            'columns': [{'name': '*'}],
            'table': {'name': 'employees'},
            'limit': 5
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_aggregation(self):
        sql = "SELECT department, COUNT(*) FROM employees GROUP BY department"
        expected = {
            'operation': 'SELECT',
            'columns': [{'name': 'department'}, {'aggregation': 'COUNT', 'column': '*'}],
            'table': {'name': 'employees'},
            'group_by': ['department']
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_aggregation_and_having(self):
        sql = "SELECT department, SUM(salary) FROM employees GROUP BY department HAVING SUM(salary) > 100000"
        expected = {
            'operation': 'SELECT',
            'columns': [{'name': 'department'}, {'aggregation': 'SUM', 'column': 'salary'}],
            'table': {'name': 'employees'},
            'group_by': ['department'],
            'having': {
                'aggregation': 'SUM',
                'column': 'salary',
                'operator': '>',
                'value': 100000
            }
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_greater_than(self):
        sql = "SELECT * FROM employees WHERE salary > 50000"
        expected = {
            'operation': 'SELECT',
            'columns': [{'name': '*'}],
            'table': {'name': 'employees'},
            'where': {
                'column': 'salary',
                'operator': '>',
                'value': 50000
            }
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_less_than(self):
        sql = "SELECT * FROM employees WHERE salary < 30000"
        expected = {
            'operation': 'SELECT',
            'columns': [{'name': '*'}],
            'table': {'name': 'employees'},
            'where': {
                'column': 'salary',
                'operator': '<',
                'value': 30000
            }
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_like(self):
        sql = "SELECT * FROM employees WHERE name LIKE 'A%'"
        expected = {
            'operation': 'SELECT',
            'columns': [{'name': '*'}],
            'table': {'name': 'employees'},
            'where': {
                'column': 'name',
                'operator': 'LIKE',
                'value': 'A%'
            }
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_inner_join(self):
        sql = """SELECT e.name, d.name FROM employees e 
            INNER JOIN departments d ON e.department_id = d.id
        """
        expected = {
            'operation': 'SELECT',
            'columns': [{'name': 'e.name'}, {'name': 'd.name'}],
            'table': {'name': 'employees'},
            'joins': [
                {
                    'type': 'INNER',
                    'table': {'name': 'departments', 'alias': 'd'},
                    'on': {'left': 'e.department_id', 'operator': '=', 'right': 'd.id'}
                }
            ]
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_multiple_joins(self):
        sql = """SELECT o.id, e.name, d.name
            FROM orders o
            JOIN employees e ON o.employee_id = e.id
            JOIN departments d ON e.department_id = d.id
        """
        expected = {
            'operation': 'SELECT',
            'columns': [
                {'name': 'o.id'},
                {'name': 'e.name'},
                {'name': 'd.name'}
            ],
            'table': {'name': 'orders', 'alias': 'o'},
            'joins': [
                {
                    'type': 'INNER',
                    'table': {'name': 'employees', 'alias': 'e'},
                    'on': {'left': 'o.employee_id', 'operator': '=', 'right': 'e.id'}
                },
                {
                    'type': 'INNER',
                    'table': {'name': 'departments', 'alias': 'd'},
                    'on': {'left': 'e.department_id', 'operator': '=', 'right': 'd.id'}
                }
            ]
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_left_join_and_where(self):
        sql = """SELECT c.name, o.id
            FROM customers c
            LEFT JOIN orders o ON c.id = o.customer_id
            WHERE o.status = 'pending'
        """
        expected = {
            'operation': 'SELECT',
            'columns': [{'name': 'c.name'}, {'name': 'o.id'}],
            'table': {'name': 'customers', 'alias': 'c'},
            'joins': [
                {
                    'type': 'LEFT',
                    'table': {'name': 'orders', 'alias': 'o'},
                    'on': {'left': 'c.id', 'operator': '=', 'right': 'o.customer_id'}
                }
            ],
            'where': {
                'column': 'o.status',
                'operator': '=',
                'value': 'pending'
            }
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_join_and_column_aliases(self):
        sql = """
            SELECT e.id AS employee_id, d.name AS department_name
            FROM employees e
            JOIN departments d ON e.department_id = d.id \
        """
        expected = {
            'operation': 'SELECT',
            'columns': [
                {'name': 'e.id', 'alias': 'employee_id'},
                {'name': 'd.name', 'alias': 'department_name'}
            ],
            'table': {'name': 'employees', 'alias': 'e'},
            'joins': [
                {
                    'type': 'INNER',
                    'table': {'name': 'departments', 'alias': 'd'},
                    'on': {'left': 'e.department_id', 'operator': '=', 'right': 'd.id'}
                }
            ]
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

if __name__ == '__main__':
    unittest.main()
