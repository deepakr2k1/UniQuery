import unittest

from uniquery.src.query_engine.translators.sql_parser import SqlParser

class TestSqlParserDataReadQueries(unittest.TestCase):
    def setUp(self):
        self.sql_parser = SqlParser()

    def test_select_all_columns(self):
        sql = "SELECT * FROM employees"
        expected = {
            'operation': 'SELECT',
            'table': {'name': 'employees', 'alias': 'employees'},
            'columns': [{'name': '*', 'alias': None}]
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_specific_columns(self):
        sql = "SELECT _id AS id, name FROM employees"
        expected = {
            'operation': 'SELECT',
            'table': {'name': 'employees', 'alias': 'employees'},
            'columns': [{'name': '_id', 'alias': 'id'}, {'name': 'name', 'alias': None}]
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_table_alias(self):
        sql = "SELECT emp.id as Id, emp.name AS Name FROM employees emp"
        expected = {
            'operation': 'SELECT',
            'table': {'name': 'employees','alias': 'emp'},
            'columns': [
                {'name': 'emp.id', 'alias': 'Id'},
                {'name': 'emp.name', 'alias': 'Name'}
            ]
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_where_clause(self):
        sql = "SELECT * FROM employees WHERE department = 'Sales'"
        expected = {
            'operation': 'SELECT',
            'table': {'name': 'employees', 'alias': 'employees'},
            'columns': [{'name': '*', 'alias': None}],
            'filter': {
                'operator': '=',
                'column': 'department',
                'value': 'Sales'
            }
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_where_clause_multiple_conditions(self):
        sql = "SELECT * FROM employees WHERE (department = 'Marketing' OR department = 'Sales') AND salary > 5000"
        expected = {
            'operation': 'SELECT',
            'table': {'name': 'employees', 'alias': 'employees'},
            'columns': [{'name': '*', 'alias': None}],
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

    def test_select_with_like(self):
        sql = "SELECT * FROM employees WHERE name LIKE '%Art _'"
        expected = {
            'operation': 'SELECT',
            'table': {'name': 'employees', 'alias': 'employees'},
            'columns': [{'name': '*', 'alias': None}],
            'filter': {
                'operator': 'LIKE',
                'column': 'name',
                'value': '%Art _'
            }
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_in(self):
        sql = "SELECT * FROM employees WHERE name IN ('Alice', 'Bob')"
        expected = {
            'operation': 'SELECT',
            'table': {'name': 'employees', 'alias': 'employees'},
            'columns': [{'name': '*', 'alias': None}],
            'filter': {
                'operator': 'IN',
                'column': 'name',
                'values': ['Alice', 'Bob']
            }
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_range_check(self):
        sql = "SELECT * FROM employees WHERE salary BETWEEN 3000 AND 6000"
        expected = {
            'operation': 'SELECT',
            'table': {'name': 'employees', 'alias': 'employees'},
            'columns': [{'name': '*', 'alias': None}],
            'filter': {
                'operator': 'BETWEEN',
                'column': 'salary',
                'low': 3000,
                'high': 6000
            }
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_null_check(self):
        sql = "SELECT * FROM employees WHERE name IS NULL"
        expected = {
            'operation': 'SELECT',
            'table': {'name': 'employees', 'alias': 'employees'},
            'columns': [{'name': '*', 'alias': None}],
            'filter': {
                'operator': 'IS_NULL',
                'column': 'name'
            }
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_not_null_check(self):
        sql = "SELECT * FROM employees WHERE name IS NOT NULL"
        expected = {
            'operation': 'SELECT',
            'table': {'name': 'employees', 'alias': 'employees'},
            'columns': [{'name': '*', 'alias': None}],
            'filter': {
                'operator': 'NOT',
                'operand': {
                    'operator': 'IS_NULL',
                    'column': 'name'
                }
            }
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_order_by(self):
        sql = "SELECT * FROM employees ORDER BY id ASC, salary DESC"
        expected = {
            'operation': 'SELECT',
            'table': {'name': 'employees', 'alias': 'employees'},
            'columns': [{'name': '*', 'alias': None}],
            'order_by': [
                {
                    'column': 'id',
                    'order': 'ASC'
                },
                {
                    'column': 'salary',
                    'order': 'DESC'
                },
            ]
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_limit(self):
        sql = "SELECT * FROM employees LIMIT 5"
        expected = {
            'operation': 'SELECT',
            'table': {'name': 'employees', 'alias': 'employees'},
            'columns': [{'name': '*', 'alias': None}],
            'limit': 5
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_aggregation(self):
        sql = "SELECT department, COUNT(*) FROM employees GROUP BY department"
        expected = {
            'operation': 'SELECT',
            'table': {'name': 'employees', 'alias': 'employees'},
            'columns': [
                {'name': 'department', 'alias': None},
                {'aggregation_function': 'COUNT', 'column': '*', 'alias': None}
            ],
            'aggregate': ['department']
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_aggregation_with_having(self):
        sql = "SELECT department, SUM(salary) FROM employees GROUP BY department HAVING SUM(salary) > 1000"
        expected = {
            'operation': 'SELECT',
            'table': {'name': 'employees', 'alias': 'employees'},
            'columns': [
                {'name': 'department', 'alias': None},
                {'aggregation_function': 'SUM', 'column': 'salary', 'alias': None}
            ],
            'aggregate': ['department'],
            'having': {
                'aggregation_function': 'SUM',
                'column': 'salary',
                'operator': '>',
                'value': 1000
            }
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_select_with_inner_join(self):
        sql = """SELECT e.name, d.name FROM employees e 
            INNER JOIN departments d ON e.department_id = d.id
        """
        expected = {
            'operation': 'SELECT',
            'columns': [
                {'name': 'e.name', 'alias': 'e.name'},
                {'name': 'd.name', 'alias': 'd.name'}
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
