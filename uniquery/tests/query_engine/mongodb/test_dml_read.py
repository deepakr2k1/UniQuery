import unittest

from pymongo.results import *

from uniquery.src.query_engine import QueryEngine
from uniquery.src.connectors.mongodb_connector import MongoDBConnector
from uniquery.src.utils import DatabaseType

class TestEngineMongo(unittest.TestCase):
    def setUp(self):
        mongodb = DatabaseType.MONGO_DB
        self.connector = MongoDBConnector(
            "127.0.0.1",
            27017,
            "admin",
            "admin123",
            "test_database"
        )
        self.query_engine = QueryEngine(mongodb, self.connector, False)

    def tearDown(self):
        self.connector.close()

    def test_select_all_columns(self):
        sql = "SELECT * FROM employees"
        result = self.query_engine.execute_query(sql)
        self.assertGreater(len(result), 0)

    def test_select_specific_columns(self):
        sql = "SELECT _id AS employee_id, name FROM employees"
        result = self.query_engine.execute_query(sql)
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
        self.assertIn("employee_id", result[0])
        self.assertIn("name", result[0])

    def test_where_clause(self):
        sql = "SELECT * FROM employees WHERE name = 'Alice'"
        result = self.query_engine.execute_query(sql)
        self.assertGreater(len(result), 0)
        self.assertIn("Alice", result[0]['name'])

    def test_multiple_conditions(self):
        sql = """
            SELECT * FROM employees
            WHERE (department_id = 1 OR department_id = 2)
            AND salary >= 5000
        """
        result = self.query_engine.execute_query(sql)
        self.assertGreater(len(result), 0)
        self.assertGreaterEqual(result[0]['salary'], 5000)

    def test_like_clause(self):
        sql = "SELECT * FROM employees WHERE name LIKE '%li%'"
        result = self.query_engine.execute_query(sql)
        self.assertGreater(len(result), 0)
        self.assertTrue(any("li" in doc["name"] for doc in result))

    def test_in_clause(self):
        sql = "SELECT * FROM employees WHERE name IN ('Alice', 'Bob')"
        result = self.query_engine.execute_query(sql)
        self.assertEqual(len(result), 2)

    def test_range_check(self):
        sql = "SELECT * FROM employees WHERE salary BETWEEN 3000 AND 5000"
        result = self.query_engine.execute_query(sql)
        self.assertEqual(len(result), 5)

    def test_null_check(self):
        sql = "SELECT * FROM employees WHERE department_id IS NULL"
        result = self.query_engine.execute_query(sql)
        self.assertEqual(result[0]['_id'], 109)

    def test_not_null_check(self):
        sql = "SELECT * FROM employees WHERE department_id IS NOT NULL"
        result = self.query_engine.execute_query(sql)
        self.assertEqual(len(result), 8)

    def test_order_by(self):
        sql = "SELECT * FROM employees ORDER BY _id ASC, salary DESC"
        result = self.query_engine.execute_query(sql)
        self.assertEqual(result[0]['salary'], 6000)

    def test_limit(self):
        sql = "SELECT * FROM employees LIMIT 5"
        result = self.query_engine.execute_query(sql)
        self.assertEqual(len(result), 5)

    def test_group_by_count(self):
        sql = "SELECT department, COUNT(*) AS dept_count FROM employees GROUP BY department"
        result = self.query_engine.execute_query(sql)
        self.assertEqual(result[0]['dept_count'], 9)

    def test_group_by_with_having(self):
        sql = """
            SELECT department_id, SUM(salary) as department_salary
            FROM employees
            GROUP BY department_id
            HAVING SUM(salary) >= 10000 \
        """
        result = self.query_engine.execute_query(sql)
        self.assertEqual(len(result), 2)
        self.assertGreaterEqual(result[0]['department_salary'], 10000)

    def test_inner_join(self):
        sql = """
            SELECT emp.name, dept.name
            FROM employees emp
            JOIN departments dept ON emp.department_id = dept._id \
        """
        result = self.query_engine.execute_query(sql)
        self.assertEqual(len(result), 8)
        self.assertEqual(result[0]['emp']['name'], 'Alice')
        self.assertEqual(result[0]['dept']['name'], 'Sales')

    def test_left_join_with_filter(self):
        sql = """
            SELECT emp.name, dept.name
            FROM employees emp
            LEFT JOIN departments dept ON emp.department_id = dept._id
        """
        result = self.query_engine.execute_query(sql)
        self.assertEqual(len(result), 9)

    def test_multiple_joins(self):
        sql = """
            SELECT emp.name, dept.name, ord.amount
            FROM employees emp
            LEFT JOIN departments dept ON emp.department_id = dept._id
            JOIN orders ord ON ord.employee_id = emp._id \
        """
        result = self.query_engine.execute_query(sql)
        self.assertEqual(len(result), 4)

if __name__ == "__main__":
    unittest.main()