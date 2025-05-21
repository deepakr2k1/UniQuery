import unittest
from uniquery.query_engine.translators import QueryTranslator

class TestCypherGenerator(unittest.TestCase):
    def setUp(self):
        self.translator = QueryTranslator();

    def test_basic_select(self):
        sql = "SELECT p.name FROM Person p"
        expected = "MATCH (p:Person)\nRETURN p.name;"
        self.assertEqual(self.translator.translate(sql, "CYPHER"), expected)

    def test_select_with_alias(self):
        sql = "SELECT p.name AS person_name FROM Person p"
        expected = "MATCH (p:Person)\nRETURN p.name AS person_name;"
        self.assertEqual(self.translator.translate(sql, "CYPHER"), expected)

    def test_select_distinct(self):
        sql = "SELECT DISTINCT p.name FROM Person p"
        expected = "MATCH (p:Person)\nRETURN DISTINCT p.name;"
        self.assertEqual(self.translator.translate(sql, "CYPHER"), expected)

    def test_single_relationship(self):
        sql = """
        SELECT p.name, c.name
        FROM Person p
        JOIN Company c ON RELATION('WORKS_AT', w)
        """
        expected = "MATCH (p:Person)-[w:WORKS_AT]->(c:Company)\nRETURN p.name, c.name;"
        self.assertEqual(self.translator.translate(sql, "CYPHER"), expected)

    def test_multiple_relationships(self):
        sql = """
        SELECT p.name, f.name, c.name
        FROM Person p
        JOIN Person f ON RELATION('FRIEND', _f)
        JOIN Company c ON RELATION('WORKS_AT', w)
        """
        expected = "MATCH (p:Person)-[_f:FRIEND]->(f:Person)-[w:WORKS_AT]->(c:Company)\nRETURN p.name, f.name, c.name;"
        self.assertEqual(self.translator.translate(sql, "CYPHER"), expected)

    def test_relationship_with_depth(self):
        sql = """
        SELECT p.name, f.name
        FROM Person p
        JOIN Person f ON RELATION('FRIEND*1..3', _f)
        """
        expected = "MATCH (p:Person)-[_f:FRIEND*1..3]->(f:Person)\nRETURN p.name, f.name;"
        self.assertEqual(self.translator.translate(sql, "CYPHER"), expected)

    def test_where_clause(self):
        sql = """
        SELECT p.name
        FROM Person p
        WHERE p.age > 30
        """
        expected = "MATCH (p:Person)\nWHERE p.age > 30\nRETURN p.name;"
        self.assertEqual(self.translator.translate(sql, "CYPHER"), expected)

    def test_order_by_no_direction(self):
        sql = """
        SELECT p.name
        FROM Person p
        ORDER BY p.name
        """
        expected = "MATCH (p:Person)\nRETURN p.name\nORDER BY p.name ASC;"
        self.assertEqual(self.translator.translate(sql, "CYPHER"), expected)

    def test_order_by_asc(self):
        sql = """
        SELECT p.name
        FROM Person p
        ORDER BY p.name ASC
        """
        expected = "MATCH (p:Person)\nRETURN p.name\nORDER BY p.name ASC;"
        self.assertEqual(self.translator.translate(sql, "CYPHER"), expected)

    def test_order_by_desc(self):
        sql = """
        SELECT p.name
        FROM Person p
        ORDER BY p.name DESC
        """
        expected = "MATCH (p:Person)\nRETURN p.name\nORDER BY p.name DESC;"
        self.assertEqual(self.translator.translate(sql, "CYPHER"), expected)

    def test_order_by_multiple_fields(self):
        sql = """
        SELECT p.name, p.age
        FROM Person p
        ORDER BY p.age DESC, p.name ASC
        """
        expected = "MATCH (p:Person)\nRETURN p.name, p.age\nORDER BY p.age DESC, p.name ASC;"
        self.assertEqual(self.translator.translate(sql, "CYPHER"), expected)

    def test_limit(self):
        sql = """
        SELECT p.name
        FROM Person p
        LIMIT 10
        """
        expected = "MATCH (p:Person)\nRETURN p.name\nLIMIT 10;"
        self.assertEqual(self.translator.translate(sql, "CYPHER"), expected)

    def test_complex_query(self):
        sql = """
        SELECT DISTINCT p.name as person_name, f.name as friend_name, c.name AS company_name
        FROM Person p
        RIGHT JOIN Person f ON RELATION('FRIEND*3..3', _f)
        RIGHT JOIN Company c ON RELATION('WORKS_AT', w)
        WHERE c.name = 'ACME Corp' AND p.name != f.name
        ORDER BY p.name
        LIMIT 5
        """
        expected = """MATCH (p:Person)-[_f:FRIEND*3..3]->(f:Person)-[w:WORKS_AT]->(c:Company)
WHERE c.name = 'ACME Corp' AND p.name <> f.name
RETURN DISTINCT p.name AS person_name, f.name AS friend_name, c.name AS company_name
ORDER BY p.name ASC
LIMIT 5;"""
        self.assertEqual(self.translator.translate(sql, "CYPHER"), expected)

    def test_relationship_with_or(self):
        sql = """
        SELECT p.name
        FROM Person p
        JOIN Person f ON RELATION('FRIEND OR COLLEAGUE', _f)
        """
        expected = "MATCH (p:Person)-[_f:FRIEND|COLLEAGUE]->(f:Person)\nRETURN p.name;"
        self.assertEqual(self.translator.translate(sql, "CYPHER"), expected)

    def test_multiple_where_conditions(self):
        sql = """
        SELECT p.name, c.name
        FROM Person p
        JOIN Company c ON RELATION('WORKS_AT', w)
        WHERE p.age > 30 AND c.name = 'Tech Corp' OR p.salary > 100000
        """
        expected = "MATCH (p:Person)-[w:WORKS_AT]->(c:Company)\nWHERE p.age > 30 AND c.name = 'Tech Corp' OR p.salary > 100000\nRETURN p.name, c.name;"
        self.assertEqual(self.translator.translate(sql, "CYPHER"), expected)

if __name__ == '__main__':
    unittest.main() 