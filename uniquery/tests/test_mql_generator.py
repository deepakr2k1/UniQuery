import unittest
from uniquery.query_engine.translators import QueryTranslator

class TestCypherGenerator(unittest.TestCase):
    def setUp(self):
        self.translator = QueryTranslator();

    def test_basic_select(self):
        sql = "SELECT p.name FROM students"
        expected = "{}"
        self.assertEqual(self.translator.translate(sql, "MQL"), expected)

if __name__ == '__main__':
    unittest.main()