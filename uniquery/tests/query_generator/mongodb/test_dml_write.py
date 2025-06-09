import unittest

from uniquery.src.query_engine.translators.query_generator import get_mongodb_query

class TestTableOperationsMqlGenerator(unittest.TestCase):

    def test_insert_into_table(self):
        """MongoDB: CREATE_COLLECTION from SQL CREATE TABLE"""
        parsed_sql = {
            'operation': 'INSERT_DATA',
            'table_name': 'employees',
            'columns': ['id', 'name', 'salary'],
            'values': [
                [1, 'Alice', 5000],
                [2, 'Bob', 6000]
            ]
        }
        expected_mql = {
            'operation': 'INSERT_DATA',
            'collection': 'employees',
            'documents': [
                {'id': 1, 'name': 'Alice', 'salary': 5000},
                {'id': 2, 'name': 'Bob', 'salary': 6000}
            ]
        }
        self.assertEqual(expected_mql, get_mongodb_query(parsed_sql))

    def test_update_table(self):
        """MongoDB: UPDATE operation"""
        parsed_sql = {
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
        expected_mql = {
            'operation': 'UPDATE_DATA',
            'collection': 'employees',
            'updates': {'salary': 5000},
            'filter': {
                "$and": [
                    {
                        "$or": [
                            { "department": "Marketing" },
                            { "department": "Sales" }
                        ]
                    },
                    {
                        "salary": { "$gt": 5000 }
                    }
                ]
            }
        }
        self.assertEqual(expected_mql, get_mongodb_query(parsed_sql))

    def test_delete_from_table(self):
        """MongoDB: DELETE operation"""
        parsed_sql = {
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
                        'operator': '>',
                        'column': 'salary',
                        'value': 5000
                    },
                ]
            }
        }
        expected_mql = {
            'operation': 'DELETE_DATA',
            'collection': 'employees',
            'filter': {
                "$and": [
                    {
                        "$or": [
                            { "department": "Marketing" },
                            { "department": "Sales" }
                        ]
                    },
                    {
                        "salary": { "$gt": 5000 }
                    }
                ]
            }
        }
        self.assertEqual(expected_mql, get_mongodb_query(parsed_sql))

if __name__ == '__main__':
    unittest.main()