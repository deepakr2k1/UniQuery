import unittest

from uniquery.src.query_engine.translators.query_generator import get_mongodb_query

class TestTableOperationsMqlGenerator(unittest.TestCase):

    def test_select_all_columns(self):
        parsed_sql = {
            'operation': 'SELECT',
            'table': {'name': 'employees', 'alias': 'employees'},
            'columns': [{'name': '*', 'alias': None}]
        }
        expected_mql = {
            'operation': 'FIND',
            'collection': 'employees',
            'filter': {},
            'projection': None
        }
        self.assertEqual(get_mongodb_query(parsed_sql), expected_mql)

    def test_select_specific_columns(self):
        parsed_sql = {
            'operation': 'SELECT',
            'table': {'name': 'employees', 'alias': 'employees'},
            'columns': [{'name': '_id', 'alias': 'id'}, {'name': 'name', 'alias': None}]
        }
        expected_mql = {
            'operation': 'FIND',
            'collection': 'employees',
            'filter': {},
            'projection': {'id': 1, 'name': 1}
        }
        self.assertEqual(get_mongodb_query(parsed_sql), expected_mql)

    def test_select_with_where_clause(self):
        parsed_sql = {
            'operation': 'SELECT',
            'table': {'name': 'employees', 'alias': 'employees'},
            'columns': [{'name': '*', 'alias': None}],
            'filter': {
                'operator': '=',
                'column': 'department',
                'value': 'Sales'
            }
        }
        expected_mql = {
            'operation': 'FIND',
            'collection': 'employees',
            'filter': {'department': 'Sales'},
            'projection': None
        }
        self.assertEqual(get_mongodb_query(parsed_sql), expected_mql)

    def test_select_with_where_clause_multiple_conditions(self):
        parsed_sql = {
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
        expected_mql = {
            'operation': 'FIND',
            'collection': 'employees',
            'filter': {
                '$and': [
                    {
                        '$or': [
                            {'department': 'Marketing'},
                            {'department': 'Sales'}
                        ]
                    },
                    {'salary': {'$gt': 5000}}
                ]
            },
            'projection': None
        }
        self.assertEqual(get_mongodb_query(parsed_sql), expected_mql)

    def test_select_with_like(self):
        parsed_sql = {
            'operation': 'SELECT',
            'table': {'name': 'employees', 'alias': 'employees'},
            'columns': [{'name': '*', 'alias': None}],
            'filter': {
                'operator': 'LIKE',
                'column': 'name',
                'value': '%Art _'
            }
        }
        expected_mql = {
            'operation': 'FIND',
            'collection': 'employees',
            'filter': {
                'name': {'$regex': '^.*Art .$'}  # Convert SQL LIKE '%Art _' to regex
            },
            'projection': None
        }
        self.assertEqual(get_mongodb_query(parsed_sql), expected_mql)

    def test_select_with_in(self):
        parsed_sql = {
            'operation': 'SELECT',
            'table': {'name': 'employees', 'alias': 'employees'},
            'columns': [{'name': '*', 'alias': None}],
            'filter': {
                'operator': 'IN',
                'column': 'name',
                'values': ['Alice', 'Bob']
            }
        }
        expected_mql = {
            'operation': 'FIND',
            'collection': 'employees',
            'filter': {
                'name': {'$in': ['Alice', 'Bob']}
            },
            'projection': None
        }
        self.assertEqual(get_mongodb_query(parsed_sql), expected_mql)

    def test_select_with_range_check(self):
        parsed_sql = {
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
        expected_mql = {
            'operation': 'FIND',
            'collection': 'employees',
            'filter': {
                'salary': {'$gte': 3000, '$lte': 6000}
            },
            'projection': None
        }
        self.assertEqual(get_mongodb_query(parsed_sql), expected_mql)

    def test_select_with_null_check(self):
        parsed_sql = {
            'operation': 'SELECT',
            'table': {'name': 'employees', 'alias': 'employees'},
            'columns': [{'name': '*', 'alias': None}],
            'filter': {
                'operator': 'IS_NULL',
                'column': 'name'
            }
        }
        expected_mql = {
            'operation': 'FIND',
            'collection': 'employees',
            'filter': {
                'name': None
            },
            'projection': None
        }
        self.assertEqual(get_mongodb_query(parsed_sql), expected_mql)

    def test_select_with_not_null_check(self):
        parsed_sql = {
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
        expected_mql = {
            'operation': 'FIND',
            'collection': 'employees',
            'filter': {
                'name': {'$ne': None}
            },
            'projection': None
        }
        self.assertEqual(get_mongodb_query(parsed_sql), expected_mql)

    def test_select_with_order_by(self):
        parsed_sql = {
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
        expected_mql = {
            'operation': 'FIND',
            'collection': 'employees',
            'filter': {},
            'projection': None,
            'sort': [('id', 1), ('salary', -1)]
        }
        self.assertEqual(get_mongodb_query(parsed_sql), expected_mql)

    def test_select_with_limit(self):
        parsed_sql = {
            'operation': 'SELECT',
            'table': {'name': 'employees', 'alias': 'employees'},
            'columns': [{'name': '*', 'alias': None}],
            'limit': 5
        }
        expected_mql = {
            'operation': 'FIND',
            'collection': 'employees',
            'filter': {},
            'projection': None,
            'limit': 5
        }
        self.assertEqual(get_mongodb_query(parsed_sql), expected_mql)

    def test_select_with_aggregation(self):
        parsed_sql = {
            'operation': 'SELECT',
            'table': {'name': 'employees', 'alias': 'employees'},
            'columns': [
                {'name': 'department', 'alias': None},
                {'aggregation_function': 'COUNT', 'name': '*', 'alias': None}
            ],
            'aggregate': ['department']
        }
        expected_mql = {
            'operation': 'AGGREGATE',
            'collection': 'employees',
            'pipeline': [
                {
                    '$group': {
                        '_id': {
                            'department': '$department',
                        },
                        'count_all': {'$sum': 1}
                    }
                }
            ]
        }
        print(get_mongodb_query(parsed_sql))
        self.assertEqual(get_mongodb_query(parsed_sql), expected_mql)

    def test_select_with_aggregation_with_alias(self):
        parsed_sql = {
            'operation': 'SELECT',
            'table': {'name': 'employees', 'alias': 'employees'},
            'columns': [
                {'name': 'department', 'alias': None},
                {'aggregation_function': 'COUNT', 'name': '*', 'alias': 'dept_count'}
            ],
            'aggregate': ['department']
        }
        expected_mql = {
            'operation': 'AGGREGATE',
            'collection': 'employees',
            'pipeline': [
                {
                    '$group': {
                        '_id': {
                            'department': '$department',
                        },
                        'dept_count': {'$sum': 1}
                    }
                }
            ]
        }
        print(get_mongodb_query(parsed_sql))
        self.assertEqual(get_mongodb_query(parsed_sql), expected_mql)

    def test_select_with_aggregation_with_having(self):
        parsed_sql = {
            'operation': 'SELECT',
            'table': {'name': 'employees', 'alias': 'employees'},
            'columns': [
                {'name': 'department', 'alias': None},
                {'aggregation_function': 'SUM', 'name': 'salary', 'alias': None}
            ],
            'aggregate': ['department'],
            'having': {
                'aggregation_function': 'SUM',
                'column': 'salary',
                'operator': '>',
                'value': 1000
            }
        }
        expected_mql = {
            'operation': 'AGGREGATE',
            'collection': 'employees',
            'pipeline': [
                {
                    '$group': {
                        '_id': {
                            'department': '$department'
                        },
                        'sum_salary': {'$sum': '$salary'}
                    }
                },
                {
                    '$match': {
                        'sum_salary': {'$gt': 1000}
                    }
                }
            ]
        }
        self.assertEqual(get_mongodb_query(parsed_sql), expected_mql)

    def test_select_with_aggregation_with_having_alias(self):
        parsed_sql = {
            'operation': 'SELECT',
            'table': {'name': 'employees', 'alias': 'employees'},
            'columns': [
                {'name': 'department', 'alias': None},
                {'aggregation_function': 'SUM', 'name': 'salary', 'alias': 'total_dept_salary'}
            ],
            'aggregate': ['department'],
            'having': {
                'aggregation_function': 'SUM',
                'column': 'salary',
                'operator': '>',
                'value': 1000
            }
        }
        expected_mql = {
            'operation': 'AGGREGATE',
            'collection': 'employees',
            'pipeline': [
                {
                    '$group': {
                        '_id': {
                            'department': '$department'
                        },
                        'total_dept_salary': {'$sum': '$salary'}
                    }
                },
                {
                    '$match': {
                        'total_dept_salary': {'$gt': 1000}
                    }
                }
            ]
        }
        self.assertEqual(get_mongodb_query(parsed_sql), expected_mql)

    def test_select_with_inner_join(self):
        parsed_sql = {
            'operation': 'SELECT',
            'columns': [
                {'name': 'e.name', 'alias': None},
                {'name': 'd.name', 'alias': None}
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
        expected_mql = {
            'operation': 'AGGREGATE',
            'collection': 'employees',
            'pipeline': [
                {
                    '$lookup': {
                        'from': 'departments',
                        'localField': 'department_id',
                        'foreignField': 'id',
                        'as': 'd'
                    }
                },
                {
                    '$unwind': '$d'
                },
                {
                    '$project': {
                        'e.name': '$name',
                        'd.name': '$d.name'
                    }
                }
            ]
        }
        self.assertEqual(get_mongodb_query(parsed_sql), expected_mql)

    def test_select_with_join_and_column_aliases(self):
        parsed_sql = {
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
        expected_mql = {
            'operation': 'AGGREGATE',
            'collection': 'employees',
            'pipeline': [
                {
                    '$lookup': {
                        'from': 'departments',
                        'localField': 'department_id',
                        'foreignField': 'id',
                        'as': 'd'
                    }
                },
                {
                    '$unwind': '$d'
                },
                {
                    '$project': {
                        'employee_id': '$id',
                        'department_name': '$d.name'
                    }
                }
            ]
        }
        self.assertEqual(get_mongodb_query(parsed_sql), expected_mql)

    def test_select_with_left_join_with_filter(self):
        parsed_sql = {
            'operation': 'SELECT',
            'table': {'name': 'customers', 'alias': 'c'},
            'columns': [
                {'name': 'c.name', 'alias': None},
                {'name': 'o.id', 'alias': None}
            ],
            'joins': [
                {
                    'type': 'LEFT',
                    'table': {'name': 'orders', 'alias': 'o'},
                    'on': {'left': 'c.id', 'operator': '=', 'right': 'o.customer_id'}
                }
            ],
            'filter': {
                'column': 'o.status',
                'operator': '=',
                'value': 'pending'
            }
        }
        expected_mql = {
            'operation': 'AGGREGATE',
            'collection': 'customers',
            'pipeline': [
                {
                    '$lookup': {
                        'from': 'orders',
                        'localField': 'id',
                        'foreignField': 'customer_id',
                        'as': 'o'
                    }
                },
                {
                    '$unwind': {
                        'path': '$o',
                        'preserveNullAndEmptyArrays': True
                    }
                },
                {
                    '$project': {
                        'c.name': '$name',
                        'o.id': '$o.id'
                    }
                },
                {
                    '$match': {
                        'o.status': 'pending'
                    }
                }
            ]
        }
        self.assertEqual(get_mongodb_query(parsed_sql), expected_mql)

    def test_select_with_left_join_with_filter_and_base_alias(self):
        parsed_sql = {
            'operation': 'SELECT',
            'table': {'name': 'customers', 'alias': 'c'},
            'columns': [
                {'name': 'c.name', 'alias': None},
                {'name': 'o.id', 'alias': None}
            ],
            'joins': [
                {
                    'type': 'LEFT',
                    'table': {'name': 'orders', 'alias': 'o'},
                    'on': {'left': 'c.id', 'operator': '=', 'right': 'o.customer_id'}
                }
            ],
            'filter': {
                'column': 'c.status',
                'operator': '=',
                'value': 'pending'
            }
        }
        expected_mql = {
            'operation': 'AGGREGATE',
            'collection': 'customers',
            'pipeline': [
                {
                    '$lookup': {
                        'from': 'orders',
                        'localField': 'id',
                        'foreignField': 'customer_id',
                        'as': 'o'
                    }
                },
                {
                    '$unwind': {
                        'path': '$o',
                        'preserveNullAndEmptyArrays': True
                    }
                },
                {
                    '$project': {
                        'c.name': '$name',
                        'o.id': '$o.id'
                    }
                },
                {
                    '$match': {
                        'status': 'pending'
                    }
                }
            ]
        }
        self.assertEqual(get_mongodb_query(parsed_sql), expected_mql)

    def test_select_with_multiple_joins(self):
        parsed_sql = {
            'operation': 'SELECT',
            'columns': [
                {'name': 'o.id', 'alias': None},
                {'name': 'e.name', 'alias': None},
                {'name': 'd.name', 'alias': None}
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
        expected_mql = {
            'operation': 'AGGREGATE',
            'collection': 'orders',
            'pipeline': [
                {
                    '$lookup': {
                        'from': 'employees',
                        'localField': 'employee_id',
                        'foreignField': 'id',
                        'as': 'e'
                    }
                },
                {
                    '$unwind': '$e'
                },
                {
                    '$lookup': {
                        'from': 'departments',
                        'localField': 'e.department_id',
                        'foreignField': 'id',
                        'as': 'd'
                    }
                },
                {
                    '$unwind': '$d'
                },
                {
                    '$project': {
                        'o.id': '$id',
                        'e.name': '$e.name',
                        'd.name': '$d.name'
                    }
                }
            ]
        }
        self.assertEqual(get_mongodb_query(parsed_sql), expected_mql)


if __name__ == '__main__':
    unittest.main()