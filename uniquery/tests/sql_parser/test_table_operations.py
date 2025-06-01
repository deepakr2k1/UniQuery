import unittest

from uniquery.src.query_engine.translators.sql_parser import SqlParser

class TestSqlParserTableQueries(unittest.TestCase):
    def setUp(self):
        self.sql_parser = SqlParser()

    def test_create_table(self):
        sql = """CREATE TABLE employees (
            id INT PRIMARY KEY,
            name VARCHAR(100),
            department VARCHAR(50),
            salary DECIMAL(10,2)
        )"""
        expected = {
            'operation': 'CREATE_TABLE',
            'table_name': 'employees',
            'columns': [
                {'name': 'id', 'type': 'INT', 'constraints': ['PRIMARY KEY']},
                {'name': 'name', 'type': 'VARCHAR(100)', 'constraints': []},
                {'name': 'department', 'type': 'VARCHAR(50)', 'constraints': []},
                {'name': 'salary', 'type': 'DECIMAL(10, 2)', 'constraints': []}
            ],
            "constraints": []
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_create_table_with_foreign_key(self):
        sql = """CREATE TABLE orders (
            order_id INT PRIMARY KEY,
            employee_id INT,
            FOREIGN KEY (employee_id) REFERENCES employees(id)
        )"""
        expected = {
            'operation': 'CREATE_TABLE',
            'table_name': 'orders',
            'columns': [
                {'name': 'order_id', 'type': 'INT', 'constraints': ['PRIMARY KEY']},
                {'name': 'employee_id', 'type': 'INT', 'constraints': []},
            ],
            'constraints': [
                {'type': 'FOREIGN KEY', 'columns': ['employee_id'], 'references': {'table_name': 'employees', 'columns': ['id']}}
            ]
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_alter_table_add_column(self):
        sql = """ALTER TABLE employees
            ADD COLUMN building VARCHAR(10) DEFAULT 'BER-12',
            ADD COLUMN building_addr VARCHAR(100)
        """
        expected = {
            'operation': 'ALTER_TABLE',
            'table_name': 'employees',
            'actions': [
                {'action_type': 'ADD_COLUMN', 'column': 'building', 'type': 'VARCHAR', 'default_value': 'BER-12'},
                {'action_type': 'ADD_COLUMN', 'column': 'building_addr', 'type': 'VARCHAR', 'default_value': None}
            ]
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_alter_table_drop_column(self):
        sql = "ALTER TABLE employees DROP COLUMN building"
        expected = {
            'operation': 'ALTER_TABLE',
            'table_name': 'employees',
            'actions': [
                {'action_type': 'DROP_COLUMN', 'column': 'building'}
            ]
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_alter_table_rename_column(self):
        sql = "ALTER TABLE employees RENAME COLUMN building TO location"
        expected = {
            'operation': 'ALTER_TABLE',
            'table_name': 'employees',
            'actions': [
                {'action_type': 'RENAME_COLUMN', 'old_name': 'building', 'new_name': 'location'}
            ]
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_alter_table_set_default(self):
        sql = "ALTER TABLE employees ALTER COLUMN salary SET DEFAULT 0"
        expected = {
            'operation': 'ALTER_TABLE',
            'table_name': 'employees',
            'actions': [
                {'action_type': 'SET_DEFAULT', 'column': 'salary', 'value': '0'}
            ]
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_alter_table_drop_default(self):
        sql = "ALTER TABLE employees ALTER COLUMN salary DROP DEFAULT"
        expected = {
            'operation': 'ALTER_TABLE',
            'table_name': 'employees',
            'actions': [
                {'action_type': 'DROP_DEFAULT', 'column': 'salary'}
            ]
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_alter_table_add_primary_key(self):
        sql = "ALTER TABLE employees ADD PRIMARY KEY (id)"
        expected = {
            'operation': 'ALTER_TABLE',
            'table_name': 'employees',
            'actions': [
                {'action_type': 'ADD_CONSTRAINT', 'constraint_type': 'PRIMARY_KEY', 'columns': ['id']}
            ]
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_alter_table_add_foreign_key(self):
        sql = "ALTER TABLE orders ADD FOREIGN KEY (employee_id) REFERENCES employees(id)"
        expected = {
            'operation': 'ALTER_TABLE',
            'table_name': 'orders',
            'actions': [
                {
                    'action_type': 'ADD_CONSTRAINT',
                    'constraint_type': 'FOREIGN_KEY',
                    'columns': ['employee_id'],
                    'references': {'table': 'employees', 'columns': ['id']}
                }
            ]
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_alter_table_drop_constraint(self):
        sql = "ALTER TABLE employees DROP CONSTRAINT PK_employees;"
        expected = {
            'operation': 'ALTER_TABLE',
            'table_name': 'employees',
            'actions': [
                {'action_type': 'DROP_CONSTRAINT', 'constraint_name': 'PK_employees'}
            ]
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_rename_table(self):
        sql = "ALTER TABLE employees RENAME TO staff"
        expected = {
            'operation': 'RENAME_TABLE',
            'old_name': 'employees',
            'new_name': 'staff'
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_delete_table(self):
        sql = "DROP TABLE employees"
        expected = {
            'operation': 'DROP_TABLE',
            'table_name': 'employees'
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_create_index(self):
        sql = "CREATE INDEX idx_name ON employees(email)"
        expected = {
            'operation': 'CREATE_INDEX',
            'index_name': 'idx_name',
            'table': 'employees',
            'columns': ['email']
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

    def test_delete_index(self):
        sql = "DROP INDEX idx_name ON employees"
        expected = {
            'operation': 'DROP_INDEX',
            'index_name': 'idx_name',
            'table': 'employees'
        }
        self.assertEqual(self.sql_parser.parse(sql), expected)

if __name__ == '__main__':
    unittest.main()
