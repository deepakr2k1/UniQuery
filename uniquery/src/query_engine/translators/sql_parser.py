from sqlglot import expressions as exp, parse_one, TokenType
from pprint import pprint

def extract_details_from_reference(node):
    fk_columns = [c.this for c in node.args["expressions"]]
    ref_table = node.args["reference"].this.this.this.this
    ref_columns = [c.sql() for c in node.args["reference"].this.expressions]
    return fk_columns, ref_table, ref_columns


class SqlParser:

    def parse(self, sql_query):
        try:
            expression = parse_one(sql_query)

            if isinstance(expression, exp.Create):
                if expression.kind == 'DATABASE':
                    database_name = expression.this.this.this
                    return { 'operation': 'CREATE_DATABASE', 'database_name': database_name }
                elif expression.kind == 'INDEX':
                    index_name = expression.this.this.this
                    table_name = expression.this.args["table"].this.this
                    column_names = [col.this.this.name for col in expression.this.args["params"].args["columns"]]

                    return {
                        'operation': 'CREATE_INDEX',
                        'index_name': index_name,
                        'table': table_name,
                        'columns': column_names
                    }
                elif expression.kind == 'TABLE':
                    table_name = expression.this.this.this.this
                    columns = []
                    constraints = []

                    for col_def in expression.this.expressions:
                        if isinstance(col_def, exp.ColumnDef):
                            col_name = col_def.this.this
                            col_type = col_def.kind.sql() if col_def.kind else None
                            col_constraints = []
                            if col_def.constraints:
                                for constraint in col_def.constraints:
                                    col_constraints.append(constraint.sql())
                            columns.append({'name': col_name, 'type': col_type, 'constraints': col_constraints})

                        elif isinstance(col_def, exp.ForeignKey):
                            fk_columns = [c.this for c in col_def.args["expressions"]]
                            reference = col_def.args["reference"]
                            ref_table = reference.this.this.this.this
                            ref_columns = [c.this for c in reference.this.expressions]
                            constraints.append({
                                'type': 'FOREIGN KEY',
                                'columns': fk_columns,
                                'references': {'table_name': ref_table, 'columns': ref_columns}
                            })

                    return {
                        "operation": "CREATE_TABLE",
                        "table_name": table_name,
                        "columns": columns,
                        "constraints": constraints
                    }

            if isinstance(expression, exp.Alter):
                if expression.kind == 'TABLE':
                    table_name = expression.this.this.this
                    actions = []

                    for action in expression.actions:
                        if isinstance(action, exp.ColumnDef):  # ADD COLUMN
                            col_name = action.this.this
                            col_type = action.kind.this.name
                            default_value = None
                            if hasattr(action, "constraints"):
                                for constraint in action.constraints:
                                    if hasattr(constraint, "kind") and constraint.kind.__class__.__name__ == "DefaultColumnConstraint":
                                        default_value = constraint.kind.this.this
                            actions.append({
                                "column": col_name,
                                "type": col_type,
                                "default_value": default_value,
                                "action_type": "ADD_COLUMN"
                            })

                        elif isinstance(action, exp.Drop):
                            if action.kind == 'COLUMN':
                                col_name = action.this.name
                                actions.append({
                                    "action_type": "DROP_COLUMN",
                                    "column": col_name,
                                })
                            elif action.kind == 'CONSTRAINT':
                                col_name = action.this.name
                                actions.append({
                                    "action_type": "DROP_CONSTRAINT",
                                    "constraint_name": col_name,
                                })

                        elif isinstance(action, exp.RenameColumn):
                            old_name = action.args["this"].this.name
                            new_name = action.args['to'].this.name
                            actions.append({
                                "old_name": old_name,
                                "new_name": new_name,
                                "action_type": "RENAME_COLUMN"
                            })

                        elif isinstance(action, exp.AlterColumn):
                            if action.args.get('default'):
                                column = action.this.this
                                actions.append({
                                    'action_type': 'SET_DEFAULT',
                                    'column': column,
                                    'value': action.args.get('default').this
                                })
                            elif action.args.get('drop'):
                                column = action.this.this
                                actions.append({
                                    'action_type': 'DROP_DEFAULT',
                                    'column': column
                                })

                        elif isinstance(action, exp.AddConstraint):
                            for constraint_expr in action.expressions:
                                if isinstance(constraint_expr, exp.PrimaryKey):
                                    columns = [col.this.name for col in constraint_expr.expressions]
                                    actions.append({
                                        'action_type': 'ADD_CONSTRAINT',
                                        'constraint_type': 'PRIMARY_KEY',
                                        'columns': columns
                                    })
                                elif isinstance(constraint_expr, exp.ForeignKey):
                                    columns = [col.this for col in constraint_expr.args['expressions']]
                                    ref_table = constraint_expr.args["reference"].this.this.this.this
                                    ref_columns = [col.this for col in constraint_expr.args["reference"].this.expressions]
                                    actions.append({
                                        'action_type': 'ADD_CONSTRAINT',
                                        'constraint_type': 'FOREIGN_KEY',
                                        'columns': columns,
                                        'references': {
                                            'table': ref_table,
                                            'columns': ref_columns
                                        }
                                    })

                        elif isinstance(action, exp.AlterRename):
                            old_name = expression.this.this.this
                            new_name = action.this.this.this
                            return {
                                'operation': 'RENAME_TABLE',
                                'old_name': old_name,
                                'new_name': new_name
                            }

                    result = {
                        "operation": "ALTER_TABLE",
                        "table_name": table_name,
                        "actions": actions
                    }
                    return result

            if isinstance(expression, exp.Drop):
                if expression.kind == 'DATABASE':
                    database_name = expression.this.this.this
                    return {
                        'operation': 'DROP_DATABASE',
                        'database_name': database_name
                    }
                if expression.kind == 'TABLE':
                    table_name = expression.this.this.this
                    return {
                        'operation': 'DROP_TABLE',
                        'table_name': table_name
                    }
                if expression.kind == 'INDEX':
                    index_name = expression.this.this.this
                    table_name = expression.args["cluster"].this.this
                    return {
                        'operation': 'DROP_INDEX',
                        'index_name': index_name,
                        'table': table_name
                    }

            if isinstance(expression, exp.Use):
                database_name = expression.this.this.this
                return {
                    'operation': 'USE_DATABASE',
                    'database_name': database_name
                }

            if isinstance(expression, exp.Command):
                cmd_part = [part for part in expression.expression.this.split(" ") if part.strip()]
                command = expression.this.upper()
                if command == 'SHOW':
                    if len(cmd_part) == 1 and cmd_part[0].upper() == 'DATABASES':
                        return {'operation': 'SHOW_DATABASES'}
                    if len(cmd_part) == 2 and cmd_part[0].upper() == 'TABLE':
                        return {'operation': 'SHOW_TABLE', 'table_name': cmd_part[1]}
                    else:
                        raise Exception(f"Unsupported SHOW command")

            if isinstance(expression, exp.Insert):
                table_name = expression.this.this.this.this
                columns = [col.sql() for col in expression.this.expressions]
                values_exp = expression.expression.expressions[0].expressions
                values = [val.to_py() for val in values_exp]
                return {
                    'operation': 'INSERT_DATA',
                    'table_name': table_name,
                    'columns': columns,
                    'values': values
                }

            if isinstance(expression, exp.Update):
                table_name = expression.this.this.this
                columns = []
                values = []
                condition = {}
                for assignment in expression.expressions:
                    col_name = assignment.this.this.this
                    value = assignment.expression.to_py() if hasattr(assignment.expression, 'to_py') else assignment.expression.sql()
                    columns.append(col_name)
                    values.append(value)

                return {
                    'operation': 'UPDATE_DATA',
                    'table_name': table_name,
                    'columns': columns,
                    'values': values,
                    'condition': condition
                }

            if isinstance(expression, exp.Delete):
                table_name = expression.this.this.this
                where = expression.args.get("where")
                condition = {}

                return {
                    'operation': 'DELETE_DATA',
                    'table_name': table_name,
                    'condition': condition,
                }

            raise Exception(f"Unsupported SQL query: {sql_query}")

        except Exception as e:
            raise Exception(f"Error parsing SQL query: {e}")