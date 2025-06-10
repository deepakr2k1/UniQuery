import io
import contextlib
from sqlglot import expressions as exp, parse_one, TokenType

_OPERATOR_MAP = {
    exp.EQ: '=',
    exp.NEQ: '!=',
    exp.GT: '>',
    exp.GTE: '>=',
    exp.LT: '<',
    exp.LTE: '<=',
    exp.Like: 'LIKE'
}

_AGGREGATION_FUNCTION_MAP = {
    exp.Sum: 'SUM'
}

def extract_details_from_reference(node):
    fk_columns = [c.this for c in node.args["expressions"]]
    ref_table = node.args["reference"].this.this.this.this
    ref_columns = [c.sql() for c in node.args["reference"].this.expressions]
    return fk_columns, ref_table, ref_columns

def extract_table(expression):
    from_expr = expression.args.get("from")
    table = {}
    if from_expr:
        table['label'] = from_expr.this.name
        table['alias'] = from_expr.this.alias_or_name
    return table


def extract_relationship_joins(expression):
    joins = []
    for join in expression.find_all(exp.Join):
        join_type = join.args.get('side', '').upper() if join.args.get('side') else "INNER"
        table_expr = join.this
        on_expr = join.args.get("on")

        if isinstance(on_expr, exp.Condition) or isinstance(on_expr, exp.EQ):
            joins.append({
                "type": join_type,
                "table": {
                    "name": table_expr.name,
                    "alias": table_expr.alias,
                },
                "on": {
                    "left": on_expr.left.sql(),
                    "operator": _OPERATOR_MAP.get(type(on_expr), "="),
                    "right": on_expr.right.sql(),
                }
            })
        else:
            raise ValueError(f"Unsupported ON clause format in JOIN: {on_expr}")
    return joins

def extract_group_by_fields(expression):
    group_expr = expression.args.get("group")
    group_fields = []
    if group_expr:
        for e in group_expr.expressions:
            group_fields.append(e.sql())
    having = extract_having_conditions(expression)
    return group_fields, having

def extract_having_conditions(expression):
    having_expr = expression.args.get("having")
    return _parse_condition(having_expr.this) if (having_expr and having_expr.this)  else None


def extract_where_conditions(expression):
    where_expr = expression.args.get("where")
    return _parse_condition(where_expr.this) if (where_expr and where_expr.this)  else None


def _parse_condition(expr):
    if isinstance(expr, (exp.And, exp.Or)):
        return {
            "operator": expr.key.upper(),
            "operands": [_parse_condition(arg) for arg in expr.flatten()]
        }
    elif isinstance(expr, exp.Not):
        return {
            "operator": "NOT",
            "operand": _parse_condition(expr.this)
        }
    elif isinstance(expr, exp.Paren):
        return _parse_condition(expr.this)
    elif isinstance(expr, exp.Is):
        column = expr.this.sql()
        return {
            "operator": "IS_NULL",
            "column": column
        }
    elif isinstance(expr, exp.In):
        column = expr.this.sql()
        values = [_literal(val) for val in expr.expressions]
        return {
            "operator": "IN",
            "column": column,
            "values": values
        }
    elif isinstance(expr, exp.Between):
        return {
            'operator': 'BETWEEN',
            'column': expr.this.sql(),
            'low': _literal(expr.args['low']),
            'high': _literal(expr.args['high'])
        }
    elif type(expr) in _OPERATOR_MAP:
        if type(expr.this) in _AGGREGATION_FUNCTION_MAP:
            return {
                "aggregation_function": expr.left.sql_name().upper(),
                "column": expr.left.this.sql(),
                "operator": _OPERATOR_MAP[type(expr)],
                "value": _literal(expr.right)
            }
        return {
            "column": expr.left.sql(),
            "operator": _OPERATOR_MAP[type(expr)],
            "value": _literal(expr.right)
        }
    else:
        return expr.sql()

def _literal(node):
    if isinstance(node, exp.Literal):
        return node.to_py()
    elif isinstance(node, exp.Identifier):
        return node.name  # For identifiers like column names
    elif hasattr(node, "this"):
        return node.this
    return node.sql()

def extract_return_fields(expression):
    select_exprs = expression.args.get("expressions")
    is_distinct = expression.args.get("distinct", False)
    fields = []
    if select_exprs:
        for expr in select_exprs:
            if isinstance(expr, exp.Column):
                fields.append({
                    'name': expr.sql(),
                    'alias': None
                })
            if isinstance(expr, exp.Func) or isinstance(expr.this, exp.Func):
                _alias = None
                if not isinstance(expr, exp.Func):
                    _alias = expr.alias if expr.alias else None
                    expr = expr.this
                fields.append({
                    "aggregation_function": expr.sql().split('(')[0] if expr.sql() else None,
                    "name": expr.this.sql() if expr.this else None,
                    "alias": _alias if _alias else expr.alias if expr.alias else None
                })
            elif isinstance(expr, exp.Alias):
                fields.append({
                    'name': expr.this.sql(),
                    'alias': expr.alias_or_name
                })
            elif isinstance(expr, exp.Star):
                fields.append({
                    'name': '*',
                    'alias': None
                })

    return fields, is_distinct


def extract_order_by(expression):
    order_expr = expression.args.get("order")
    if not order_expr:
        return None

    order_items = []
    for e in order_expr.expressions:
        if isinstance(e, exp.Ordered):
            order_items.append({
                "column": e.this.name if hasattr(e.this, "name") else e.this.sql(),
                "order": "DESC" if e.args.get("desc") else "ASC"
            })
        else:
            order_items.append({
                "column": e.sql(),
                "order": "ASC"
            })
    return order_items


def extract_limit(expression):
    """
    Extracts the LIMIT value from a SQL expression.
    Returns the limit as an integer or string, or None if not present.
    """
    limit_expr = expression.args.get("limit")
    if limit_expr and hasattr(limit_expr, "this"):
        return _literal(limit_expr['this'].this.args['expression'])
    return None


def parse_sql_silently(sql):
    with contextlib.redirect_stderr(io.StringIO()):
        return parse_one(sql)


class SqlParser:

    def parse(self, sql_query):
        try:
            expression = parse_sql_silently(sql_query)

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
                    if len(cmd_part) == 1 and cmd_part[0].upper() == 'TABLES':
                        return {'operation': 'SHOW_TABLES'}
                    if len(cmd_part) == 2 and cmd_part[0].upper() == 'TABLE':
                        return {'operation': 'SHOW_TABLE', 'table_name': cmd_part[1]}
                    else:
                        raise Exception(f"Unsupported SHOW command")

            if isinstance(expression, exp.Insert):
                table_name = expression.this.this.this.this
                columns = [col.sql() for col in expression.this.expressions]
                values = []
                for row in expression.expression.expressions:
                    values.append([val.to_py() for val in row.expressions])
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
                filter = extract_where_conditions(expression)
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
                    'filter': filter
                }

            if isinstance(expression, exp.Delete):
                table_name = expression.this.this.this
                filter = extract_where_conditions(expression)

                return {
                    'operation': 'DELETE_DATA',
                    'table_name': table_name,
                    'filter': filter,
                }

            if isinstance(expression, (exp.Select, exp.Join)):
                table = extract_table(expression)
                return_fields, is_distinct = extract_return_fields(expression)
                where_clause = extract_where_conditions(expression)
                order_by_clause = extract_order_by(expression)
                limit_clause = extract_limit(expression)
                aggregate, having = extract_group_by_fields(expression)
                joins = extract_relationship_joins(expression)

                select_obj = {
                    'operation': 'SELECT',
                    'table': {'name': table['label'], 'alias': table['alias']},
                    'columns': return_fields
                }

                if where_clause:
                    select_obj['filter'] = where_clause
                if order_by_clause:
                    select_obj['order_by'] = order_by_clause
                if limit_clause:
                    select_obj['limit'] = limit_clause
                if aggregate:
                    select_obj['aggregate'] = aggregate
                if having:
                    select_obj['having'] = having
                if joins:
                    select_obj['joins'] = joins
                return select_obj

            raise Exception(f"Unsupported SQL query: {sql_query}")

        except Exception as e:
            raise Exception(f"Error parsing SQL query: {e}")