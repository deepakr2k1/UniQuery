_DATABASE_OPERATIONS = [
    'CREATE_DATABASE',
    'USE_DATABASE',
    'DROP_DATABASE',
    'SHOW_DATABASES'
]

_INDEX_QUERIES = [
    'CREATE_INDEX',
    'DROP_INDEX'
]

def get_mongodb_query(parsed_data: dict):
    if parsed_data['operation'] in _DATABASE_OPERATIONS:
        return parsed_data

    if parsed_data['operation'] == 'CREATE_TABLE':
        return {
            'operation': 'CREATE_COLLECTION',
            'table': parsed_data['table_name']
        }
    elif parsed_data['operation'] == 'DROP_TABLE':
        return {
            'operation': 'DROP_COLLECTION',
            'table': parsed_data['table_name']
        }
    elif parsed_data['operation'] == 'RENAME_TABLE':
        return {
            'operation': 'RENAME_COLLECTION',
            'old_name': parsed_data['old_name'],
            'new_name': parsed_data['new_name']
        }
    elif parsed_data['operation'] == 'SHOW_TABLES':
        return {
            'operation': 'SHOW_COLLECTIONS'
        }
    elif parsed_data['operation'] == 'SHOW_TABLE':
        return {
            'operation': 'SHOW_COLLECTION',
            'table_name': parsed_data['table_name']
        }
    elif parsed_data['operation'] in _INDEX_QUERIES:
        return parsed_data

    elif parsed_data['operation'] == 'INSERT_DATA':
        documents = []
        for row in parsed_data['values']:
            document = dict(zip(parsed_data['columns'], row))
            documents.append(document)
        return {
            'operation': 'INSERT_DATA',
            'collection': parsed_data['table_name'],
            'documents': documents
        }
    elif parsed_data['operation'] == 'UPDATE_DATA':
        update_fields = dict(zip(parsed_data['columns'], parsed_data['values']))
        return {
            'operation': 'UPDATE_DATA',
            'collection': parsed_data['table_name'],
            'updates': update_fields,
            'filter': _convert_filter_to_mql(parsed_data.get('filter', {}))
        }
    elif parsed_data['operation'] == 'DELETE_DATA':
        return {
            'operation': 'DELETE_DATA',
            'collection': parsed_data['table_name'],
            'filter': _convert_filter_to_mql(parsed_data.get('filter', {}))
        }
    elif parsed_data["operation"] == 'SELECT':
        return get_mongodb_find_query(parsed_data)

    raise Exception("This operation is not supported for MQL translation")


# Helper function to convert SQL-style filters to MongoDB MQL filters
def _convert_filter_to_mql(filter_expr):
    if not filter_expr:
        return {}

    operator = filter_expr['operator'].upper()
    if operator == '=':
        return {filter_expr['column']: filter_expr['value']}
    elif operator == '>':
        return {filter_expr['column']: {'$gt': filter_expr['value']}}
    elif operator == '<':
        return {filter_expr['column']: {'$lt': filter_expr['value']}}
    elif operator in ('AND', 'OR'):
        operands = [_convert_filter_to_mql(op) for op in filter_expr['operands']]
        return {'$' + operator.lower(): operands}
    else:
        raise Exception(f"Unsupported filter operator: {operator}")

def get_mongodb_find_query(parsed_sql):
    operation = parsed_sql.get('operation')
    collection = parsed_sql['table']['name']
    columns = parsed_sql.get('columns', [])
    filter_clause = parsed_sql.get('filter', {})
    order_by = parsed_sql.get('order_by')
    limit = parsed_sql.get('limit')
    aggregate = parsed_sql.get('aggregate')
    having = parsed_sql.get('having')
    joins = parsed_sql.get('joins')

    # Projection (fields to include)
    projection = None
    if columns and not (len(columns) == 1 and columns[0]['name'] == '*'):
        projection = {}
        for col in columns:
            col_name = col['name']
            alias = col.get('alias')
            if alias:
                projection[alias] = 1
            else:
                projection[col_name] = 1

    # Parse filter to MongoDB filter dict
    filter_ = parse_filter(filter_clause) if filter_clause else {}

    # Handle aggregation
    if aggregate:
        pipeline = []
        group_id = {}
        for key in aggregate:
            group_id[key] = f"${key}"
        group_stage = {'_id': group_id}

        func_col_map = {}

        # Add aggregation fields
        for col in columns:
            if 'aggregation_function' in col:
                func = col['aggregation_function'].upper()
                col_name = col.get('name').replace("*", "all")
                alias = col.get('alias') or f"{func.lower()}_{col_name}"
                func_col_map[f"{func.lower()}_{col_name}"] = alias
                if func == 'COUNT':
                    group_stage[alias] = {'$sum': 1}
                elif func == 'SUM':
                    group_stage[alias] = {'$sum': f"${col_name}"}
                # Add more aggregation functions if needed

        pipeline.append({'$group': group_stage})

        # Handle having clause
        if having:
            having_func = having.get('aggregation_function', '').upper()
            having_col = having.get('column').replace("*", "all")
            func = f"{having_func.lower()}_{having_col}"
            having_alias = func_col_map[func]
            operator = having.get('operator')
            value = having.get('value')
            mongo_op = convert_operator_to_mongo(operator)
            pipeline.append({'$match': {having_alias: {mongo_op: value}}})

        return {
            'operation': 'AGGREGATE',
            'collection': collection,
            'pipeline': pipeline
        }

    # Handle joins
    if joins:
        pipeline = []
        base_alias = parsed_sql['table'].get('alias') or parsed_sql['table']['name']

        for join in joins:
            join_type = join.get('type', 'INNER').upper()
            join_table = join['table']['name']
            join_alias = join['table'].get('alias') or join_table
            on = join.get('on', {})

            left = on.get('left', '')
            right = on.get('right', '')

            # Initialize local and foreign fields
            local_field = foreign_field = None

            # Determine which side refers to base table and which to joined table
            if '.' in left and '.' in right:
                left_prefix, left_field = left.split('.', 1)
                right_prefix, right_field = right.split('.', 1)

                if left_prefix == join_alias:
                    foreign_field = left_field
                    if right_prefix == base_alias:
                        local_field = right_field
                    else:
                        local_field = right
                elif right_prefix == join_alias:
                    foreign_field = right_field
                    if left_prefix == base_alias:
                        local_field = left_field
                    else:
                        local_field = left

            pipeline.append({
                '$lookup': {
                    'from': join_table,
                    'localField': local_field,
                    'foreignField': foreign_field,
                    'as': join_alias
                }
            })

            unwind_stage = {'$unwind': f"${join_alias}"}
            if join_type == 'LEFT':
                unwind_stage['$unwind'] = {'path': f"${join_alias}", 'preserveNullAndEmptyArrays': True}
            pipeline.append(unwind_stage)

        # After joins, project columns with aliases handled
        project_stage = {}
        for col in columns:
            name = col['name']
            alias = col.get('alias')
            if '.' in name:
                # name like e.name or d.name
                prefix, field = name.split('.', 1)
                if prefix == base_alias:
                    project_key = alias or name
                    project_stage[project_key] = f"${field}"  # from base collection
                else:
                    project_key = alias or name
                    project_stage[project_key] = f"${prefix}.{field}"  # from joined alias
            else:
                project_key = alias or name
                project_stage[project_key] = 1
        pipeline.append({'$project': project_stage})

        # Apply any filter on joined data after joins as $match
        if filter_:
            base_alias = parsed_sql['table'].get('alias') or parsed_sql['table']['name']
            base_filters = {}

            for key, value in filter_.items():
                if '.' in key and key.startswith(base_alias + '.'):
                    # Strip alias from key if present
                    actual_key = key.split('.', 1)[1] if '.' in key else key
                    base_filters[actual_key] = value
                else:
                    base_filters[key] = value

            if base_filters:
                pipeline.append({'$match': base_filters})

        return {
            'operation': 'AGGREGATE',
            'collection': collection,
            'pipeline': pipeline
        }

    # Simple find query
    query = {
        'operation': 'FIND',
        'collection': collection,
        'filter': filter_,
        'projection': projection,
    }
    if order_by:
        query['sort'] = []
        for order in order_by:
            col = order['column']
            direction = 1 if order.get('order', 'ASC').upper() == 'ASC' else -1
            query['sort'].append((col, direction))
    if limit is not None:
        query['limit'] = limit

    return query


def parse_filter(filter_clause):
    op = filter_clause.get('operator')
    if not op:
        return {}
    op = op.upper()

    if op == '=':
        return {filter_clause['column']: filter_clause['value']}
    elif op == 'AND':
        operands = filter_clause.get('operands', [])
        return {'$and': [parse_filter(cond) for cond in operands]}
    elif op == 'OR':
        operands = filter_clause.get('operands', [])
        return {'$or': [parse_filter(cond) for cond in operands]}
    elif op == '>':
        return {filter_clause['column']: {'$gt': filter_clause['value']}}
    elif op == '<':
        return {filter_clause['column']: {'$lt': filter_clause['value']}}
    elif op == '>=':
        return {filter_clause['column']: {'$gte': filter_clause['value']}}
    elif op == '<=':
        return {filter_clause['column']: {'$lte': filter_clause['value']}}
    elif op == 'LIKE':
        pattern = filter_clause['value']
        regex_pattern = '^' + pattern.replace('%', '.*').replace('_', '.') + '$'
        return {filter_clause['column']: {'$regex': regex_pattern}}
    elif op == 'IN':
        return {filter_clause['column']: {'$in': filter_clause.get('values', [])}}
    elif op == 'BETWEEN':
        low = filter_clause.get('low')
        high = filter_clause.get('high')
        return {filter_clause['column']: {'$gte': low, '$lte': high}}
    elif op == 'IS_NULL':
        return {filter_clause['column']: None}
    elif op == 'NOT':
        operand = filter_clause.get('operand')
        inner_filter = parse_filter(operand) if operand else {}
        if inner_filter:
            key, val = next(iter(inner_filter.items()))
            if val is None:
                return {key: {'$ne': None}}
        return {}
    else:
        return {}

def convert_operator_to_mongo(op):
    mapping = {
        '=': '$eq',
        '>': '$gt',
        '>=': '$gte',
        '<': '$lt',
        '<=': '$lte',
        '!=': '$ne'
    }
    return mapping.get(op, '$eq')