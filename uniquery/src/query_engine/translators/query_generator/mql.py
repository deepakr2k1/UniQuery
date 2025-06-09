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