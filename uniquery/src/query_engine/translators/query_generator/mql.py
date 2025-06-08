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

    raise Exception("This operation is not supported for MQL translation")