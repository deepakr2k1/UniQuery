_DATABASE_OPERATIONS = [
    'CREATE_DATABASE',
    'USE_DATABASE',
    'DROP_DATABASE',
    'SHOW_DATABASES'
]

def get_mongodb_query(parsed_data: dict):
    if parsed_data['operation'] in _DATABASE_OPERATIONS:
        return parsed_data

    raise Exception("This operation is not supported for MQL translation")