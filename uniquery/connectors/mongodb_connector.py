from pymongo import MongoClient
from tabulate import tabulate

class MongoDBConnector():
    def __init__(self, host, port, username, password, database=None, auth_source="admin"):
        uri = f"mongodb://{username}:{password}@{host}:{port}/"
        uri += f"?authSource={auth_source}"
        self.client = MongoClient(uri)
        self.database = self.client[database] if database else None

    def close(self):
        if self.client:
            self.client.close()

    def run_query(self, query):
        try:
            collection = query.get('collection')
            operation = query.get('operation')
            filter = query.get('filter', {})

            if not collection or not operation:
                raise Exception("Query must specify table and operation")

            results = []
            if operation == 'find':
                results = list(self.database[collection].find(filter))
            elif operation == 'aggregate':
                pipeline = query.get('pipeline', [])
                results = list(self.database[collection].aggregate(pipeline))

            if not results:
                return None

            headers = results[0].keys()
            rows = [[str(doc.get(h)) for h in headers] for doc in results]
            return tabulate(rows, headers=headers, tablefmt="fancy_grid")

        except Exception as err:
            raise Exception(f"MongoDB Error: {str(err)}")
