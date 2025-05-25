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

            collection = query.get("collection")
            operation = query.get("operation")

            if not collection or not operation:
                raise Exception("Query must specify 'collection' and 'operation'")

            col = self.database[collection]
            result = None

            if operation == "find":
                result = list(col.find(query.get("filter", {}), query.get("projection")))
            elif operation == "findOne":
                result = col.find_one(query.get("filter", {}))
                result = [result] if result else []
            elif operation == "aggregate":
                result = list(col.aggregate(query.get("pipeline", [])))
            elif operation == "countDocuments":
                count = col.count_documents(query.get("filter", {}))
                result = [{"count": count}]
            elif operation == "distinct":
                values = col.distinct(query["field"])
                result = [{query["field"]: v} for v in values]
            elif operation == "insertOne":
                res = col.insert_one(query["document"])
                result = [{"inserted_id": str(res.inserted_id)}]
            elif operation == "insertMany":
                res = col.insert_many(query["documents"])
                result = [{"inserted_ids": [str(i) for i in res.inserted_ids]}]
            elif operation == "updateOne":
                res = col.update_one(query["filter"], query["update"])
                result = [{"matched": res.matched_count, "modified": res.modified_count}]
            elif operation == "updateMany":
                res = col.update_many(query["filter"], query["update"])
                result = [{"matched": res.matched_count, "modified": res.modified_count}]
            elif operation == "replaceOne":
                res = col.replace_one(query["filter"], query["replacement"])
                result = [{"matched": res.matched_count, "replaced": res.modified_count}]
            elif operation == "deleteOne":
                res = col.delete_one(query["filter"])
                result = [{"deleted": res.deleted_count}]
            elif operation == "deleteMany":
                res = col.delete_many(query["filter"])
                result = [{"deleted": res.deleted_count}]
            else:
                raise Exception(f"Unsupported operation: {operation}")

            if not result:
                return "No results."

            headers = result[0].keys()
            rows = [[str(row.get(h)) for h in headers] for row in result]
            return tabulate(rows, headers=headers, tablefmt="fancy_grid")

        except Exception as err:
            raise Exception(f"MongoDB Error: {str(err)}")
