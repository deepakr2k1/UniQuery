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
            operation = query.get("operation")

            # Database
            if operation == 'CREATE_DATABASE':
                database_name = query.get("database_name")
                if database_name in self.client.list_database_names():
                    raise Exception(f"Database `{database_name}` already exist")
                db = self.client[database_name]
                db["__init__"].insert_one({"created_by": "uniquery"})
                return True
            elif operation == 'USE_DATABASE':
                database_name = query.get("database_name")
                if database_name not in self.client.list_database_names():
                    raise Exception(f"Database `{database_name}` does not exist")
                self.database = self.client[database_name]
                print(self.database)
                return True
            elif operation == 'DROP_DATABASE':
                database_name = query.get("database_name")
                if database_name not in self.client.list_database_names():
                    raise Exception(f"Database `{database_name}` does not exist")
                self.client.drop_database(database_name)
                return True
            elif operation == 'SHOW_DATABASES':
                database_names = self.client.list_database_names()
                return database_names

            # Table
            if operation == 'CREATE_COLLECTION':
                table = query.get("table")
                if table in self.database.list_collection_names():
                    raise Exception(f"Collection `{table}` already exist")
                self.database.create_collection(table)
                return True
            elif operation == 'DROP_COLLECTION':
                table = query.get("table")
                if table not in self.database.list_collection_names():
                    raise Exception(f"Collection `{table}` does not exist")
                self.database[table].drop()
                return True
            elif operation == 'RENAME_COLLECTION':
                old_name = query.get("old_name")
                new_name = query.get("new_name")
                if old_name not in self.database.list_collection_names():
                    raise Exception(f"Collection `{old_name}` does not exist")
                self.database[old_name].rename(new_name)
                return True
            elif operation == 'SHOW_COLLECTIONS':
                return self.database.list_collection_names()
            elif operation == 'SHOW_COLLECTION':
                table = query.get("table_name")
                if table not in self.database.list_collection_names():
                    raise Exception(f"Collection `{table}` does not exist")
                collection = self.database[table]
                stats = collection.estimated_document_count()
                coll_stats = self.database.command("collstats", table)
                indexes = list(collection.list_indexes())
                index_data = [{"name": idx["name"], "keys": list(idx["key"].items())} for idx in indexes]
                sample = collection.find_one()
                sample_fields = list(sample.keys()) if sample else []
                return {
                    "collection": table,
                    "indexes": index_data,
                    "stats": {
                        "count": stats,
                        "size": coll_stats.get("size", 0),
                        "storageSize": coll_stats.get("storageSize", 0)
                    },
                    "sample_fields": sample_fields
                }
            elif operation == 'CREATE_INDEX':
                table = query.get("table")
                index_name = query.get("index_name")
                columns = query.get("columns")
                if table not in self.database.list_collection_names():
                    raise Exception(f"Collection `{table}` does not exist")
                collection = self.database[table]
                existing_indexes = [idx["name"] for idx in collection.list_indexes()]
                if index_name in existing_indexes:
                    raise Exception(f"Index `{index_name}` already exist")
                keys = [(col, 1) for col in columns]
                collection.create_index(keys, name=index_name)
                return True
            elif operation == 'DROP_INDEX':
                table = query.get("table")
                index_name = query.get("index_name")
                if table not in self.database.list_collection_names():
                    raise Exception(f"Collection `{table}` does not exist")
                collection = self.database[table]
                existing_indexes = [idx["name"] for idx in collection.list_indexes()]
                if index_name not in existing_indexes:
                    raise Exception(f"Index `{index_name}` does not exist")
                collection.drop_index(index_name)
                return True
            elif operation == "INSERT_DATA":
                table = query.get("collection")
                documents = query.get("documents", [])
                if table not in self.database.list_collection_names():
                    self.database.create_collection(table)
                result = self.database[table].insert_many(documents)
                return result
            elif operation == "UPDATE_DATA":
                table = query.get("collection")
                updates = query.get("updates", {})
                filter_criteria = query.get("filter", {})
                if table not in self.database.list_collection_names():
                    raise Exception(f"Collection `{table}` does not exist")
                result = self.database[table].update_many(filter_criteria, {"$set": updates})
                return result
            elif operation == "DELETE_DATA":
                table = query.get("collection")
                filter_criteria = query.get("filter", {})
                if table not in self.database.list_collection_names():
                    raise Exception(f"Collection `{table}` does not exist")
                result = self.database[table].delete_many(filter_criteria)
                return result
            elif operation == "FIND":
                table = query.get("collection")
                collection = self.database[table]
                filter_criteria = query.get("filter", {})
                projection = query.get("projection", {})
                sort = query.get("sort")
                limit = query.get("limit")
                cursor = collection.find(filter_criteria, projection if projection else None)
                if sort:
                    cursor = cursor.sort(sort)
                if limit:
                    cursor = cursor.limit(limit)
                result = list(cursor)
                return result
            elif operation == "AGGREGATE":
                table = query.get("collection")
                pipeline = query.get("pipeline", [])
                collection = self.database[table]
                return list(collection.aggregate(pipeline))

            raise Exception(f"Unsupported operation: {operation}")

        except Exception as err:
            raise Exception(f"MongoDB Error: {str(err)}")
