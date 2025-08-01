from neo4j import GraphDatabase
from tabulate import tabulate

class Neo4jConnector():
    def __init__(self, uri, username, password):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))

    def close(self):
        if self.driver:
            self.driver.close()

    def run_query(self, query):
        try:
            with self.driver.session() as session:
                result = session.run(query)
                records = [record.data() for record in result]

                if not records:
                    return None

                # Flatten if the result is nested
                flat_records = []
                for record in records:
                    flat = {}
                    for key, value in record.items():
                        if isinstance(value, dict):
                            # Flatten nested dictionary
                            for sub_key, sub_value in value.items():
                                flat[sub_key] = sub_value
                        else:
                            flat[key] = value
                    flat_records.append(flat)

                headers = flat_records[0].keys()
                rows = [list(r.values()) for r in flat_records]

                return tabulate(rows, headers=headers, tablefmt="fancy_grid")

        except Exception as err:
            raise Exception(f"Neo4j Error: {str(err)}")