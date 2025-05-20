import mysql.connector
from tabulate import tabulate

class MySQLConnector:
    def __init__(self, host, port, username, password, database=None):
        self.connection = mysql.connector.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database
        )
        self.cursor = self.connection.cursor(dictionary=True)

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def run_query(self, query):
        try:
            self.cursor.execute(query)
            results = self.cursor.fetchall()

            if not results:
                print("\nNo records found.")
                return

            headers = results[0].keys()
            rows = [list(r.values()) for r in results]
            return tabulate(rows, headers=headers, tablefmt="fancy_grid")

        except mysql.connector.Error as err:
            raise Exception(f"MySQL Error: {str(err)}")