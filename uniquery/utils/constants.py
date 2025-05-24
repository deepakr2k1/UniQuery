from enum import Enum

class DatabaseType(Enum):
    MYSQL = "mysql"
    POSTGRE_SQL = "postgresql"
    ORACLE = "oracle"
    SQL_SERVER = "sqlserver"
    MONGO_DB = "mongodb"
    NEO4J = "neo4j"

    @classmethod
    def from_str(cls, value: str):
        try:
            return cls(value.lower())
        except ValueError:
            raise Exception(f"Unsupported database type: {value}")

    def is_sql(self):
        return (
                self == DatabaseType.MYSQL or
                self == DatabaseType.POSTGRE_SQL or
                self == DatabaseType.ORACLE or
                self == DatabaseType.SQL_SERVER
        )

    def is_mql(self):
        return self == DatabaseType.MONGO_DB

    def is_cypher(self):
        return self == DatabaseType.NEO4J