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

TOOL_DESCRIPTION = ("UniQuery seamlessly transforms SQL queries into Cypher, document, key-value, and other database query languages, "
                    "enabling unified access to diverse database systems through an intuitive interface.")

TOOL_SHORT_DESCRIPTION = "Transform SQL into native queries for seamless access across different database types."

AVAILABLE_COMMANDS_INFO = f"""Available commands:
    alias list                            - List all saved database configurations
    alias add <alias> [options]           - Add a new database alias
    alias edit <alias> [options]          - Edit an existing database alias
    alias delete <alias>                  - Delete a database alias
    alias use <alias>                     - Connect to a configured database alias
    <query>                               - Execute SQL or native query
    set_native <true|false>               - Enable/disable native query mode
    set_output <tabular|json|raw>         - Set query output format
    info, help                            - Show command help and usage
    exit, quit, Ctrl+D                    - Exit the application
"""

ALIAS_SUBCOMMANDS_INFO = """Please provide one of these subcommand to alias:
    list                          - List all saved database configurations
    add <alias> [options]         - Add a new database alias with connection details
    edit <alias> [options]        - Edit an existing database alias configuration
    delete <alias>                - Delete a database alias
    use <alias>                   - Connect to a configured database
"""

NO_ALIAS_FOUND = "No database alias found! Create new alias using command: `alias add <alias> [options]`\n"

ALIAS_CONNECTION_OPTIONS_INFO = """Expected connection options:
    --type [neo4j|mysql|mongodb] : Database type
    --host : Database host (required for mysql/mongodb)
    --port : Database port (required for mysql/mongodb)  
    --uri : Database URI (required for neo4j)
    --username : Database username
    --password : Database password
    --database : Database name (optional)
"""

ALIAS_CONNECTION_DETAILS_PATH = 'alias_connection_details.json'
