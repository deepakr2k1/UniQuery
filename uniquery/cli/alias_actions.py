import argparse

from rich.table import Table

from uniquery.cli import NO_ALIAS_FOUND, ALIAS_CONNECTION_OPTIONS_INFO
from uniquery.connectors import Neo4jConnector, MySQLConnector, MongoDBConnector, get_connection
from uniquery.query_engine.main import QueryEngine
from uniquery.utils import Console, DatabaseType


def list_aliases(self):
    configs = self.connection_details_manager.list_connections()
    if not configs:
        Console.warn(NO_ALIAS_FOUND)
        return

    Console.out("Saved alias and its connection details:")
    table = Table(title=None, show_header=True, header_style="bold magenta")
    table.add_column("Alias", style="magenta", no_wrap=True)
    table.add_column("Connection Details", style="green")

    for alias, config in configs.items():
        connection_details = f"Type: {config['type']}\n"
        if config.get('uri') is not None:
            connection_details += f"URI: {config['uri']}\n"
        if config.get('host') is not None:
            connection_details += f"Host: {config['host']}\n"
        if config.get('port') is not None:
            connection_details += f"Port: {config['port']}\n"
        if config.get('username') is not None:
            connection_details += f"Username: {config['username']}\n"
        if config.get('database') is not None:
            connection_details += f"Database: {config['database']}\n"
        table.add_row(alias, connection_details)

    Console.out(table)


def get_arg_parser():
    parser = argparse.ArgumentParser(description='Database connection arguments')
    parser.add_argument('--type', choices=['mysql', 'mongodb', 'neo4j'], help='Database type')
    parser.add_argument('--host', help='Database host')
    parser.add_argument('--port', type=int, help='Database port')
    parser.add_argument('--uri', help='Database URI')
    parser.add_argument('--username', help='Database username')
    parser.add_argument('--password', help='Database password')
    parser.add_argument('--database', help='Database name')
    return parser


def validate_options(self, options):
    _type = options.get('type')
    host = options.get('host')
    port = options.get('port')
    uri = options.get('uri')
    username = options.get('username')
    password = options.get('password')

    if not _type:
        Console.warn("Please specify database type using `--type [neo4j|mysql|mongodb]`")
        return False
    if _type == 'mysql' and not (host and port):
        Console.warn("For MySQL, please provide `--host` and `--port`")
        return False
    elif _type == 'mongodb' and not (host and port):
        Console.warn("For MongoDB, please provide `--host` and `--port`")
        return False
    elif _type == 'neo4j' and not uri:
        Console.warn("For Neo4j, please provide `--uri`")
        return False
    if not username:
        Console.warn("Please provide `--username`")
        return False
    if not password:
        Console.warn("Please provide `--password`")
        return False
    return True


def test_connection(self, connection_details):
    _type = connection_details.get('type')
    host = connection_details.get('host')
    port = connection_details.get('port')
    uri = connection_details.get('uri')
    username = connection_details.get('username')
    password = connection_details.get('password')
    database = connection_details.get('database')

    connection_details = {}

    try:
        match _type:
            case 'mysql':
                test_connector = MySQLConnector(host, port, username, password, database)
                test_connector.close()
                connection_details['type'] = _type
                connection_details['host'] = host
                connection_details['port'] = port
                connection_details['username'] = username
                connection_details['password'] = password
                connection_details['database'] = database
            case 'mongodb':
                test_connector = MongoDBConnector(host, port, username, password, database)
                test_connector.close()
                connection_details['type'] = _type
                connection_details['host'] = host
                connection_details['port'] = port
                connection_details['username'] = username
                connection_details['password'] = password
                connection_details['database'] = database
            case 'neo4j':
                test_connector = Neo4jConnector(uri, username, password)
                test_connector.close()
                connection_details['type'] = _type
                connection_details['uri'] = uri
                connection_details['username'] = username
                connection_details['password'] = password
                connection_details['database'] = database
            case _:
                Console.warn(f"Unsupported database type '{type}'")
                return None

        return connection_details

    except Exception as e:
        Console.error(f"Failed to connect to {type}: {e}")
        return None


def print_alias_connection_details(self, alias, connection_details):
    connection_details_table = Table(show_header=False, header_style="bold magenta")
    connection_details_table.add_column(style="magenta")
    connection_details_table.add_column(style="green")

    for key, value in connection_details.items():
        if key != 'password':
            connection_details_table.add_row(key.capitalize(), str(value))

    Console.out(connection_details_table)


# Add Database Alias
def add_alias(self, alias, args):
    existing_connection_details = self.connection_details_manager.get_connection(alias)
    if existing_connection_details:
        Console.warn(f"Alias already exist with name `{alias}`")
        print_alias_connection_details(self, alias, existing_connection_details)
        return

    parser = get_arg_parser()

    try:
        options = parser.parse_args(args)
        options = vars(options)

        are_valid_options = validate_options(self, options)
        if not are_valid_options:
            Console.info(ALIAS_CONNECTION_OPTIONS_INFO)
            return

        connection_details = test_connection(self, options)
        if not connection_details:
            return

        # Save the database alias and its configuration
        self.connection_details_manager.add_connection(alias, connection_details)
        Console.success(f"Alias `{alias}` added with connection details:")
        print_alias_connection_details(self, alias, connection_details)


    except SystemExit:
        # Prevent argparse from calling sys.exit()
        Console.warn("Invalid arguments provided.")


def edit_alias(self, alias, args):
    existing_connection_details = self.connection_details_manager.get_connection(alias)
    if not existing_connection_details:
        Console.warn(f"No alias exist with name `{alias}`")
        return

    try:
        parser = get_arg_parser()

        options = parser.parse_args(args)
        updates = {k: v for k, v in vars(options).items() if v is not None}
        new_options = {**existing_connection_details, **updates}

        are_valid_options = validate_options(self, new_options)
        if not are_valid_options:
            Console.info(ALIAS_CONNECTION_OPTIONS_INFO)
            return

        connection_details = test_connection(self, new_options)
        if not connection_details:
            return

        # Save the database alias and its configuration
        self.connection_details_manager.add_connection(alias, connection_details)
        Console.success(f"Alias `{alias}` updated with connection details:")
        print_alias_connection_details(self, alias, connection_details)

    except SystemExit:
        # Prevent argparse from calling sys.exit()
        Console.warn("Invalid arguments provided.")


def delete_alias(self, alias):
    existing_connection_details = self.connection_details_manager.get_connection(alias)
    if not existing_connection_details:
        Console.warn(f"No alias exist with name `{alias}`")
        return

    from rich.prompt import Confirm
    confirm = Confirm.ask(f"Are you sure you want to delete alias '{alias}'?")
    if confirm:
        self.connection_details_manager.remove_connection(alias)
        Console.success(f"Alias '{alias}' deleted successfully.")
    else:
        Console.out(f"Delete operation cancelled")


def use_alias(self, alias):
    connection_details = self.connection_details_manager.get_connection(alias)
    if not connection_details:
        Console.warn(f"No alias exist with name `{alias}`")
        return

    if self.active_connection and self.active_connection['connector']:
        self.active_connection['connector'].close()
        del self.active_connection['connector']

    self.active_connection['connector'] = get_connection(connection_details)
    self.active_connection['connector_type'] = connection_details['type']
    self.active_alias = alias

    run_query_prompt(self, alias)


def handle_set_native(command: str, query_engine: QueryEngine):
    command_parts = command.lower().split()
    if len(command_parts) != 2 or command_parts[1] not in ('true', 'false'):
        Console.warn("Invalid syntax. Usage: set_native <true|false>")
        return
    query_engine.set_is_native_mode(command_parts[1] == 'true')
    Console.out(f"Native mode is {'enabled' if query_engine.is_native_mode else 'disabled'}.")


def handle_set_output(command: str, query_engine: QueryEngine):
    command_parts = command.lower().split()
    if len(command_parts) != 2 or command_parts[1] not in ('table', 'json', 'raw'):
        Console.warn("Invalid syntax. Usage: set_output <table|json|raw>")
        return
    query_engine.set_output_format(command_parts[1])
    Console.out(f"Output format set to '{command_parts[1]}'")


def handle_query_execution(query: str, query_engine: QueryEngine):
    try:
        result = query_engine.execute_query(query)
        Console.out(result)
    except Exception as err:
        Console.error(err)


def run_query_prompt(self, alias):
    Console.out(f"Entering query mode for alias '{alias}'. Type 'exit' or 'quit' to leave.")
    self.prompt = f"{alias} > "
    if not self.active_alias or not self.active_connection['connector']:
        Console.warn("No active database connection. Use 'alias use <alias>' to connect.")
        return

    connector = self.active_connection['connector']
    database_type = DatabaseType(self.active_connection['connector_type'])

    query_engine = QueryEngine(database_type, connector)

    while True:
        try:
            query = input(self.prompt).strip()
            if not query:
                continue

            if query.lower() in ('exit', 'quit'):
                break

            if query.lower().startswith("set_native"):
                handle_set_native(query, query_engine)
            elif query.lower().startswith("set_output"):
                handle_set_output(query, query_engine)
            else:
                handle_query_execution(query, query_engine)

        except Exception as e:
            self.prompt = "UniQuery > "
            Console.error(f"Error executing query: {e}")

    Console.out("Exiting query mode.")
    self.prompt = "UniQuery > "
