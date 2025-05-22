import argparse

from rich.table import Table
from rich.text import Text

from uniquery.cli import NO_ALIAS_FOUND, ALIAS_CONNECTION_OPTIONS_INFO
from uniquery.connectors import Neo4jConnector, MySQLConnector, MongoDBConnector, get_connection
from uniquery.query_engine.main import QueryEngine


def list_aliases(self):
    configs = self.connection_details_manager.list_configs()
    if not configs:
        self.console.print(Text(NO_ALIAS_FOUND, style='yellow'))
        return

    self.console.print("Saved alias and its connection details:")
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

    self.console.print(table)


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
        self.console.print(Text("Please specify database type using `--type [neo4j|mysql|mongodb]`"), style='yellow')
        return False
    if _type == 'mysql' and not (host and port):
        self.console.print(Text("For MySQL, please provide `--host` and `--port`"), style='yellow')
        return False
    elif _type == 'mongodb' and not (host and port):
        self.console.print(Text("For MongoDB, please provide `--host` and `--port`"), style='yellow')
        return False
    elif _type == 'neo4j' and not uri:
        self.console.print(Text("For Neo4j, please provide `--uri`"), style='yellow')
        return False
    if not username:
        self.console.print(Text("Please provide `--username`"), style='yellow')
        return False
    if not password:
        self.console.print(Text("Please provide `--password`"), style='yellow')
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
                self.console.print(Text(f"Unsupported database type '{type}'"), style='yellow')
                return None

        return connection_details

    except Exception as e:
        self.console.print(Text(f"Failed to connect to {type}: {e}"), style='red')
        return None


def print_alias_connection_details(self, alias, connection_details):
    connection_details_table = Table(show_header=False, header_style="bold magenta")
    connection_details_table.add_column(style="magenta")
    connection_details_table.add_column(style="green")

    for key, value in connection_details.items():
        if key != 'password':
            connection_details_table.add_row(key.capitalize(), str(value))

    self.console.print(connection_details_table)


# Add Database Alias
def add_alias(self, alias, args):
    existing_connection_details = self.connection_details_manager.get_config(alias)
    if existing_connection_details:
        self.console.print(Text(f"Alias already exist with name `{alias}`"), style='yellow')
        print_alias_connection_details(self, alias, existing_connection_details)
        return

    parser = get_arg_parser()

    try:
        options = parser.parse_args(args)
        options = vars(options)

        are_valid_options = validate_options(self, options)
        if not are_valid_options:
            self.console.print(Text(ALIAS_CONNECTION_OPTIONS_INFO), style = 'blue')
            return

        connection_details = test_connection(self, options)
        if not connection_details:
            return

        # Save the database alias and its configuration
        self.connection_details_manager.add_config(alias, connection_details)
        self.console.print(Text(f"Alias `{alias}` added with connection details:"), style='italic green')
        print_alias_connection_details(self, alias, connection_details)


    except SystemExit:
        # Prevent argparse from calling sys.exit()
        self.console.print(Text("Invalid arguments provided."), style = 'yellow')


def edit_alias(self, alias, args):
    existing_connection_details = self.connection_details_manager.get_config(alias)
    if not existing_connection_details:
        self.console.print(Text(f"No alias exist with name `{alias}`"), style='yellow')
        return

    parser = get_arg_parser()

    try:
        options = parser.parse_args(args)
        updates = {k: v for k, v in vars(options).items() if v is not None}
        new_options = {**existing_connection_details, **updates}

        are_valid_options = validate_options(self, new_options)
        if not are_valid_options:
            self.console.print(Text(ALIAS_CONNECTION_OPTIONS_INFO), style = 'blue')
            return

        connection_details = test_connection(self, new_options)
        if not connection_details:
            return

        # Save the database alias and its configuration
        self.connection_details_manager.add_config(alias, connection_details)
        self.console.print(Text(f"Alias `{alias}` updated with connection details:"), style='italic green')
        print_alias_connection_details(self, alias, connection_details)

    except SystemExit:
        # Prevent argparse from calling sys.exit()
        self.console.print(Text("Invalid arguments provided."), style = 'yellow')


def delete_alias(self, alias):
    existing_connection_details = self.connection_details_manager.get_config(alias)
    if not existing_connection_details:
        self.console.print(Text(f"No alias exist with name `{alias}`"), style='yellow')
        return

    from rich.prompt import Confirm
    confirm = Confirm.ask(f"Are you sure you want to delete alias '{alias}'?")
    if confirm:
        self.connection_details_manager.remove_config(alias)
        self.console.print(Text(f"Alias '{alias}' deleted successfully."), style='green')
    else:
        self.console.print(Text(f"Delete operation cancelled"), style='blue')


def use_alias(self, alias):
    connection_details = self.connection_details_manager.get_config(alias)
    if not connection_details:
        self.console.print(Text(f"No alias exist with name `{alias}`"), style='yellow')
        return

    if self.active_connection and self.active_connection['connector']:
        self.active_connection['connector'].close()
        del self.active_connection['connector']

    self.active_connection['connector'] = get_connection(connection_details)
    self.active_connection['connector_type'] = connection_details['type']
    self.active_alias = alias

    run_query_prompt(self, alias)


def run_query_prompt(self, alias):
    """Prompt user to enter queries and execute them on current connection"""
    print(f"Entering query mode for alias '{alias}'. Type 'exit' or 'quit' to leave.")
    self.prompt = f"{alias} > "
    if not self.active_alias or not self.active_connection['connector']:
        print("No active database connection. Use 'alias use <alias>' to connect.")
        return

    connector = self.active_connection['connector']
    db_type = self.active_connection['connector_type']

    queryEngine = QueryEngine(db_type, connector)

    while True:
        try:
            query = input(self.prompt).strip()
            if query.lower() in ('exit', 'quit'):
                print("Exiting query mode.")
                self.prompt = "UniQuery > "
                break

            if not query:
                continue

            splitted_query = query.lower().split(" ")

            if splitted_query[0] == 'set_native':
                if len(splitted_query) != 2:
                    print("Invalid syntax. Usage: set_native <true|false>")
                    continue
                elif splitted_query[1] not in ('true', 'false'):
                    print("Invalid syntax. Usage: set_native <true|false>")
                    continue
                else:
                    queryEngine.set_is_native_mode(splitted_query[1] == 'true')
                    print(
                        f"Native mode is {'enabled' if queryEngine.is_native_mode else 'disabled'}."
                    )
                continue

            if splitted_query[0] == 'set_output':
                if len(splitted_query) != 2:
                    print("Invalid syntax. Usage: set_output <table|json|raw>")
                    continue
                elif splitted_query[1] not in ('table', 'json', 'raw'):
                    print("Invalid output format. Choose from: table, json, raw")
                    continue
                else:
                    queryEngine.set_output_format(splitted_query[1])
                    print(f"Output format set to '{splitted_query[1]}'")
                continue

            result = queryEngine.execute_query(query)
            print(result)

        except Exception as e:
            self.prompt = "UniQuery > "
            print(f"Error executing query: {e}")
