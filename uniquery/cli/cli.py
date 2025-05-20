import cmd
import argparse
import shlex
import sys
from pyfiglet import Figlet
from uniquery.connectors import Neo4jConnector, MySQLConnector, MongoDBConnector
from uniquery.translators import QueryTranslator
from uniquery.utils.config_manager import ConfigManager
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

class UniQueryCLI(cmd.Cmd):
    def __init__(self):
        super().__init__()
        console = Console(width=100)

        project_name = "UniQuery"
        project_version = "1.0.0"
        author = "Deepak Rathore"
        description = ("This tool transform SQL queries into Cypher, document, key-value, and other query languages, "
                       "unlocking seamless access to diverse databases through one powerful interface.")
        try:
            figlet = Figlet(font='slant')
            figlet_text = figlet.renderText('Uni Query')
            figlet_text_object = Text(figlet_text, style="green", justify="center")

            if sys.stdin.isatty():
                console.clear()
            console.print(figlet_text_object)
            console.print(Panel(description, title="Project Description", subtitle=project_version))

            table = Table(title="\nProject Details", show_header=True, header_style="bold magenta")
            table.add_column("Key", style="magenta", no_wrap=True)
            table.add_column("Value", style="green")
            table.add_row("Name", project_name)
            table.add_row("Version", project_version)
            table.add_row("Author", author)
            table.add_row("Description", description)
            console.print(table)
        except Exception as e:
            print("UI initialization failed.")

        # Set CLI properties
        self.intro = ''  # Clear intro to prevent double printing
        self.prompt = "UniQuery > "
        self.config_manager = ConfigManager()
        self.neo4j_connector = None
        self.mysql_connector = None
        self.current_alias = None
        self.current_db_type = None
        self.translator = QueryTranslator()
        # Store active connections
        self.connections = {}

    def do_info(self, arg):
        """Show information about this tool"""
        print("This tool transforms SQL queries into Cypher syntax, allowing users to access both relational and graph data with ease.")
        print("\nAvailable commands:")
        print("  dbalias list - List all saved database configurations")
        print("  set_database - Configure database connection")
        print("  use_config   - Use a saved database configuration")
        print("  query        - Execute a SQL query")
        print("  info         - Show this help message")
        print("  exit         - Exit the application")

    def do_alias(self, arg):
        if not arg:
            print("""Please provide one of these subcommand to alias
                  1. list
                  2. add <alias>
                  3. edit <alias>
                  4. delete <alias>""")
            return

        # Split the input safely (handles quoted args)
        parts = shlex.split(arg)
        command = parts[0].lower()
        
        match command:
            case 'list':
                self.list_aliases()
            case 'add':
                self.add_alias(parts[1], parts[2:])
            case 'edit':
                self.edit_alias(parts[1], parts[2:])
            case 'delete':
                self.delete_alias(parts[1])
            case 'use':
                self.use_alias(parts[1])
            case _:
                print("Invalid command. Use: add, edit, delete, or list")
    
    # List all database alias and its configurations
    def list_aliases(self):
        configs = self.config_manager.list_configs()
        if not configs:
            print("No database alias and configurations found.\n")
            return
        
        print("Saved database alias and its configurations:\n")
        for alias, config in configs.items():
            print(f"Alias: {alias}")
            print(f"Type: {config['type']}")
            if config.get('uri') is not None:
                print(f"URI: {config['uri']}")
            if config.get('host') is not None:
                print(f"Host: {config['host']}")
            if config.get('port') is not None:
                print(f"Port: {config['port']}")
            if config.get('username') is not None:
                print(f"Username: {config['username']}")
            if config.get('database') is not None:
                print(f"Database: {config['database']}")
            print("")

    # Add Database Alias 
    def add_alias(self, alias, args):
        parser = argparse.ArgumentParser(description='Database connection arguments')
        parser.add_argument('--type', choices=['neo4j', 'mysql', 'mongodb'], help='Database type')
        parser.add_argument('--host', help='Database host')
        parser.add_argument('--port', type=int, help='Database port')
        parser.add_argument('--uri', help='Database URI')
        parser.add_argument('--username', help='Database username')
        parser.add_argument('--password', help='Database password')
        parser.add_argument('--database', help='Database name')

        try:
            parsed_args = parser.parse_args(args)

            # Validate required arguments
            if not parsed_args.type:
                print("Please specify database type using --type [neo4j|mysql|mongodb]")
                return

            if parsed_args.type == 'neo4j' and not parsed_args.uri:
                print("For Neo4j, please provide --uri")
                return
            elif parsed_args.type == 'mysql' and not (parsed_args.host and parsed_args.port):
                print("For MySQL, please provide --host and --port")
                return
            elif parsed_args.type == 'mongodb' and not (parsed_args.host and parsed_args.port):
                print("For MongoDB, please provide --host and --port")
                return

            if not parsed_args.username:
                print("Please provide --username")
                return

            if not parsed_args.password:
                print("Please provide --password")
                return
            
            match parsed_args.type:
                case 'neo4j':
                    try:
                        test_connector = Neo4jConnector(parsed_args.uri, parsed_args.username, parsed_args.password)
                        test_connector.close()
                    except Exception as e:
                        print(f"Failed to connect to Neo4j: {e}")
                        return
                    config = {
                        'type': 'neo4j',
                        'uri': parsed_args.uri,
                        'username': parsed_args.username,
                        'password': parsed_args.password
                    }
                case 'mysql':
                    try:
                        test_connector = MySQLConnector(parsed_args.host, parsed_args.port, parsed_args.username, parsed_args.password, parsed_args.database)
                        test_connector.close()
                    except Exception as e:
                        print(f"Failed to connect to MySQL: {e}")
                        return
                    config = {
                        'type': 'mysql',
                        'host': parsed_args.host,
                        'port': parsed_args.port,
                        'username': parsed_args.username,
                        'password': parsed_args.password
                    }
                case 'mongodb':
                    try:
                        test_connector = MongoDBConnector(parsed_args.host, parsed_args.port, parsed_args.username, parsed_args.password, parsed_args.database)
                        test_connector.close()
                    except Exception as e:
                        print(f"Failed to connect to MongoDB: {e}")
                        return
                    config = {
                        'type': 'mongodb',
                        'host': parsed_args.host,
                        'port': parsed_args.port,
                        'username': parsed_args.username,
                        'password': parsed_args.password
                    }

            # Save the database alias and its configuration
            self.config_manager.add_config(alias, config)
            print(f"Alias '{alias}' added with config: {vars(parsed_args)}")

        except SystemExit:
            # Prevent argparse from calling sys.exit()
            print("Invalid arguments provided.")

    def edit_alias(self, alias, args):
        existing_config = self.config_manager.get_config(alias)
        if not existing_config:
            print(f"No configuration found for alias '{alias}'")
            return

        parser = argparse.ArgumentParser(description='Edit database connection arguments')
        parser.add_argument('--type', choices=['neo4j', 'mysql'], help='Database type')
        parser.add_argument('--host', help='Database host')
        parser.add_argument('--port', type=int, help='Database port')
        parser.add_argument('--uri', help='Database URI')
        parser.add_argument('--username', help='Database username')
        parser.add_argument('--password', help='Database password')
        parser.add_argument('--database', help='Database name')

        try:
            parsed_args = parser.parse_args(args)
            updates = {k: v for k, v in vars(parsed_args).items() if v is not None}
            new_config = {**existing_config, **updates}

            type = new_config.get('type')
            host = new_config.get('host')
            port = new_config.get('port')
            uri = new_config.get('uri')
            username = new_config.get('username')
            password = new_config.get('password')
            database = new_config.get('database')

            # Validate required arguments
            if not type:
                print("Please specify database type using --type [neo4j|mysql]")
                return

            if type == 'neo4j' and not uri:
                print("For Neo4j, please provide --uri")
                return
            elif type == 'mysql' and not (host and port):
                print("For MySQL, please provide --host and --port")
                return
            elif type == 'mongodb' and not (host and port):
                print("For MongoDB, please provide --host and --port")
                return

            if not username:
                print("Please provide --username")
                return

            if not password:
                print("Please provide --password")
                return

            match type:
                case 'neo4j':
                    try:
                        test_connector = Neo4jConnector(uri, username, password)
                        test_connector.close()
                    except Exception as e:
                        print(f"Failed to connect to Neo4j: {e}")
                        return
                    config = {
                        'type': 'neo4j',
                        'uri': uri,
                        'username': username,
                        'password': password
                    }
                case 'mysql':
                    try:
                        test_connector = MySQLConnector(host, port, username, password, database)
                        test_connector.close()
                    except Exception as e:
                        print(f"Failed to connect to MySQL: {e}")
                        return
                    config = {
                        'type': 'mysql',
                        'host': host,
                        'port': port,
                        'username': username,
                        'password': password
                    }
                case 'mongodb':
                    try:
                        test_connector = MongoDBConnector(host, port, username, password, database)
                        test_connector.close()
                    except Exception as e:
                        print(f"Failed to connect to MongoDB: {e}")
                        return
                    config = {
                        'type': 'mongodb',
                        'host': parsed_args.host,
                        'port': parsed_args.port,
                        'username': parsed_args.username,
                        'password': parsed_args.password
                    }

            # Save the database alias and its configuration
            self.config_manager.add_config(alias, config)
            print(f"Alias '{alias}' added with config: {vars(parsed_args)}")

        except SystemExit:
            # Prevent argparse from calling sys.exit()
            print("Invalid arguments provided.")

    def delete_alias(self, alias):
        existing_config = self.config_manager.get_config(alias)
        if not existing_config:
            print(f"No configuration found for alias '{alias}'")
            return

        confirm = input(f"Are you sure you want to delete alias '{alias}'? (y/n): ").strip().lower()
        if confirm == 'y':
            self.config_manager.remove_config(alias)
            print(f"Alias '{alias}' deleted successfully.")
        else:
            print("Delete operation cancelled.")

    def do_exit(self, arg):
        """Exit the CLI"""
        # Close all active connections
        for connection in self.connections.values():
            connection['connector'].close()
        self.connections.clear()
        print("Exiting Hybrid CLI.")
        return True

    def do_EOF(self, arg):
        """Handle Ctrl+D"""
        return self.do_exit(arg)

    def use_alias(self, alias):
        """Use a saved database alias to establish a connection"""
        config = self.config_manager.get_config(alias)
        if not config:
            print(f"No configuration found for alias '{alias}'")
            return
        db_type = config.get('type')
        try:
            if db_type == 'neo4j':
                connector = Neo4jConnector(config['uri'], config['username'], config['password'])
            elif db_type == 'mysql':
                connector = MySQLConnector(config['host'], config['port'], config['username'], config['password'], config.get('database'))
            elif db_type == 'mongodb':
                connector = MongoDBConnector(config['host'], config['port'], config['username'], config['password'], config.get('database'))
            else:
                print(f"Unsupported database type '{db_type}' for alias '{alias}'")
                return
            # Do not close here, keep connection open for queries
            # connector.close()
        except Exception as e:
            print(f"Failed to connect using alias '{alias}': {e}")
            return
        # Close previous connection if any
        if self.current_alias and self.current_alias in self.connections:
            self.connections[self.current_alias]['connector'].close()
            del self.connections[self.current_alias]
        # Store new connection
        self.connections[alias] = {'connector': connector, 'type': db_type}
        self.current_alias = alias
        self.current_db_type = db_type
        self.run_query_prompt(alias)

    def run_query_prompt(self, alias):
        """Prompt user to enter queries and execute them on current connection"""
        print(f"Entering query mode for alias '{alias}'. Type 'exit' or 'quit' to leave.")
        self.prompt = f"{alias} > "
        if not self.current_alias or self.current_alias not in self.connections:
            print("No active database connection. Use 'use_alias <alias>' to connect.")
            return

        connector = self.connections[self.current_alias]['connector']
        db_type = self.current_db_type

        while True:
            try:
                query = input(self.prompt).strip()
                if query.lower() in ('exit', 'quit'):
                    print("Exiting query mode.")
                    self.prompt = "UniQuery > "
                    break
                if not query:
                    continue

                if db_type == 'neo4j':
                    cypher_query = self.translator.translate(query, "CYPHER")
                    print(f"Translated Cypher Query: {cypher_query}")
                    # Execute Cypher query
                    result = connector.run_query(cypher_query)
                    print(result)
                elif db_type == 'mysql':
                    # Execute SQL query
                    result = connector.run_query(query)
                    print(result)
                elif db_type == 'mongodb':
                    mql_query = self.translator.translate(query, "MQL")
                    print(f"Translated MQL Query: {mql_query}")
                    # Execute SQL query
                    result = connector.run_query(mql_query)
                    print(result)
                else:
                    print(f"Unsupported database type '{db_type}'")
            except Exception as e:
                self.prompt = "UniQuery > "
                print(f"Error executing query: {e}")