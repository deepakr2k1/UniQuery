from .cli import UniQueryCLI
from .query_engine.main import QueryEngine
from .utils import DatabaseType
from .connection_details_manager import ConnectionDetailsManager
from .connectors.connector import get_connection

def main():
    cli = UniQueryCLI()
    cli.cmdloop()


if __name__ == "__main__":
    main()