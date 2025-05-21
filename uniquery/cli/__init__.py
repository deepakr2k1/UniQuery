"""
UniQuery CLI
===========

This module provides a command-line interface for managing database configurations and
converting queries between different database languages adnd execute them against the
configured databases..
"""

TOOL_DESCRIPTION = """UniQuery seamlessly transforms SQL queries into Cypher, document, key-value, and other database query languages, 
enabling unified access to diverse database systems through an intuitive interface."""

TOOL_SHORT_DESCRIPTION = "Transform SQL into native queries for seamless access across different database types."

AVAILABLE_COMMANDS_INFO = f"""Available commands:
    alias list                            - List all saved database configurations
    alias add <alias> [options]           - Add a new database alias
    alias edit <alias> [options]          - Edit an existing database alias
    alias delete <alias>                  - Delete a database alias
    alias use <alias>                     - Connect to a configured database alias
    <query>                               - Execute SQL or native query
    set_native <true|false>               - Enable/disable native query mode
    set_output <tabular|json>             - Set query output format
    info                                  - Show command help and usage
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

from .main import UniQueryCLI
from .welcome_screen import display_welcome_screen
from .alias_actions import list_aliases

__all__ = ['UniQueryCLI', 'display_welcome_screen']