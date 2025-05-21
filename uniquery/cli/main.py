import cmd
import shlex

from rich.console import Console
from rich.text import Text

from uniquery.connection_details_manager import ConnectionDetailsManager
from uniquery.cli.welcome_screen import display_welcome_screen
from uniquery.cli import AVAILABLE_COMMANDS_INFO, ALIAS_SUBCOMMANDS_INFO
from uniquery.cli.alias_actions import list_aliases, add_alias, edit_alias, delete_alias, use_alias

class UniQueryCLI(cmd.Cmd):
    def __init__(self):
        super().__init__()
        self.console = Console(width=100)

        self._setup_cli()
        self._init_state()

    def _setup_cli(self):
        display_welcome_screen(self.console)
        self.prompt = "UniQuery > "

    def _init_state(self):
        self.connection_details_manager = ConnectionDetailsManager()
        self.active_alias = None
        self.active_connection = {}

    def do_info(self, arg):
        self.console.print(Text(AVAILABLE_COMMANDS_INFO), style='blue')

    def do_alias(self, arg):
        if not arg:
            self.console.print(Text(ALIAS_SUBCOMMANDS_INFO), style='yellow')
            return

        # Split the input safely (handles quoted args)
        parts = shlex.split(arg)
        command = parts[0].lower()
        
        match command:
            case 'list':
                list_aliases(self)
            case 'add':
                add_alias(self, parts[1], parts[2:])
            case 'edit':
                edit_alias(self, parts[1], parts[2:])
            case 'delete':
                delete_alias(self, parts[1])
            case 'use':
                use_alias(self, parts[1])
            case _:
                self.console.print(Text(ALIAS_SUBCOMMANDS_INFO), style='yellow')

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
