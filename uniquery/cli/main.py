import cmd
import shlex

from rich.text import Text

from uniquery.connection_details_manager import ConnectionDetailsManager
from uniquery.cli.welcome_screen import display_welcome_screen
from uniquery.cli import AVAILABLE_COMMANDS_INFO, ALIAS_SUBCOMMANDS_INFO
from uniquery.cli.alias_actions import list_aliases, add_alias, edit_alias, delete_alias, use_alias
from uniquery.utils import Console

class UniQueryCLI(cmd.Cmd):
    def __init__(self):
        super().__init__()
        self._setup_cli()
        self._init_state()

    def _setup_cli(self):
        display_welcome_screen()
        self.prompt = "UniQuery > "

    def _init_state(self):
        self.connection_details_manager = ConnectionDetailsManager()
        self.active_alias = None
        self.active_connection = {}

    def do_info(self, arg):
        Console.info(AVAILABLE_COMMANDS_INFO)

    def do_help(self, arg):
        self.do_info(arg)

    def do_alias(self, arg):
        if not arg:
            Console.warn(ALIAS_SUBCOMMANDS_INFO)
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
                Console.warn(ALIAS_SUBCOMMANDS_INFO)

    def do_exit(self, arg):
        """Exit the CLI"""
        # Close all active connections
        if self.active_connection.get('connector') is not None:
            self.active_connection['connector'].close()
        self.active_connection.clear()
        Console.out("Exiting UniQuery!")
        return True

    def do_EOF(self, arg):
        """Handle Ctrl+D"""
        return self.do_exit(arg)
