from pyfiglet import Figlet
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
import sys
from uniquery.cli import TOOL_DESCRIPTION

def display_welcome_screen(console):
    try:
        project_info = {
            "name": "UniQuery",
            "version": "1.0.0",
            "author": "Deepak Rathore",
            "description": (TOOL_DESCRIPTION)
        }

        figlet = Figlet(font='slant')
        figlet_text = figlet.renderText('Uni Query')
        figlet_text_obj = Text(figlet_text, style="green", justify="center")

        if sys.stdin.isatty():
            console.clear()
        console.print(figlet_text_obj)
        console.print(Panel(project_info['description'], title="Project Description", subtitle=project_info['version']))

        table = Table(title="\nProject Details", show_header=False, header_style="bold magenta")
        table.add_column("Key", style="magenta", no_wrap=True)
        table.add_column("Value", style="green")
        for key, value in project_info.items():
            table.add_row(key.title(), value)
        console.print(table)

    except Exception:
        print("UI initialization failed.")
