from pyfiglet import Figlet
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from ..utils import Console, TOOL_DESCRIPTION

def display_welcome_screen():
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

        Console.out(figlet_text_obj)
        Console.out(Panel(project_info['description'], title="Project Description", subtitle=project_info['version']))

        table = Table(title="\nProject Details", show_header=False, header_style="bold magenta")
        table.add_column("Key", style="magenta", no_wrap=True)
        table.add_column("Value", style="green")
        for key, value in project_info.items():
            table.add_row(key.title(), value)
        Console.out(table)

    except Exception:
        Console.error("UI initialization failed.")
