from rich.console import Console as _console
from rich.text import Text


class Console:
    console = _console(width=100)

    @classmethod
    def _print(cls, msg, style: str=""):
        if isinstance(msg, str):
            cls.console.print(Text(msg, style=style))
        else:
            cls.console.print(msg, style=style)

    @classmethod
    def out(cls, msg, style: str=""):
        cls._print(msg, style)

    @classmethod
    def info(cls, msg, style: str="blue"):
        cls._print(msg, style)

    @classmethod
    def success(cls, msg, style: str="italic green"):
        cls._print(msg, style)

    @classmethod
    def warn(cls, msg, style: str="yellow"):
        cls._print(msg, style)

    @classmethod
    def error(cls, msg, style: str="red"):
        cls._print(msg, style)

    @classmethod
    def fatal(cls, msg, style: str="bold red"):
        cls._print(msg, style)