"""
UniQuery CLI
===========

This module provides a command-line interface for managing database configurations and
converting queries between different database languages adnd execute them against the
configured databases..
"""

from .cli import UniQueryCLI

__all__ = ['UniQueryCLI']