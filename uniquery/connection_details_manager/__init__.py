"""Uniquery utilities package providing configuration management and helper functions.

This package contains utility modules and classes for the Uniquery application:

- ConfigManager: Handles database configuration storage and management
"""

ALIAS_CONNECTION_DETAILS_PATH = 'alias_connection_details.json'


from .main import ConnectionDetailsManager

__all__ = ['ConnectionDetailsManager']
