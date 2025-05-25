import json
import os
from typing import Dict, Optional

from ..utils import Console, ALIAS_CONNECTION_DETAILS_PATH

class ConnectionDetailsManager:
    def __init__(self, filepath: str = ALIAS_CONNECTION_DETAILS_PATH):
        self.filepath = filepath
        self.connection_details: Dict[str, dict] = {}
        self.load_connection_details()

    # Load configurations from the config file
    def load_connection_details(self) -> None:
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, 'r') as f:
                    self.connection_details = json.load(f)
            except json.JSONDecodeError:
                Console.error(f"Invalid JSON in {self.filepath}")
                self.connection_details = {}
        else:
            self.connection_details = {}

    # List all database configurations
    def list_connections(self) -> Dict[str, dict]:
        return self.connection_details

    # Get a database configuration by alias
    def get_connection(self, alias: str) -> Optional[dict]:
        return self.connection_details.get(alias)

    # Add or update a database configuration
    def add_connection(self, alias: str, config: dict) -> None:
        self.connection_details[alias] = config
        self.save_connection_details()

    # Remove a database configuration
    def remove_connection(self, alias: str) -> None:
        if alias in self.connection_details:
            del self.connection_details[alias]
            self.save_connection_details()

    # Save configurations to the config file
    def save_connection_details(self) -> None:
        try:
            with open(self.filepath, 'w') as f:
                json.dump(self.connection_details, f, indent=4)
        except json.JSONDecodeError:
            Console.error(f"Error in saving to {self.filepath}")
            self.connection_details = {}