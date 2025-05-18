import json
import os
from typing import Dict, Optional

class ConfigManager:
    def __init__(self, config_file: str = "database_config.json"):
        self.config_file = config_file
        self.configs: Dict[str, dict] = {}
        self.load_configs()

    # Load configurations from the config file
    def load_configs(self) -> None:
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    self.configs = json.load(f)
            except json.JSONDecodeError:
                print(f"Error: Invalid JSON in {self.config_file}")
                self.configs = {}
        else:
            self.configs = {}

    # List all database configurations
    def list_configs(self) -> Dict[str, dict]:
        return self.configs

    # Get a database configuration by alias
    def get_config(self, alias: str) -> Optional[dict]:
        return self.configs.get(alias)

    # Add or update a database configuration
    def add_config(self, alias: str, config: dict) -> None:
        self.configs[alias] = config
        self.save_configs()

    # Remove a database configuration
    def remove_config(self, alias: str) -> None:
        if alias in self.configs:
            del self.configs[alias]
            self.save_configs()

    # Save configurations to the config file
    def save_configs(self) -> None:
        with open(self.config_file, 'w') as f:
            json.dump(self.configs, f, indent=4)