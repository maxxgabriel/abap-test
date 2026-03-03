"""
Configuration Management Module
Centralized configuration loading and management for the ETL framework.
"""

import os
import yaml
from typing import Any, Dict, Optional
from pathlib import Path


class ConfigManager:
    """Singleton configuration manager for ETL framework."""
    
    _instance: Optional['ConfigManager'] = None
    _config: Dict[str, Any] = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._config:
            self.load_config()
    
    def load_config(self, config_path: Optional[str] = None) -> None:
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to configuration file. If None, searches default locations.
        """
        if config_path is None:
            config_path = self._find_config_file()
        
        with open(config_path, 'r') as f:
            self._config = yaml.safe_load(f)
        
        # Substitute environment variables
        self._substitute_env_vars(self._config)
    
    def _find_config_file(self) -> str:
        """Find configuration file in standard locations."""
        search_paths = [
            'config.yaml',
            'config/config.yaml',
            '../config.yaml',
            os.path.join(os.path.dirname(__file__), '../config.yaml')
        ]
        
        for path in search_paths:
            if os.path.exists(path):
                return path
        
        raise FileNotFoundError("Configuration file not found in standard locations")
    
    def _substitute_env_vars(self, config: Dict[str, Any]) -> None:
        """Recursively substitute environment variables in config."""
        for key, value in config.items():
            if isinstance(value, dict):
                self._substitute_env_vars(value)
            elif isinstance(value, str) and value.startswith('${') and value.endswith('}'):
                env_var = value[2:-1]
                config[key] = os.getenv(env_var, value)
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.
        
        Args:
            key_path: Dot-separated path to config value (e.g., 'spark.app_name')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key_path.split('.')
        value = self._config
        
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_spark_config(self) -> Dict[str, str]:
        """Get Spark configuration as dictionary."""
        return self.get('spark.config', {})
    
    def get_database_config(self, db_name: str = 'default') -> Dict[str, Any]:
        """Get database configuration."""
        return self.get(f'database.{db_name}', {})
    
    def get_etl_config(self) -> Dict[str, Any]:
        """Get ETL configuration."""
        return self.get('etl', {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration."""
        return self.get('logging', {})
    
    def get_data_quality_config(self) -> Dict[str, Any]:
        """Get data quality configuration."""
        return self.get('data_quality', {})
    
    def get_monitoring_config(self) -> Dict[str, Any]:
        """Get monitoring configuration."""
        return self.get('monitoring', {})
    
    def get_business_rules(self) -> Dict[str, Any]:
        """Get business rules configuration."""
        return self.get('business_rules', {})
    
    def get_schema(self, schema_name: str) -> Dict[str, str]:
        """Get schema definition."""
        return self.get(f'schemas.{schema_name}', {})
    
    def set(self, key_path: str, value: Any) -> None:
        """
        Set configuration value using dot notation.
        
        Args:
            key_path: Dot-separated path to config value
            value: Value to set
        """
        keys = key_path.split('.')
        config = self._config
        
        for key in keys[:-1]:
            if key not in config:
                config[key] = {}
            config = config[key]
        
        config[keys[-1]] = value
    
    def reload(self) -> None:
        """Reload configuration from file."""
        self._config = {}
        self.load_config()
    
    @property
    def config(self) -> Dict[str, Any]:
        """Get entire configuration dictionary."""
        return self._config


# Global configuration instance
config = ConfigManager()