"""
Centralized configuration management for ETL framework.
Handles loading, validation, and access to configuration parameters.
"""

import os
import yaml
from typing import Any, Dict, Optional
from pathlib import Path


class ConfigurationError(Exception):
    """Custom exception for configuration-related errors."""
    pass


class ConfigManager:
    """
    Singleton configuration manager for ETL framework.
    Provides centralized access to configuration parameters.
    """
    
    _instance: Optional['ConfigManager'] = None
    _config: Dict[str, Any] = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize configuration manager."""
        if not self._config:
            self._load_config()
    
    def _load_config(self, config_path: Optional[str] = None) -> None:
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to configuration file. Defaults to config.yaml
        """
        if config_path is None:
            # Look for config.yaml in project root
            current_dir = Path(__file__).parent.parent
            config_path = current_dir / "config.yaml"
        
        try:
            with open(config_path, 'r') as f:
                self._config = yaml.safe_load(f)
            
            # Validate required sections
            self._validate_config()
            
            # Resolve environment variables
            self._resolve_env_vars()
            
        except FileNotFoundError:
            raise ConfigurationError(f"Configuration file not found: {config_path}")
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Error parsing configuration file: {e}")
    
    def _validate_config(self) -> None:
        """Validate that all required configuration sections exist."""
        required_sections = ['spark', 'database', 'logging', 'etl']
        
        for section in required_sections:
            if section not in self._config:
                raise ConfigurationError(f"Missing required configuration section: {section}")
    
    def _resolve_env_vars(self) -> None:
        """Resolve environment variables in configuration."""
        # Resolve database passwords from environment
        if 'database' in self._config:
            for db_type in ['source', 'target']:
                if db_type in self._config['database']:
                    password_env = self._config['database'][db_type].get('password_env')
                    if password_env:
                        password = os.getenv(password_env)
                        if password:
                            self._config['database'][db_type]['password'] = password
                        else:
                            raise ConfigurationError(
                                f"Environment variable {password_env} not set"
                            )
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key path (dot notation).
        
        Args:
            key: Configuration key in dot notation (e.g., 'spark.app_name')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        
        return value
    
    def get_spark_config(self) -> Dict[str, Any]:
        """Get Spark-specific configuration."""
        return self._config.get('spark', {})
    
    def get_database_config(self, db_type: str = 'source') -> Dict[str, Any]:
        """
        Get database configuration.
        
        Args:
            db_type: 'source' or 'target'
            
        Returns:
            Database configuration dictionary
        """
        return self._config.get('database', {}).get(db_type, {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration."""
        return self._config.get('logging', {})
    
    def get_etl_config(self) -> Dict[str, Any]:
        """Get ETL-specific configuration."""
        return self._config.get('etl', {})
    
    def get_quality_config(self) -> Dict[str, Any]:
        """Get data quality configuration."""
        return self._config.get('quality', {})
    
    def get_monitoring_config(self) -> Dict[str, Any]:
        """Get monitoring configuration."""
        return self._config.get('monitoring', {})
    
    def get_performance_config(self) -> Dict[str, Any]:
        """Get performance tuning configuration."""
        return self._config.get('performance', {})
    
    def set(self, key: str, value: Any) -> None:
        """
        Set configuration value (for testing/override purposes).
        
        Args:
            key: Configuration key in dot notation
            value: Value to set
        """
        keys = key.split('.')
        config = self._config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
    
    def reload(self, config_path: Optional[str] = None) -> None:
        """
        Reload configuration from file.
        
        Args:
            config_path: Path to configuration file
        """
        self._config = {}
        self._load_config(config_path)
    
    def to_dict(self) -> Dict[str, Any]:
        """Return complete configuration as dictionary."""
        return self._config.copy()


# Global configuration instance
config = ConfigManager()