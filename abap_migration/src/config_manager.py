"""
Configuration Manager for ETL Shared Framework

Provides centralized configuration management with environment variable
substitution, validation, and dynamic reloading capabilities.
"""

import os
import re
import yaml
from typing import Any, Dict, Optional
from pathlib import Path


class ConfigurationError(Exception):
    """Custom exception for configuration errors"""
    pass


class ConfigManager:
    """
    Centralized configuration manager for ETL framework.
    
    Handles loading, parsing, and accessing configuration from YAML files
    with support for environment variable substitution and validation.
    """
    
    _instance: Optional['ConfigManager'] = None
    _config: Dict[str, Any] = {}
    _config_path: Optional[Path] = None
    
    def __new__(cls):
        """Singleton pattern to ensure single configuration instance"""
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize configuration manager"""
        if not self._config:
            self._load_default_config()
    
    def _load_default_config(self) -> None:
        """Load default configuration file"""
        default_paths = [
            Path("config.yaml"),
            Path("config/config.yaml"),
            Path("/opt/etl/config/config.yaml"),
        ]
        
        for path in default_paths:
            if path.exists():
                self.load_config(path)
                return
        
        raise ConfigurationError("No configuration file found in default locations")
    
    def load_config(self, config_path: str | Path) -> None:
        """
        Load configuration from YAML file
        
        Args:
            config_path: Path to configuration file
            
        Raises:
            ConfigurationError: If file cannot be loaded or parsed
        """
        config_path = Path(config_path)
        
        if not config_path.exists():
            raise ConfigurationError(f"Configuration file not found: {config_path}")
        
        try:
            with open(config_path, 'r') as f:
                raw_config = yaml.safe_load(f)
            
            self._config = self._substitute_env_vars(raw_config)
            self._config_path = config_path
            self._validate_config()
            
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in config file: {e}")
        except Exception as e:
            raise ConfigurationError(f"Failed to load configuration: {e}")
    
    def _substitute_env_vars(self, config: Any) -> Any:
        """
        Recursively substitute environment variables in configuration
        
        Args:
            config: Configuration object (dict, list, or string)
            
        Returns:
            Configuration with environment variables substituted
        """
        if isinstance(config, dict):
            return {k: self._substitute_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._substitute_env_vars(item) for item in config]
        elif isinstance(config, str):
            return self._substitute_string_env_vars(config)
        else:
            return config
    
    def _substitute_string_env_vars(self, value: str) -> str:
        """
        Substitute environment variables in string value
        
        Supports ${VAR_NAME} and ${VAR_NAME:default} syntax
        
        Args:
            value: String potentially containing environment variables
            
        Returns:
            String with environment variables substituted
        """
        pattern = r'\$\{([^}:]+)(?::([^}]*))?\}'
        
        def replace_env_var(match):
            var_name = match.group(1)
            default_value = match.group(2) if match.group(2) is not None else ""
            return os.environ.get(var_name, default_value)
        
        return re.sub(pattern, replace_env_var, value)
    
    def _validate_config(self) -> None:
        """
        Validate configuration structure and required fields
        
        Raises:
            ConfigurationError: If configuration is invalid
        """
        required_sections = ['app', 'spark', 'logging', 'database', 'etl']
        
        for section in required_sections:
            if section not in self._config:
                raise ConfigurationError(f"Missing required configuration section: {section}")
        
        # Validate database configurations
        db_types = ['source', 'target', 'staging']
        for db_type in db_types:
            if db_type not in self._config['database']:
                raise ConfigurationError(f"Missing database configuration: {db_type}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by dot-separated key path
        
        Args:
            key: Dot-separated key path (e.g., 'spark.app_name')
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def get_spark_config(self) -> Dict[str, str]:
        """
        Get Spark configuration as dictionary
        
        Returns:
            Dictionary of Spark configuration parameters
        """
        spark_config = self.get('spark.config', {})
        return {k: str(v) for k, v in spark_config.items()}
    
    def get_database_config(self, db_type: str = 'source') -> Dict[str, Any]:
        """
        Get database configuration
        
        Args:
            db_type: Database type ('source', 'target', or 'staging')
            
        Returns:
            Database configuration dictionary
        """
        return self.get(f'database.{db_type}', {})
    
    def get_jdbc_url(self, db_type: str = 'source') -> str:
        """
        Get JDBC URL for database connection
        
        Args:
            db_type: Database type ('source', 'target', or 'staging')
            
        Returns:
            JDBC connection URL
        """
        db_config = self.get_database_config(db_type)
        return db_config.get('jdbc_url', '')
    
    def get_jdbc_properties(self, db_type: str = 'source') -> Dict[str, str]:
        """
        Get JDBC connection properties
        
        Args:
            db_type: Database type ('source', 'target', or 'staging')
            
        Returns:
            Dictionary of JDBC properties
        """
        db_config = self.get_database_config(db_type)
        return {
            'user': db_config.get('user', ''),
            'password': db_config.get('password', ''),
            'driver': db_config.get('driver', ''),
        }
    
    def get_logging_config(self) -> Dict[str, Any]:
        """
        Get logging configuration
        
        Returns:
            Logging configuration dictionary
        """
        return self.get('logging', {})
    
    def get_etl_config(self) -> Dict[str, Any]:
        """
        Get ETL configuration
        
        Returns:
            ETL configuration dictionary
        """
        return self.get('etl', {})
    
    def get_business_rules(self) -> Dict[str, Any]:
        """
        Get business rules configuration
        
        Returns:
            Business rules configuration dictionary
        """
        return self.get('business_rules', {})
    
    def get_data_quality_config(self) -> Dict[str, Any]:
        """
        Get data quality configuration
        
        Returns:
            Data quality configuration dictionary
        """
        return self.get('data_quality', {})
    
    def get_monitoring_config(self) -> Dict[str, Any]:
        """
        Get monitoring configuration
        
        Returns:
            Monitoring configuration dictionary
        """
        return self.get('monitoring', {})
    
    def reload_config(self) -> None:
        """Reload configuration from file"""
        if self._config_path:
            self.load_config(self._config_path)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Get full configuration as dictionary
        
        Returns:
            Complete configuration dictionary
        """
        return self._config.copy()
    
    def __getitem__(self, key: str) -> Any:
        """Allow dictionary-style access to configuration"""
        return self.get(key)
    
    def __contains__(self, key: str) -> bool:
        """Check if configuration key exists"""
        return self.get(key) is not None


# Singleton instance
config = ConfigManager()