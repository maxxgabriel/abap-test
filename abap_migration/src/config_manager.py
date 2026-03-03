"""
Configuration Manager for ETL Framework
Provides centralized configuration management with validation and environment variable support.
"""

import os
import yaml
from typing import Any, Dict, Optional
from pathlib import Path
import logging


class ConfigurationError(Exception):
    """Custom exception for configuration errors."""
    pass


class ConfigManager:
    """
    Centralized configuration management for ETL framework.
    
    Supports:
    - YAML configuration files
    - Environment variable overrides
    - Configuration validation
    - Nested configuration access
    """
    
    _instance: Optional['ConfigManager'] = None
    _config: Dict[str, Any] = {}
    
    def __new__(cls, config_path: Optional[str] = None):
        """Singleton pattern implementation."""
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Path to configuration file
        """
        if self._initialized:
            return
            
        if config_path is None:
            config_path = os.getenv('ETL_CONFIG_PATH', 'config.yaml')
        
        self._config_path = Path(config_path)
        self._load_config()
        self._apply_env_overrides()
        self._validate_config()
        self._initialized = True
    
    def _load_config(self) -> None:
        """Load configuration from YAML file."""
        try:
            if not self._config_path.exists():
                raise ConfigurationError(f"Configuration file not found: {self._config_path}")
            
            with open(self._config_path, 'r') as f:
                self._config = yaml.safe_load(f)
            
            if not self._config:
                raise ConfigurationError("Configuration file is empty")
                
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Error parsing configuration file: {e}")
        except Exception as e:
            raise ConfigurationError(f"Error loading configuration: {e}")
    
    def _apply_env_overrides(self) -> None:
        """Apply environment variable overrides to configuration."""
        # Database passwords from environment
        if 'database' in self._config:
            if 'source' in self._config['database']:
                password_env = self._config['database']['source'].get('password_env')
                if password_env:
                    self._config['database']['source']['password'] = os.getenv(password_env, '')
            
            if 'target' in self._config['database']:
                password_env = self._config['database']['target'].get('password_env')
                if password_env:
                    self._config['database']['target']['password'] = os.getenv(password_env, '')
        
        # SMTP credentials from environment
        if 'monitoring' in self._config and 'alerts' in self._config['monitoring']:
            alerts = self._config['monitoring']['alerts']
            smtp_user_env = alerts.get('smtp_user_env')
            smtp_pass_env = alerts.get('smtp_password_env')
            
            if smtp_user_env:
                alerts['smtp_user'] = os.getenv(smtp_user_env, '')
            if smtp_pass_env:
                alerts['smtp_password'] = os.getenv(smtp_pass_env, '')
        
        # Override with ETL_* environment variables
        etl_env_vars = {k: v for k, v in os.environ.items() if k.startswith('ETL_')}
        for key, value in etl_env_vars.items():
            config_key = key[4:].lower().replace('_', '.')
            self._set_nested_value(config_key, value)
    
    def _validate_config(self) -> None:
        """Validate required configuration parameters."""
        required_keys = [
            'spark',
            'database',
            'logging',
            'etl'
        ]
        
        for key in required_keys:
            if key not in self._config:
                raise ConfigurationError(f"Missing required configuration key: {key}")
        
        # Validate database configuration
        if 'source' not in self._config['database'] or 'target' not in self._config['database']:
            raise ConfigurationError("Database source and target configuration required")
        
        # Validate critical paths
        self._ensure_directories()
    
    def _ensure_directories(self) -> None:
        """Ensure required directories exist."""
        if 'paths' in self._config:
            for path_key, path_value in self._config['paths'].items():
                path = Path(path_value)
                path.mkdir(parents=True, exist_ok=True)
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key (supports dot notation).
        
        Args:
            key: Configuration key (e.g., 'spark.app_name' or 'database.source.url')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def _set_nested_value(self, key: str, value: Any) -> None:
        """Set nested configuration value using dot notation."""
        keys = key.split('.')
        config = self._config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
    
    def get_spark_config(self) -> Dict[str, Any]:
        """Get Spark configuration."""
        return self.get('spark', {})
    
    def get_database_config(self, db_type: str = 'source') -> Dict[str, Any]:
        """
        Get database configuration.
        
        Args:
            db_type: 'source' or 'target'
            
        Returns:
            Database configuration dictionary
        """
        return self.get(f'database.{db_type}', {})
    
    def get_etl_config(self) -> Dict[str, Any]:
        """Get ETL configuration."""
        return self.get('etl', {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration."""
        return self.get('logging', {})
    
    def get_monitoring_config(self) -> Dict[str, Any]:
        """Get monitoring configuration."""
        return self.get('monitoring', {})
    
    def get_data_quality_config(self) -> Dict[str, Any]:
        """Get data quality configuration."""
        return self.get('data_quality', {})
    
    def get_business_rules_config(self) -> Dict[str, Any]:
        """Get business rules configuration."""
        return self.get('business_rules', {})
    
    def reload(self) -> None:
        """Reload configuration from file."""
        self._load_config()
        self._apply_env_overrides()
        self._validate_config()
    
    def __getitem__(self, key: str) -> Any:
        """Allow dictionary-style access."""
        return self.get(key)
    
    def __contains__(self, key: str) -> bool:
        """Check if key exists in configuration."""
        return self.get(key) is not None


# Singleton instance
config = ConfigManager()