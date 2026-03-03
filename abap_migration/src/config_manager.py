"""
Configuration Manager Module
Centralized configuration management for ETL framework
"""

import os
import yaml
from typing import Any, Dict, Optional
from pathlib import Path
from pyspark.sql import SparkSession


class ConfigManager:
    """Singleton configuration manager for ETL framework"""
    
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
        """Load configuration from YAML file"""
        if config_path is None:
            config_path = os.environ.get('ETL_CONFIG_PATH', 'config.yaml')
        
        config_file = Path(config_path)
        
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_file, 'r') as f:
            self._config = yaml.safe_load(f)
        
        # Resolve environment variables
        self._resolve_env_vars(self._config)
    
    def _resolve_env_vars(self, config: Dict[str, Any]) -> None:
        """Recursively resolve environment variables in configuration"""
        for key, value in config.items():
            if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
                env_var = value[2:-1]
                config[key] = os.environ.get(env_var, value)
            elif isinstance(value, dict):
                self._resolve_env_vars(value)
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value by dot-notation path
        
        Args:
            key_path: Dot-separated path (e.g., 'database.jdbc_url')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key_path.split('.')
        value = self._config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value
    
    def get_section(self, section: str) -> Dict[str, Any]:
        """Get entire configuration section"""
        return self._config.get(section, {})
    
    def set(self, key_path: str, value: Any) -> None:
        """Set configuration value (runtime only)"""
        keys = key_path.split('.')
        config = self._config
        
        for key in keys[:-1]:
            if key not in config:
                config[key] = {}
            config = config[key]
        
        config[keys[-1]] = value
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration"""
        return self.get_section('database')
    
    def get_source_config(self) -> Dict[str, Any]:
        """Get source configuration"""
        return self.get_section('source')
    
    def get_target_config(self) -> Dict[str, Any]:
        """Get target configuration"""
        return self.get_section('target')
    
    def get_spark_config(self) -> Dict[str, str]:
        """Get Spark-specific configuration"""
        db_config = self.get_database_config()
        
        return {
            'spark.app.name': 'ETL Framework',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.driver.memory': '4g',
            'spark.executor.memory': '4g',
            'spark.sql.shuffle.partitions': '200'
        }
    
    def is_feature_enabled(self, feature: str) -> bool:
        """Check if a feature flag is enabled"""
        return self.get(f'features.{feature}', False)
    
    def get_batch_size(self) -> int:
        """Get configured batch size"""
        return self.get('target.batch_size', 1000)
    
    def get_log_level(self) -> str:
        """Get logging level"""
        return self.get('logging.level', 'INFO')
    
    def reload(self) -> None:
        """Reload configuration from file"""
        self._config = {}
        self.load_config()


# Global configuration instance
config = ConfigManager()