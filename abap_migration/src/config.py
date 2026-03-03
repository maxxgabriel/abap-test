"""
Configuration Management Module
"""
import yaml
from typing import Any, Dict
from pathlib import Path


class Config:
    """Configuration manager for ETL process"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize configuration
        
        Args:
            config_path: Path to configuration file
        """
        self.config_path = config_path
        self.config: Dict[str, Any] = {}
        self._load_config()
    
    def _load_config(self):
        """Load configuration from YAML file"""
        try:
            with open(self.config_path, 'r') as f:
                self.config = yaml.safe_load(f)
        except FileNotFoundError:
            # Use default configuration
            self.config = self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration values"""
        return {
            "SOURCE_PATH": "/data/source/etl_source_data",
            "TARGET_PATH": "/data/target/etl_target_data",
            "STAGING_PATH": "/data/staging/etl_staging",
            "RUN_LOG_PATH": "/data/logs/etl_run_log",
            "SOURCE_FORMAT": "delta",
            "TARGET_FORMAT": "delta",
            "BATCH_SIZE": 1000,
            "MAX_RETRIES": 3,
            "ENABLE_RECONCILIATION": True,
            "ENABLE_ZORDER": True,
            "LOG_RETENTION_DAYS": 90,
            "PREMIUM_MULTIPLIER": 1.5,
            "SPARK_CONFIG": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true"
            }
        }
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value
        
        Args:
            key: Configuration key
            default: Default value if key not found
        
        Returns:
            Configuration value
        """
        return self.config.get(key, default)
    
    def get_source_path(self) -> str:
        """Get source data path"""
        return self.config.get("SOURCE_PATH")
    
    def get_target_path(self) -> str:
        """Get target data path"""
        return self.config.get("TARGET_PATH")
    
    def get_spark_config(self) -> Dict[str, str]:
        """Get Spark configuration dictionary"""
        return self.config.get("SPARK_CONFIG", {})