"""
Configuration management for ETL Shared Framework.
Loads configuration from YAML files and environment variables.
"""

import os
import yaml
from typing import Any, Dict, Optional
from pathlib import Path
from dataclasses import dataclass, field


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    type: str
    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str
    connection_pool: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    format: str = "json"
    console_output: bool = True
    file_output: bool = True
    log_dir: str = "/var/log/etl"
    max_file_size_mb: int = 100
    backup_count: int = 10
    components: list = field(default_factory=list)


@dataclass
class SparkConfig:
    """Spark configuration."""
    app_name: str = "ETL_Shared_Framework"
    master: str = "local[*]"
    config: Dict[str, str] = field(default_factory=dict)


@dataclass
class ETLConfig:
    """ETL processing configuration."""
    batch_size: int = 1000
    max_retries: int = 3
    retry_delay_seconds: int = 5
    enable_reconciliation: bool = True
    enable_data_quality: bool = True
    max_records_per_run: int = 0


@dataclass
class MonitoringConfig:
    """Monitoring configuration."""
    enable_metrics: bool = True
    metrics_port: int = 9090
    dashboard_refresh_seconds: int = 30
    health_check_interval_seconds: int = 60
    alert_email: str = ""
    alert_thresholds: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataQualityConfig:
    """Data quality configuration."""
    enable_profiling: bool = True
    enable_anomaly_detection: bool = True
    completeness_threshold: float = 95.0
    uniqueness_threshold: float = 100.0
    validity_threshold: float = 98.0
    consistency_threshold: float = 95.0


class ConfigManager:
    """
    Centralized configuration manager.
    Loads configuration from YAML files with environment variable substitution.
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Path to configuration file. If None, uses default path.
        """
        if config_path is None:
            config_path = os.getenv("ETL_CONFIG_PATH", "config.yaml")
        
        self.config_path = Path(config_path)
        self._config: Dict[str, Any] = {}
        self._load_config()

    def _load_config(self) -> None:
        """Load configuration from YAML file."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        with open(self.config_path, 'r') as f:
            content = f.read()
            # Substitute environment variables
            content = self._substitute_env_vars(content)
            self._config = yaml.safe_load(content)

    def _substitute_env_vars(self, content: str) -> str:
        """
        Substitute environment variables in configuration content.
        
        Args:
            content: YAML content as string
            
        Returns:
            Content with environment variables substituted
        """
        import re
        pattern = r'\$\{([^}]+)\}'
        
        def replace_env_var(match):
            env_var = match.group(1)
            return os.getenv(env_var, match.group(0))
        
        return re.sub(pattern, replace_env_var, content)

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key.
        
        Args:
            key: Configuration key (supports dot notation, e.g., 'database.source.host')
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

    def get_database_config(self, db_type: str = "source") -> DatabaseConfig:
        """
        Get database configuration.
        
        Args:
            db_type: Type of database ('source' or 'target')
            
        Returns:
            DatabaseConfig object
        """
        db_config = self.get(f"database.{db_type}", {})
        return DatabaseConfig(
            type=db_config.get("type", "postgresql"),
            host=db_config.get("host", "localhost"),
            port=db_config.get("port", 5432),
            database=db_config.get("database", ""),
            user=db_config.get("user", ""),
            password=db_config.get("password", ""),
            schema=db_config.get("schema", "public"),
            connection_pool=db_config.get("connection_pool", {})
        )

    def get_logging_config(self) -> LoggingConfig:
        """Get logging configuration."""
        log_config = self.get("logging", {})
        return LoggingConfig(
            level=log_config.get("level", "INFO"),
            format=log_config.get("format", "json"),
            console_output=log_config.get("console_output", True),
            file_output=log_config.get("file_output", True),
            log_dir=log_config.get("log_dir", "/var/log/etl"),
            max_file_size_mb=log_config.get("max_file_size_mb", 100),
            backup_count=log_config.get("backup_count", 10),
            components=log_config.get("components", [])
        )

    def get_spark_config(self) -> SparkConfig:
        """Get Spark configuration."""
        spark_config = self.get("spark", {})
        return SparkConfig(
            app_name=spark_config.get("app_name", "ETL_Shared_Framework"),
            master=spark_config.get("master", "local[*]"),
            config=spark_config.get("config", {})
        )

    def get_etl_config(self) -> ETLConfig:
        """Get ETL configuration."""
        etl_config = self.get("etl", {})
        return ETLConfig(
            batch_size=etl_config.get("batch_size", 1000),
            max_retries=etl_config.get("max_retries", 3),
            retry_delay_seconds=etl_config.get("retry_delay_seconds", 5),
            enable_reconciliation=etl_config.get("enable_reconciliation", True),
            enable_data_quality=etl_config.get("enable_data_quality", True),
            max_records_per_run=etl_config.get("max_records_per_run", 0)
        )

    def get_monitoring_config(self) -> MonitoringConfig:
        """Get monitoring configuration."""
        mon_config = self.get("monitoring", {})
        return MonitoringConfig(
            enable_metrics=mon_config.get("enable_metrics", True),
            metrics_port=mon_config.get("metrics_port", 9090),
            dashboard_refresh_seconds=mon_config.get("dashboard_refresh_seconds", 30),
            health_check_interval_seconds=mon_config.get("health_check_interval_seconds", 60),
            alert_email=mon_config.get("alert_email", ""),
            alert_thresholds=mon_config.get("alert_thresholds", {})
        )

    def get_data_quality_config(self) -> DataQualityConfig:
        """Get data quality configuration."""
        dq_config = self.get("data_quality", {})
        return DataQualityConfig(
            enable_profiling=dq_config.get("enable_profiling", True),
            enable_anomaly_detection=dq_config.get("enable_anomaly_detection", True),
            completeness_threshold=dq_config.get("completeness_threshold", 95.0),
            uniqueness_threshold=dq_config.get("uniqueness_threshold", 100.0),
            validity_threshold=dq_config.get("validity_threshold", 98.0),
            consistency_threshold=dq_config.get("consistency_threshold", 95.0)
        )

    def reload(self) -> None:
        """Reload configuration from file."""
        self._load_config()

    def to_dict(self) -> Dict[str, Any]:
        """
        Get complete configuration as dictionary.
        
        Returns:
            Configuration dictionary
        """
        return self._config.copy()


# Singleton instance
_config_manager: Optional[ConfigManager] = None


def get_config_manager(config_path: Optional[str] = None) -> ConfigManager:
    """
    Get singleton ConfigManager instance.
    
    Args:
        config_path: Path to configuration file (only used on first call)
        
    Returns:
        ConfigManager instance
    """
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager(config_path)
    return _config_manager