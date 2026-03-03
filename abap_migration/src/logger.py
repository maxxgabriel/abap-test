"""
ETL Logger Module
Provides centralized logging functionality for ETL pipeline.
"""

import logging
from typing import Optional, List, Dict
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


class ETLLogger:
    """
    Singleton logger for ETL operations.
    Replaces ABAP zcl_etl_logger with Python logging.
    """
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ETLLogger, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize logger."""
        if self._initialized:
            return
        
        self.logger = logging.getLogger("ETL")
        self.log_entries = []
        self._initialized = True
    
    @classmethod
    def get_instance(cls) -> 'ETLLogger':
        """Get singleton instance."""
        return cls()
    
    def log_info(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ) -> None:
        """Log info level message."""
        self._add_log_entry("INFO", component, message, details)
    
    def log_error(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ) -> None:
        """Log error level message."""
        self._add_log_entry("ERROR", component, message, details)
    
    def log_warning(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ) -> None:
        """Log warning level message."""
        self._add_log_entry("WARNING", component, message, details)
    
    def _add_log_entry(
        self,
        level: str,
        component: str,
        message: str,
        details: Optional[str] = None
    ) -> None:
        """Add log entry to collection."""
        log_entry = {
            "timestamp": datetime.now(),
            "level": level,
            "component": component,
            "message": message,
            "details": details or ""
        }
        
        self.log_entries.append(log_entry)
        
        # Also log to Python logger
        log_msg = f"[{component}] {message}"
        if details:
            log_msg += f" - {details}"
        
        if level == "INFO":
            self.logger.info(log_msg)
        elif level == "ERROR":
            self.logger.error(log_msg)
        elif level == "WARNING":
            self.logger.warning(log_msg)
    
    def get_logs(self) -> List[Dict]:
        """Get all log entries."""
        return self.log_entries
    
    def clear_logs(self) -> None:
        """Clear log entries."""
        self.log_entries = []
    
    def export_logs(self, spark: SparkSession, output_path: str) -> None:
        """Export logs to Parquet."""
        if not self.log_entries:
            return
        
        schema = StructType([
            StructField("timestamp", TimestampType(), False),
            StructField("level", StringType(), False),
            StructField("component", StringType(), False),
            StructField("message", StringType(), False),
            StructField("details", StringType(), True)
        ])
        
        logs_df = spark.createDataFrame(self.log_entries, schema)
        logs_df.write.mode("append").parquet(output_path)