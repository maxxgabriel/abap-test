"""
Logging utility for ETL processes.
"""

import logging
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


class ETLLogger:
    """Singleton logger for ETL operations."""
    
    _instance: Optional['ETLLogger'] = None
    
    def __new__(cls, spark: Optional[SparkSession] = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, spark: Optional[SparkSession] = None):
        if not hasattr(self, 'initialized'):
            self.spark = spark
            self.logger = logging.getLogger('ETL')
            self.logger.setLevel(logging.INFO)
            
            # Console handler
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '[%(asctime)s] %(levelname)s: %(component)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            
            self.initialized = True
    
    @classmethod
    def get_instance(cls, spark: Optional[SparkSession] = None) -> 'ETLLogger':
        """Get singleton logger instance."""
        if cls._instance is None:
            cls._instance = cls(spark)
        return cls._instance
    
    def log_info(self, component: str, message: str, details: str = ""):
        """Log info message."""
        self.logger.info(message, extra={'component': component})
        if details:
            self.logger.info(f"  Details: {details}", extra={'component': component})
    
    def log_error(self, component: str, message: str, details: str = ""):
        """Log error message."""
        self.logger.error(message, extra={'component': component})
        if details:
            self.logger.error(f"  Details: {details}", extra={'component': component})
    
    def log_warning(self, component: str, message: str, details: str = ""):
        """Log warning message."""
        self.logger.warning(message, extra={'component': component})
        if details:
            self.logger.warning(f"  Details: {details}", extra={'component': component})