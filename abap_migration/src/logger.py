"""
Standardized logging utilities for ETL framework.
Provides structured logging with multiple handlers and formatters.
"""

import logging
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional
from pyspark.sql import SparkSession

from src.config_manager import config


class ETLLogger:
    """
    ETL framework logger with multiple output handlers.
    Supports console, file, and database logging.
    """
    
    _loggers = {}
    
    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        """
        Get or create logger instance.
        
        Args:
            name: Logger name (typically module name)
            
        Returns:
            Configured logger instance
        """
        if name in ETLLogger._loggers:
            return ETLLogger._loggers[name]
        
        logger = logging.getLogger(name)
        
        # Get logging configuration
        log_config = config.get_logging_config()
        level = log_config.get('level', 'INFO')
        logger.setLevel(getattr(logging, level))
        
        # Remove existing handlers to avoid duplicates
        logger.handlers.clear()
        
        # Create formatter
        log_format = log_config.get('format', 
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        formatter = logging.Formatter(log_format)
        
        # Console handler
        if log_config.get('console', {}).get('enabled', True):
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        
        # File handler
        file_config = log_config.get('file', {})
        if file_config.get('enabled', True):
            log_file = file_config.get('path', 'logs/etl_framework.log')
            
            # Ensure log directory exists
            Path(log_file).parent.mkdir(parents=True, exist_ok=True)
            
            max_bytes = file_config.get('max_bytes', 10485760)  # 10MB
            backup_count = file_config.get('backup_count', 5)
            
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=max_bytes,
                backupCount=backup_count
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        # Prevent propagation to root logger
        logger.propagate = False
        
        ETLLogger._loggers[name] = logger
        return logger
    
    @staticmethod
    def log_to_database(
        spark: SparkSession,
        run_id: str,
        level: str,
        component: str,
        message: str,
        details: Optional[str] = None
    ) -> None:
        """
        Log message to database table.
        
        Args:
            spark: SparkSession instance
            run_id: ETL run identifier
            level: Log level (INFO, WARNING, ERROR)
            component: Component name
            message: Log message
            details: Additional details
        """
        log_config = config.get_logging_config()
        
        if not log_config.get('database', {}).get('enabled', False):
            return
        
        try:
            from pyspark.sql.types import StructType, StructField, StringType, TimestampType
            
            schema = StructType([
                StructField("run_id", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("level", StringType(), False),
                StructField("component", StringType(), False),
                StructField("message", StringType(), False),
                StructField("details", StringType(), True)
            ])
            
            log_data = [(
                run_id,
                datetime.now(),
                level,
                component,
                message,
                details
            )]
            
            log_df = spark.createDataFrame(log_data, schema)
            
            # Get database configuration
            db_config = config.get_database_config('target')
            table_name = log_config.get('database', {}).get('table', 'etl_run_log')
            
            log_df.write \
                .format("jdbc") \
                .option("url", db_config['jdbc_url']) \
                .option("dbtable", table_name) \
                .option("user", db_config['user']) \
                .option("password", db_config.get('password', '')) \
                .mode("append") \
                .save()
                
        except Exception as e:
            # Fallback to console logging if database logging fails
            logger = ETLLogger.get_logger(__name__)
            logger.warning(f"Failed to log to database: {str(e)}")


class ComponentLogger:
    """
    Component-specific logger wrapper with context.
    Automatically adds component name to all log messages.
    """
    
    def __init__(self, component_name: str, run_id: Optional[str] = None):
        """
        Initialize component logger.
        
        Args:
            component_name: Name of the component
            run_id: Optional run identifier for correlation
        """
        self.component_name = component_name
        self.run_id = run_id
        self.logger = ETLLogger.get_logger(component_name)
        self.spark: Optional[SparkSession] = None
    
    def set_spark(self, spark: SparkSession) -> None:
        """Set SparkSession for database logging."""
        self.spark = spark
    
    def _format_message(self, message: str) -> str:
        """Format message with component context."""
        if self.run_id:
            return f"[{self.component_name}] [{self.run_id}] {message}"
        return f"[{self.component_name}] {message}"
    
    def info(self, message: str, details: Optional[str] = None) -> None:
        """Log info message."""
        formatted_msg = self._format_message(message)
        self.logger.info(formatted_msg)
        
        if self.spark and self.run_id:
            ETLLogger.log_to_database(
                self.spark, self.run_id, 'INFO', 
                self.component_name, message, details
            )
    
    def warning(self, message: str, details: Optional[str] = None) -> None:
        """Log warning message."""
        formatted_msg = self._format_message(message)
        self.logger.warning(formatted_msg)
        
        if self.spark and self.run_id:
            ETLLogger.log_to_database(
                self.spark, self.run_id, 'WARNING',
                self.component_name, message, details
            )
    
    def error(self, message: str, details: Optional[str] = None, 
              exception: Optional[Exception] = None) -> None:
        """Log error message."""
        formatted_msg = self._format_message(message)
        
        if exception:
            self.logger.error(formatted_msg, exc_info=True)
            if details is None:
                details = str(exception)
        else:
            self.logger.error(formatted_msg)
        
        if self.spark and self.run_id:
            ETLLogger.log_to_database(
                self.spark, self.run_id, 'ERROR',
                self.component_name, message, details
            )
    
    def debug(self, message: str) -> None:
        """Log debug message."""
        formatted_msg = self._format_message(message)
        self.logger.debug(formatted_msg)
    
    def exception(self, message: str) -> None:
        """Log exception with traceback."""
        formatted_msg = self._format_message(message)
        self.logger.exception(formatted_msg)