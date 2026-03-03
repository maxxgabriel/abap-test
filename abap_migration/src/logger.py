"""
Logging Utility Module
Standardized logging for ETL framework
"""

import logging
import sys
from typing import Optional
from pathlib import Path
from logging.handlers import RotatingFileHandler
from datetime import datetime

from src.config_manager import config


class ETLLogger:
    """Centralized logging utility for ETL processes"""
    
    _loggers = {}
    
    @staticmethod
    def get_logger(component: str, spark_session=None) -> logging.Logger:
        """
        Get or create logger for a component
        
        Args:
            component: Component name (e.g., 'EXTRACTOR', 'TRANSFORMER')
            spark_session: Optional Spark session for Spark logging
            
        Returns:
            Configured logger instance
        """
        if component in ETLLogger._loggers:
            return ETLLogger._loggers[component]
        
        logger = logging.getLogger(component)
        logger.setLevel(getattr(logging, config.get_log_level()))
        
        # Remove existing handlers
        logger.handlers = []
        
        # Create formatter
        log_format = config.get('logging.format',
                               '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        formatter = logging.Formatter(log_format)
        
        # Console handler
        if config.get('logging.console_output', True):
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        
        # File handler
        log_file = config.get('logging.file_path', 'logs/etl_process.log')
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        max_bytes = config.get('logging.max_file_size', 10485760)
        backup_count = config.get('logging.backup_count', 5)
        
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # Spark logging integration
        if spark_session is not None:
            spark_logger = spark_session.sparkContext._jvm.org.apache.log4j.Logger
            spark_logger.getLogger(component).setLevel(
                getattr(spark_session.sparkContext._jvm.org.apache.log4j.Level,
                       config.get_log_level())
            )
        
        ETLLogger._loggers[component] = logger
        return logger
    
    @staticmethod
    def log_info(component: str, message: str, details: Optional[str] = None):
        """Log info message"""
        logger = ETLLogger.get_logger(component)
        full_message = f"{message}"
        if details:
            full_message += f" | Details: {details}"
        logger.info(full_message)
    
    @staticmethod
    def log_error(component: str, message: str, details: Optional[str] = None,
                  exception: Optional[Exception] = None):
        """Log error message"""
        logger = ETLLogger.get_logger(component)
        full_message = f"{message}"
        if details:
            full_message += f" | Details: {details}"
        if exception:
            logger.error(full_message, exc_info=True)
        else:
            logger.error(full_message)
    
    @staticmethod
    def log_warning(component: str, message: str, details: Optional[str] = None):
        """Log warning message"""
        logger = ETLLogger.get_logger(component)
        full_message = f"{message}"
        if details:
            full_message += f" | Details: {details}"
        logger.warning(full_message)
    
    @staticmethod
    def log_debug(component: str, message: str, details: Optional[str] = None):
        """Log debug message"""
        logger = ETLLogger.get_logger(component)
        full_message = f"{message}"
        if details:
            full_message += f" | Details: {details}"
        logger.debug(full_message)
    
    @staticmethod
    def log_execution_time(component: str, operation: str, start_time: datetime):
        """Log execution time for an operation"""
        duration = (datetime.now() - start_time).total_seconds()
        logger = ETLLogger.get_logger(component)
        logger.info(f"{operation} completed in {duration:.2f} seconds")
    
    @staticmethod
    def log_metrics(component: str, metrics: dict):
        """Log performance metrics"""
        logger = ETLLogger.get_logger(component)
        metrics_str = " | ".join([f"{k}: {v}" for k, v in metrics.items()])
        logger.info(f"Metrics: {metrics_str}")