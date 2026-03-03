"""
Logging Utility for ETL Framework
Provides standardized logging with multiple handlers and formatters.
"""

import logging
import logging.config
from typing import Optional
from pathlib import Path
from datetime import datetime
from .config_manager import config


class ETLLogger:
    """
    Centralized logging utility for ETL framework.
    
    Features:
    - Multiple log levels
    - File and console handlers
    - JSON formatting support
    - Component-based logging
    - Error tracking
    """
    
    _instance: Optional['ETLLogger'] = None
    _loggers: dict = {}
    
    def __new__(cls):
        """Singleton pattern implementation."""
        if cls._instance is None:
            cls._instance = super(ETLLogger, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize logging configuration."""
        if self._initialized:
            return
        
        self._setup_logging()
        self._initialized = True
    
    def _setup_logging(self) -> None:
        """Setup logging configuration from config."""
        logging_config = config.get_logging_config()
        
        # Ensure log directory exists
        log_dir = Path('logs')
        log_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            logging.config.dictConfig(logging_config)
        except Exception as e:
            # Fallback to basic configuration
            logging.basicConfig(
                level=logging.INFO,
                format='[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            logging.error(f"Error configuring logging: {e}")
    
    def get_logger(self, component: str = 'etl_framework') -> logging.Logger:
        """
        Get or create logger for component.
        
        Args:
            component: Component name for the logger
            
        Returns:
            Logger instance
        """
        if component not in self._loggers:
            self._loggers[component] = logging.getLogger(f'etl_framework.{component}')
        
        return self._loggers[component]
    
    def log_info(self, component: str, message: str, details: Optional[str] = None) -> None:
        """
        Log info message.
        
        Args:
            component: Component name
            message: Log message
            details: Optional additional details
        """
        logger = self.get_logger(component)
        log_msg = message
        if details:
            log_msg = f"{message} | Details: {details}"
        logger.info(log_msg)
    
    def log_error(self, component: str, message: str, details: Optional[str] = None, 
                  exception: Optional[Exception] = None) -> None:
        """
        Log error message.
        
        Args:
            component: Component name
            message: Log message
            details: Optional additional details
            exception: Optional exception object
        """
        logger = self.get_logger(component)
        log_msg = message
        if details:
            log_msg = f"{message} | Details: {details}"
        
        if exception:
            logger.error(log_msg, exc_info=True)
        else:
            logger.error(log_msg)
    
    def log_warning(self, component: str, message: str, details: Optional[str] = None) -> None:
        """
        Log warning message.
        
        Args:
            component: Component name
            message: Log message
            details: Optional additional details
        """
        logger = self.get_logger(component)
        log_msg = message
        if details:
            log_msg = f"{message} | Details: {details}"
        logger.warning(log_msg)
    
    def log_debug(self, component: str, message: str, details: Optional[str] = None) -> None:
        """
        Log debug message.
        
        Args:
            component: Component name
            message: Log message
            details: Optional additional details
        """
        logger = self.get_logger(component)
        log_msg = message
        if details:
            log_msg = f"{message} | Details: {details}"
        logger.debug(log_msg)
    
    def log_metrics(self, component: str, metrics: dict) -> None:
        """
        Log metrics data.
        
        Args:
            component: Component name
            metrics: Dictionary of metrics
        """
        logger = self.get_logger(component)
        metrics_str = " | ".join([f"{k}={v}" for k, v in metrics.items()])
        logger.info(f"METRICS: {metrics_str}")
    
    def create_run_log(self, run_id: str) -> logging.Logger:
        """
        Create dedicated logger for specific ETL run.
        
        Args:
            run_id: ETL run identifier
            
        Returns:
            Logger instance for the run
        """
        run_logger = logging.getLogger(f'etl_framework.run.{run_id}')
        
        # Add file handler for this specific run
        log_dir = Path('logs') / 'runs'
        log_dir.mkdir(parents=True, exist_ok=True)
        
        log_file = log_dir / f'{run_id}.log'
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        
        formatter = logging.Formatter(
            '[%(asctime)s] %(levelname)s [%(name)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        
        run_logger.addHandler(file_handler)
        run_logger.setLevel(logging.DEBUG)
        
        return run_logger


# Singleton instance
logger = ETLLogger()