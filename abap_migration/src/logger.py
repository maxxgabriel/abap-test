"""
Centralized Logging Framework for ETL

Provides structured logging with multiple outputs, component-based filtering,
and performance metrics tracking.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any
from logging.handlers import RotatingFileHandler
from enum import Enum

from src.config_manager import config


class LogLevel(Enum):
    """Log level enumeration"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class ETLLogger:
    """
    Centralized logging framework for ETL operations.
    
    Provides structured logging with component-based filtering,
    multiple output handlers, and performance tracking.
    """
    
    _instance: Optional['ETLLogger'] = None
    _loggers: Dict[str, logging.Logger] = {}
    
    def __new__(cls):
        """Singleton pattern to ensure single logger instance"""
        if cls._instance is None:
            cls._instance = super(ETLLogger, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self) -> None:
        """Initialize logging framework"""
        self._log_config = config.get_logging_config()
        self._log_dir = Path(self._log_config.get('log_dir', 'logs'))
        self._log_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize root logger
        self._setup_root_logger()
    
    def _setup_root_logger(self) -> None:
        """Configure root logger with handlers"""
        root_logger = logging.getLogger('etl')
        root_logger.setLevel(getattr(logging, self._log_config.get('level', 'INFO')))
        root_logger.handlers.clear()
        
        # Console handler
        if self._log_config.get('console_enabled', True):
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(self._get_formatter())
            root_logger.addHandler(console_handler)
        
        # File handler
        if self._log_config.get('file_enabled', True):
            log_file = self._log_dir / f"etl_{datetime.now().strftime('%Y%m%d')}.log"
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=self._log_config.get('max_file_size', 10485760),
                backupCount=self._log_config.get('backup_count', 10)
            )
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(self._get_formatter())
            root_logger.addHandler(file_handler)
        
        self._loggers['root'] = root_logger
    
    def _get_formatter(self) -> logging.Formatter:
        """
        Create log formatter
        
        Returns:
            Configured log formatter
        """
        log_format = self._log_config.get(
            'format',
            '[%(asctime)s] [%(levelname)s] [%(component)s] %(message)s'
        )
        date_format = self._log_config.get('date_format', '%Y-%m-%d %H:%M:%S')
        return logging.Formatter(log_format, datefmt=date_format)
    
    def get_logger(self, component: str) -> logging.Logger:
        """
        Get or create logger for specific component
        
        Args:
            component: Component name (e.g., 'EXTRACTOR', 'TRANSFORMER')
            
        Returns:
            Logger instance for component
        """
        if component not in self._loggers:
            logger = logging.getLogger(f'etl.{component}')
            self._loggers[component] = logger
        return self._loggers[component]
    
    def log(self, level: str, component: str, message: str, 
            details: Optional[str] = None, **kwargs) -> None:
        """
        Log message with component context
        
        Args:
            level: Log level (INFO, WARNING, ERROR, etc.)
            component: Component name
            message: Log message
            details: Additional details
            **kwargs: Additional context data
        """
        logger = self.get_logger(component)
        
        # Add component to extra data
        extra = {'component': component}
        extra.update(kwargs)
        
        # Format message with details
        full_message = message
        if details:
            full_message = f"{message} | Details: {details}"
        
        # Log at appropriate level
        log_method = getattr(logger, level.lower(), logger.info)
        log_method(full_message, extra=extra)
    
    def info(self, component: str, message: str, details: Optional[str] = None, 
             **kwargs) -> None:
        """Log info message"""
        self.log('INFO', component, message, details, **kwargs)
    
    def warning(self, component: str, message: str, details: Optional[str] = None,
                **kwargs) -> None:
        """Log warning message"""
        self.log('WARNING', component, message, details, **kwargs)
    
    def error(self, component: str, message: str, details: Optional[str] = None,
              exception: Optional[Exception] = None, **kwargs) -> None:
        """
        Log error message
        
        Args:
            component: Component name
            message: Error message
            details: Additional details
            exception: Exception object if available
            **kwargs: Additional context
        """
        if exception:
            details = f"{details or ''} | Exception: {str(exception)}"
        self.log('ERROR', component, message, details, **kwargs)
    
    def critical(self, component: str, message: str, details: Optional[str] = None,
                 **kwargs) -> None:
        """Log critical message"""
        self.log('CRITICAL', component, message, details, **kwargs)
    
    def debug(self, component: str, message: str, details: Optional[str] = None,
              **kwargs) -> None:
        """Log debug message"""
        self.log('DEBUG', component, message, details, **kwargs)
    
    def log_performance(self, component: str, operation: str, 
                       duration: float, records: int = 0, **kwargs) -> None:
        """
        Log performance metrics
        
        Args:
            component: Component name
            operation: Operation name
            duration: Duration in seconds
            records: Number of records processed
            **kwargs: Additional metrics
        """
        throughput = records / duration if duration > 0 else 0
        message = f"{operation} completed in {duration:.2f}s"
        
        if records > 0:
            message += f" | Records: {records} | Throughput: {throughput:.2f} rec/s"
        
        self.info(component, message, **kwargs)
    
    def log_etl_start(self, run_id: str, etl_type: str, **kwargs) -> None:
        """
        Log ETL process start
        
        Args:
            run_id: ETL run identifier
            etl_type: Type of ETL process
            **kwargs: Additional context
        """
        self.info(
            'ORCHESTRATOR',
            f"ETL process started | Run ID: {run_id} | Type: {etl_type}",
            **kwargs
        )
    
    def log_etl_end(self, run_id: str, status: str, duration: float,
                    records_extracted: int, records_loaded: int, 
                    errors: int = 0, **kwargs) -> None:
        """
        Log ETL process completion
        
        Args:
            run_id: ETL run identifier
            status: Final status
            duration: Total duration
            records_extracted: Number of records extracted
            records_loaded: Number of records loaded
            errors: Number of errors
            **kwargs: Additional context
        """
        message = (
            f"ETL process completed | Run ID: {run_id} | Status: {status} | "
            f"Duration: {duration:.2f}s | Extracted: {records_extracted} | "
            f"Loaded: {records_loaded} | Errors: {errors}"
        )
        
        if status == 'SUCCESS':
            self.info('ORCHESTRATOR', message, **kwargs)
        elif errors > 0:
            self.warning('ORCHESTRATOR', message, **kwargs)
        else:
            self.error('ORCHESTRATOR', message, **kwargs)
    
    def get_log_file_path(self, date: Optional[datetime] = None) -> Path:
        """
        Get log file path for specific date
        
        Args:
            date: Date for log file (defaults to today)
            
        Returns:
            Path to log file
        """
        if date is None:
            date = datetime.now()
        return self._log_dir / f"etl_{date.strftime('%Y%m%d')}.log"


# Singleton instance
logger = ETLLogger()