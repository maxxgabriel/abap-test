"""
Centralized Logging Framework
Provides consistent logging across all ETL components.
"""

import logging
import logging.handlers
import os
from datetime import datetime
from pathlib import Path
from typing import Optional
from src.config_manager import config


class ETLLogger:
    """Centralized logger for ETL framework."""
    
    _loggers = {}
    _initialized = False
    
    @classmethod
    def get_logger(cls, component: str) -> logging.Logger:
        """
        Get or create logger for a component.
        
        Args:
            component: Component name (e.g., 'EXTRACTOR', 'TRANSFORMER')
            
        Returns:
            Configured logger instance
        """
        if not cls._initialized:
            cls._setup_logging()
        
        if component not in cls._loggers:
            cls._loggers[component] = cls._create_logger(component)
        
        return cls._loggers[component]
    
    @classmethod
    def _setup_logging(cls) -> None:
        """Setup base logging configuration."""
        log_config = config.get_logging_config()
        
        # Create logs directory if needed
        if log_config.get('file', {}).get('enabled', False):
            log_path = Path(log_config.get('file', {}).get('path', 'logs'))
            log_path.mkdir(parents=True, exist_ok=True)
        
        cls._initialized = True
    
    @classmethod
    def _create_logger(cls, component: str) -> logging.Logger:
        """
        Create and configure logger for component.
        
        Args:
            component: Component name
            
        Returns:
            Configured logger
        """
        log_config = config.get_logging_config()
        
        logger = logging.getLogger(f"ETL.{component}")
        logger.setLevel(getattr(logging, log_config.get('level', 'INFO')))
        
        # Remove existing handlers
        logger.handlers.clear()
        
        # Create formatter
        formatter = logging.Formatter(
            fmt=log_config.get('format'),
            datefmt=log_config.get('date_format')
        )
        
        # Console handler
        if log_config.get('console', {}).get('enabled', True):
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        
        # File handler
        if log_config.get('file', {}).get('enabled', False):
            file_config = log_config['file']
            log_path = Path(file_config.get('path', 'logs'))
            log_file = log_path / f"{component.lower()}_{datetime.now().strftime('%Y%m%d')}.log"
            
            file_handler = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=file_config.get('max_bytes', 10485760),
                backupCount=file_config.get('backup_count', 5)
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        return logger
    
    @classmethod
    def log_info(cls, component: str, message: str, **kwargs) -> None:
        """
        Log info message.
        
        Args:
            component: Component name
            message: Log message
            **kwargs: Additional key-value pairs to log
        """
        logger = cls.get_logger(component)
        extra_info = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
        full_message = f"{message} | {extra_info}" if extra_info else message
        logger.info(full_message)
    
    @classmethod
    def log_error(cls, component: str, message: str, error: Optional[Exception] = None, **kwargs) -> None:
        """
        Log error message.
        
        Args:
            component: Component name
            message: Log message
            error: Exception object if available
            **kwargs: Additional key-value pairs to log
        """
        logger = cls.get_logger(component)
        extra_info = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
        full_message = f"{message} | {extra_info}" if extra_info else message
        
        if error:
            logger.error(full_message, exc_info=error)
        else:
            logger.error(full_message)
    
    @classmethod
    def log_warning(cls, component: str, message: str, **kwargs) -> None:
        """
        Log warning message.
        
        Args:
            component: Component name
            message: Log message
            **kwargs: Additional key-value pairs to log
        """
        logger = cls.get_logger(component)
        extra_info = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
        full_message = f"{message} | {extra_info}" if extra_info else message
        logger.warning(full_message)
    
    @classmethod
    def log_debug(cls, component: str, message: str, **kwargs) -> None:
        """
        Log debug message.
        
        Args:
            component: Component name
            message: Log message
            **kwargs: Additional key-value pairs to log
        """
        logger = cls.get_logger(component)
        extra_info = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
        full_message = f"{message} | {extra_info}" if extra_info else message
        logger.debug(full_message)


# Convenience functions
def get_logger(component: str) -> logging.Logger:
    """Get logger for component."""
    return ETLLogger.get_logger(component)


def log_info(component: str, message: str, **kwargs) -> None:
    """Log info message."""
    ETLLogger.log_info(component, message, **kwargs)


def log_error(component: str, message: str, error: Optional[Exception] = None, **kwargs) -> None:
    """Log error message."""
    ETLLogger.log_error(component, message, error, **kwargs)


def log_warning(component: str, message: str, **kwargs) -> None:
    """Log warning message."""
    ETLLogger.log_warning(component, message, **kwargs)


def log_debug(component: str, message: str, **kwargs) -> None:
    """Log debug message."""
    ETLLogger.log_debug(component, message, **kwargs)