"""
Centralized logging framework for ETL processes.
Provides structured logging with multiple output formats and handlers.
"""

import logging
import json
import sys
from datetime import datetime
from typing import Any, Dict, Optional
from pathlib import Path
from logging.handlers import RotatingFileHandler
from enum import Enum

from src.config import get_config_manager, LoggingConfig


class LogLevel(Enum):
    """Log level enumeration."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON.
        
        Args:
            record: Log record to format
            
        Returns:
            JSON-formatted log string
        """
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "component": getattr(record, "component", "UNKNOWN"),
            "message": record.getMessage(),
            "logger": record.name,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add extra fields
        if hasattr(record, "extra_fields"):
            log_data.update(record.extra_fields)

        return json.dumps(log_data)


class ETLLogger:
    """
    Centralized logger for ETL processes.
    Provides structured logging with component tracking and multiple output formats.
    """

    def __init__(
        self,
        name: str,
        component: str,
        config: Optional[LoggingConfig] = None
    ):
        """
        Initialize ETL logger.
        
        Args:
            name: Logger name
            component: Component name (e.g., 'EXTRACTOR', 'TRANSFORMER')
            config: Logging configuration (if None, loads from config manager)
        """
        self.name = name
        self.component = component
        
        if config is None:
            config_manager = get_config_manager()
            config = config_manager.get_logging_config()
        
        self.config = config
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, config.level.upper()))
        
        # Prevent duplicate handlers
        if not self.logger.handlers:
            self._setup_handlers()

    def _setup_handlers(self) -> None:
        """Setup log handlers based on configuration."""
        # Console handler
        if self.config.console_output:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(getattr(logging, self.config.level.upper()))
            
            if self.config.format == "json":
                console_handler.setFormatter(JSONFormatter())
            else:
                console_handler.setFormatter(
                    logging.Formatter(
                        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                    )
                )
            
            self.logger.addHandler(console_handler)

        # File handler
        if self.config.file_output:
            log_dir = Path(self.config.log_dir)
            log_dir.mkdir(parents=True, exist_ok=True)
            
            log_file = log_dir / f"{self.component.lower()}.log"
            
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=self.config.max_file_size_mb * 1024 * 1024,
                backupCount=self.config.backup_count
            )
            file_handler.setLevel(getattr(logging, self.config.level.upper()))
            
            if self.config.format == "json":
                file_handler.setFormatter(JSONFormatter())
            else:
                file_handler.setFormatter(
                    logging.Formatter(
                        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                    )
                )
            
            self.logger.addHandler(file_handler)

    def _log(
        self,
        level: LogLevel,
        message: str,
        details: Optional[str] = None,
        extra_fields: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Internal logging method.
        
        Args:
            level: Log level
            message: Log message
            details: Additional details
            extra_fields: Extra fields to include in log
        """
        extra = {
            "component": self.component,
            "extra_fields": extra_fields or {}
        }
        
        if details:
            extra["extra_fields"]["details"] = details

        log_method = getattr(self.logger, level.value.lower())
        log_method(message, extra=extra)

    def debug(
        self,
        message: str,
        details: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log debug message."""
        self._log(LogLevel.DEBUG, message, details, kwargs)

    def info(
        self,
        message: str,
        details: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log info message."""
        self._log(LogLevel.INFO, message, details, kwargs)

    def warning(
        self,
        message: str,
        details: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log warning message."""
        self._log(LogLevel.WARNING, message, details, kwargs)

    def error(
        self,
        message: str,
        details: Optional[str] = None,
        exception: Optional[Exception] = None,
        **kwargs
    ) -> None:
        """
        Log error message.
        
        Args:
            message: Error message
            details: Additional details
            exception: Exception object
            **kwargs: Extra fields
        """
        if exception:
            kwargs["exception_type"] = type(exception).__name__
            kwargs["exception_message"] = str(exception)
        
        self._log(LogLevel.ERROR, message, details, kwargs)
        
        if exception:
            self.logger.exception("Exception details:", exc_info=exception)

    def critical(
        self,
        message: str,
        details: Optional[str] = None,
        exception: Optional[Exception] = None,
        **kwargs
    ) -> None:
        """
        Log critical message.
        
        Args:
            message: Critical message
            details: Additional details
            exception: Exception object
            **kwargs: Extra fields
        """
        if exception:
            kwargs["exception_type"] = type(exception).__name__
            kwargs["exception_message"] = str(exception)
        
        self._log(LogLevel.CRITICAL, message, details, kwargs)
        
        if exception:
            self.logger.exception("Exception details:", exc_info=exception)

    def log_execution_time(
        self,
        operation: str,
        start_time: datetime,
        end_time: datetime,
        **kwargs
    ) -> None:
        """
        Log execution time for an operation.
        
        Args:
            operation: Operation name
            start_time: Start timestamp
            end_time: End timestamp
            **kwargs: Extra fields
        """
        duration = (end_time - start_time).total_seconds()
        self.info(
            f"Execution time for {operation}",
            duration_seconds=duration,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            **kwargs
        )

    def log_metrics(
        self,
        operation: str,
        metrics: Dict[str, Any]
    ) -> None:
        """
        Log performance metrics.
        
        Args:
            operation: Operation name
            metrics: Metrics dictionary
        """
        self.info(
            f"Metrics for {operation}",
            **metrics
        )


class LoggerFactory:
    """Factory for creating ETL loggers."""

    _loggers: Dict[str, ETLLogger] = {}

    @classmethod
    def get_logger(
        cls,
        component: str,
        config: Optional[LoggingConfig] = None
    ) -> ETLLogger:
        """
        Get or create logger for component.
        
        Args:
            component: Component name
            config: Optional logging configuration
            
        Returns:
            ETLLogger instance
        """
        if component not in cls._loggers:
            logger_name = f"etl.{component.lower()}"
            cls._loggers[component] = ETLLogger(
                name=logger_name,
                component=component,
                config=config
            )
        
        return cls._loggers[component]

    @classmethod
    def clear_loggers(cls) -> None:
        """Clear all cached loggers."""
        cls._loggers.clear()