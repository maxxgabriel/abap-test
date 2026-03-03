"""
ETL Logger Module
Provides structured logging capabilities with different log levels
"""
from typing import Optional, List, Dict
from datetime import datetime
import logging
from enum import Enum


class LogLevel(Enum):
    """Log level enumeration"""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    DEBUG = "DEBUG"


class ETLLogger:
    """
    Singleton logger class for ETL operations
    """
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ETLLogger, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._logs: List[Dict] = []
        self._setup_logger()
        self._initialized = True
    
    def _setup_logger(self):
        """Setup Python logging"""
        self.logger = logging.getLogger("ETL")
        self.logger.setLevel(logging.DEBUG)
        
        # Console handler
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        
        # Formatter
        formatter = logging.Formatter(
            '[%(asctime)s] %(levelname)s: %(component)s - %(message)s'
        )
        handler.setFormatter(formatter)
        
        self.logger.addHandler(handler)
    
    @classmethod
    def get_instance(cls) -> 'ETLLogger':
        """Get singleton instance"""
        return cls()
    
    def log_info(self, component: str, message: str, details: Optional[str] = None):
        """Log info level message"""
        self._add_log_entry(LogLevel.INFO, component, message, details)
    
    def log_error(self, component: str, message: str, details: Optional[str] = None):
        """Log error level message"""
        self._add_log_entry(LogLevel.ERROR, component, message, details)
    
    def log_warning(self, component: str, message: str, details: Optional[str] = None):
        """Log warning level message"""
        self._add_log_entry(LogLevel.WARNING, component, message, details)
    
    def log_debug(self, component: str, message: str, details: Optional[str] = None):
        """Log debug level message"""
        self._add_log_entry(LogLevel.DEBUG, component, message, details)
    
    def _add_log_entry(
        self, 
        level: LogLevel, 
        component: str, 
        message: str, 
        details: Optional[str] = None
    ):
        """Add log entry to internal log and Python logger"""
        timestamp = datetime.now()
        
        log_entry = {
            "timestamp": timestamp,
            "level": level.value,
            "component": component,
            "message": message,
            "details": details or ""
        }
        
        self._logs.append(log_entry)
        
        # Log to Python logger
        log_message = f"{component} - {message}"
        if details:
            log_message += f" | Details: {details}"
        
        extra = {"component": component}
        
        if level == LogLevel.INFO:
            self.logger.info(log_message, extra=extra)
        elif level == LogLevel.WARNING:
            self.logger.warning(log_message, extra=extra)
        elif level == LogLevel.ERROR:
            self.logger.error(log_message, extra=extra)
        elif level == LogLevel.DEBUG:
            self.logger.debug(log_message, extra=extra)
    
    def get_logs(self) -> List[Dict]:
        """Get all logged entries"""
        return self._logs.copy()
    
    def clear_logs(self):
        """Clear internal log cache"""
        self._logs.clear()