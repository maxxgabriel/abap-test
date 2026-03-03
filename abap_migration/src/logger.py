"""
ETL Logging Module
Provides centralized logging functionality for the ETL process.
"""

import logging
from typing import Optional, List, Dict
from datetime import datetime
from dataclasses import dataclass, field


@dataclass
class LogEntry:
    """Represents a single log entry."""
    timestamp: datetime
    level: str
    component: str
    message: str
    details: Optional[str] = None


class ETLLogger:
    """
    Singleton logger for ETL operations.
    """
    _instance = None
    
    def __init__(self):
        """Initialize the logger."""
        if ETLLogger._instance is not None:
            raise RuntimeError("ETLLogger is a singleton. Use get_instance() instead.")
        
        self.logs: List[LogEntry] = []
        self.logger = logging.getLogger("ETL")
        self.logger.setLevel(logging.INFO)
        
        # Console handler
        if not self.logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
    
    @classmethod
    def get_instance(cls) -> 'ETLLogger':
        """
        Get the singleton instance of ETLLogger.
        
        Returns:
            ETLLogger: The singleton instance
        """
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def log_info(self, component: str, message: str, details: Optional[str] = None):
        """
        Log an info message.
        
        Args:
            component: Component name
            message: Log message
            details: Optional details
        """
        self._add_log_entry("INFO", component, message, details)
        self.logger.info(f"{component}: {message}")
        if details:
            self.logger.info(f"  Details: {details}")
    
    def log_error(self, component: str, message: str, details: Optional[str] = None):
        """
        Log an error message.
        
        Args:
            component: Component name
            message: Log message
            details: Optional details
        """
        self._add_log_entry("ERROR", component, message, details)
        self.logger.error(f"{component}: {message}")
        if details:
            self.logger.error(f"  Details: {details}")
    
    def log_warning(self, component: str, message: str, details: Optional[str] = None):
        """
        Log a warning message.
        
        Args:
            component: Component name
            message: Log message
            details: Optional details
        """
        self._add_log_entry("WARNING", component, message, details)
        self.logger.warning(f"{component}: {message}")
        if details:
            self.logger.warning(f"  Details: {details}")
    
    def _add_log_entry(self, level: str, component: str, message: str, 
                      details: Optional[str] = None):
        """
        Add a log entry to the internal log list.
        
        Args:
            level: Log level
            component: Component name
            message: Log message
            details: Optional details
        """
        entry = LogEntry(
            timestamp=datetime.now(),
            level=level,
            component=component,
            message=message,
            details=details
        )
        self.logs.append(entry)
    
    def get_logs(self) -> List[LogEntry]:
        """
        Get all log entries.
        
        Returns:
            List[LogEntry]: All log entries
        """
        return self.logs.copy()
    
    def clear_logs(self):
        """Clear all log entries."""
        self.logs.clear()