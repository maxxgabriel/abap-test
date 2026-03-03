"""
ETL Logging Module
Provides centralized logging functionality for ETL processes.
"""

import logging
from datetime import datetime
from typing import List, Optional
from dataclasses import dataclass, field


@dataclass
class LogEntry:
    """Represents a log entry."""
    timestamp: datetime
    level: str
    component: str
    message: str
    details: Optional[str] = None


class ETLLogger:
    """Singleton logger for ETL operations."""
    
    _instance = None
    
    def __init__(self):
        """Initialize logger."""
        if ETLLogger._instance is not None:
            raise Exception("ETLLogger is a singleton. Use get_instance().")
        
        self.logs: List[LogEntry] = []
        self._setup_logger()
        ETLLogger._instance = self
    
    @staticmethod
    def get_instance():
        """Get singleton instance of ETLLogger."""
        if ETLLogger._instance is None:
            ETLLogger()
        return ETLLogger._instance
    
    def _setup_logger(self):
        """Setup Python logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.python_logger = logging.getLogger('ETL')
    
    def log_info(self, component: str, message: str, details: str = None):
        """Log an info message."""
        self._add_log_entry("INFO", component, message, details)
        self.python_logger.info(f"[{component}] {message}")
        if details:
            self.python_logger.info(f"  Details: {details}")
    
    def log_error(self, component: str, message: str, details: str = None):
        """Log an error message."""
        self._add_log_entry("ERROR", component, message, details)
        self.python_logger.error(f"[{component}] {message}")
        if details:
            self.python_logger.error(f"  Details: {details}")
    
    def log_warning(self, component: str, message: str, details: str = None):
        """Log a warning message."""
        self._add_log_entry("WARNING", component, message, details)
        self.python_logger.warning(f"[{component}] {message}")
        if details:
            self.python_logger.warning(f"  Details: {details}")
    
    def _add_log_entry(self, level: str, component: str, message: str, details: str = None):
        """Add entry to internal log list."""
        entry = LogEntry(
            timestamp=datetime.now(),
            level=level,
            component=component,
            message=message,
            details=details
        )
        self.logs.append(entry)
    
    def get_logs(self) -> List[LogEntry]:
        """Get all log entries."""
        return self.logs.copy()
    
    def clear_logs(self):
        """Clear all log entries."""
        self.logs.clear()