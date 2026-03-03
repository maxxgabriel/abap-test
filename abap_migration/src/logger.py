"""
ETL Logger Module
Centralized logging for ETL operations.
"""

from datetime import datetime
from typing import Optional, List
from dataclasses import dataclass, field
import logging


@dataclass
class LogEntry:
    """Represents a single log entry."""
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
        self.logs: List[LogEntry] = []
        self._setup_logging()
    
    @classmethod
    def get_instance(cls) -> 'ETLLogger':
        """Get singleton instance of logger."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def _setup_logging(self):
        """Setup Python logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='[%(asctime)s] %(levelname)s: %(name)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.python_logger = logging.getLogger('ETL')
    
    def log_info(self, component: str, message: str, details: Optional[str] = None):
        """Log info level message."""
        self._add_log_entry('INFO', component, message, details)
        self.python_logger.info(f"{component} - {message}")
    
    def log_error(self, component: str, message: str, details: Optional[str] = None):
        """Log error level message."""
        self._add_log_entry('ERROR', component, message, details)
        self.python_logger.error(f"{component} - {message}")
        if details:
            self.python_logger.error(f"  Details: {details}")
    
    def log_warning(self, component: str, message: str, details: Optional[str] = None):
        """Log warning level message."""
        self._add_log_entry('WARNING', component, message, details)
        self.python_logger.warning(f"{component} - {message}")
    
    def _add_log_entry(
        self,
        level: str,
        component: str,
        message: str,
        details: Optional[str] = None
    ):
        """Add log entry to internal log storage."""
        entry = LogEntry(
            timestamp=datetime.now(),
            level=level,
            component=component,
            message=message,
            details=details
        )
        self.logs.append(entry)
    
    def get_logs(self) -> List[LogEntry]:
        """Retrieve all log entries."""
        return self.logs.copy()
    
    def clear_logs(self):
        """Clear all log entries."""
        self.logs.clear()
    
    def get_error_count(self) -> int:
        """Get count of error log entries."""
        return sum(1 for log in self.logs if log.level == 'ERROR')
    
    def get_warning_count(self) -> int:
        """Get count of warning log entries."""
        return sum(1 for log in self.logs if log.level == 'WARNING')