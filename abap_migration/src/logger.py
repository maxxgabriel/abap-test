"""
ETL Logger Module
Centralized logging with multiple levels and component tracking.
"""
from typing import List, Optional
from dataclasses import dataclass
from datetime import datetime
import logging


@dataclass
class LogEntry:
    """Single log entry."""
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
    def get_instance(cls) -> "ETLLogger":
        """Get singleton instance."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def _setup_logging(self):
        """Setup Python logging."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.python_logger = logging.getLogger("ETL")
    
    def log_info(self, component: str, message: str, details: str = None):
        """Log info message."""
        self._add_log_entry("INFO", component, message, details)
        self.python_logger.info(f"[{component}] {message}")
    
    def log_error(self, component: str, message: str, details: str = None):
        """Log error message."""
        self._add_log_entry("ERROR", component, message, details)
        self.python_logger.error(f"[{component}] {message} - {details}")
    
    def log_warning(self, component: str, message: str, details: str = None):
        """Log warning message."""
        self._add_log_entry("WARNING", component, message, details)
        self.python_logger.warning(f"[{component}] {message}")
    
    def _add_log_entry(
        self,
        level: str,
        component: str,
        message: str,
        details: Optional[str] = None
    ):
        """Add log entry to internal log list."""
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