"""
ETL Logger - Centralized logging functionality
"""
from typing import Optional
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum


class LogLevel(Enum):
    """Log levels"""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


@dataclass
class LogEntry:
    """Single log entry"""
    timestamp: datetime
    level: str
    component: str
    message: str
    details: Optional[str] = None


class ETLLogger:
    """Centralized logger for ETL processes"""
    
    def __init__(self):
        self.logs: list[LogEntry] = []
    
    def log_info(self, component: str, message: str, details: Optional[str] = None):
        """Log info message"""
        self._add_log(LogLevel.INFO, component, message, details)
    
    def log_warning(self, component: str, message: str, details: Optional[str] = None):
        """Log warning message"""
        self._add_log(LogLevel.WARNING, component, message, details)
    
    def log_error(self, component: str, message: str, details: Optional[str] = None):
        """Log error message"""
        self._add_log(LogLevel.ERROR, component, message, details)
    
    def _add_log(
        self,
        level: LogLevel,
        component: str,
        message: str,
        details: Optional[str]
    ):
        """Add log entry"""
        entry = LogEntry(
            timestamp=datetime.now(),
            level=level.value,
            component=component,
            message=message,
            details=details
        )
        self.logs.append(entry)
        print(f"[{entry.timestamp}] {entry.level}: {entry.component} - {entry.message}")
        if details:
            print(f"  Details: {details}")
    
    def get_logs(self) -> list[LogEntry]:
        """Get all logs"""
        return self.logs
    
    def clear_logs(self):
        """Clear all logs"""
        self.logs.clear()