"""
ETL Logger Module
"""
from datetime import datetime
from typing import List, Dict
import logging


class LogEntry:
    """Container for log entries"""
    def __init__(
        self,
        timestamp: datetime,
        level: str,
        component: str,
        message: str,
        details: str = ""
    ):
        self.timestamp = timestamp
        self.level = level
        self.component = component
        self.message = message
        self.details = details


class ETLLogger:
    """Singleton logger for ETL operations"""
    
    _instance = None
    
    @classmethod
    def get_instance(cls) -> 'ETLLogger':
        """Get singleton instance"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def __init__(self):
        if ETLLogger._instance is not None:
            raise Exception("Use get_instance() method")
        
        self.logs: List[LogEntry] = []
        self._setup_logger()
    
    def _setup_logger(self):
        """Setup Python logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='[%(asctime)s] %(levelname)s: %(component)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def log_info(self, component: str, message: str, details: str = ""):
        """Log info message"""
        self._add_log_entry("INFO", component, message, details)
        self.logger.info(f"{component} - {message}")
    
    def log_error(self, component: str, message: str, details: str = ""):
        """Log error message"""
        self._add_log_entry("ERROR", component, message, details)
        self.logger.error(f"{component} - {message} - {details}")
    
    def log_warning(self, component: str, message: str, details: str = ""):
        """Log warning message"""
        self._add_log_entry("WARNING", component, message, details)
        self.logger.warning(f"{component} - {message}")
    
    def _add_log_entry(
        self,
        level: str,
        component: str,
        message: str,
        details: str = ""
    ):
        """Add entry to log list"""
        entry = LogEntry(
            timestamp=datetime.now(),
            level=level,
            component=component,
            message=message,
            details=details
        )
        self.logs.append(entry)
    
    def get_logs(self) -> List[Dict]:
        """Get all log entries as dictionaries"""
        return [
            {
                "timestamp": log.timestamp,
                "level": log.level,
                "component": log.component,
                "message": log.message,
                "details": log.details
            }
            for log in self.logs
        ]
    
    def clear_logs(self):
        """Clear all logs"""
        self.logs.clear()