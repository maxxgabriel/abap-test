"""
PySpark ETL Logger Module
Migrated from ABAP zcl_etl_logger
Singleton pattern for logging
"""
import logging
from datetime import datetime
from typing import Optional, List, Dict
from dataclasses import dataclass, field


@dataclass
class LogEntry:
    """Log entry structure"""
    timestamp: datetime
    level: str
    component: str
    message: str
    details: Optional[str] = None


class ETLLogger:
    """Singleton logger for ETL operations"""
    
    _instance = None
    
    def __init__(self):
        if ETLLogger._instance is not None:
            raise Exception("Use get_instance() to get logger instance")
        
        self.logs: List[LogEntry] = []
        
        # Configure Python logging
        logging.basicConfig(
            level=logging.INFO,
            format='[%(asctime)s] %(levelname)s: %(name)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger('ETL')
    
    @classmethod
    def get_instance(cls):
        """Get singleton instance"""
        if cls._instance is None:
            cls._instance = ETLLogger()
        return cls._instance
    
    def log_info(self, component: str, message: str, details: Optional[str] = None):
        """Log info message"""
        self._add_log_entry("INFO", component, message, details)
        self.logger.info(f"{component} - {message}")
        if details:
            self.logger.info(f"  Details: {details}")
    
    def log_error(self, component: str, message: str, details: Optional[str] = None):
        """Log error message"""
        self._add_log_entry("ERROR", component, message, details)
        self.logger.error(f"{component} - {message}")
        if details:
            self.logger.error(f"  Details: {details}")
    
    def log_warning(self, component: str, message: str, details: Optional[str] = None):
        """Log warning message"""
        self._add_log_entry("WARNING", component, message, details)
        self.logger.warning(f"{component} - {message}")
        if details:
            self.logger.warning(f"  Details: {details}")
    
    def _add_log_entry(self, level: str, component: str, message: str, details: Optional[str] = None):
        """Add log entry to internal storage"""
        entry = LogEntry(
            timestamp=datetime.now(),
            level=level,
            component=component,
            message=message,
            details=details
        )
        self.logs.append(entry)
    
    def get_logs(self) -> List[LogEntry]:
        """Get all log entries"""
        return self.logs
    
    def clear_logs(self):
        """Clear all log entries"""
        self.logs.clear()