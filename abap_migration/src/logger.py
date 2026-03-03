"""
ETL Logging Module
Provides centralized logging capabilities for ETL processes
"""
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging


class LogLevel(Enum):
    """Log level enumeration"""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    DEBUG = "DEBUG"


@dataclass
class LogEntry:
    """Log entry structure"""
    timestamp: datetime
    level: LogLevel
    component: str
    message: str
    details: Optional[str] = None
    run_id: Optional[str] = None


class ETLLogger:
    """Singleton logger for ETL processes"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self._initialized = True
        self.logs: List[LogEntry] = []
        self.logger = logging.getLogger('ETLLogger')
        
        # Configure Python logger
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '[%(asctime)s] %(levelname)s: %(name)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    @classmethod
    def get_instance(cls) -> 'ETLLogger':
        """Get logger singleton instance"""
        return cls()
    
    def log_info(self, component: str, message: str, details: str = None, run_id: str = None) -> None:
        """Log info message"""
        entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            component=component,
            message=message,
            details=details,
            run_id=run_id
        )
        self._add_log_entry(entry)
        self.logger.info(f"{component}: {message}")
        
    def log_error(self, component: str, message: str, details: str = None, run_id: str = None) -> None:
        """Log error message"""
        entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.ERROR,
            component=component,
            message=message,
            details=details,
            run_id=run_id
        )
        self._add_log_entry(entry)
        self.logger.error(f"{component}: {message}")
        if details:
            self.logger.error(f"  Details: {details}")
            
    def log_warning(self, component: str, message: str, details: str = None, run_id: str = None) -> None:
        """Log warning message"""
        entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.WARNING,
            component=component,
            message=message,
            details=details,
            run_id=run_id
        )
        self._add_log_entry(entry)
        self.logger.warning(f"{component}: {message}")
        
    def log_debug(self, component: str, message: str, details: str = None, run_id: str = None) -> None:
        """Log debug message"""
        entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.DEBUG,
            component=component,
            message=message,
            details=details,
            run_id=run_id
        )
        self._add_log_entry(entry)
        self.logger.debug(f"{component}: {message}")
    
    def _add_log_entry(self, entry: LogEntry) -> None:
        """Add log entry to internal storage"""
        self.logs.append(entry)
        
    def get_logs(self, level: Optional[LogLevel] = None, component: Optional[str] = None) -> List[LogEntry]:
        """Get logs with optional filtering"""
        filtered_logs = self.logs
        
        if level:
            filtered_logs = [log for log in filtered_logs if log.level == level]
            
        if component:
            filtered_logs = [log for log in filtered_logs if log.component == component]
            
        return filtered_logs
        
    def get_logs_as_dict(self) -> List[Dict]:
        """Get logs as dictionary list"""
        return [
            {
                'timestamp': log.timestamp.isoformat(),
                'level': log.level.value,
                'component': log.component,
                'message': log.message,
                'details': log.details,
                'run_id': log.run_id
            }
            for log in self.logs
        ]
        
    def clear_logs(self) -> None:
        """Clear all logs"""
        self.logs.clear()
        self.logger.info("Logs cleared")
        
    def get_error_count(self) -> int:
        """Get count of error logs"""
        return len([log for log in self.logs if log.level == LogLevel.ERROR])
        
    def get_warning_count(self) -> int:
        """Get count of warning logs"""
        return len([log for log in self.logs if log.level == LogLevel.WARNING])