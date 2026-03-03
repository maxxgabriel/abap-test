"""
ETL Logger - Singleton logging class
"""
import logging
from datetime import datetime
from typing import List, Optional
from dataclasses import dataclass, field


@dataclass
class LogEntry:
    """Log entry data class"""
    timestamp: datetime
    level: str
    component: str
    message: str
    details: Optional[str] = None


class ETLLogger:
    """Singleton logger for ETL operations"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ETLLogger, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize logger"""
        if self._initialized:
            return
            
        self.logs: List[LogEntry] = []
        self._setup_logging()
        self._initialized = True
    
    def _setup_logging(self):
        """Setup Python logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='[%(asctime)s] %(levelname)s: %(name)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.python_logger = logging.getLogger('ETL')
    
    @classmethod
    def get_instance(cls) -> 'ETLLogger':
        """Get singleton instance"""
        if cls._instance is None:
            cls._instance = ETLLogger()
        return cls._instance
    
    def log_info(
        self, 
        component: str, 
        message: str, 
        details: Optional[str] = None
    ) -> None:
        """Log info message"""
        self._add_log_entry("INFO", component, message, details)
        self.python_logger.info(f"{component} - {message}")
    
    def log_error(
        self, 
        component: str, 
        message: str, 
        details: Optional[str] = None
    ) -> None:
        """Log error message"""
        self._add_log_entry("ERROR", component, message, details)
        self.python_logger.error(f"{component} - {message}")
        if details:
            self.python_logger.error(f"Details: {details}")
    
    def log_warning(
        self, 
        component: str, 
        message: str, 
        details: Optional[str] = None
    ) -> None:
        """Log warning message"""
        self._add_log_entry("WARNING", component, message, details)
        self.python_logger.warning(f"{component} - {message}")
    
    def _add_log_entry(
        self,
        level: str,
        component: str,
        message: str,
        details: Optional[str] = None
    ) -> None:
        """Add log entry to internal list"""
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
        return self.logs.copy()
    
    def clear_logs(self) -> None:
        """Clear all logs"""
        self.logs.clear()