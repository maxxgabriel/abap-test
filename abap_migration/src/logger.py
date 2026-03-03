"""
PySpark ETL Logger Utility - Singleton logger with in-memory storage
Provides structured logging for ETL processes with component-level tracking
"""

import logging
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict
from threading import Lock


@dataclass
class LogEntry:
    """Structured log entry for ETL processes"""
    timestamp: str
    level: str
    component: str
    message: str
    details: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """Convert log entry to dictionary"""
        return asdict(self)


class ETLLogger:
    """
    Singleton logger class for ETL process logging with in-memory storage.
    Thread-safe implementation using double-checked locking pattern.
    """
    
    _instance: Optional['ETLLogger'] = None
    _lock: Lock = Lock()
    
    def __new__(cls):
        """Singleton pattern implementation with thread safety"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ETLLogger, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize logger with in-memory storage"""
        if self._initialized:
            return
            
        self._logs: List[LogEntry] = []
        self._logger = logging.getLogger('ETLLogger')
        self._logger.setLevel(logging.DEBUG)
        
        # Configure console handler with formatting
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        
        formatter = logging.Formatter(
            '[%(asctime)s] %(levelname)s: %(component)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        
        self._logger.addHandler(console_handler)
        self._logger.propagate = False
        
        self._initialized = True
    
    @classmethod
    def get_instance(cls) -> 'ETLLogger':
        """
        Get singleton instance of ETL Logger
        
        Returns:
            ETLLogger: Singleton logger instance
        """
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def _add_log_entry(
        self,
        level: str,
        component: str,
        message: str,
        details: Optional[str] = None
    ) -> None:
        """
        Add log entry to in-memory storage and write to logger
        
        Args:
            level: Log level (INFO, WARNING, ERROR)
            component: Component name generating the log
            message: Log message
            details: Optional additional details
        """
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
        log_entry = LogEntry(
            timestamp=timestamp,
            level=level,
            component=component,
            message=message,
            details=details
        )
        
        # Add to in-memory storage
        self._logs.append(log_entry)
        
        # Write to Python logger
        log_message = f"{component} - {message}"
        if details:
            log_message += f" | Details: {details}"
        
        extra = {'component': component}
        
        if level == 'INFO':
            self._logger.info(log_message, extra=extra)
        elif level == 'WARNING':
            self._logger.warning(log_message, extra=extra)
        elif level == 'ERROR':
            self._logger.error(log_message, extra=extra)
        elif level == 'DEBUG':
            self._logger.debug(log_message, extra=extra)
    
    def log_info(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ) -> None:
        """
        Log INFO level message
        
        Args:
            component: Component name (e.g., 'EXTRACTOR', 'TRANSFORMER')
            message: Log message
            details: Optional additional details
        """
        self._add_log_entry('INFO', component, message, details)
    
    def log_warning(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ) -> None:
        """
        Log WARNING level message
        
        Args:
            component: Component name
            message: Log message
            details: Optional additional details
        """
        self._add_log_entry('WARNING', component, message, details)
    
    def log_error(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ) -> None:
        """
        Log ERROR level message
        
        Args:
            component: Component name
            message: Log message
            details: Optional additional details
        """
        self._add_log_entry('ERROR', component, message, details)
    
    def log_debug(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ) -> None:
        """
        Log DEBUG level message
        
        Args:
            component: Component name
            message: Log message
            details: Optional additional details
        """
        self._add_log_entry('DEBUG', component, message, details)
    
    def get_logs(self) -> List[Dict]:
        """
        Retrieve all log entries from in-memory storage
        
        Returns:
            List of log entries as dictionaries
        """
        return [log.to_dict() for log in self._logs]
    
    def get_logs_by_level(self, level: str) -> List[Dict]:
        """
        Retrieve log entries filtered by level
        
        Args:
            level: Log level to filter (INFO, WARNING, ERROR, DEBUG)
            
        Returns:
            List of filtered log entries
        """
        return [
            log.to_dict() 
            for log in self._logs 
            if log.level == level.upper()
        ]
    
    def get_logs_by_component(self, component: str) -> List[Dict]:
        """
        Retrieve log entries filtered by component
        
        Args:
            component: Component name to filter
            
        Returns:
            List of filtered log entries
        """
        return [
            log.to_dict() 
            for log in self._logs 
            if log.component == component.upper()
        ]
    
    def get_error_count(self) -> int:
        """
        Get count of error log entries
        
        Returns:
            Number of error logs
        """
        return sum(1 for log in self._logs if log.level == 'ERROR')
    
    def get_warning_count(self) -> int:
        """
        Get count of warning log entries
        
        Returns:
            Number of warning logs
        """
        return sum(1 for log in self._logs if log.level == 'WARNING')
    
    def clear_logs(self) -> None:
        """Clear all log entries from in-memory storage"""
        self._logs.clear()
        self._logger.info("Log storage cleared")
    
    def get_log_summary(self) -> Dict:
        """
        Get summary statistics of logs
        
        Returns:
            Dictionary with log counts by level
        """
        summary = {
            'total': len(self._logs),
            'info': sum(1 for log in self._logs if log.level == 'INFO'),
            'warning': sum(1 for log in self._logs if log.level == 'WARNING'),
            'error': sum(1 for log in self._logs if log.level == 'ERROR'),
            'debug': sum(1 for log in self._logs if log.level == 'DEBUG')
        }
        return summary
    
    def export_logs_to_dict(self) -> Dict:
        """
        Export all logs with summary
        
        Returns:
            Dictionary containing logs and summary
        """
        return {
            'summary': self.get_log_summary(),
            'logs': self.get_logs()
        }


# Convenience function for getting logger instance
def get_logger() -> ETLLogger:
    """
    Get singleton ETL logger instance
    
    Returns:
        ETLLogger instance
    """
    return ETLLogger.get_instance()