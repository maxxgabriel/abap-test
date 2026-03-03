"""
ETL Logger utility class with singleton pattern, in-memory storage,
timestamp formatting, and log level management.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field
from enum import Enum
from threading import Lock


class LogLevel(Enum):
    """Log level enumeration"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class LogEntry:
    """Log entry data structure"""
    timestamp: datetime
    level: str
    component: str
    message: str
    details: Optional[str] = None
    run_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert log entry to dictionary"""
        return {
            'timestamp': self.timestamp.isoformat(),
            'level': self.level,
            'component': self.component,
            'message': self.message,
            'details': self.details,
            'run_id': self.run_id
        }
    
    def format_message(self) -> str:
        """Format log entry as string"""
        timestamp_str = self.timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        base_msg = f"[{timestamp_str}] {self.level}: {self.component} - {self.message}"
        if self.details:
            base_msg += f"\n  Details: {self.details}"
        if self.run_id:
            base_msg += f"\n  Run ID: {self.run_id}"
        return base_msg


class ETLLogger:
    """
    Singleton logger class for centralized ETL logging with in-memory storage.
    
    Features:
    - Singleton pattern for centralized logging
    - In-memory log storage
    - Timestamp formatting
    - Log level management
    - Thread-safe operations
    - Console and file output support
    """
    
    _instance: Optional[ETLLogger] = None
    _lock: Lock = Lock()
    
    def __new__(cls) -> ETLLogger:
        """Singleton pattern implementation"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize logger instance"""
        if self._initialized:
            return
            
        self._logs: List[LogEntry] = []
        self._log_level: LogLevel = LogLevel.INFO
        self._console_output: bool = True
        self._file_output: bool = False
        self._log_file_path: Optional[str] = None
        self._max_logs_in_memory: int = 10000
        
        # Configure Python logging
        self._configure_python_logger()
        
        self._initialized = True
    
    def _configure_python_logger(self) -> None:
        """Configure Python's built-in logger"""
        self._python_logger = logging.getLogger('ETLLogger')
        self._python_logger.setLevel(logging.DEBUG)
        
        # Console handler
        if not self._python_logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            console_handler.setFormatter(formatter)
            self._python_logger.addHandler(console_handler)
    
    @classmethod
    def get_instance(cls) -> ETLLogger:
        """
        Get singleton instance of ETLLogger
        
        Returns:
            ETLLogger: Singleton logger instance
        """
        return cls()
    
    def set_log_level(self, level: LogLevel) -> None:
        """
        Set minimum log level
        
        Args:
            level: Log level to set
        """
        self._log_level = level
        self._python_logger.info(f"Log level set to: {level.value}")
    
    def enable_console_output(self, enabled: bool = True) -> None:
        """
        Enable or disable console output
        
        Args:
            enabled: Whether to enable console output
        """
        self._console_output = enabled
    
    def enable_file_output(self, file_path: str) -> None:
        """
        Enable file output for logs
        
        Args:
            file_path: Path to log file
        """
        self._file_output = True
        self._log_file_path = file_path
        
        # Add file handler to Python logger
        file_handler = logging.FileHandler(file_path)
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        self._python_logger.addHandler(file_handler)
    
    def set_max_logs_in_memory(self, max_logs: int) -> None:
        """
        Set maximum number of logs to keep in memory
        
        Args:
            max_logs: Maximum number of logs
        """
        self._max_logs_in_memory = max_logs
        self._trim_logs()
    
    def _add_log_entry(
        self,
        level: str,
        component: str,
        message: str,
        details: Optional[str] = None,
        run_id: Optional[str] = None
    ) -> None:
        """
        Internal method to add log entry
        
        Args:
            level: Log level
            component: Component name
            message: Log message
            details: Optional details
            run_id: Optional run ID
        """
        log_entry = LogEntry(
            timestamp=datetime.now(),
            level=level,
            component=component,
            message=message,
            details=details,
            run_id=run_id
        )
        
        # Add to in-memory storage
        with self._lock:
            self._logs.append(log_entry)
            self._trim_logs()
        
        # Console output
        if self._console_output:
            print(log_entry.format_message())
        
        # Python logger output (for file logging)
        log_method = getattr(self._python_logger, level.lower())
        log_msg = f"{component} - {message}"
        if details:
            log_msg += f" | Details: {details}"
        if run_id:
            log_msg += f" | Run ID: {run_id}"
        log_method(log_msg)
    
    def _trim_logs(self) -> None:
        """Trim logs to maximum size"""
        if len(self._logs) > self._max_logs_in_memory:
            self._logs = self._logs[-self._max_logs_in_memory:]
    
    def _should_log(self, level: LogLevel) -> bool:
        """
        Check if message should be logged based on log level
        
        Args:
            level: Log level to check
            
        Returns:
            bool: Whether to log the message
        """
        level_priority = {
            LogLevel.DEBUG: 0,
            LogLevel.INFO: 1,
            LogLevel.WARNING: 2,
            LogLevel.ERROR: 3,
            LogLevel.CRITICAL: 4
        }
        return level_priority[level] >= level_priority[self._log_level]
    
    def log_debug(
        self,
        component: str,
        message: str,
        details: Optional[str] = None,
        run_id: Optional[str] = None
    ) -> None:
        """
        Log debug message
        
        Args:
            component: Component name
            message: Log message
            details: Optional details
            run_id: Optional run ID
        """
        if self._should_log(LogLevel.DEBUG):
            self._add_log_entry(LogLevel.DEBUG.value, component, message, details, run_id)
    
    def log_info(
        self,
        component: str,
        message: str,
        details: Optional[str] = None,
        run_id: Optional[str] = None
    ) -> None:
        """
        Log info message
        
        Args:
            component: Component name
            message: Log message
            details: Optional details
            run_id: Optional run ID
        """
        if self._should_log(LogLevel.INFO):
            self._add_log_entry(LogLevel.INFO.value, component, message, details, run_id)
    
    def log_warning(
        self,
        component: str,
        message: str,
        details: Optional[str] = None,
        run_id: Optional[str] = None
    ) -> None:
        """
        Log warning message
        
        Args:
            component: Component name
            message: Log message
            details: Optional details
            run_id: Optional run ID
        """
        if self._should_log(LogLevel.WARNING):
            self._add_log_entry(LogLevel.WARNING.value, component, message, details, run_id)
    
    def log_error(
        self,
        component: str,
        message: str,
        details: Optional[str] = None,
        run_id: Optional[str] = None
    ) -> None:
        """
        Log error message
        
        Args:
            component: Component name
            message: Log message
            details: Optional details
            run_id: Optional run ID
        """
        if self._should_log(LogLevel.ERROR):
            self._add_log_entry(LogLevel.ERROR.value, component, message, details, run_id)
    
    def log_critical(
        self,
        component: str,
        message: str,
        details: Optional[str] = None,
        run_id: Optional[str] = None
    ) -> None:
        """
        Log critical message
        
        Args:
            component: Component name
            message: Log message
            details: Optional details
            run_id: Optional run ID
        """
        if self._should_log(LogLevel.CRITICAL):
            self._add_log_entry(LogLevel.CRITICAL.value, component, message, details, run_id)
    
    def get_logs(
        self,
        level: Optional[str] = None,
        component: Optional[str] = None,
        run_id: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[LogEntry]:
        """
        Get logs with optional filtering
        
        Args:
            level: Filter by log level
            component: Filter by component
            run_id: Filter by run ID
            limit: Maximum number of logs to return
            
        Returns:
            List of log entries
        """
        with self._lock:
            filtered_logs = self._logs.copy()
        
        # Apply filters
        if level:
            filtered_logs = [log for log in filtered_logs if log.level == level]
        if component:
            filtered_logs = [log for log in filtered_logs if log.component == component]
        if run_id:
            filtered_logs = [log for log in filtered_logs if log.run_id == run_id]
        
        # Apply limit
        if limit:
            filtered_logs = filtered_logs[-limit:]
        
        return filtered_logs
    
    def get_logs_as_dict(
        self,
        level: Optional[str] = None,
        component: Optional[str] = None,
        run_id: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get logs as dictionaries
        
        Args:
            level: Filter by log level
            component: Filter by component
            run_id: Filter by run ID
            limit: Maximum number of logs to return
            
        Returns:
            List of log dictionaries
        """
        logs = self.get_logs(level, component, run_id, limit)
        return [log.to_dict() for log in logs]
    
    def clear_logs(self) -> None:
        """Clear all logs from memory"""
        with self._lock:
            self._logs.clear()
        self._python_logger.info("Logs cleared from memory")
    
    def get_log_count(self) -> int:
        """
        Get total number of logs in memory
        
        Returns:
            Number of logs
        """
        with self._lock:
            return len(self._logs)
    
    def get_log_summary(self) -> Dict[str, int]:
        """
        Get summary of logs by level
        
        Returns:
            Dictionary with counts per level
        """
        summary = {
            'DEBUG': 0,
            'INFO': 0,
            'WARNING': 0,
            'ERROR': 0,
            'CRITICAL': 0
        }
        
        with self._lock:
            for log in self._logs:
                if log.level in summary:
                    summary[log.level] += 1
        
        return summary
    
    def export_logs_to_file(self, file_path: str, format: str = 'text') -> None:
        """
        Export logs to file
        
        Args:
            file_path: Path to export file
            format: Export format ('text' or 'json')
        """
        with self._lock:
            logs = self._logs.copy()
        
        if format == 'json':
            import json
            with open(file_path, 'w') as f:
                json.dump([log.to_dict() for log in logs], f, indent=2)
        else:
            with open(file_path, 'w') as f:
                for log in logs:
                    f.write(log.format_message() + '\n')
        
        self._python_logger.info(f"Logs exported to {file_path}")


# Convenience functions for direct access
def get_logger() -> ETLLogger:
    """Get logger instance"""
    return ETLLogger.get_instance()


def log_info(component: str, message: str, details: Optional[str] = None, run_id: Optional[str] = None) -> None:
    """Log info message"""
    ETLLogger.get_instance().log_info(component, message, details, run_id)


def log_error(component: str, message: str, details: Optional[str] = None, run_id: Optional[str] = None) -> None:
    """Log error message"""
    ETLLogger.get_instance().log_error(component, message, details, run_id)


def log_warning(component: str, message: str, details: Optional[str] = None, run_id: Optional[str] = None) -> None:
    """Log warning message"""
    ETLLogger.get_instance().log_warning(component, message, details, run_id)


def log_debug(component: str, message: str, details: Optional[str] = None, run_id: Optional[str] = None) -> None:
    """Log debug message"""
    ETLLogger.get_instance().log_debug(component, message, details, run_id)


def log_critical(component: str, message: str, details: Optional[str] = None, run_id: Optional[str] = None) -> None:
    """Log critical message"""
    ETLLogger.get_instance().log_critical(component, message, details, run_id)