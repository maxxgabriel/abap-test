"""
ETL Logger utility class - Singleton pattern with in-memory storage.
Provides centralized logging with timestamp formatting and log level management.
"""
from datetime import datetime
from typing import List, Dict, Optional
from enum import Enum
import threading


class LogLevel(Enum):
    """Log level enumeration"""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    DEBUG = "DEBUG"


class LogEntry:
    """Represents a single log entry"""
    
    def __init__(
        self,
        level: LogLevel,
        component: str,
        message: str,
        details: Optional[str] = None
    ):
        self.timestamp = datetime.now()
        self.level = level
        self.component = component
        self.message = message
        self.details = details
    
    def to_dict(self) -> Dict:
        """Convert log entry to dictionary"""
        return {
            'timestamp': self.timestamp.isoformat(),
            'level': self.level.value,
            'component': self.component,
            'message': self.message,
            'details': self.details
        }
    
    def __str__(self) -> str:
        """Format log entry as string"""
        formatted = f"[{self.format_timestamp()}] {self.level.value}: {self.component} - {self.message}"
        if self.details:
            formatted += f"\n  Details: {self.details}"
        return formatted
    
    def format_timestamp(self, fmt: str = "%Y-%m-%d %H:%M:%S.%f") -> str:
        """Format timestamp with customizable format"""
        return self.timestamp.strftime(fmt)[:-3]  # Remove microseconds last 3 digits


class ETLLogger:
    """
    Singleton logger class for ETL processes.
    Provides thread-safe logging with in-memory storage.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """Ensure singleton pattern"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ETLLogger, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize logger (only once due to singleton)"""
        if self._initialized:
            return
        
        self._logs: List[LogEntry] = []
        self._log_level = LogLevel.INFO
        self._console_output = True
        self._max_logs = 10000  # Maximum logs to keep in memory
        self._initialized = True
    
    @classmethod
    def get_instance(cls) -> 'ETLLogger':
        """Get singleton instance of logger"""
        return cls()
    
    def set_log_level(self, level: LogLevel) -> None:
        """Set minimum log level to record"""
        self._log_level = level
    
    def enable_console_output(self, enabled: bool = True) -> None:
        """Enable or disable console output"""
        self._console_output = enabled
    
    def set_max_logs(self, max_logs: int) -> None:
        """Set maximum number of logs to keep in memory"""
        self._max_logs = max_logs
    
    def _add_log_entry(
        self,
        level: LogLevel,
        component: str,
        message: str,
        details: Optional[str] = None
    ) -> None:
        """Add log entry to internal storage"""
        entry = LogEntry(level, component, message, details)
        
        with self._lock:
            self._logs.append(entry)
            
            # Trim logs if exceeding max size
            if len(self._logs) > self._max_logs:
                self._logs = self._logs[-self._max_logs:]
        
        # Console output
        if self._console_output:
            print(str(entry))
    
    def log_info(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ) -> None:
        """Log info level message"""
        self._add_log_entry(LogLevel.INFO, component, message, details)
    
    def log_warning(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ) -> None:
        """Log warning level message"""
        self._add_log_entry(LogLevel.WARNING, component, message, details)
    
    def log_error(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ) -> None:
        """Log error level message"""
        self._add_log_entry(LogLevel.ERROR, component, message, details)
    
    def log_debug(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ) -> None:
        """Log debug level message"""
        if self._log_level == LogLevel.DEBUG:
            self._add_log_entry(LogLevel.DEBUG, component, message, details)
    
    def get_logs(
        self,
        level: Optional[LogLevel] = None,
        component: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[LogEntry]:
        """
        Get logs with optional filtering
        
        Args:
            level: Filter by log level
            component: Filter by component name
            limit: Maximum number of logs to return (most recent)
        
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
        
        # Apply limit (most recent)
        if limit and limit < len(filtered_logs):
            filtered_logs = filtered_logs[-limit:]
        
        return filtered_logs
    
    def get_logs_as_dict(
        self,
        level: Optional[LogLevel] = None,
        component: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict]:
        """Get logs as list of dictionaries"""
        logs = self.get_logs(level, component, limit)
        return [log.to_dict() for log in logs]
    
    def get_error_count(self, component: Optional[str] = None) -> int:
        """Get count of error logs"""
        return len(self.get_logs(level=LogLevel.ERROR, component=component))
    
    def get_warning_count(self, component: Optional[str] = None) -> int:
        """Get count of warning logs"""
        return len(self.get_logs(level=LogLevel.WARNING, component=component))
    
    def clear_logs(self) -> None:
        """Clear all logs from memory"""
        with self._lock:
            self._logs.clear()
    
    def export_logs(self, filepath: str, format: str = "text") -> None:
        """
        Export logs to file
        
        Args:
            filepath: Output file path
            format: Output format ('text' or 'json')
        """
        with self._lock:
            logs = self._logs.copy()
        
        if format == "json":
            import json
            with open(filepath, 'w') as f:
                json.dump([log.to_dict() for log in logs], f, indent=2)
        else:
            with open(filepath, 'w') as f:
                for log in logs:
                    f.write(str(log) + "\n")
    
    def get_summary(self) -> Dict:
        """Get summary statistics of logs"""
        with self._lock:
            logs = self._logs.copy()
        
        summary = {
            'total_logs': len(logs),
            'info_count': sum(1 for log in logs if log.level == LogLevel.INFO),
            'warning_count': sum(1 for log in logs if log.level == LogLevel.WARNING),
            'error_count': sum(1 for log in logs if log.level == LogLevel.ERROR),
            'debug_count': sum(1 for log in logs if log.level == LogLevel.DEBUG),
            'components': list(set(log.component for log in logs)),
            'first_log_time': logs[0].timestamp.isoformat() if logs else None,
            'last_log_time': logs[-1].timestamp.isoformat() if logs else None
        }
        
        return summary
    
    def print_summary(self) -> None:
        """Print log summary to console"""
        summary = self.get_summary()
        print("\n" + "="*60)
        print("ETL Logger Summary")
        print("="*60)
        print(f"Total Logs:     {summary['total_logs']}")
        print(f"Info:           {summary['info_count']}")
        print(f"Warnings:       {summary['warning_count']}")
        print(f"Errors:         {summary['error_count']}")
        print(f"Debug:          {summary['debug_count']}")
        print(f"Components:     {', '.join(summary['components'])}")
        if summary['first_log_time']:
            print(f"First Log:      {summary['first_log_time']}")
        if summary['last_log_time']:
            print(f"Last Log:       {summary['last_log_time']}")
        print("="*60 + "\n")


# Convenience functions for global logger access
def get_logger() -> ETLLogger:
    """Get global logger instance"""
    return ETLLogger.get_instance()


def log_info(component: str, message: str, details: Optional[str] = None) -> None:
    """Log info message to global logger"""
    get_logger().log_info(component, message, details)


def log_warning(component: str, message: str, details: Optional[str] = None) -> None:
    """Log warning message to global logger"""
    get_logger().log_warning(component, message, details)


def log_error(component: str, message: str, details: Optional[str] = None) -> None:
    """Log error message to global logger"""
    get_logger().log_error(component, message, details)


def log_debug(component: str, message: str, details: Optional[str] = None) -> None:
    """Log debug message to global logger"""
    get_logger().log_debug(component, message, details)