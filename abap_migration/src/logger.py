"""
ETL Logger utility class with singleton pattern, in-memory storage,
timestamp formatting, and log level management.
"""
from datetime import datetime
from typing import List, Optional, Dict, Any
from enum import Enum
from dataclasses import dataclass, field, asdict
import threading


class LogLevel(Enum):
    """Log level enumeration"""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    DEBUG = "DEBUG"


@dataclass
class LogEntry:
    """Data class for log entries"""
    timestamp: datetime
    level: str
    component: str
    message: str
    details: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert log entry to dictionary"""
        return {
            'timestamp': self.timestamp.isoformat(),
            'level': self.level,
            'component': self.component,
            'message': self.message,
            'details': self.details
        }
    
    def format_message(self) -> str:
        """Format log entry as string"""
        base_msg = f"[{self.timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] {self.level}: {self.component} - {self.message}"
        if self.details:
            return f"{base_msg}\n  Details: {self.details}"
        return base_msg


class ETLLogger:
    """
    Singleton logger class for centralized ETL logging with in-memory storage.
    
    Features:
    - Singleton pattern for global access
    - Thread-safe operations
    - In-memory log storage
    - Timestamp formatting
    - Log level management
    - Component-based logging
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """Ensure singleton instance"""
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
        self._log_lock = threading.Lock()
        self._min_level = LogLevel.INFO
        self._enabled = True
        self._max_entries = 10000  # Prevent unlimited memory growth
        self._initialized = True
    
    @classmethod
    def get_instance(cls) -> 'ETLLogger':
        """
        Get singleton logger instance.
        
        Returns:
            ETLLogger: Singleton logger instance
        """
        return cls()
    
    def log_info(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ) -> None:
        """
        Log informational message.
        
        Args:
            component: Component name (e.g., 'EXTRACTOR', 'TRANSFORMER')
            message: Log message
            details: Optional detailed information
        """
        self._add_log_entry(
            level=LogLevel.INFO,
            component=component,
            message=message,
            details=details
        )
    
    def log_warning(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ) -> None:
        """
        Log warning message.
        
        Args:
            component: Component name
            message: Warning message
            details: Optional detailed information
        """
        self._add_log_entry(
            level=LogLevel.WARNING,
            component=component,
            message=message,
            details=details
        )
    
    def log_error(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ) -> None:
        """
        Log error message.
        
        Args:
            component: Component name
            message: Error message
            details: Optional detailed information (e.g., exception traceback)
        """
        self._add_log_entry(
            level=LogLevel.ERROR,
            component=component,
            message=message,
            details=details
        )
    
    def log_debug(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ) -> None:
        """
        Log debug message.
        
        Args:
            component: Component name
            message: Debug message
            details: Optional detailed information
        """
        self._add_log_entry(
            level=LogLevel.DEBUG,
            component=component,
            message=message,
            details=details
        )
    
    def _add_log_entry(
        self,
        level: LogLevel,
        component: str,
        message: str,
        details: Optional[str] = None
    ) -> None:
        """
        Internal method to add log entry with thread safety.
        
        Args:
            level: Log level enum
            component: Component name
            message: Log message
            details: Optional detailed information
        """
        if not self._enabled:
            return
        
        if self._should_log(level):
            log_entry = LogEntry(
                timestamp=datetime.now(),
                level=level.value,
                component=component,
                message=message,
                details=details
            )
            
            with self._log_lock:
                self._logs.append(log_entry)
                
                # Prevent unlimited memory growth
                if len(self._logs) > self._max_entries:
                    self._logs = self._logs[-self._max_entries:]
            
            # Output to console
            print(log_entry.format_message())
    
    def _should_log(self, level: LogLevel) -> bool:
        """
        Check if message should be logged based on minimum level.
        
        Args:
            level: Log level to check
            
        Returns:
            bool: True if should log, False otherwise
        """
        level_priority = {
            LogLevel.DEBUG: 0,
            LogLevel.INFO: 1,
            LogLevel.WARNING: 2,
            LogLevel.ERROR: 3
        }
        return level_priority.get(level, 0) >= level_priority.get(self._min_level, 0)
    
    def get_logs(
        self,
        level: Optional[LogLevel] = None,
        component: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[LogEntry]:
        """
        Retrieve log entries with optional filtering.
        
        Args:
            level: Filter by log level
            component: Filter by component name
            limit: Maximum number of entries to return
            
        Returns:
            List[LogEntry]: List of log entries
        """
        with self._log_lock:
            logs = self._logs.copy()
        
        # Apply filters
        if level:
            logs = [log for log in logs if log.level == level.value]
        
        if component:
            logs = [log for log in logs if log.component == component]
        
        # Apply limit
        if limit:
            logs = logs[-limit:]
        
        return logs
    
    def get_logs_as_dicts(
        self,
        level: Optional[LogLevel] = None,
        component: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieve log entries as dictionaries.
        
        Args:
            level: Filter by log level
            component: Filter by component name
            limit: Maximum number of entries to return
            
        Returns:
            List[Dict]: List of log entries as dictionaries
        """
        logs = self.get_logs(level=level, component=component, limit=limit)
        return [log.to_dict() for log in logs]
    
    def clear_logs(self) -> None:
        """Clear all log entries from memory"""
        with self._log_lock:
            self._logs.clear()
    
    def set_min_level(self, level: LogLevel) -> None:
        """
        Set minimum log level.
        
        Args:
            level: Minimum log level to capture
        """
        self._min_level = level
    
    def set_max_entries(self, max_entries: int) -> None:
        """
        Set maximum number of log entries to keep in memory.
        
        Args:
            max_entries: Maximum number of entries
        """
        if max_entries < 1:
            raise ValueError("max_entries must be at least 1")
        self._max_entries = max_entries
    
    def enable(self) -> None:
        """Enable logging"""
        self._enabled = True
    
    def disable(self) -> None:
        """Disable logging"""
        self._enabled = False
    
    def is_enabled(self) -> bool:
        """
        Check if logging is enabled.
        
        Returns:
            bool: True if enabled, False otherwise
        """
        return self._enabled
    
    def get_log_count(self) -> int:
        """
        Get total number of log entries.
        
        Returns:
            int: Number of log entries
        """
        with self._log_lock:
            return len(self._logs)
    
    def get_log_count_by_level(self) -> Dict[str, int]:
        """
        Get count of log entries by level.
        
        Returns:
            Dict[str, int]: Dictionary mapping log level to count
        """
        with self._log_lock:
            counts = {}
            for log in self._logs:
                counts[log.level] = counts.get(log.level, 0) + 1
            return counts
    
    def get_recent_errors(self, limit: int = 10) -> List[LogEntry]:
        """
        Get most recent error log entries.
        
        Args:
            limit: Maximum number of errors to return
            
        Returns:
            List[LogEntry]: List of recent error entries
        """
        return self.get_logs(level=LogLevel.ERROR, limit=limit)
    
    def export_logs_to_file(self, filepath: str) -> None:
        """
        Export all logs to a file.
        
        Args:
            filepath: Path to output file
        """
        with self._log_lock:
            logs = self._logs.copy()
        
        with open(filepath, 'w') as f:
            for log in logs:
                f.write(log.format_message() + '\n')
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics of logs.
        
        Returns:
            Dict: Summary statistics
        """
        with self._log_lock:
            logs = self._logs.copy()
        
        if not logs:
            return {
                'total_count': 0,
                'by_level': {},
                'by_component': {},
                'first_entry': None,
                'last_entry': None
            }
        
        by_level = {}
        by_component = {}
        
        for log in logs:
            by_level[log.level] = by_level.get(log.level, 0) + 1
            by_component[log.component] = by_component.get(log.component, 0) + 1
        
        return {
            'total_count': len(logs),
            'by_level': by_level,
            'by_component': by_component,
            'first_entry': logs[0].timestamp.isoformat(),
            'last_entry': logs[-1].timestamp.isoformat()
        }


# Module-level convenience function
def get_logger() -> ETLLogger:
    """
    Convenience function to get logger instance.
    
    Returns:
        ETLLogger: Singleton logger instance
    """
    return ETLLogger.get_instance()