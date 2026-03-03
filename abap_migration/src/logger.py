"""
PySpark ETL Logger Utility Module
Singleton logger class with in-memory storage for ETL process logging.
"""
import logging
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass, field, asdict
from threading import Lock


@dataclass
class LogEntry:
    """Data class representing a single log entry."""
    timestamp: str
    level: str
    component: str
    message: str
    details: Optional[str] = None

    def to_dict(self) -> Dict:
        """Convert log entry to dictionary."""
        return asdict(self)


class ETLLogger:
    """
    Singleton logger class for ETL processes.
    Provides in-memory storage of log entries with timestamp, level, component, and message fields.
    """
    _instance = None
    _lock = Lock()

    def __new__(cls):
        """Ensure singleton pattern with thread-safe instantiation."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ETLLogger, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Initialize logger with in-memory storage."""
        if self._initialized:
            return

        self._logs: List[LogEntry] = []
        self._python_logger = logging.getLogger('ETLLogger')
        self._python_logger.setLevel(logging.DEBUG)

        # Configure console handler
        if not self._python_logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter(
                '[%(asctime)s] %(levelname)s: %(component)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            console_handler.setFormatter(formatter)
            self._python_logger.addHandler(console_handler)

        self._initialized = True

    @classmethod
    def get_instance(cls) -> 'ETLLogger':
        """
        Get singleton instance of ETLLogger.
        
        Returns:
            ETLLogger: Singleton instance
        """
        return cls()

    def _get_timestamp(self) -> str:
        """
        Get current timestamp in ISO format.
        
        Returns:
            str: Current timestamp
        """
        return datetime.utcnow().isoformat()

    def _add_log_entry(
        self,
        level: str,
        component: str,
        message: str,
        details: Optional[str] = None
    ) -> None:
        """
        Add a log entry to in-memory storage.
        
        Args:
            level: Log level (INFO, WARNING, ERROR, DEBUG)
            component: Component name generating the log
            message: Log message
            details: Optional additional details
        """
        entry = LogEntry(
            timestamp=self._get_timestamp(),
            level=level,
            component=component,
            message=message,
            details=details
        )
        
        self._logs.append(entry)
        
        # Also log to Python logger
        log_msg = f"{component} - {message}"
        extra = {'component': component}
        
        if details:
            log_msg += f" | Details: {details}"
        
        level_mapping = {
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'DEBUG': logging.DEBUG
        }
        
        self._python_logger.log(
            level_mapping.get(level, logging.INFO),
            log_msg,
            extra=extra
        )

    def log_info(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ) -> None:
        """
        Log an informational message.
        
        Args:
            component: Component name generating the log
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
        Log a warning message.
        
        Args:
            component: Component name generating the log
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
        Log an error message.
        
        Args:
            component: Component name generating the log
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
        Log a debug message.
        
        Args:
            component: Component name generating the log
            message: Log message
            details: Optional additional details
        """
        self._add_log_entry('DEBUG', component, message, details)

    def get_logs(self, level: Optional[str] = None, component: Optional[str] = None) -> List[Dict]:
        """
        Get all log entries, optionally filtered by level and/or component.
        
        Args:
            level: Optional log level filter
            component: Optional component filter
            
        Returns:
            List[Dict]: List of log entries as dictionaries
        """
        filtered_logs = self._logs
        
        if level:
            filtered_logs = [log for log in filtered_logs if log.level == level]
        
        if component:
            filtered_logs = [log for log in filtered_logs if log.component == component]
        
        return [log.to_dict() for log in filtered_logs]

    def clear_logs(self) -> None:
        """Clear all log entries from in-memory storage."""
        self._logs.clear()
        self.log_info('LOGGER', 'Log entries cleared')

    def get_log_count(self) -> int:
        """
        Get total number of log entries.
        
        Returns:
            int: Number of log entries
        """
        return len(self._logs)

    def get_log_summary(self) -> Dict[str, int]:
        """
        Get summary of log entries by level.
        
        Returns:
            Dict[str, int]: Count of logs by level
        """
        summary = {
            'INFO': 0,
            'WARNING': 0,
            'ERROR': 0,
            'DEBUG': 0
        }
        
        for log in self._logs:
            if log.level in summary:
                summary[log.level] += 1
        
        return summary

    def export_logs(self, format: str = 'json') -> str:
        """
        Export logs in specified format.
        
        Args:
            format: Export format ('json' or 'csv')
            
        Returns:
            str: Formatted log data
        """
        import json
        import csv
        from io import StringIO
        
        if format == 'json':
            return json.dumps(self.get_logs(), indent=2)
        elif format == 'csv':
            output = StringIO()
            if self._logs:
                fieldnames = ['timestamp', 'level', 'component', 'message', 'details']
                writer = csv.DictWriter(output, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.get_logs())
            return output.getvalue()
        else:
            raise ValueError(f"Unsupported format: {format}")