"""
PySpark ETL Logger Utility

Singleton logger class with in-memory storage for ETL process logging.
Formats log entries with timestamp, level, component, and message fields.
"""

import logging
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass, field, asdict
from threading import Lock


@dataclass
class LogEntry:
    """Data class representing a single log entry"""
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
    Singleton logger class for ETL processes with in-memory storage.
    
    Features:
    - Thread-safe singleton pattern
    - In-memory log storage
    - Standard Python logging integration
    - Structured log entries with component tracking
    """
    
    _instance: Optional['ETLLogger'] = None
    _lock: Lock = Lock()
    
    def __new__(cls):
        """Thread-safe singleton implementation"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize logger (only once due to singleton pattern)"""
        if self._initialized:
            return
            
        self._initialized = True
        self._logs: List[LogEntry] = []
        self._setup_python_logger()
    
    def _setup_python_logger(self):
        """Configure Python's standard logging"""
        self._python_logger = logging.getLogger('ETLLogger')
        self._python_logger.setLevel(logging.DEBUG)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Format: [timestamp] LEVEL: component - message
        formatter = logging.Formatter(
            '[%(asctime)s] %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        
        # Avoid duplicate handlers
        if not self._python_logger.handlers:
            self._python_logger.addHandler(console_handler)
    
    @classmethod
    def get_instance(cls) -> 'ETLLogger':
        """
        Get the singleton logger instance.
        
        Returns:
            ETLLogger: The singleton logger instance
        """
        return cls()
    
    def _add_log_entry(
        self,
        level: str,
        component: str,
        message: str,
        details: Optional[str] = None
    ):
        """
        Add a log entry to in-memory storage and Python logger.
        
        Args:
            level: Log level (INFO, WARNING, ERROR)
            component: Component name (e.g., 'EXTRACTOR', 'TRANSFORMER')
            message: Log message
            details: Optional detailed information
        """
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
        log_entry = LogEntry(
            timestamp=timestamp,
            level=level,
            component=component,
            message=message,
            details=details
        )
        
        # Store in memory
        with self._lock:
            self._logs.append(log_entry)
        
        # Log to Python logger
        log_msg = f"{component} - {message}"
        if details:
            log_msg += f" | Details: {details}"
        
        log_level = getattr(logging, level, logging.INFO)
        self._python_logger.log(log_level, log_msg)
    
    def log_info(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ):
        """
        Log an informational message.
        
        Args:
            component: Component name
            message: Log message
            details: Optional detailed information
        """
        self._add_log_entry('INFO', component, message, details)
    
    def log_warning(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ):
        """
        Log a warning message.
        
        Args:
            component: Component name
            message: Log message
            details: Optional detailed information
        """
        self._add_log_entry('WARNING', component, message, details)
    
    def log_error(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ):
        """
        Log an error message.
        
        Args:
            component: Component name
            message: Log message
            details: Optional detailed information
        """
        self._add_log_entry('ERROR', component, message, details)
    
    def get_logs(self) -> List[Dict]:
        """
        Retrieve all stored log entries.
        
        Returns:
            List of log entries as dictionaries
        """
        with self._lock:
            return [log.to_dict() for log in self._logs]
    
    def get_logs_by_level(self, level: str) -> List[Dict]:
        """
        Retrieve log entries filtered by level.
        
        Args:
            level: Log level to filter (INFO, WARNING, ERROR)
            
        Returns:
            List of filtered log entries
        """
        with self._lock:
            return [
                log.to_dict() 
                for log in self._logs 
                if log.level == level
            ]
    
    def get_logs_by_component(self, component: str) -> List[Dict]:
        """
        Retrieve log entries filtered by component.
        
        Args:
            component: Component name to filter
            
        Returns:
            List of filtered log entries
        """
        with self._lock:
            return [
                log.to_dict() 
                for log in self._logs 
                if log.component == component
            ]
    
    def clear_logs(self):
        """Clear all stored log entries"""
        with self._lock:
            self._logs.clear()
        self._python_logger.info("Logs cleared")
    
    def get_log_count(self) -> Dict[str, int]:
        """
        Get count of logs by level.
        
        Returns:
            Dictionary with counts per level
        """
        with self._lock:
            counts = {'INFO': 0, 'WARNING': 0, 'ERROR': 0}
            for log in self._logs:
                if log.level in counts:
                    counts[log.level] += 1
            return counts
    
    def export_logs_to_dict(self) -> Dict:
        """
        Export all logs with metadata.
        
        Returns:
            Dictionary containing logs and summary statistics
        """
        logs = self.get_logs()
        counts = self.get_log_count()
        
        return {
            'total_logs': len(logs),
            'counts_by_level': counts,
            'logs': logs
        }


# Convenience function for getting logger instance
def get_logger() -> ETLLogger:
    """
    Convenience function to get the singleton logger instance.
    
    Returns:
        ETLLogger: The singleton logger instance
    """
    return ETLLogger.get_instance()


# Example usage in other modules
if __name__ == '__main__':
    # Demonstrate logger functionality
    logger = get_logger()
    
    # Log various messages
    logger.log_info('EXTRACTOR', 'Starting data extraction')
    logger.log_info('EXTRACTOR', 'Extracted 1000 records', 'Query completed in 2.3s')
    logger.log_warning('TRANSFORMER', 'Missing category for 5 records')
    logger.log_error('LOADER', 'Failed to load batch', 'Connection timeout')
    
    # Retrieve logs
    print("\n=== All Logs ===")
    for log in logger.get_logs():
        print(log)
    
    print("\n=== Log Counts ===")
    print(logger.get_log_count())
    
    print("\n=== Error Logs Only ===")
    for log in logger.get_logs_by_level('ERROR'):
        print(log)
    
    print("\n=== Extractor Logs Only ===")
    for log in logger.get_logs_by_component('EXTRACTOR'):
        print(log)