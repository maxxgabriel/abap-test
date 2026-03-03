"""
PySpark ETL Logger Utility - Singleton logger with in-memory storage
"""
import logging
from datetime import datetime
from typing import List, Optional, Dict
from dataclasses import dataclass, field
from threading import Lock


@dataclass
class LogEntry:
    """Structure for log entries"""
    timestamp: datetime
    level: str
    component: str
    message: str
    details: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """Convert log entry to dictionary"""
        return {
            'timestamp': self.timestamp.isoformat(),
            'level': self.level,
            'component': self.component,
            'message': self.message,
            'details': self.details
        }


class ETLLogger:
    """
    Singleton logger class for ETL process logging with in-memory storage.
    Thread-safe implementation using double-checked locking pattern.
    """
    
    _instance = None
    _lock = Lock()
    
    def __new__(cls):
        """Implement singleton pattern with thread safety"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ETLLogger, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize logger (only once due to singleton pattern)"""
        if self._initialized:
            return
            
        self._initialized = True
        self._log_entries: List[LogEntry] = []
        self._entry_lock = Lock()
        
        # Configure Python logging
        self._configure_logging()
    
    def _configure_logging(self):
        """Configure standard Python logging"""
        self.logger = logging.getLogger('ETLLogger')
        self.logger.setLevel(logging.DEBUG)
        
        # Console handler
        if not self.logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            
            # Format with timestamp, level, component, and message
            formatter = logging.Formatter(
                fmt='%(asctime)s - %(levelname)s - %(component)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
    
    @classmethod
    def get_instance(cls) -> 'ETLLogger':
        """
        Get singleton instance of ETLLogger
        
        Returns:
            ETLLogger: Singleton logger instance
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
        Add log entry to in-memory storage and Python logger
        
        Args:
            level: Log level (INFO, WARNING, ERROR, DEBUG)
            component: Component name generating the log
            message: Log message
            details: Optional detailed information
        """
        # Create log entry
        entry = LogEntry(
            timestamp=datetime.now(),
            level=level,
            component=component,
            message=message,
            details=details
        )
        
        # Thread-safe append to in-memory storage
        with self._entry_lock:
            self._log_entries.append(entry)
        
        # Also log using Python logging with component as extra field
        log_func = getattr(self.logger, level.lower())
        log_message = message
        if details:
            log_message = f"{message} | Details: {details}"
        
        log_func(log_message, extra={'component': component})
    
    def log_info(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ):
        """
        Log INFO level message
        
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
        Log WARNING level message
        
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
        Log ERROR level message
        
        Args:
            component: Component name
            message: Log message
            details: Optional detailed information
        """
        self._add_log_entry('ERROR', component, message, details)
    
    def log_debug(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ):
        """
        Log DEBUG level message
        
        Args:
            component: Component name
            message: Log message
            details: Optional detailed information
        """
        self._add_log_entry('DEBUG', component, message, details)
    
    def get_logs(
        self,
        level: Optional[str] = None,
        component: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[LogEntry]:
        """
        Retrieve log entries from in-memory storage with optional filtering
        
        Args:
            level: Filter by log level
            component: Filter by component name
            limit: Maximum number of entries to return
            
        Returns:
            List of LogEntry objects
        """
        with self._entry_lock:
            filtered_logs = self._log_entries.copy()
        
        # Apply filters
        if level:
            filtered_logs = [log for log in filtered_logs if log.level == level]
        
        if component:
            filtered_logs = [log for log in filtered_logs if log.component == component]
        
        # Apply limit
        if limit and limit > 0:
            filtered_logs = filtered_logs[-limit:]
        
        return filtered_logs
    
    def get_logs_as_dicts(
        self,
        level: Optional[str] = None,
        component: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict]:
        """
        Retrieve log entries as dictionaries
        
        Args:
            level: Filter by log level
            component: Filter by component name
            limit: Maximum number of entries to return
            
        Returns:
            List of log dictionaries
        """
        logs = self.get_logs(level, component, limit)
        return [log.to_dict() for log in logs]
    
    def clear_logs(self):
        """Clear all log entries from in-memory storage"""
        with self._entry_lock:
            self._log_entries.clear()
        self.logger.info('Log entries cleared', extra={'component': 'LOGGER'})
    
    def get_log_count(self) -> int:
        """
        Get total number of log entries in memory
        
        Returns:
            Number of log entries
        """
        with self._entry_lock:
            return len(self._log_entries)
    
    def get_log_statistics(self) -> Dict[str, int]:
        """
        Get statistics about log entries by level
        
        Returns:
            Dictionary with counts per log level
        """
        with self._entry_lock:
            logs = self._log_entries.copy()
        
        stats = {
            'total': len(logs),
            'info': 0,
            'warning': 0,
            'error': 0,
            'debug': 0
        }
        
        for log in logs:
            level_key = log.level.lower()
            if level_key in stats:
                stats[level_key] += 1
        
        return stats
    
    def format_logs_for_display(
        self,
        level: Optional[str] = None,
        component: Optional[str] = None,
        limit: Optional[int] = None
    ) -> str:
        """
        Format logs as human-readable string
        
        Args:
            level: Filter by log level
            component: Filter by component name
            limit: Maximum number of entries to return
            
        Returns:
            Formatted log string
        """
        logs = self.get_logs(level, component, limit)
        
        if not logs:
            return "No log entries found"
        
        lines = ["=" * 80]
        lines.append("ETL Logger - Log Entries")
        lines.append("=" * 80)
        
        for log in logs:
            lines.append(f"[{log.timestamp.strftime('%Y-%m-%d %H:%M:%S')}] "
                        f"{log.level:8s} {log.component:15s} {log.message}")
            if log.details:
                lines.append(f"  Details: {log.details}")
            lines.append("-" * 80)
        
        return "\n".join(lines)
    
    def export_logs_to_spark_df(self, spark):
        """
        Export logs to Spark DataFrame
        
        Args:
            spark: SparkSession instance
            
        Returns:
            Spark DataFrame with log entries
        """
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType
        
        schema = StructType([
            StructField("timestamp", TimestampType(), False),
            StructField("level", StringType(), False),
            StructField("component", StringType(), False),
            StructField("message", StringType(), False),
            StructField("details", StringType(), True)
        ])
        
        log_data = []
        with self._entry_lock:
            for log in self._log_entries:
                log_data.append((
                    log.timestamp,
                    log.level,
                    log.component,
                    log.message,
                    log.details
                ))
        
        return spark.createDataFrame(log_data, schema)


# Convenience function for easy access
def get_logger() -> ETLLogger:
    """
    Convenience function to get logger instance
    
    Returns:
        ETLLogger singleton instance
    """
    return ETLLogger.get_instance()


if __name__ == "__main__":
    # Demo usage
    logger = get_logger()
    
    logger.log_info('DEMO', 'Logger initialized successfully')
    logger.log_warning('DEMO', 'This is a warning message', 'Additional warning details')
    logger.log_error('DEMO', 'This is an error message', 'Stack trace details here')
    logger.log_debug('DEMO', 'Debug information', 'Verbose debug details')
    
    print("\n" + logger.format_logs_for_display())
    
    print("\nLog Statistics:")
    stats = logger.get_log_statistics()
    for level, count in stats.items():
        print(f"  {level}: {count}")