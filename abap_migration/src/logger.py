"""
PySpark ETL Logger Utility - Singleton logger with in-memory storage.
"""
import logging
from datetime import datetime
from typing import List, Dict, Optional
from pyspark.sql import SparkSession


class ETLLogger:
    """
    Singleton logger class for ETL process logging.
    Stores log entries in-memory with timestamp, level, component, and message.
    """
    
    _instance: Optional['ETLLogger'] = None
    _initialized: bool = False
    
    def __new__(cls):
        """Ensure singleton pattern - only one instance exists."""
        if cls._instance is None:
            cls._instance = super(ETLLogger, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize logger with in-memory storage."""
        if not ETLLogger._initialized:
            self._logs: List[Dict[str, str]] = []
            self._setup_python_logger()
            ETLLogger._initialized = True
    
    def _setup_python_logger(self):
        """Configure Python logging standards."""
        self.python_logger = logging.getLogger('etl_logger')
        self.python_logger.setLevel(logging.DEBUG)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Formatter following Python logging standards
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        
        if not self.python_logger.handlers:
            self.python_logger.addHandler(console_handler)
    
    @classmethod
    def get_instance(cls) -> 'ETLLogger':
        """
        Get singleton logger instance.
        
        Returns:
            ETLLogger: The singleton logger instance
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
    ):
        """
        Add log entry to in-memory storage.
        
        Args:
            level: Log level (INFO, ERROR, WARNING, DEBUG)
            component: Component name generating the log
            message: Log message
            details: Optional additional details
        """
        timestamp = datetime.now().isoformat()
        
        log_entry = {
            'timestamp': timestamp,
            'level': level,
            'component': component,
            'message': message,
            'details': details or ''
        }
        
        self._logs.append(log_entry)
        
        # Also log to Python logger
        log_text = f"{component} - {message}"
        if details:
            log_text += f" | Details: {details}"
        
        log_level = getattr(logging, level, logging.INFO)
        self.python_logger.log(log_level, log_text)
    
    def log_info(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ):
        """
        Log info level message.
        
        Args:
            component: Component name
            message: Log message
            details: Optional details
        """
        self._add_log_entry('INFO', component, message, details)
    
    def log_error(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ):
        """
        Log error level message.
        
        Args:
            component: Component name
            message: Log message
            details: Optional error details
        """
        self._add_log_entry('ERROR', component, message, details)
    
    def log_warning(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ):
        """
        Log warning level message.
        
        Args:
            component: Component name
            message: Log message
            details: Optional warning details
        """
        self._add_log_entry('WARNING', component, message, details)
    
    def log_debug(
        self,
        component: str,
        message: str,
        details: Optional[str] = None
    ):
        """
        Log debug level message.
        
        Args:
            component: Component name
            message: Log message
            details: Optional debug details
        """
        self._add_log_entry('DEBUG', component, message, details)
    
    def get_logs(self) -> List[Dict[str, str]]:
        """
        Retrieve all in-memory log entries.
        
        Returns:
            List[Dict]: List of log entry dictionaries
        """
        return self._logs.copy()
    
    def get_logs_by_level(self, level: str) -> List[Dict[str, str]]:
        """
        Get logs filtered by level.
        
        Args:
            level: Log level to filter (INFO, ERROR, WARNING, DEBUG)
            
        Returns:
            List[Dict]: Filtered log entries
        """
        return [log for log in self._logs if log['level'] == level]
    
    def get_logs_by_component(self, component: str) -> List[Dict[str, str]]:
        """
        Get logs filtered by component.
        
        Args:
            component: Component name to filter
            
        Returns:
            List[Dict]: Filtered log entries
        """
        return [log for log in self._logs if log['component'] == component]
    
    def clear_logs(self):
        """Clear all in-memory log entries."""
        self._logs.clear()
        self.log_info('LOGGER', 'Log storage cleared')
    
    def get_log_summary(self) -> Dict[str, int]:
        """
        Get summary statistics of logs.
        
        Returns:
            Dict: Summary with counts per level
        """
        summary = {
            'total': len(self._logs),
            'INFO': 0,
            'ERROR': 0,
            'WARNING': 0,
            'DEBUG': 0
        }
        
        for log in self._logs:
            level = log['level']
            if level in summary:
                summary[level] += 1
        
        return summary
    
    def export_logs_to_dataframe(self, spark: SparkSession):
        """
        Export logs to Spark DataFrame.
        
        Args:
            spark: SparkSession instance
            
        Returns:
            DataFrame: Spark DataFrame containing all logs
        """
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("timestamp", StringType(), False),
            StructField("level", StringType(), False),
            StructField("component", StringType(), False),
            StructField("message", StringType(), False),
            StructField("details", StringType(), True)
        ])
        
        return spark.createDataFrame(self._logs, schema=schema)
    
    def print_logs(self, max_entries: Optional[int] = None):
        """
        Print formatted logs to console.
        
        Args:
            max_entries: Maximum number of entries to print (None for all)
        """
        logs_to_print = self._logs[-max_entries:] if max_entries else self._logs
        
        print("\n" + "="*80)
        print("ETL LOGGER - IN-MEMORY LOG ENTRIES")
        print("="*80)
        
        for log in logs_to_print:
            details_str = f" | {log['details']}" if log['details'] else ""
            print(f"[{log['timestamp']}] {log['level']:8} | {log['component']:15} | {log['message']}{details_str}")
        
        print("="*80)
        
        summary = self.get_log_summary()
        print(f"\nSummary: Total={summary['total']}, "
              f"INFO={summary['INFO']}, "
              f"WARNING={summary['WARNING']}, "
              f"ERROR={summary['ERROR']}, "
              f"DEBUG={summary['DEBUG']}\n")