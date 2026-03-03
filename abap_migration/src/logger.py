"""ETL Logging Module"""
from datetime import datetime
from typing import List, Dict, Any
import logging


class ETLLogger:
    """Singleton logger for ETL operations"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self.logs: List[Dict[str, Any]] = []
        self.logger = logging.getLogger("ETL")
        self._initialized = True
        
        # Configure logging
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '[%(asctime)s] %(levelname)s: %(name)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    @classmethod
    def get_instance(cls) -> 'ETLLogger':
        """Get singleton instance"""
        return cls()
    
    def log_info(self, component: str, message: str, details: str = ""):
        """Log info message"""
        self._add_log_entry("INFO", component, message, details)
        self.logger.info(f"{component} - {message}")
    
    def log_error(self, component: str, message: str, details: str = ""):
        """Log error message"""
        self._add_log_entry("ERROR", component, message, details)
        self.logger.error(f"{component} - {message}")
    
    def log_warning(self, component: str, message: str, details: str = ""):
        """Log warning message"""
        self._add_log_entry("WARNING", component, message, details)
        self.logger.warning(f"{component} - {message}")
    
    def _add_log_entry(self, level: str, component: str, message: str, details: str = ""):
        """Add log entry to internal storage"""
        entry = {
            "timestamp": datetime.now(),
            "level": level,
            "component": component,
            "message": message,
            "details": details
        }
        self.logs.append(entry)
    
    def get_logs(self) -> List[Dict[str, Any]]:
        """Get all log entries"""
        return self.logs.copy()
    
    def clear_logs(self):
        """Clear all log entries"""
        self.logs.clear()