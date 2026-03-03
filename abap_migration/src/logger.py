"""
ETL Logger
Provides structured logging for ETL operations.
"""

import logging
from datetime import datetime
from typing import Optional


class ETLLogger:
    """
    Structured logger for ETL operations.
    """

    def __init__(self, component: str, log_level: str = "INFO"):
        """
        Initialize ETL Logger.

        Args:
            component: Component name (EXTRACTOR, TRANSFORMER, LOADER, etc.)
            log_level: Logging level
        """
        self.component = component
        self.logger = logging.getLogger(f"ETL.{component}")
        self.logger.setLevel(getattr(logging, log_level))

        # Create console handler if not exists
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def log_info(self, message: str, details: Optional[str] = None):
        """Log info message"""
        full_message = f"{message}"
        if details:
            full_message += f" | Details: {details}"
        self.logger.info(full_message)

    def log_error(self, message: str, details: Optional[str] = None):
        """Log error message"""
        full_message = f"{message}"
        if details:
            full_message += f" | Details: {details}"
        self.logger.error(full_message)

    def log_warning(self, message: str, details: Optional[str] = None):
        """Log warning message"""
        full_message = f"{message}"
        if details:
            full_message += f" | Details: {details}"
        self.logger.warning(full_message)

    def log_debug(self, message: str, details: Optional[str] = None):
        """Log debug message"""
        full_message = f"{message}"
        if details:
            full_message += f" | Details: {details}"
        self.logger.debug(full_message)