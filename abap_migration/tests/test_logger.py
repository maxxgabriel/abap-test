"""
Unit tests for ETL Logger
"""

import pytest
import logging
from pathlib import Path
import tempfile
import shutil

from src.logger import ETLLogger, ComponentLogger
from src.config_manager import config


class TestETLLogger:
    
    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        """Setup and teardown for each test."""
        # Create temp directory for logs
        self.temp_dir = tempfile.mkdtemp()
        
        # Override log path in config
        config.set('logging.file.path', f"{self.temp_dir}/test.log")
        
        yield
        
        # Cleanup
        shutil.rmtree(self.temp_dir)
        ETLLogger._loggers.clear()
    
    def test_get_logger(self):
        """Test getting logger instance."""
        logger = ETLLogger.get_logger('test_module')
        
        assert isinstance(logger, logging.Logger)
        assert logger.name == 'test_module'
    
    def test_logger_singleton(self):
        """Test same logger returned for same name."""
        logger1 = ETLLogger.get_logger('test_module')
        logger2 = ETLLogger.get_logger('test_module')
        
        assert logger1 is logger2
    
    def test_log_to_file(self):
        """Test logging to file."""
        logger = ETLLogger.get_logger('test_file_logger')
        
        logger.info("Test info message")
        logger.warning("Test warning message")
        logger.error("Test error message")
        
        log_file = Path(self.temp_dir) / "test.log"
        assert log_file.exists()
        
        content = log_file.read_text()
        assert "Test info message" in content
        assert "Test warning message" in content
        assert "Test error message" in content
    
    def test_component_logger(self):
        """Test ComponentLogger wrapper."""
        comp_logger = ComponentLogger('TestComponent', run_id='RUN001')
        
        assert comp_logger.component_name == 'TestComponent'
        assert comp_logger.run_id == 'RUN001'
    
    def test_component_logger_message_formatting(self):
        """Test message formatting in ComponentLogger."""
        comp_logger = ComponentLogger('TestComponent', run_id='RUN001')
        
        formatted = comp_logger._format_message("Test message")
        
        assert "[TestComponent]" in formatted
        assert "[RUN001]" in formatted
        assert "Test message" in formatted
    
    def test_component_logger_without_run_id(self):
        """Test ComponentLogger without run_id."""
        comp_logger = ComponentLogger('TestComponent')
        
        formatted = comp_logger._format_message("Test message")
        
        assert "[TestComponent]" in formatted
        assert "Test message" in formatted
    
    def test_log_levels(self):
        """Test different log levels."""
        logger = ETLLogger.get_logger('test_levels')
        
        # Should not raise exceptions
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")