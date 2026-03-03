"""
Unit tests for ETL Logger
"""

import pytest
import logging
import tempfile
import os
from pathlib import Path
from datetime import datetime

from src.logger import ETLLogger
from src.config_manager import config


class TestETLLogger:
    """Test suite for ETLLogger"""
    
    @pytest.fixture(autouse=True)
    def setup_logger(self):
        """Setup test logger configuration"""
        # Create temporary log directory
        self.temp_dir = tempfile.mkdtemp()
        log_file = os.path.join(self.temp_dir, 'test.log')
        
        config.set('logging.file_path', log_file)
        config.set('logging.console_output', False)
        config.set('logging.level', 'DEBUG')
        
        # Clear existing loggers
        ETLLogger._loggers = {}
        
        yield
        
        # Cleanup
        for logger in ETLLogger._loggers.values():
            for handler in logger.handlers:
                handler.close()
        
        # Remove temp directory
        for file in Path(self.temp_dir).glob('*'):
            file.unlink()
        os.rmdir(self.temp_dir)
    
    def test_get_logger(self):
        """Test getting logger instance"""
        logger = ETLLogger.get_logger('TEST_COMPONENT')
        
        assert logger is not None
        assert logger.name == 'TEST_COMPONENT'
        assert isinstance(logger, logging.Logger)
    
    def test_logger_singleton_per_component(self):
        """Test that same logger is returned for same component"""
        logger1 = ETLLogger.get_logger('TEST')
        logger2 = ETLLogger.get_logger('TEST')
        
        assert logger1 is logger2
    
    def test_different_loggers_for_different_components(self):
        """Test that different components get different loggers"""
        logger1 = ETLLogger.get_logger('COMPONENT1')
        logger2 = ETLLogger.get_logger('COMPONENT2')
        
        assert logger1 is not logger2
        assert logger1.name != logger2.name
    
    def test_log_info(self):
        """Test logging info message"""
        ETLLogger.log_info('TEST', 'Test info message', 'Additional details')
        
        log_file = config.get('logging.file_path')
        with open(log_file, 'r') as f:
            content = f.read()
            assert 'Test info message' in content
            assert 'Additional details' in content
    
    def test_log_error(self):
        """Test logging error message"""
        ETLLogger.log_error('TEST', 'Test error message', 'Error details')
        
        log_file = config.get('logging.file_path')
        with open(log_file, 'r') as f:
            content = f.read()
            assert 'Test error message' in content
            assert 'ERROR' in content
    
    def test_log_error_with_exception(self):
        """Test logging error with exception"""
        try:
            raise ValueError("Test exception")
        except ValueError as e:
            ETLLogger.log_error('TEST', 'Error occurred', exception=e)
        
        log_file = config.get('logging.file_path')
        with open(log_file, 'r') as f:
            content = f.read()
            assert 'Error occurred' in content
            assert 'ValueError' in content
            assert 'Test exception' in content
    
    def test_log_warning(self):
        """Test logging warning message"""
        ETLLogger.log_warning('TEST', 'Test warning message')
        
        log_file = config.get('logging.file_path')
        with open(log_file, 'r') as f:
            content = f.read()
            assert 'Test warning message' in content
            assert 'WARNING' in content
    
    def test_log_debug(self):
        """Test logging debug message"""
        ETLLogger.log_debug('TEST', 'Test debug message')
        
        log_file = config.get('logging.file_path')
        with open(log_file, 'r') as f:
            content = f.read()
            assert 'Test debug message' in content
            assert 'DEBUG' in content
    
    def test_log_execution_time(self):
        """Test logging execution time"""
        start_time = datetime.now()
        
        ETLLogger.log_execution_time('TEST', 'Test operation', start_time)
        
        log_file = config.get('logging.file_path')
        with open(log_file, 'r') as f:
            content = f.read()
            assert 'Test operation completed' in content
            assert 'seconds' in content
    
    def test_log_metrics(self):
        """Test logging metrics"""
        metrics = {
            'records_processed': 1000,
            'duration': 45.5,
            'throughput': 22.0
        }
        
        ETLLogger.log_metrics('TEST', metrics)
        
        log_file = config.get('logging.file_path')
        with open(log_file, 'r') as f:
            content = f.read()
            assert 'Metrics:' in content
            assert 'records_processed: 1000' in content
            assert 'duration: 45.5' in content
    
    def test_log_level_filtering(self):
        """Test that log level filtering works"""
        config.set('logging.level', 'WARNING')
        
        # Clear existing loggers
        ETLLogger._loggers = {}
        
        ETLLogger.log_debug('TEST', 'Debug message')
        ETLLogger.log_info('TEST', 'Info message')
        ETLLogger.log_warning('TEST', 'Warning message')
        
        log_file = config.get('logging.file_path')
        with open(log_file, 'r') as f:
            content = f.read()
            assert 'Debug message' not in content
            assert 'Info message' not in content
            assert 'Warning message' in content