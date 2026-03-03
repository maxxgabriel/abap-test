"""
Unit tests for ETL Logger utility
"""

import pytest
import time
from datetime import datetime
from src.logger import ETLLogger, LogEntry, get_logger


class TestLogEntry:
    """Test LogEntry dataclass"""
    
    def test_log_entry_creation(self):
        """Test creating a log entry"""
        entry = LogEntry(
            timestamp="2024-01-01 12:00:00.000",
            level="INFO",
            component="TEST",
            message="Test message",
            details="Test details"
        )
        
        assert entry.timestamp == "2024-01-01 12:00:00.000"
        assert entry.level == "INFO"
        assert entry.component == "TEST"
        assert entry.message == "Test message"
        assert entry.details == "Test details"
    
    def test_log_entry_to_dict(self):
        """Test converting log entry to dictionary"""
        entry = LogEntry(
            timestamp="2024-01-01 12:00:00.000",
            level="INFO",
            component="TEST",
            message="Test message"
        )
        
        entry_dict = entry.to_dict()
        
        assert isinstance(entry_dict, dict)
        assert entry_dict['timestamp'] == "2024-01-01 12:00:00.000"
        assert entry_dict['level'] == "INFO"
        assert entry_dict['component'] == "TEST"
        assert entry_dict['message'] == "Test message"


class TestETLLoggerSingleton:
    """Test singleton pattern implementation"""
    
    def test_singleton_instance(self):
        """Test that only one instance is created"""
        logger1 = ETLLogger.get_instance()
        logger2 = ETLLogger.get_instance()
        
        assert logger1 is logger2
    
    def test_get_logger_convenience_function(self):
        """Test convenience function returns same instance"""
        logger1 = get_logger()
        logger2 = ETLLogger.get_instance()
        
        assert logger1 is logger2


class TestETLLoggerBasicLogging:
    """Test basic logging functionality"""
    
    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        """Setup and teardown for each test"""
        logger = get_logger()
        logger.clear_logs()
        yield
        logger.clear_logs()
    
    def test_log_info(self):
        """Test logging info message"""
        logger = get_logger()
        
        logger.log_info('TEST', 'Test info message')
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0]['level'] == 'INFO'
        assert logs[0]['component'] == 'TEST'
        assert logs[0]['message'] == 'Test info message'
    
    def test_log_info_with_details(self):
        """Test logging info message with details"""
        logger = get_logger()
        
        logger.log_info('TEST', 'Test message', 'Additional details')
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0]['details'] == 'Additional details'
    
    def test_log_warning(self):
        """Test logging warning message"""
        logger = get_logger()
        
        logger.log_warning('TEST', 'Test warning message')
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0]['level'] == 'WARNING'
    
    def test_log_error(self):
        """Test logging error message"""
        logger = get_logger()
        
        logger.log_error('TEST', 'Test error message')
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0]['level'] == 'ERROR'
    
    def test_multiple_logs(self):
        """Test logging multiple messages"""
        logger = get_logger()
        
        logger.log_info('EXTRACTOR', 'Starting extraction')
        logger.log_info('EXTRACTOR', 'Extraction complete')
        logger.log_warning('TRANSFORMER', 'Missing values detected')
        logger.log_error('LOADER', 'Load failed')
        
        logs = logger.get_logs()
        assert len(logs) == 4


class TestETLLoggerRetrieval:
    """Test log retrieval functionality"""
    
    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        """Setup test data and cleanup"""
        logger = get_logger()
        logger.clear_logs()
        
        # Create test logs
        logger.log_info('EXTRACTOR', 'Extract started')
        logger.log_info('EXTRACTOR', 'Extract completed')
        logger.log_warning('TRANSFORMER', 'Data quality warning')
        logger.log_error('LOADER', 'Load failed')
        logger.log_error('LOADER', 'Retry failed')
        
        yield
        
        logger.clear_logs()
    
    def test_get_all_logs(self):
        """Test retrieving all logs"""
        logger = get_logger()
        logs = logger.get_logs()
        
        assert len(logs) == 5
    
    def test_get_logs_by_level_info(self):
        """Test filtering logs by INFO level"""
        logger = get_logger()
        info_logs = logger.get_logs_by_level('INFO')
        
        assert len(info_logs) == 2
        assert all(log['level'] == 'INFO' for log in info_logs)
    
    def test_get_logs_by_level_warning(self):
        """Test filtering logs by WARNING level"""
        logger = get_logger()
        warning_logs = logger.get_logs_by_level('WARNING')
        
        assert len(warning_logs) == 1
        assert warning_logs[0]['level'] == 'WARNING'
    
    def test_get_logs_by_level_error(self):
        """Test filtering logs by ERROR level"""
        logger = get_logger()
        error_logs = logger.get_logs_by_level('ERROR')
        
        assert len(error_logs) == 2
        assert all(log['level'] == 'ERROR' for log in error_logs)
    
    def test_get_logs_by_component(self):
        """Test filtering logs by component"""
        logger = get_logger()
        
        extractor_logs = logger.get_logs_by_component('EXTRACTOR')
        assert len(extractor_logs) == 2
        assert all(log['component'] == 'EXTRACTOR' for log in extractor_logs)
        
        loader_logs = logger.get_logs_by_component('LOADER')
        assert len(loader_logs) == 2
        assert all(log['component'] == 'LOADER' for log in loader_logs)
    
    def test_get_log_count(self):
        """Test getting log counts by level"""
        logger = get_logger()
        counts = logger.get_log_count()
        
        assert counts['INFO'] == 2
        assert counts['WARNING'] == 1
        assert counts['ERROR'] == 2
    
    def test_export_logs_to_dict(self):
        """Test exporting logs with metadata"""
        logger = get_logger()
        export = logger.export_logs_to_dict()
        
        assert 'total_logs' in export
        assert 'counts_by_level' in export
        assert 'logs' in export
        
        assert export['total_logs'] == 5
        assert export['counts_by_level']['INFO'] == 2
        assert export['counts_by_level']['WARNING'] == 1
        assert export['counts_by_level']['ERROR'] == 2
        assert len(export['logs']) == 5


class TestETLLoggerClearLogs:
    """Test log clearing functionality"""
    
    def test_clear_logs(self):
        """Test clearing all logs"""
        logger = get_logger()
        
        logger.log_info('TEST', 'Message 1')
        logger.log_info('TEST', 'Message 2')
        
        assert len(logger.get_logs()) == 2
        
        logger.clear_logs()
        
        assert len(logger.get_logs()) == 0
    
    def test_clear_logs_resets_counts(self):
        """Test that clearing logs resets counts"""
        logger = get_logger()
        
        logger.log_info('TEST', 'Info message')
        logger.log_error('TEST', 'Error message')
        
        logger.clear_logs()
        
        counts = logger.get_log_count()
        assert counts['INFO'] == 0
        assert counts['WARNING'] == 0
        assert counts['ERROR'] == 0


class TestETLLoggerTimestamp:
    """Test timestamp functionality"""
    
    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        """Setup and teardown"""
        logger = get_logger()
        logger.clear_logs()
        yield
        logger.clear_logs()
    
    def test_timestamp_format(self):
        """Test that timestamps are properly formatted"""
        logger = get_logger()
        
        logger.log_info('TEST', 'Test message')
        
        logs = logger.get_logs()
        timestamp = logs[0]['timestamp']
        
        # Verify timestamp format: YYYY-MM-DD HH:MM:SS.mmm
        assert len(timestamp) == 23
        assert timestamp[4] == '-'
        assert timestamp[7] == '-'
        assert timestamp[10] == ' '
        assert timestamp[13] == ':'
        assert timestamp[16] == ':'
        assert timestamp[19] == '.'
    
    def test_timestamps_are_sequential(self):
        """Test that timestamps increase sequentially"""
        logger = get_logger()
        
        logger.log_info('TEST', 'Message 1')
        time.sleep(0.01)  # Small delay
        logger.log_info('TEST', 'Message 2')
        
        logs = logger.get_logs()
        timestamp1 = logs[0]['timestamp']
        timestamp2 = logs[1]['timestamp']
        
        assert timestamp1 < timestamp2


class TestETLLoggerThreadSafety:
    """Test thread-safety of logger"""
    
    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        """Setup and teardown"""
        logger = get_logger()
        logger.clear_logs()
        yield
        logger.clear_logs()
    
    def test_concurrent_logging(self):
        """Test thread-safe logging (basic test)"""
        import threading
        
        logger = get_logger()
        
        def log_messages(component, count):
            for i in range(count):
                logger.log_info(component, f'Message {i}')
        
        threads = []
        for i in range(5):
            thread = threading.Thread(
                target=log_messages,
                args=(f'THREAD_{i}', 10)
            )
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Should have 50 log entries (5 threads * 10 messages)
        logs = logger.get_logs()
        assert len(logs) == 50


class TestETLLoggerEdgeCases:
    """Test edge cases and error handling"""
    
    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        """Setup and teardown"""
        logger = get_logger()
        logger.clear_logs()
        yield
        logger.clear_logs()
    
    def test_empty_component(self):
        """Test logging with empty component"""
        logger = get_logger()
        
        logger.log_info('', 'Message with empty component')
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0]['component'] == ''
    
    def test_empty_message(self):
        """Test logging with empty message"""
        logger = get_logger()
        
        logger.log_info('TEST', '')
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0]['message'] == ''
    
    def test_none_details(self):
        """Test logging with None details"""
        logger = get_logger()
        
        logger.log_info('TEST', 'Message', None)
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0]['details'] is None
    
    def test_special_characters_in_message(self):
        """Test logging with special characters"""
        logger = get_logger()
        
        special_msg = "Test with special chars: @#$%^&*(){}[]|\\:;\"'<>,.?/"
        logger.log_info('TEST', special_msg)
        
        logs = logger.get_logs()
        assert logs[0]['message'] == special_msg
    
    def test_unicode_characters(self):
        """Test logging with unicode characters"""
        logger = get_logger()
        
        unicode_msg = "Test with unicode: 你好世界 🎉 ñ ü"
        logger.log_info('TEST', unicode_msg)
        
        logs = logger.get_logs()
        assert logs[0]['message'] == unicode_msg
    
    def test_very_long_message(self):
        """Test logging with very long message"""
        logger = get_logger()
        
        long_msg = "A" * 10000
        logger.log_info('TEST', long_msg)
        
        logs = logger.get_logs()
        assert len(logs[0]['message']) == 10000


if __name__ == '__main__':
    pytest.main([__file__, '-v'])