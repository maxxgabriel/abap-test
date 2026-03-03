"""
Unit tests for ETL Logger utility
Tests singleton pattern, logging functionality, and in-memory storage
"""

import pytest
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
        assert id(logger1) == id(logger2)
    
    def test_get_logger_convenience_function(self):
        """Test convenience function returns same instance"""
        logger1 = get_logger()
        logger2 = ETLLogger.get_instance()
        
        assert logger1 is logger2
    
    def test_singleton_state_persistence(self):
        """Test that state persists across instance calls"""
        logger1 = ETLLogger.get_instance()
        logger1.log_info("TEST", "First message")
        
        logger2 = ETLLogger.get_instance()
        logs = logger2.get_logs()
        
        assert len(logs) >= 1
        assert any(log['message'] == "First message" for log in logs)


class TestETLLoggerLogging:
    """Test logging functionality"""
    
    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        """Clear logs before and after each test"""
        logger = ETLLogger.get_instance()
        logger.clear_logs()
        yield
        logger.clear_logs()
    
    def test_log_info(self):
        """Test INFO level logging"""
        logger = ETLLogger.get_instance()
        logger.log_info("EXTRACTOR", "Extraction started")
        
        logs = logger.get_logs()
        assert len(logs) == 1
        
        log = logs[0]
        assert log['level'] == 'INFO'
        assert log['component'] == 'EXTRACTOR'
        assert log['message'] == 'Extraction started'
    
    def test_log_warning(self):
        """Test WARNING level logging"""
        logger = ETLLogger.get_instance()
        logger.log_warning("TRANSFORMER", "Missing values detected")
        
        logs = logger.get_logs()
        assert len(logs) == 1
        
        log = logs[0]
        assert log['level'] == 'WARNING'
        assert log['component'] == 'TRANSFORMER'
    
    def test_log_error(self):
        """Test ERROR level logging"""
        logger = ETLLogger.get_instance()
        logger.log_error("LOADER", "Failed to load data", "Connection timeout")
        
        logs = logger.get_logs()
        assert len(logs) == 1
        
        log = logs[0]
        assert log['level'] == 'ERROR'
        assert log['component'] == 'LOADER'
        assert log['details'] == 'Connection timeout'
    
    def test_log_debug(self):
        """Test DEBUG level logging"""
        logger = ETLLogger.get_instance()
        logger.log_debug("ORCHESTRATOR", "Processing batch 1")
        
        logs = logger.get_logs()
        assert len(logs) == 1
        
        log = logs[0]
        assert log['level'] == 'DEBUG'
    
    def test_log_with_details(self):
        """Test logging with additional details"""
        logger = ETLLogger.get_instance()
        logger.log_info(
            "EXTRACTOR",
            "Extracted records",
            "Total: 1000 records"
        )
        
        logs = logger.get_logs()
        log = logs[0]
        
        assert log['details'] == 'Total: 1000 records'
    
    def test_log_timestamp_format(self):
        """Test that timestamps are properly formatted"""
        logger = ETLLogger.get_instance()
        logger.log_info("TEST", "Test message")
        
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


class TestETLLoggerRetrieval:
    """Test log retrieval functionality"""
    
    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        """Setup test data and cleanup"""
        logger = ETLLogger.get_instance()
        logger.clear_logs()
        
        # Add test logs
        logger.log_info("EXTRACTOR", "Info message 1")
        logger.log_info("TRANSFORMER", "Info message 2")
        logger.log_warning("EXTRACTOR", "Warning message")
        logger.log_error("LOADER", "Error message")
        logger.log_debug("ORCHESTRATOR", "Debug message")
        
        yield
        logger.clear_logs()
    
    def test_get_all_logs(self):
        """Test retrieving all logs"""
        logger = ETLLogger.get_instance()
        logs = logger.get_logs()
        
        assert len(logs) == 5
        assert all(isinstance(log, dict) for log in logs)
    
    def test_get_logs_by_level(self):
        """Test filtering logs by level"""
        logger = ETLLogger.get_instance()
        
        info_logs = logger.get_logs_by_level('INFO')
        assert len(info_logs) == 2
        assert all(log['level'] == 'INFO' for log in info_logs)
        
        warning_logs = logger.get_logs_by_level('WARNING')
        assert len(warning_logs) == 1
        
        error_logs = logger.get_logs_by_level('ERROR')
        assert len(error_logs) == 1
    
    def test_get_logs_by_component(self):
        """Test filtering logs by component"""
        logger = ETLLogger.get_instance()
        
        extractor_logs = logger.get_logs_by_component('EXTRACTOR')
        assert len(extractor_logs) == 2
        assert all(log['component'] == 'EXTRACTOR' for log in extractor_logs)
        
        transformer_logs = logger.get_logs_by_component('TRANSFORMER')
        assert len(transformer_logs) == 1
    
    def test_get_error_count(self):
        """Test counting error logs"""
        logger = ETLLogger.get_instance()
        error_count = logger.get_error_count()
        
        assert error_count == 1
    
    def test_get_warning_count(self):
        """Test counting warning logs"""
        logger = ETLLogger.get_instance()
        warning_count = logger.get_warning_count()
        
        assert warning_count == 1
    
    def test_get_log_summary(self):
        """Test log summary statistics"""
        logger = ETLLogger.get_instance()
        summary = logger.get_log_summary()
        
        assert summary['total'] == 5
        assert summary['info'] == 2
        assert summary['warning'] == 1
        assert summary['error'] == 1
        assert summary['debug'] == 1
    
    def test_export_logs_to_dict(self):
        """Test exporting logs with summary"""
        logger = ETLLogger.get_instance()
        export = logger.export_logs_to_dict()
        
        assert 'summary' in export
        assert 'logs' in export
        assert export['summary']['total'] == 5
        assert len(export['logs']) == 5


class TestETLLoggerStorage:
    """Test in-memory storage management"""
    
    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        """Clear logs before and after each test"""
        logger = ETLLogger.get_instance()
        logger.clear_logs()
        yield
        logger.clear_logs()
    
    def test_clear_logs(self):
        """Test clearing log storage"""
        logger = ETLLogger.get_instance()
        
        logger.log_info("TEST", "Message 1")
        logger.log_info("TEST", "Message 2")
        assert len(logger.get_logs()) == 2
        
        logger.clear_logs()
        assert len(logger.get_logs()) == 0
    
    def test_multiple_logs_storage(self):
        """Test storing multiple log entries"""
        logger = ETLLogger.get_instance()
        
        for i in range(10):
            logger.log_info("TEST", f"Message {i}")
        
        logs = logger.get_logs()
        assert len(logs) == 10
        
        # Verify order preservation
        for i, log in enumerate(logs):
            assert log['message'] == f"Message {i}"
    
    def test_log_persistence_across_operations(self):
        """Test that logs persist across different operations"""
        logger = ETLLogger.get_instance()
        
        logger.log_info("EXTRACTOR", "Extract started")
        logger.log_info("TRANSFORMER", "Transform started")
        
        # Retrieve by level
        info_logs = logger.get_logs_by_level('INFO')
        assert len(info_logs) == 2
        
        # All logs should still be available
        all_logs = logger.get_logs()
        assert len(all_logs) == 2


class TestETLLoggerThreadSafety:
    """Test thread safety of singleton pattern"""
    
    def test_concurrent_instance_creation(self):
        """Test that concurrent access returns same instance"""
        import threading
        
        instances = []
        
        def get_instance():
            instances.append(ETLLogger.get_instance())
        
        threads = [threading.Thread(target=get_instance) for _ in range(10)]
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # All instances should be the same object
        assert all(inst is instances[0] for inst in instances)
        assert len(set(id(inst) for inst in instances)) == 1


class TestETLLoggerEdgeCases:
    """Test edge cases and error handling"""
    
    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        """Clear logs before and after each test"""
        logger = ETLLogger.get_instance()
        logger.clear_logs()
        yield
        logger.clear_logs()
    
    def test_empty_message(self):
        """Test logging with empty message"""
        logger = ETLLogger.get_instance()
        logger.log_info("TEST", "")
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0]['message'] == ""
    
    def test_none_details(self):
        """Test logging without details"""
        logger = ETLLogger.get_instance()
        logger.log_info("TEST", "Message", None)
        
        logs = logger.get_logs()
        assert logs[0]['details'] is None
    
    def test_special_characters_in_message(self):
        """Test logging with special characters"""
        logger = ETLLogger.get_instance()
        message = "Test with special chars: @#$%^&*()[]{}|"
        logger.log_info("TEST", message)
        
        logs = logger.get_logs()
        assert logs[0]['message'] == message
    
    def test_unicode_characters(self):
        """Test logging with unicode characters"""
        logger = ETLLogger.get_instance()
        message = "Unicode test: 中文 العربية 日本語"
        logger.log_info("TEST", message)
        
        logs = logger.get_logs()
        assert logs[0]['message'] == message
    
    def test_long_message(self):
        """Test logging with very long message"""
        logger = ETLLogger.get_instance()
        long_message = "A" * 10000
        logger.log_info("TEST", long_message)
        
        logs = logger.get_logs()
        assert len(logs[0]['message']) == 10000


if __name__ == '__main__':
    pytest.main([__file__, '-v'])