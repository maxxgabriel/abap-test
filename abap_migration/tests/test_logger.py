"""
Unit tests for ETL Logger utility class.
"""
import pytest
from datetime import datetime
import threading
import time
from src.logger import (
    ETLLogger,
    LogLevel,
    LogEntry,
    get_logger
)


class TestLogEntry:
    """Test LogEntry dataclass"""
    
    def test_log_entry_creation(self):
        """Test creating a log entry"""
        timestamp = datetime.now()
        entry = LogEntry(
            timestamp=timestamp,
            level="INFO",
            component="TEST",
            message="Test message",
            details="Test details"
        )
        
        assert entry.timestamp == timestamp
        assert entry.level == "INFO"
        assert entry.component == "TEST"
        assert entry.message == "Test message"
        assert entry.details == "Test details"
    
    def test_log_entry_to_dict(self):
        """Test converting log entry to dictionary"""
        timestamp = datetime.now()
        entry = LogEntry(
            timestamp=timestamp,
            level="ERROR",
            component="LOADER",
            message="Load failed",
            details="Connection timeout"
        )
        
        result = entry.to_dict()
        
        assert result['timestamp'] == timestamp.isoformat()
        assert result['level'] == "ERROR"
        assert result['component'] == "LOADER"
        assert result['message'] == "Load failed"
        assert result['details'] == "Connection timeout"
    
    def test_log_entry_format_message(self):
        """Test formatting log entry as string"""
        timestamp = datetime(2024, 1, 15, 10, 30, 45, 123456)
        entry = LogEntry(
            timestamp=timestamp,
            level="WARNING",
            component="TRANSFORMER",
            message="Data quality issue"
        )
        
        formatted = entry.format_message()
        
        assert "2024-01-15 10:30:45" in formatted
        assert "WARNING" in formatted
        assert "TRANSFORMER" in formatted
        assert "Data quality issue" in formatted
    
    def test_log_entry_format_with_details(self):
        """Test formatting log entry with details"""
        entry = LogEntry(
            timestamp=datetime.now(),
            level="ERROR",
            component="EXTRACTOR",
            message="Extraction failed",
            details="Table not found"
        )
        
        formatted = entry.format_message()
        
        assert "Details: Table not found" in formatted


class TestETLLogger:
    """Test ETLLogger class"""
    
    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        """Setup and teardown for each test"""
        # Clear logs before each test
        logger = ETLLogger.get_instance()
        logger.clear_logs()
        logger.enable()
        logger.set_min_level(LogLevel.INFO)
        yield
        # Clear logs after each test
        logger.clear_logs()
    
    def test_singleton_pattern(self):
        """Test that ETLLogger follows singleton pattern"""
        logger1 = ETLLogger()
        logger2 = ETLLogger()
        logger3 = ETLLogger.get_instance()
        
        assert logger1 is logger2
        assert logger2 is logger3
    
    def test_get_instance(self):
        """Test get_instance class method"""
        logger = ETLLogger.get_instance()
        assert isinstance(logger, ETLLogger)
    
    def test_log_info(self):
        """Test logging info message"""
        logger = ETLLogger.get_instance()
        
        logger.log_info(
            component="TEST",
            message="Test info message",
            details="Additional info"
        )
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0].level == "INFO"
        assert logs[0].component == "TEST"
        assert logs[0].message == "Test info message"
        assert logs[0].details == "Additional info"
    
    def test_log_warning(self):
        """Test logging warning message"""
        logger = ETLLogger.get_instance()
        
        logger.log_warning(
            component="TRANSFORMER",
            message="Data quality warning"
        )
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0].level == "WARNING"
        assert logs[0].component == "TRANSFORMER"
    
    def test_log_error(self):
        """Test logging error message"""
        logger = ETLLogger.get_instance()
        
        logger.log_error(
            component="LOADER",
            message="Load failed",
            details="Connection timeout after 30s"
        )
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0].level == "ERROR"
        assert logs[0].details == "Connection timeout after 30s"
    
    def test_log_debug(self):
        """Test logging debug message"""
        logger = ETLLogger.get_instance()
        logger.set_min_level(LogLevel.DEBUG)
        
        logger.log_debug(
            component="EXTRACTOR",
            message="Debug information"
        )
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0].level == "DEBUG"
    
    def test_multiple_logs(self):
        """Test logging multiple messages"""
        logger = ETLLogger.get_instance()
        
        logger.log_info("EXTRACTOR", "Starting extraction")
        logger.log_warning("TRANSFORMER", "Missing values detected")
        logger.log_error("LOADER", "Load failed")
        
        logs = logger.get_logs()
        assert len(logs) == 3
        assert logs[0].component == "EXTRACTOR"
        assert logs[1].component == "TRANSFORMER"
        assert logs[2].component == "LOADER"
    
    def test_get_logs_with_level_filter(self):
        """Test retrieving logs with level filter"""
        logger = ETLLogger.get_instance()
        
        logger.log_info("TEST", "Info message")
        logger.log_warning("TEST", "Warning message")
        logger.log_error("TEST", "Error message")
        
        error_logs = logger.get_logs(level=LogLevel.ERROR)
        assert len(error_logs) == 1
        assert error_logs[0].level == "ERROR"
    
    def test_get_logs_with_component_filter(self):
        """Test retrieving logs with component filter"""
        logger = ETLLogger.get_instance()
        
        logger.log_info("EXTRACTOR", "Extraction message")
        logger.log_info("TRANSFORMER", "Transform message")
        logger.log_info("EXTRACTOR", "Another extraction message")
        
        extractor_logs = logger.get_logs(component="EXTRACTOR")
        assert len(extractor_logs) == 2
        assert all(log.component == "EXTRACTOR" for log in extractor_logs)
    
    def test_get_logs_with_limit(self):
        """Test retrieving logs with limit"""
        logger = ETLLogger.get_instance()
        
        for i in range(10):
            logger.log_info("TEST", f"Message {i}")
        
        limited_logs = logger.get_logs(limit=5)
        assert len(limited_logs) == 5
        # Should return most recent 5
        assert limited_logs[-1].message == "Message 9"
    
    def test_get_logs_as_dicts(self):
        """Test retrieving logs as dictionaries"""
        logger = ETLLogger.get_instance()
        
        logger.log_info("TEST", "Test message")
        
        dicts = logger.get_logs_as_dicts()
        assert len(dicts) == 1
        assert isinstance(dicts[0], dict)
        assert 'timestamp' in dicts[0]
        assert dicts[0]['level'] == "INFO"
    
    def test_clear_logs(self):
        """Test clearing logs"""
        logger = ETLLogger.get_instance()
        
        logger.log_info("TEST", "Message 1")
        logger.log_info("TEST", "Message 2")
        assert logger.get_log_count() == 2
        
        logger.clear_logs()
        assert logger.get_log_count() == 0
        assert len(logger.get_logs()) == 0
    
    def test_min_level_filtering(self):
        """Test minimum level filtering"""
        logger = ETLLogger.get_instance()
        logger.set_min_level(LogLevel.WARNING)
        
        logger.log_debug("TEST", "Debug message")
        logger.log_info("TEST", "Info message")
        logger.log_warning("TEST", "Warning message")
        logger.log_error("TEST", "Error message")
        
        logs = logger.get_logs()
        # Only WARNING and ERROR should be logged
        assert len(logs) == 2
        assert logs[0].level == "WARNING"
        assert logs[1].level == "ERROR"
    
    def test_max_entries_limit(self):
        """Test maximum entries limit"""
        logger = ETLLogger.get_instance()
        logger.set_max_entries(5)
        
        for i in range(10):
            logger.log_info("TEST", f"Message {i}")
        
        logs = logger.get_logs()
        # Should only keep last 5 entries
        assert len(logs) == 5
        assert logs[-1].message == "Message 9"
        assert logs[0].message == "Message 5"
    
    def test_set_max_entries_validation(self):
        """Test validation of max_entries"""
        logger = ETLLogger.get_instance()
        
        with pytest.raises(ValueError):
            logger.set_max_entries(0)
        
        with pytest.raises(ValueError):
            logger.set_max_entries(-1)
    
    def test_enable_disable(self):
        """Test enabling and disabling logger"""
        logger = ETLLogger.get_instance()
        
        logger.log_info("TEST", "Message 1")
        assert logger.get_log_count() == 1
        
        logger.disable()
        assert not logger.is_enabled()
        
        logger.log_info("TEST", "Message 2")
        # Should not be logged when disabled
        assert logger.get_log_count() == 1
        
        logger.enable()
        assert logger.is_enabled()
        
        logger.log_info("TEST", "Message 3")
        assert logger.get_log_count() == 2
    
    def test_get_log_count(self):
        """Test getting log count"""
        logger = ETLLogger.get_instance()
        
        assert logger.get_log_count() == 0
        
        logger.log_info("TEST", "Message 1")
        assert logger.get_log_count() == 1
        
        logger.log_error("TEST", "Message 2")
        assert logger.get_log_count() == 2
    
    def test_get_log_count_by_level(self):
        """Test getting log count by level"""
        logger = ETLLogger.get_instance()
        
        logger.log_info("TEST", "Info 1")
        logger.log_info("TEST", "Info 2")
        logger.log_warning("TEST", "Warning 1")
        logger.log_error("TEST", "Error 1")
        logger.log_error("TEST", "Error 2")
        logger.log_error("TEST", "Error 3")
        
        counts = logger.get_log_count_by_level()
        
        assert counts["INFO"] == 2
        assert counts["WARNING"] == 1
        assert counts["ERROR"] == 3
    
    def test_get_recent_errors(self):
        """Test getting recent errors"""
        logger = ETLLogger.get_instance()
        
        logger.log_info("TEST", "Info message")
        logger.log_error("TEST", "Error 1")
        logger.log_warning("TEST", "Warning")
        logger.log_error("TEST", "Error 2")
        logger.log_error("TEST", "Error 3")
        
        recent_errors = logger.get_recent_errors(limit=2)
        
        assert len(recent_errors) == 2
        assert all(log.level == "ERROR" for log in recent_errors)
        assert recent_errors[-1].message == "Error 3"
    
    def test_get_summary(self):
        """Test getting log summary"""
        logger = ETLLogger.get_instance()
        
        logger.log_info("EXTRACTOR", "Extract message")
        logger.log_warning("TRANSFORMER", "Transform warning")
        logger.log_error("LOADER", "Load error")
        
        summary = logger.get_summary()
        
        assert summary['total_count'] == 3
        assert summary['by_level']['INFO'] == 1
        assert summary['by_level']['WARNING'] == 1
        assert summary['by_level']['ERROR'] == 1
        assert summary['by_component']['EXTRACTOR'] == 1
        assert summary['by_component']['TRANSFORMER'] == 1
        assert summary['by_component']['LOADER'] == 1
        assert 'first_entry' in summary
        assert 'last_entry' in summary
    
    def test_get_summary_empty(self):
        """Test getting summary with no logs"""
        logger = ETLLogger.get_instance()
        
        summary = logger.get_summary()
        
        assert summary['total_count'] == 0
        assert summary['by_level'] == {}
        assert summary['by_component'] == {}
        assert summary['first_entry'] is None
        assert summary['last_entry'] is None
    
    def test_export_logs_to_file(self, tmp_path):
        """Test exporting logs to file"""
        logger = ETLLogger.get_instance()
        
        logger.log_info("TEST", "Info message")
        logger.log_error("TEST", "Error message")
        
        filepath = tmp_path / "test_logs.txt"
        logger.export_logs_to_file(str(filepath))
        
        assert filepath.exists()
        
        with open(filepath, 'r') as f:
            content = f.read()
            assert "INFO" in content
            assert "ERROR" in content
            assert "Info message" in content
            assert "Error message" in content
    
    def test_thread_safety(self):
        """Test thread-safe operations"""
        logger = ETLLogger.get_instance()
        
        def log_messages(thread_id):
            for i in range(100):
                logger.log_info(f"THREAD_{thread_id}", f"Message {i}")
        
        threads = []
        for i in range(5):
            thread = threading.Thread(target=log_messages, args=(i,))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Should have 500 total logs (5 threads * 100 messages)
        assert logger.get_log_count() == 500
    
    def test_timestamp_ordering(self):
        """Test that logs are ordered by timestamp"""
        logger = ETLLogger.get_instance()
        
        logger.log_info("TEST", "Message 1")
        time.sleep(0.01)
        logger.log_info("TEST", "Message 2")
        time.sleep(0.01)
        logger.log_info("TEST", "Message 3")
        
        logs = logger.get_logs()
        
        # Verify timestamps are in order
        for i in range(len(logs) - 1):
            assert logs[i].timestamp <= logs[i + 1].timestamp


class TestConvenienceFunction:
    """Test module-level convenience function"""
    
    def test_get_logger_function(self):
        """Test get_logger convenience function"""
        logger1 = get_logger()
        logger2 = get_logger()
        logger3 = ETLLogger.get_instance()
        
        assert logger1 is logger2
        assert logger2 is logger3
        assert isinstance(logger1, ETLLogger)


class TestLogLevelEnum:
    """Test LogLevel enumeration"""
    
    def test_log_level_values(self):
        """Test log level enum values"""
        assert LogLevel.INFO.value == "INFO"
        assert LogLevel.WARNING.value == "WARNING"
        assert LogLevel.ERROR.value == "ERROR"
        assert LogLevel.DEBUG.value == "DEBUG"
    
    def test_log_level_comparison(self):
        """Test log level enum usage"""
        level = LogLevel.INFO
        assert level == LogLevel.INFO
        assert level != LogLevel.ERROR


if __name__ == "__main__":
    pytest.main([__file__, "-v"])