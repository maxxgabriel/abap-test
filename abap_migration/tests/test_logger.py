"""
Unit tests for ETL Logger utility class
"""
import pytest
import time
from datetime import datetime
from src.logger import (
    ETLLogger,
    LogLevel,
    LogEntry,
    get_logger,
    log_info,
    log_warning,
    log_error
)


@pytest.fixture
def logger():
    """Fixture to provide fresh logger instance for each test"""
    logger = ETLLogger.get_instance()
    logger.clear_logs()
    logger.enable_console_output(False)  # Disable console for tests
    yield logger
    logger.clear_logs()


def test_singleton_pattern():
    """Test that logger implements singleton pattern"""
    logger1 = ETLLogger.get_instance()
    logger2 = ETLLogger.get_instance()
    logger3 = ETLLogger()
    
    assert logger1 is logger2
    assert logger1 is logger3
    assert id(logger1) == id(logger2) == id(logger3)


def test_log_info(logger):
    """Test info level logging"""
    logger.log_info("TEST_COMPONENT", "Test info message", "Additional details")
    
    logs = logger.get_logs()
    assert len(logs) == 1
    assert logs[0].level == LogLevel.INFO
    assert logs[0].component == "TEST_COMPONENT"
    assert logs[0].message == "Test info message"
    assert logs[0].details == "Additional details"


def test_log_warning(logger):
    """Test warning level logging"""
    logger.log_warning("TEST_COMPONENT", "Test warning message")
    
    logs = logger.get_logs()
    assert len(logs) == 1
    assert logs[0].level == LogLevel.WARNING


def test_log_error(logger):
    """Test error level logging"""
    logger.log_error("TEST_COMPONENT", "Test error message", "Error details")
    
    logs = logger.get_logs()
    assert len(logs) == 1
    assert logs[0].level == LogLevel.ERROR
    assert logs[0].details == "Error details"


def test_log_debug(logger):
    """Test debug level logging"""
    # Debug should not log with INFO level
    logger.set_log_level(LogLevel.INFO)
    logger.log_debug("TEST_COMPONENT", "Debug message")
    assert len(logger.get_logs()) == 0
    
    # Debug should log with DEBUG level
    logger.set_log_level(LogLevel.DEBUG)
    logger.log_debug("TEST_COMPONENT", "Debug message")
    assert len(logger.get_logs()) == 1
    assert logger.get_logs()[0].level == LogLevel.DEBUG


def test_multiple_logs(logger):
    """Test logging multiple entries"""
    logger.log_info("COMPONENT1", "Message 1")
    logger.log_warning("COMPONENT2", "Message 2")
    logger.log_error("COMPONENT3", "Message 3")
    
    logs = logger.get_logs()
    assert len(logs) == 3
    assert logs[0].component == "COMPONENT1"
    assert logs[1].component == "COMPONENT2"
    assert logs[2].component == "COMPONENT3"


def test_get_logs_by_level(logger):
    """Test filtering logs by level"""
    logger.log_info("COMPONENT", "Info message")
    logger.log_warning("COMPONENT", "Warning message")
    logger.log_error("COMPONENT", "Error message")
    logger.log_info("COMPONENT", "Another info")
    
    info_logs = logger.get_logs(level=LogLevel.INFO)
    warning_logs = logger.get_logs(level=LogLevel.WARNING)
    error_logs = logger.get_logs(level=LogLevel.ERROR)
    
    assert len(info_logs) == 2
    assert len(warning_logs) == 1
    assert len(error_logs) == 1


def test_get_logs_by_component(logger):
    """Test filtering logs by component"""
    logger.log_info("EXTRACTOR", "Extract message")
    logger.log_info("TRANSFORMER", "Transform message")
    logger.log_info("LOADER", "Load message")
    logger.log_info("EXTRACTOR", "Another extract")
    
    extractor_logs = logger.get_logs(component="EXTRACTOR")
    transformer_logs = logger.get_logs(component="TRANSFORMER")
    
    assert len(extractor_logs) == 2
    assert len(transformer_logs) == 1


def test_get_logs_with_limit(logger):
    """Test limiting number of returned logs"""
    for i in range(10):
        logger.log_info("COMPONENT", f"Message {i}")
    
    logs = logger.get_logs(limit=5)
    assert len(logs) == 5
    assert logs[-1].message == "Message 9"  # Most recent


def test_clear_logs(logger):
    """Test clearing all logs"""
    logger.log_info("COMPONENT", "Message 1")
    logger.log_info("COMPONENT", "Message 2")
    assert len(logger.get_logs()) == 2
    
    logger.clear_logs()
    assert len(logger.get_logs()) == 0


def test_error_count(logger):
    """Test error counting"""
    logger.log_info("COMPONENT", "Info")
    logger.log_error("COMPONENT", "Error 1")
    logger.log_error("COMPONENT", "Error 2")
    logger.log_warning("COMPONENT", "Warning")
    
    assert logger.get_error_count() == 2
    assert logger.get_error_count(component="COMPONENT") == 2


def test_warning_count(logger):
    """Test warning counting"""
    logger.log_info("COMPONENT", "Info")
    logger.log_warning("COMPONENT", "Warning 1")
    logger.log_warning("COMPONENT", "Warning 2")
    logger.log_warning("COMPONENT", "Warning 3")
    
    assert logger.get_warning_count() == 3


def test_get_logs_as_dict(logger):
    """Test converting logs to dictionary format"""
    logger.log_info("COMPONENT", "Test message", "Details")
    
    logs_dict = logger.get_logs_as_dict()
    assert len(logs_dict) == 1
    assert isinstance(logs_dict[0], dict)
    assert logs_dict[0]['level'] == 'INFO'
    assert logs_dict[0]['component'] == 'COMPONENT'
    assert logs_dict[0]['message'] == 'Test message'
    assert 'timestamp' in logs_dict[0]


def test_log_entry_formatting(logger):
    """Test log entry string formatting"""
    logger.log_info("COMPONENT", "Test message", "Additional details")
    
    entry = logger.get_logs()[0]
    entry_str = str(entry)
    
    assert "INFO" in entry_str
    assert "COMPONENT" in entry_str
    assert "Test message" in entry_str
    assert "Additional details" in entry_str


def test_timestamp_formatting():
    """Test timestamp formatting"""
    entry = LogEntry(LogLevel.INFO, "COMPONENT", "Message")
    formatted = entry.format_timestamp()
    
    assert len(formatted) > 0
    assert ":" in formatted
    assert "-" in formatted


def test_max_logs_limit(logger):
    """Test maximum logs limitation"""
    logger.set_max_logs(100)
    
    # Add more logs than limit
    for i in range(150):
        logger.log_info("COMPONENT", f"Message {i}")
    
    logs = logger.get_logs()
    assert len(logs) == 100
    # Should keep most recent
    assert logs[-1].message == "Message 149"


def test_get_summary(logger):
    """Test log summary generation"""
    logger.log_info("COMPONENT1", "Info 1")
    logger.log_info("COMPONENT2", "Info 2")
    logger.log_warning("COMPONENT1", "Warning 1")
    logger.log_error("COMPONENT2", "Error 1")
    
    summary = logger.get_summary()
    
    assert summary['total_logs'] == 4
    assert summary['info_count'] == 2
    assert summary['warning_count'] == 1
    assert summary['error_count'] == 1
    assert 'COMPONENT1' in summary['components']
    assert 'COMPONENT2' in summary['components']
    assert summary['first_log_time'] is not None
    assert summary['last_log_time'] is not None


def test_export_logs_text(logger, tmp_path):
    """Test exporting logs to text file"""
    logger.log_info("COMPONENT", "Message 1")
    logger.log_error("COMPONENT", "Message 2")
    
    filepath = tmp_path / "logs.txt"
    logger.export_logs(str(filepath), format="text")
    
    assert filepath.exists()
    content = filepath.read_text()
    assert "INFO" in content
    assert "ERROR" in content
    assert "Message 1" in content


def test_export_logs_json(logger, tmp_path):
    """Test exporting logs to JSON file"""
    import json
    
    logger.log_info("COMPONENT", "Message 1")
    logger.log_warning("COMPONENT", "Message 2")
    
    filepath = tmp_path / "logs.json"
    logger.export_logs(str(filepath), format="json")
    
    assert filepath.exists()
    with open(filepath) as f:
        data = json.load(f)
    
    assert len(data) == 2
    assert data[0]['level'] == 'INFO'
    assert data[1]['level'] == 'WARNING'


def test_convenience_functions():
    """Test global convenience functions"""
    logger = get_logger()
    logger.clear_logs()
    
    log_info("COMPONENT", "Info message")
    log_warning("COMPONENT", "Warning message")
    log_error("COMPONENT", "Error message")
    
    logs = logger.get_logs()
    assert len(logs) == 3
    assert logs[0].level == LogLevel.INFO
    assert logs[1].level == LogLevel.WARNING
    assert logs[2].level == LogLevel.ERROR


def test_thread_safety():
    """Test thread-safe logging"""
    import threading
    
    logger = ETLLogger.get_instance()
    logger.clear_logs()
    
    def log_messages(thread_id, count):
        for i in range(count):
            logger.log_info(f"THREAD_{thread_id}", f"Message {i}")
    
    threads = []
    for i in range(5):
        t = threading.Thread(target=log_messages, args=(i, 20))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    logs = logger.get_logs()
    assert len(logs) == 100  # 5 threads * 20 messages


def test_console_output_toggle(logger, capsys):
    """Test enabling/disabling console output"""
    logger.enable_console_output(True)
    logger.log_info("COMPONENT", "Test message")
    
    captured = capsys.readouterr()
    assert "Test message" in captured.out
    
    logger.enable_console_output(False)
    logger.log_info("COMPONENT", "Another message")
    
    captured = capsys.readouterr()
    assert captured.out == ""


def test_log_level_setting(logger):
    """Test log level configuration"""
    assert logger._log_level == LogLevel.INFO
    
    logger.set_log_level(LogLevel.ERROR)
    assert logger._log_level == LogLevel.ERROR
    
    logger.set_log_level(LogLevel.DEBUG)
    assert logger._log_level == LogLevel.DEBUG


def test_log_entry_to_dict():
    """Test LogEntry to dictionary conversion"""
    entry = LogEntry(
        LogLevel.INFO,
        "COMPONENT",
        "Test message",
        "Details"
    )
    
    entry_dict = entry.to_dict()
    
    assert entry_dict['level'] == 'INFO'
    assert entry_dict['component'] == 'COMPONENT'
    assert entry_dict['message'] == 'Test message'
    assert entry_dict['details'] == 'Details'
    assert 'timestamp' in entry_dict


def test_empty_summary(logger):
    """Test summary with no logs"""
    summary = logger.get_summary()
    
    assert summary['total_logs'] == 0
    assert summary['info_count'] == 0
    assert summary['warning_count'] == 0
    assert summary['error_count'] == 0
    assert summary['components'] == []
    assert summary['first_log_time'] is None
    assert summary['last_log_time'] is None