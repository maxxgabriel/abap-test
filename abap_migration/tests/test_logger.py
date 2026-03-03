"""
Unit tests for ETL Logger utility.
"""
import pytest
from datetime import datetime
from src.logger import ETLLogger, LogEntry


class TestLogEntry:
    """Test cases for LogEntry dataclass."""
    
    def test_log_entry_creation(self):
        """Test creating a log entry."""
        timestamp = datetime.utcnow().isoformat()
        entry = LogEntry(
            timestamp=timestamp,
            level='INFO',
            component='TEST',
            message='Test message',
            details='Test details'
        )
        
        assert entry.timestamp == timestamp
        assert entry.level == 'INFO'
        assert entry.component == 'TEST'
        assert entry.message == 'Test message'
        assert entry.details == 'Test details'
    
    def test_log_entry_to_dict(self):
        """Test converting log entry to dictionary."""
        entry = LogEntry(
            timestamp='2024-01-01T00:00:00',
            level='INFO',
            component='TEST',
            message='Test message'
        )
        
        entry_dict = entry.to_dict()
        assert isinstance(entry_dict, dict)
        assert entry_dict['timestamp'] == '2024-01-01T00:00:00'
        assert entry_dict['level'] == 'INFO'
        assert entry_dict['component'] == 'TEST'
        assert entry_dict['message'] == 'Test message'


class TestETLLogger:
    """Test cases for ETLLogger singleton class."""
    
    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        """Setup and teardown for each test."""
        logger = ETLLogger.get_instance()
        logger.clear_logs()
        yield
        logger.clear_logs()
    
    def test_singleton_pattern(self):
        """Test that ETLLogger follows singleton pattern."""
        logger1 = ETLLogger.get_instance()
        logger2 = ETLLogger.get_instance()
        logger3 = ETLLogger()
        
        assert logger1 is logger2
        assert logger1 is logger3
    
    def test_log_info(self):
        """Test logging info messages."""
        logger = ETLLogger.get_instance()
        logger.log_info('TEST', 'Test info message', 'Additional details')
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0]['level'] == 'INFO'
        assert logs[0]['component'] == 'TEST'
        assert logs[0]['message'] == 'Test info message'
        assert logs[0]['details'] == 'Additional details'
    
    def test_log_warning(self):
        """Test logging warning messages."""
        logger = ETLLogger.get_instance()
        logger.log_warning('TEST', 'Test warning message')
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0]['level'] == 'WARNING'
        assert logs[0]['component'] == 'TEST'
    
    def test_log_error(self):
        """Test logging error messages."""
        logger = ETLLogger.get_instance()
        logger.log_error('TEST', 'Test error message', 'Error details')
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0]['level'] == 'ERROR'
        assert logs[0]['details'] == 'Error details'
    
    def test_log_debug(self):
        """Test logging debug messages."""
        logger = ETLLogger.get_instance()
        logger.log_debug('TEST', 'Test debug message')
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0]['level'] == 'DEBUG'
    
    def test_get_logs_by_level(self):
        """Test filtering logs by level."""
        logger = ETLLogger.get_instance()
        logger.log_info('TEST', 'Info message')
        logger.log_warning('TEST', 'Warning message')
        logger.log_error('TEST', 'Error message')
        logger.log_debug('TEST', 'Debug message')
        
        info_logs = logger.get_logs(level='INFO')
        warning_logs = logger.get_logs(level='WARNING')
        error_logs = logger.get_logs(level='ERROR')
        debug_logs = logger.get_logs(level='DEBUG')
        
        assert len(info_logs) == 1
        assert len(warning_logs) == 1
        assert len(error_logs) == 1
        assert len(debug_logs) == 1
    
    def test_get_logs_by_component(self):
        """Test filtering logs by component."""
        logger = ETLLogger.get_instance()
        logger.log_info('EXTRACTOR', 'Extractor message')
        logger.log_info('TRANSFORMER', 'Transformer message')
        logger.log_info('LOADER', 'Loader message')
        
        extractor_logs = logger.get_logs(component='EXTRACTOR')
        transformer_logs = logger.get_logs(component='TRANSFORMER')
        loader_logs = logger.get_logs(component='LOADER')
        
        assert len(extractor_logs) == 1
        assert len(transformer_logs) == 1
        assert len(loader_logs) == 1
    
    def test_get_logs_by_level_and_component(self):
        """Test filtering logs by both level and component."""
        logger = ETLLogger.get_instance()
        logger.log_info('EXTRACTOR', 'Extractor info')
        logger.log_error('EXTRACTOR', 'Extractor error')
        logger.log_info('LOADER', 'Loader info')
        
        extractor_errors = logger.get_logs(level='ERROR', component='EXTRACTOR')
        extractor_info = logger.get_logs(level='INFO', component='EXTRACTOR')
        
        assert len(extractor_errors) == 1
        assert len(extractor_info) == 1
    
    def test_clear_logs(self):
        """Test clearing all logs."""
        logger = ETLLogger.get_instance()
        logger.log_info('TEST', 'Message 1')
        logger.log_info('TEST', 'Message 2')
        logger.log_info('TEST', 'Message 3')
        
        assert logger.get_log_count() > 0
        
        logger.clear_logs()
        # After clear, there should be 1 log (the "Log entries cleared" message)
        assert logger.get_log_count() == 1
    
    def test_get_log_count(self):
        """Test getting total log count."""
        logger = ETLLogger.get_instance()
        
        assert logger.get_log_count() == 0
        
        logger.log_info('TEST', 'Message 1')
        assert logger.get_log_count() == 1
        
        logger.log_info('TEST', 'Message 2')
        logger.log_error('TEST', 'Message 3')
        assert logger.get_log_count() == 3
    
    def test_get_log_summary(self):
        """Test getting log summary by level."""
        logger = ETLLogger.get_instance()
        logger.log_info('TEST', 'Info 1')
        logger.log_info('TEST', 'Info 2')
        logger.log_warning('TEST', 'Warning 1')
        logger.log_error('TEST', 'Error 1')
        logger.log_error('TEST', 'Error 2')
        logger.log_error('TEST', 'Error 3')
        logger.log_debug('TEST', 'Debug 1')
        
        summary = logger.get_log_summary()
        
        assert summary['INFO'] == 2
        assert summary['WARNING'] == 1
        assert summary['ERROR'] == 3
        assert summary['DEBUG'] == 1
    
    def test_export_logs_json(self):
        """Test exporting logs in JSON format."""
        logger = ETLLogger.get_instance()
        logger.log_info('TEST', 'Test message')
        
        json_output = logger.export_logs(format='json')
        
        assert isinstance(json_output, str)
        assert 'TEST' in json_output
        assert 'Test message' in json_output
        assert 'INFO' in json_output
    
    def test_export_logs_csv(self):
        """Test exporting logs in CSV format."""
        logger = ETLLogger.get_instance()
        logger.log_info('TEST', 'Test message')
        
        csv_output = logger.export_logs(format='csv')
        
        assert isinstance(csv_output, str)
        assert 'timestamp' in csv_output
        assert 'level' in csv_output
        assert 'component' in csv_output
        assert 'TEST' in csv_output
    
    def test_export_logs_invalid_format(self):
        """Test exporting logs with invalid format."""
        logger = ETLLogger.get_instance()
        
        with pytest.raises(ValueError, match="Unsupported format"):
            logger.export_logs(format='xml')
    
    def test_timestamp_format(self):
        """Test that timestamps are in ISO format."""
        logger = ETLLogger.get_instance()
        logger.log_info('TEST', 'Test message')
        
        logs = logger.get_logs()
        timestamp = logs[0]['timestamp']
        
        # Verify ISO format by parsing
        datetime.fromisoformat(timestamp)
    
    def test_multiple_components(self):
        """Test logging from multiple components."""
        logger = ETLLogger.get_instance()
        
        components = ['EXTRACTOR', 'TRANSFORMER', 'LOADER', 'ORCHESTRATOR']
        
        for component in components:
            logger.log_info(component, f'{component} message')
        
        for component in components:
            logs = logger.get_logs(component=component)
            assert len(logs) == 1
            assert logs[0]['component'] == component
    
    def test_thread_safety(self):
        """Test thread-safe singleton instantiation."""
        import threading
        
        instances = []
        
        def create_instance():
            instances.append(ETLLogger.get_instance())
        
        threads = [threading.Thread(target=create_instance) for _ in range(10)]
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # All instances should be the same object
        assert all(instance is instances[0] for instance in instances)
    
    def test_log_with_details(self):
        """Test logging with additional details."""
        logger = ETLLogger.get_instance()
        logger.log_info(
            'TEST',
            'Main message',
            'Detailed information about the event'
        )
        
        logs = logger.get_logs()
        assert logs[0]['details'] == 'Detailed information about the event'
    
    def test_log_without_details(self):
        """Test logging without details."""
        logger = ETLLogger.get_instance()
        logger.log_info('TEST', 'Main message')
        
        logs = logger.get_logs()
        assert logs[0]['details'] is None


class TestLoggerIntegration:
    """Integration tests for logger with ETL components."""
    
    def test_etl_workflow_logging(self):
        """Test logging throughout an ETL workflow."""
        logger = ETLLogger.get_instance()
        logger.clear_logs()
        
        # Simulate ETL workflow
        logger.log_info('ORCHESTRATOR', 'ETL process started')
        logger.log_info('EXTRACTOR', 'Extracting data from source')
        logger.log_info('EXTRACTOR', 'Extracted 1000 records')
        logger.log_info('TRANSFORMER', 'Starting transformation')
        logger.log_warning('TRANSFORMER', '10 records have missing values')
        logger.log_info('TRANSFORMER', 'Transformation complete')
        logger.log_info('LOADER', 'Loading data to target')
        logger.log_info('LOADER', 'Loaded 990 records successfully')
        logger.log_error('LOADER', '10 records failed to load', 'Connection timeout')
        logger.log_info('ORCHESTRATOR', 'ETL process completed')
        
        # Verify logs
        all_logs = logger.get_logs()
        assert len(all_logs) == 10
        
        summary = logger.get_log_summary()
        assert summary['INFO'] == 8
        assert summary['WARNING'] == 1
        assert summary['ERROR'] == 1
        
        # Verify component-specific logs
        orchestrator_logs = logger.get_logs(component='ORCHESTRATOR')
        assert len(orchestrator_logs) == 2
        
        error_logs = logger.get_logs(level='ERROR')
        assert len(error_logs) == 1
        assert 'Connection timeout' in error_logs[0]['details']


if __name__ == '__main__':
    pytest.main([__file__, '-v'])