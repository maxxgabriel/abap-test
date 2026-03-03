"""
Unit tests for ETL Logger utility
"""
import pytest
from datetime import datetime
from src.logger import ETLLogger, LogEntry, get_logger


class TestETLLogger:
    """Test suite for ETL Logger"""
    
    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        """Setup and teardown for each test"""
        # Setup: Clear logs before each test
        logger = ETLLogger.get_instance()
        logger.clear_logs()
        yield
        # Teardown: Clear logs after each test
        logger.clear_logs()
    
    def test_singleton_pattern(self):
        """Test that logger implements singleton pattern correctly"""
        logger1 = ETLLogger.get_instance()
        logger2 = ETLLogger.get_instance()
        logger3 = get_logger()
        
        assert logger1 is logger2
        assert logger2 is logger3
        assert id(logger1) == id(logger2) == id(logger3)
    
    def test_log_info(self):
        """Test INFO level logging"""
        logger = get_logger()
        
        logger.log_info('TEST_COMPONENT', 'Test info message')
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0].level == 'INFO'
        assert logs[0].component == 'TEST_COMPONENT'
        assert logs[0].message == 'Test info message'
    
    def test_log_warning(self):
        """Test WARNING level logging"""
        logger = get_logger()
        
        logger.log_warning('TEST_COMPONENT', 'Test warning', 'Warning details')
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0].level == 'WARNING'
        assert logs[0].details == 'Warning details'
    
    def test_log_error(self):
        """Test ERROR level logging"""
        logger = get_logger()
        
        logger.log_error('TEST_COMPONENT', 'Test error', 'Error stack trace')
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0].level == 'ERROR'
        assert logs[0].component == 'TEST_COMPONENT'
    
    def test_log_debug(self):
        """Test DEBUG level logging"""
        logger = get_logger()
        
        logger.log_debug('TEST_COMPONENT', 'Debug message')
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0].level == 'DEBUG'
    
    def test_multiple_logs(self):
        """Test logging multiple entries"""
        logger = get_logger()
        
        logger.log_info('EXTRACTOR', 'Starting extraction')
        logger.log_info('TRANSFORMER', 'Starting transformation')
        logger.log_error('LOADER', 'Load failed')
        logger.log_warning('ORCHESTRATOR', 'Retry attempt')
        
        logs = logger.get_logs()
        assert len(logs) == 4
        assert logs[0].component == 'EXTRACTOR'
        assert logs[3].component == 'ORCHESTRATOR'
    
    def test_filter_by_level(self):
        """Test filtering logs by level"""
        logger = get_logger()
        
        logger.log_info('TEST', 'Info 1')
        logger.log_error('TEST', 'Error 1')
        logger.log_info('TEST', 'Info 2')
        logger.log_error('TEST', 'Error 2')
        
        info_logs = logger.get_logs(level='INFO')
        error_logs = logger.get_logs(level='ERROR')
        
        assert len(info_logs) == 2
        assert len(error_logs) == 2
        assert all(log.level == 'INFO' for log in info_logs)
        assert all(log.level == 'ERROR' for log in error_logs)
    
    def test_filter_by_component(self):
        """Test filtering logs by component"""
        logger = get_logger()
        
        logger.log_info('EXTRACTOR', 'Extract message 1')
        logger.log_info('TRANSFORMER', 'Transform message')
        logger.log_info('EXTRACTOR', 'Extract message 2')
        logger.log_info('LOADER', 'Load message')
        
        extractor_logs = logger.get_logs(component='EXTRACTOR')
        transformer_logs = logger.get_logs(component='TRANSFORMER')
        
        assert len(extractor_logs) == 2
        assert len(transformer_logs) == 1
        assert all(log.component == 'EXTRACTOR' for log in extractor_logs)
    
    def test_filter_with_limit(self):
        """Test limiting number of returned logs"""
        logger = get_logger()
        
        for i in range(10):
            logger.log_info('TEST', f'Message {i}')
        
        logs = logger.get_logs(limit=5)
        assert len(logs) == 5
        # Should return last 5 entries
        assert logs[0].message == 'Message 5'
        assert logs[4].message == 'Message 9'
    
    def test_get_logs_as_dicts(self):
        """Test getting logs as dictionaries"""
        logger = get_logger()
        
        logger.log_info('TEST', 'Test message', 'Test details')
        
        logs = logger.get_logs_as_dicts()
        assert len(logs) == 1
        assert isinstance(logs[0], dict)
        assert 'timestamp' in logs[0]
        assert 'level' in logs[0]
        assert 'component' in logs[0]
        assert 'message' in logs[0]
        assert logs[0]['level'] == 'INFO'
    
    def test_clear_logs(self):
        """Test clearing log entries"""
        logger = get_logger()
        
        logger.log_info('TEST', 'Message 1')
        logger.log_info('TEST', 'Message 2')
        assert logger.get_log_count() == 2
        
        logger.clear_logs()
        # Clear logs also adds a log entry about clearing
        assert logger.get_log_count() == 1
        assert logger.get_logs()[0].message == 'Log entries cleared'
    
    def test_get_log_count(self):
        """Test getting log count"""
        logger = get_logger()
        
        assert logger.get_log_count() == 0
        
        logger.log_info('TEST', 'Message 1')
        assert logger.get_log_count() == 1
        
        logger.log_error('TEST', 'Message 2')
        assert logger.get_log_count() == 2
    
    def test_get_log_statistics(self):
        """Test getting log statistics"""
        logger = get_logger()
        
        logger.log_info('TEST', 'Info 1')
        logger.log_info('TEST', 'Info 2')
        logger.log_warning('TEST', 'Warning 1')
        logger.log_error('TEST', 'Error 1')
        logger.log_debug('TEST', 'Debug 1')
        
        stats = logger.get_log_statistics()
        
        assert stats['total'] == 5
        assert stats['info'] == 2
        assert stats['warning'] == 1
        assert stats['error'] == 1
        assert stats['debug'] == 1
    
    def test_format_logs_for_display(self):
        """Test formatting logs for display"""
        logger = get_logger()
        
        logger.log_info('EXTRACTOR', 'Extract started')
        logger.log_error('LOADER', 'Load failed', 'Connection timeout')
        
        formatted = logger.format_logs_for_display()
        
        assert 'ETL Logger - Log Entries' in formatted
        assert 'EXTRACTOR' in formatted
        assert 'Extract started' in formatted
        assert 'LOADER' in formatted
        assert 'Load failed' in formatted
        assert 'Connection timeout' in formatted
    
    def test_empty_logs_display(self):
        """Test formatting when no logs exist"""
        logger = get_logger()
        
        formatted = logger.format_logs_for_display()
        assert formatted == "No log entries found"
    
    def test_log_entry_timestamp(self):
        """Test that log entries have proper timestamps"""
        logger = get_logger()
        
        before = datetime.now()
        logger.log_info('TEST', 'Test message')
        after = datetime.now()
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert before <= logs[0].timestamp <= after
    
    def test_thread_safety_simulation(self):
        """Test logger behavior with multiple sequential operations"""
        logger = get_logger()
        
        # Simulate multiple components logging
        components = ['EXTRACTOR', 'TRANSFORMER', 'LOADER', 'ORCHESTRATOR']
        
        for i in range(100):
            component = components[i % len(components)]
            logger.log_info(component, f'Message {i}')
        
        logs = logger.get_logs()
        assert len(logs) == 100
        
        # Verify all components are present
        for component in components:
            component_logs = logger.get_logs(component=component)
            assert len(component_logs) == 25
    
    def test_combined_filters(self):
        """Test using multiple filters together"""
        logger = get_logger()
        
        logger.log_info('EXTRACTOR', 'Extract 1')
        logger.log_error('EXTRACTOR', 'Extract error')
        logger.log_info('TRANSFORMER', 'Transform 1')
        logger.log_info('EXTRACTOR', 'Extract 2')
        
        # Filter by both level and component
        filtered = logger.get_logs(level='INFO', component='EXTRACTOR')
        
        assert len(filtered) == 2
        assert all(log.level == 'INFO' for log in filtered)
        assert all(log.component == 'EXTRACTOR' for log in filtered)
    
    def test_log_entry_to_dict(self):
        """Test LogEntry to_dict method"""
        entry = LogEntry(
            timestamp=datetime.now(),
            level='INFO',
            component='TEST',
            message='Test message',
            details='Test details'
        )
        
        entry_dict = entry.to_dict()
        
        assert isinstance(entry_dict, dict)
        assert 'timestamp' in entry_dict
        assert entry_dict['level'] == 'INFO'
        assert entry_dict['component'] == 'TEST'
        assert entry_dict['message'] == 'Test message'
        assert entry_dict['details'] == 'Test details'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])