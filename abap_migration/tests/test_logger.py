"""
Unit tests for ETL Logger utility.
"""
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from src.logger import ETLLogger


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing."""
    spark = (SparkSession.builder
             .appName("ETLLoggerTest")
             .master("local[2]")
             .getOrCreate())
    yield spark
    spark.stop()


@pytest.fixture(autouse=True)
def reset_logger():
    """Reset logger singleton before each test."""
    ETLLogger._instance = None
    ETLLogger._initialized = False
    yield


class TestETLLogger:
    """Test suite for ETL Logger."""
    
    def test_singleton_pattern(self):
        """Test that logger follows singleton pattern."""
        logger1 = ETLLogger.get_instance()
        logger2 = ETLLogger.get_instance()
        
        assert logger1 is logger2
        assert id(logger1) == id(logger2)
    
    def test_logger_initialization(self):
        """Test logger initializes correctly."""
        logger = ETLLogger()
        
        assert logger is not None
        assert hasattr(logger, '_logs')
        assert isinstance(logger._logs, list)
        assert len(logger._logs) == 0
    
    def test_log_info(self):
        """Test INFO level logging."""
        logger = ETLLogger.get_instance()
        
        logger.log_info(
            component='TEST',
            message='Test info message',
            details='Additional details'
        )
        
        logs = logger.get_logs()
        assert len(logs) == 1
        
        log_entry = logs[0]
        assert log_entry['level'] == 'INFO'
        assert log_entry['component'] == 'TEST'
        assert log_entry['message'] == 'Test info message'
        assert log_entry['details'] == 'Additional details'
        assert 'timestamp' in log_entry
    
    def test_log_error(self):
        """Test ERROR level logging."""
        logger = ETLLogger.get_instance()
        
        logger.log_error(
            component='EXTRACTOR',
            message='Extraction failed',
            details='Connection timeout'
        )
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0]['level'] == 'ERROR'
        assert logs[0]['component'] == 'EXTRACTOR'
    
    def test_log_warning(self):
        """Test WARNING level logging."""
        logger = ETLLogger.get_instance()
        
        logger.log_warning(
            component='TRANSFORMER',
            message='Data quality issue detected'
        )
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0]['level'] == 'WARNING'
    
    def test_log_debug(self):
        """Test DEBUG level logging."""
        logger = ETLLogger.get_instance()
        
        logger.log_debug(
            component='LOADER',
            message='Processing batch 1 of 10'
        )
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0]['level'] == 'DEBUG'
    
    def test_multiple_log_entries(self):
        """Test logging multiple entries."""
        logger = ETLLogger.get_instance()
        
        logger.log_info('COMPONENT1', 'Message 1')
        logger.log_error('COMPONENT2', 'Message 2')
        logger.log_warning('COMPONENT3', 'Message 3')
        logger.log_debug('COMPONENT4', 'Message 4')
        
        logs = logger.get_logs()
        assert len(logs) == 4
        
        # Verify order is maintained
        assert logs[0]['component'] == 'COMPONENT1'
        assert logs[1]['component'] == 'COMPONENT2'
        assert logs[2]['component'] == 'COMPONENT3'
        assert logs[3]['component'] == 'COMPONENT4'
    
    def test_get_logs_by_level(self):
        """Test filtering logs by level."""
        logger = ETLLogger.get_instance()
        
        logger.log_info('TEST', 'Info 1')
        logger.log_info('TEST', 'Info 2')
        logger.log_error('TEST', 'Error 1')
        logger.log_warning('TEST', 'Warning 1')
        logger.log_error('TEST', 'Error 2')
        
        info_logs = logger.get_logs_by_level('INFO')
        error_logs = logger.get_logs_by_level('ERROR')
        warning_logs = logger.get_logs_by_level('WARNING')
        
        assert len(info_logs) == 2
        assert len(error_logs) == 2
        assert len(warning_logs) == 1
    
    def test_get_logs_by_component(self):
        """Test filtering logs by component."""
        logger = ETLLogger.get_instance()
        
        logger.log_info('EXTRACTOR', 'Message 1')
        logger.log_info('EXTRACTOR', 'Message 2')
        logger.log_info('TRANSFORMER', 'Message 3')
        logger.log_error('EXTRACTOR', 'Message 4')
        
        extractor_logs = logger.get_logs_by_component('EXTRACTOR')
        transformer_logs = logger.get_logs_by_component('TRANSFORMER')
        
        assert len(extractor_logs) == 3
        assert len(transformer_logs) == 1
    
    def test_clear_logs(self):
        """Test clearing log storage."""
        logger = ETLLogger.get_instance()
        
        logger.log_info('TEST', 'Message 1')
        logger.log_info('TEST', 'Message 2')
        logger.log_info('TEST', 'Message 3')
        
        assert len(logger.get_logs()) == 3
        
        logger.clear_logs()
        
        # After clear, should only have the "Log storage cleared" message
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0]['message'] == 'Log storage cleared'
    
    def test_get_log_summary(self):
        """Test log summary statistics."""
        logger = ETLLogger.get_instance()
        
        logger.log_info('TEST', 'Info 1')
        logger.log_info('TEST', 'Info 2')
        logger.log_info('TEST', 'Info 3')
        logger.log_error('TEST', 'Error 1')
        logger.log_error('TEST', 'Error 2')
        logger.log_warning('TEST', 'Warning 1')
        logger.log_debug('TEST', 'Debug 1')
        
        summary = logger.get_log_summary()
        
        assert summary['total'] == 7
        assert summary['INFO'] == 3
        assert summary['ERROR'] == 2
        assert summary['WARNING'] == 1
        assert summary['DEBUG'] == 1
    
    def test_timestamp_format(self):
        """Test that timestamp is in ISO format."""
        logger = ETLLogger.get_instance()
        
        logger.log_info('TEST', 'Test message')
        
        logs = logger.get_logs()
        timestamp_str = logs[0]['timestamp']
        
        # Verify can parse as ISO format
        try:
            datetime.fromisoformat(timestamp_str)
            timestamp_valid = True
        except ValueError:
            timestamp_valid = False
        
        assert timestamp_valid
    
    def test_log_without_details(self):
        """Test logging without optional details field."""
        logger = ETLLogger.get_instance()
        
        logger.log_info('TEST', 'Message without details')
        
        logs = logger.get_logs()
        assert logs[0]['details'] == ''
    
    def test_export_to_dataframe(self, spark):
        """Test exporting logs to Spark DataFrame."""
        logger = ETLLogger.get_instance()
        
        logger.log_info('EXTRACTOR', 'Extraction started')
        logger.log_error('LOADER', 'Load failed', 'Connection error')
        logger.log_warning('TRANSFORMER', 'Data quality warning')
        
        df = logger.export_logs_to_dataframe(spark)
        
        assert df.count() == 3
        assert set(df.columns) == {'timestamp', 'level', 'component', 'message', 'details'}
        
        # Verify data types
        schema_dict = {field.name: field.dataType.simpleString() 
                      for field in df.schema.fields}
        assert all(dtype == 'string' for dtype in schema_dict.values())
    
    def test_concurrent_logging(self):
        """Test that singleton works correctly with multiple references."""
        logger1 = ETLLogger.get_instance()
        logger2 = ETLLogger.get_instance()
        
        logger1.log_info('TEST1', 'Message from logger1')
        logger2.log_info('TEST2', 'Message from logger2')
        
        # Both should see same logs
        logs1 = logger1.get_logs()
        logs2 = logger2.get_logs()
        
        assert len(logs1) == 2
        assert len(logs2) == 2
        assert logs1 == logs2
    
    def test_log_entry_immutability(self):
        """Test that returned logs are copies (not references)."""
        logger = ETLLogger.get_instance()
        
        logger.log_info('TEST', 'Original message')
        
        logs = logger.get_logs()
        original_length = len(logs)
        
        # Modify returned list
        logs.append({'timestamp': 'fake', 'level': 'FAKE', 
                    'component': 'FAKE', 'message': 'fake', 'details': ''})
        
        # Original logs should not be affected
        current_logs = logger.get_logs()
        assert len(current_logs) == original_length


class TestLoggerIntegration:
    """Integration tests for logger with ETL components."""
    
    def test_logger_in_etl_flow(self):
        """Test logger usage in simulated ETL flow."""
        logger = ETLLogger.get_instance()
        
        # Simulate ETL process
        logger.log_info('ORCHESTRATOR', 'ETL process started')
        logger.log_info('EXTRACTOR', 'Extracting 1000 records')
        logger.log_info('TRANSFORMER', 'Transforming data')
        logger.log_warning('TRANSFORMER', 'Found 5 null values')
        logger.log_info('LOADER', 'Loading 995 records')
        logger.log_info('ORCHESTRATOR', 'ETL process completed successfully')
        
        logs = logger.get_logs()
        assert len(logs) == 6
        
        # Verify flow
        assert logs[0]['component'] == 'ORCHESTRATOR'
        assert logs[-1]['component'] == 'ORCHESTRATOR'
        assert logs[-1]['message'] == 'ETL process completed successfully'
        
        # Check for warnings
        warnings = logger.get_logs_by_level('WARNING')
        assert len(warnings) == 1
        assert 'null values' in warnings[0]['message']


if __name__ == '__main__':
    pytest.main([__file__, '-v'])