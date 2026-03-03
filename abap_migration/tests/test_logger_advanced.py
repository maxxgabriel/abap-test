"""
Advanced unit tests for ETL Logger - edge cases and performance.
"""
import pytest
import time
from concurrent.futures import ThreadPoolExecutor
from src.logger import ETLLogger


@pytest.fixture(autouse=True)
def reset_logger():
    """Reset logger singleton before each test."""
    ETLLogger._instance = None
    ETLLogger._initialized = False
    yield


class TestLoggerEdgeCases:
    """Test edge cases and error handling."""
    
    def test_empty_component_name(self):
        """Test logging with empty component name."""
        logger = ETLLogger.get_instance()
        
        logger.log_info('', 'Message with empty component')
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0]['component'] == ''
    
    def test_empty_message(self):
        """Test logging with empty message."""
        logger = ETLLogger.get_instance()
        
        logger.log_info('TEST', '')
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0]['message'] == ''
    
    def test_very_long_message(self):
        """Test logging with very long message."""
        logger = ETLLogger.get_instance()
        
        long_message = 'A' * 10000
        logger.log_info('TEST', long_message)
        
        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0]['message'] == long_message
    
    def test_special_characters_in_message(self):
        """Test logging with special characters."""
        logger = ETLLogger.get_instance()
        
        special_msg = "Test with special chars: \n\t\r \"'<>&"
        logger.log_info('TEST', special_msg)
        
        logs = logger.get_logs()
        assert logs[0]['message'] == special_msg
    
    def test_unicode_characters(self):
        """Test logging with unicode characters."""
        logger = ETLLogger.get_instance()
        
        unicode_msg = "Test unicode: 你好 🚀 ñ ü"
        logger.log_info('TEST', unicode_msg)
        
        logs = logger.get_logs()
        assert logs[0]['message'] == unicode_msg
    
    def test_none_details(self):
        """Test that None details is handled correctly."""
        logger = ETLLogger.get_instance()
        
        logger.log_info('TEST', 'Message', None)
        
        logs = logger.get_logs()
        assert logs[0]['details'] == ''


class TestLoggerPerformance:
    """Performance and stress tests."""
    
    def test_large_number_of_logs(self):
        """Test handling large number of log entries."""
        logger = ETLLogger.get_instance()
        
        num_logs = 10000
        start_time = time.time()
        
        for i in range(num_logs):
            logger.log_info('TEST', f'Message {i}')
        
        duration = time.time() - start_time
        
        logs = logger.get_logs()
        assert len(logs) == num_logs
        
        # Should complete in reasonable time (< 5 seconds)
        assert duration < 5.0
    
    def test_memory_efficiency(self):
        """Test memory usage with many logs."""
        logger = ETLLogger.get_instance()
        
        # Log 1000 entries
        for i in range(1000):
            logger.log_info('TEST', f'Test message {i}', f'Details {i}')
        
        logs = logger.get_logs()
        assert len(logs) == 1000
        
        # Clear and verify memory is released
        logger.clear_logs()
        logs_after_clear = logger.get_logs()
        
        # Only the "cleared" message should remain
        assert len(logs_after_clear) == 1
    
    def test_concurrent_access(self):
        """Test thread-safe concurrent logging."""
        logger = ETLLogger.get_instance()
        
        def log_messages(thread_id):
            for i in range(100):
                logger.log_info(f'THREAD_{thread_id}', f'Message {i}')
        
        # Run 10 threads concurrently
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(log_messages, i) for i in range(10)]
            for future in futures:
                future.result()
        
        logs = logger.get_logs()
        
        # Should have 1000 total messages (10 threads * 100 messages)
        assert len(logs) == 1000
        
        # Verify all thread components are present
        components = set(log['component'] for log in logs)
        expected_components = {f'THREAD_{i}' for i in range(10)}
        assert components == expected_components


class TestLoggerFiltering:
    """Test advanced filtering capabilities."""
    
    def test_complex_filtering_scenario(self):
        """Test filtering with multiple criteria."""
        logger = ETLLogger.get_instance()
        
        # Create diverse log entries
        logger.log_info('EXTRACTOR', 'Extract started')
        logger.log_error('EXTRACTOR', 'Extract failed', 'Network error')
        logger.log_info('TRANSFORMER', 'Transform started')
        logger.log_warning('TRANSFORMER', 'Null values found')
        logger.log_info('LOADER', 'Load started')
        logger.log_error('LOADER', 'Load failed', 'Database error')
        
        # Test level filtering
        errors = logger.get_logs_by_level('ERROR')
        assert len(errors) == 2
        assert all('failed' in log['message'] for log in errors)
        
        # Test component filtering
        extractor_logs = logger.get_logs_by_component('EXTRACTOR')
        assert len(extractor_logs) == 2
        
        # Combine filters manually
        extractor_errors = [
            log for log in logger.get_logs()
            if log['component'] == 'EXTRACTOR' and log['level'] == 'ERROR'
        ]
        assert len(extractor_errors) == 1
        assert extractor_errors[0]['details'] == 'Network error'
    
    def test_summary_with_mixed_levels(self):
        """Test summary statistics with various log levels."""
        logger = ETLLogger.get_instance()
        
        # Create specific distribution
        for _ in range(5):
            logger.log_info('TEST', 'Info')
        for _ in range(3):
            logger.log_error('TEST', 'Error')
        for _ in range(2):
            logger.log_warning('TEST', 'Warning')
        for _ in range(1):
            logger.log_debug('TEST', 'Debug')
        
        summary = logger.get_log_summary()
        
        assert summary['total'] == 11
        assert summary['INFO'] == 5
        assert summary['ERROR'] == 3
        assert summary['WARNING'] == 2
        assert summary['DEBUG'] == 1


class TestLoggerDataFrame:
    """Test DataFrame export functionality."""
    
    def test_dataframe_schema_validation(self, spark):
        """Test that exported DataFrame has correct schema."""
        from pyspark.sql.types import StringType
        
        logger = ETLLogger.get_instance()
        
        logger.log_info('TEST', 'Test message', 'Test details')
        
        df = logger.export_logs_to_dataframe(spark)
        
        # Check schema
        expected_fields = {'timestamp', 'level', 'component', 'message', 'details'}
        actual_fields = set(df.schema.fieldNames())
        
        assert actual_fields == expected_fields
        
        # Verify all fields are strings
        for field in df.schema.fields:
            assert isinstance(field.dataType, StringType)
    
    def test_dataframe_content_accuracy(self, spark):
        """Test that DataFrame content matches log entries."""
        logger = ETLLogger.get_instance()
        
        test_data = [
            ('INFO', 'COMP1', 'Msg1', 'Det1'),
            ('ERROR', 'COMP2', 'Msg2', 'Det2'),
            ('WARNING', 'COMP3', 'Msg3', '')
        ]
        
        for level, comp, msg, det in test_data:
            if level == 'INFO':
                logger.log_info(comp, msg, det if det else None)
            elif level == 'ERROR':
                logger.log_error(comp, msg, det if det else None)
            elif level == 'WARNING':
                logger.log_warning(comp, msg, det if det else None)
        
        df = logger.export_logs_to_dataframe(spark)
        collected = df.collect()
        
        assert len(collected) == 3
        
        for i, row in enumerate(collected):
            expected = test_data[i]
            assert row['level'] == expected[0]
            assert row['component'] == expected[1]
            assert row['message'] == expected[2]
            assert row['details'] == expected[3]


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing."""
    from pyspark.sql import SparkSession
    
    spark = (SparkSession.builder
             .appName("ETLLoggerAdvancedTest")
             .master("local[2]")
             .getOrCreate())
    yield spark
    spark.stop()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])