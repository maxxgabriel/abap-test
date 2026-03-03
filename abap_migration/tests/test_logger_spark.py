"""
Integration tests for ETL Logger with PySpark
"""
import pytest
from pyspark.sql import SparkSession
from src.logger import get_logger


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("ETLLoggerTest") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


class TestETLLoggerSpark:
    """Test suite for ETL Logger with Spark integration"""
    
    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        """Setup and teardown for each test"""
        logger = get_logger()
        logger.clear_logs()
        yield
        logger.clear_logs()
    
    def test_export_logs_to_spark_df(self, spark):
        """Test exporting logs to Spark DataFrame"""
        logger = get_logger()
        
        # Create sample logs
        logger.log_info('EXTRACTOR', 'Extract started')
        logger.log_warning('TRANSFORMER', 'Missing values detected')
        logger.log_error('LOADER', 'Connection failed', 'Timeout error')
        
        # Export to DataFrame
        df = logger.export_logs_to_spark_df(spark)
        
        assert df.count() == 3
        assert set(df.columns) == {'timestamp', 'level', 'component', 'message', 'details'}
        
        # Check data types
        schema = df.schema
        assert schema['timestamp'].dataType.typeName() == 'timestamp'
        assert schema['level'].dataType.typeName() == 'string'
        assert schema['component'].dataType.typeName() == 'string'
        assert schema['message'].dataType.typeName() == 'string'
        assert schema['details'].dataType.typeName() == 'string'
    
    def test_spark_df_content(self, spark):
        """Test content of exported Spark DataFrame"""
        logger = get_logger()
        
        logger.log_info('TEST', 'Test message 1')
        logger.log_error('TEST', 'Test message 2', 'Error details')
        
        df = logger.export_logs_to_spark_df(spark)
        rows = df.collect()
        
        assert len(rows) == 2
        assert rows[0]['level'] == 'INFO'
        assert rows[0]['component'] == 'TEST'
        assert rows[0]['message'] == 'Test message 1'
        assert rows[0]['details'] is None
        
        assert rows[1]['level'] == 'ERROR'
        assert rows[1]['details'] == 'Error details'
    
    def test_spark_df_filtering(self, spark):
        """Test filtering exported DataFrame"""
        logger = get_logger()
        
        logger.log_info('EXTRACTOR', 'Extract 1')
        logger.log_error('EXTRACTOR', 'Extract error')
        logger.log_info('LOADER', 'Load 1')
        
        df = logger.export_logs_to_spark_df(spark)
        
        # Filter by level
        error_df = df.filter(df.level == 'ERROR')
        assert error_df.count() == 1
        
        # Filter by component
        extractor_df = df.filter(df.component == 'EXTRACTOR')
        assert extractor_df.count() == 2
    
    def test_spark_df_aggregation(self, spark):
        """Test aggregation on exported DataFrame"""
        logger = get_logger()
        
        for i in range(10):
            if i % 3 == 0:
                logger.log_error('TEST', f'Error {i}')
            else:
                logger.log_info('TEST', f'Info {i}')
        
        df = logger.export_logs_to_spark_df(spark)
        
        # Count by level
        level_counts = df.groupBy('level').count().collect()
        level_dict = {row['level']: row['count'] for row in level_counts}
        
        assert level_dict['ERROR'] == 4  # indices 0, 3, 6, 9
        assert level_dict['INFO'] == 6
    
    def test_empty_logs_export(self, spark):
        """Test exporting empty logs to DataFrame"""
        logger = get_logger()
        
        df = logger.export_logs_to_spark_df(spark)
        
        assert df.count() == 0
        assert df.columns == ['timestamp', 'level', 'component', 'message', 'details']


if __name__ == '__main__':
    pytest.main([__file__, '-v'])