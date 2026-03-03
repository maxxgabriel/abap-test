"""
Unit tests for ETL extraction functionality using test framework utilities.
"""
import pytest
from pyspark.sql.functions import col, count
from decimal import Decimal

from src.extract import Extractor
from src.test_utils import (
    TestDataConfig,
    DataFrameAssertions,
    create_test_config
)


class TestExtractor:
    """Test suite for Extractor module."""
    
    def test_extract_basic(self, spark_session, test_data_generator):
        """Test basic extraction from source."""
        # Arrange
        config = create_test_config({"source_type": "DATABASE", "run_id": "TEST001"})
        extractor = Extractor(spark_session, config)
        
        # Generate and register test data
        test_config = TestDataConfig(num_records=10)
        source_df = test_data_generator.generate_source_data(spark_session, test_config)
        source_df.createOrReplaceTempView("source_data")
        
        # Act
        result_df = extractor.extract_data()
        
        # Assert
        DataFrameAssertions.assert_row_count(result_df, 10)
        DataFrameAssertions.assert_column_exists(result_df, "id")
        DataFrameAssertions.assert_no_nulls(result_df, "id")
    
    def test_extract_with_filter(self, spark_session, test_data_generator):
        """Test extraction with filter conditions."""
        # Arrange
        config = create_test_config({
            "source_type": "DATABASE",
            "run_id": "TEST002",
            "filter": "status = 'ACTIVE'"
        })
        extractor = Extractor(spark_session, config)
        
        # Generate test data
        test_config = TestDataConfig(num_records=20)
        source_df = test_data_generator.generate_source_data(spark_session, test_config)
        source_df.createOrReplaceTempView("source_data")
        
        # Act
        result_df = extractor.extract_data(filter_condition="status = 'ACTIVE'")
        
        # Assert
        assert result_df.count() > 0
        assert result_df.filter(col("status") != "ACTIVE").count() == 0
    
    def test_extract_with_max_records(self, spark_session, test_data_generator):
        """Test extraction with max records limit."""
        # Arrange
        config = create_test_config({"run_id": "TEST003"})
        extractor = Extractor(spark_session, config)
        
        test_config = TestDataConfig(num_records=50)
        source_df = test_data_generator.generate_source_data(spark_session, test_config)
        source_df.createOrReplaceTempView("source_data")
        
        # Act
        result_df = extractor.extract_data(max_records=20)
        
        # Assert
        DataFrameAssertions.assert_row_count(result_df, 20)
    
    def test_extract_empty_source(self, spark_session):
        """Test extraction from empty source."""
        # Arrange
        config = create_test_config({"run_id": "TEST004"})
        extractor = Extractor(spark_session, config)
        
        # Create empty source
        empty_df = spark_session.createDataFrame([], schema=extractor.get_source_schema())
        empty_df.createOrReplaceTempView("source_data")
        
        # Act
        result_df = extractor.extract_data()
        
        # Assert
        DataFrameAssertions.assert_row_count(result_df, 0)
    
    def test_extract_incremental(self, spark_session, test_data_generator):
        """Test incremental extraction."""
        from datetime import datetime, timedelta
        
        # Arrange
        cutoff_time = datetime.now() - timedelta(hours=24)
        config = create_test_config({
            "source_type": "INCREMENTAL",
            "run_id": "TEST005",
            "last_run_time": cutoff_time
        })
        extractor = Extractor(spark_session, config)
        
        # Generate test data with recent timestamps
        test_config = TestDataConfig(num_records=30)
        source_df = test_data_generator.generate_source_data(spark_session, test_config)
        source_df.createOrReplaceTempView("source_data")
        
        # Act
        result_df = extractor.extract_incremental(last_run_time=cutoff_time)
        
        # Assert
        assert result_df.count() > 0
        # All records should have changed_at > cutoff_time
        old_records = result_df.filter(col("changed_at") <= cutoff_time).count()
        assert old_records == 0
    
    def test_extract_with_nulls(self, spark_session, test_data_generator):
        """Test extraction handles null values correctly."""
        # Arrange
        config = create_test_config({"run_id": "TEST006"})
        extractor = Extractor(spark_session, config)
        
        test_config = TestDataConfig(num_records=20, include_nulls=True)
        source_df = test_data_generator.generate_source_data(spark_session, test_config)
        source_df.createOrReplaceTempView("source_data")
        
        # Act
        result_df = extractor.extract_data()
        
        # Assert
        DataFrameAssertions.assert_row_count(result_df, 20)
        # Should have some null values
        null_count = result_df.filter(col("name").isNull()).count()
        assert null_count > 0
    
    def test_extract_schema_validation(self, spark_session, test_data_generator):
        """Test that extracted data matches expected schema."""
        # Arrange
        config = create_test_config({"run_id": "TEST007"})
        extractor = Extractor(spark_session, config)
        
        test_config = TestDataConfig(num_records=5)
        source_df = test_data_generator.generate_source_data(spark_session, test_config)
        source_df.createOrReplaceTempView("source_data")
        
        # Act
        result_df = extractor.extract_data()
        
        # Assert
        expected_schema = extractor.get_source_schema()
        actual_fields = {f.name: f.dataType for f in result_df.schema.fields}
        expected_fields = {f.name: f.dataType for f in expected_schema.fields}
        
        assert actual_fields == expected_fields
    
    def test_extract_logs_metrics(self, test_run_context, test_data_generator):
        """Test that extraction logs appropriate metrics."""
        # Arrange
        spark = test_run_context.spark_context.spark
        config = create_test_config({"run_id": test_run_context.run_id})
        extractor = Extractor(spark, config)
        
        test_config = TestDataConfig(num_records=15)
        source_df = test_data_generator.generate_source_data(spark, test_config)
        source_df.createOrReplaceTempView("source_data")
        
        # Act
        result_df = extractor.extract_data()
        
        # Assert
        assert result_df.count() == 15
        # In real implementation, check logged metrics
        test_run_context.record_metric("extracted_count", result_df.count())
        assert test_run_context.get_metric("extracted_count") == 15