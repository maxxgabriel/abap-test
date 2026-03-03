"""
Unit tests for Extract module
"""

import pytest
from decimal import Decimal
from datetime import datetime
from pyspark.sql.functions import col
from src.extract import Extractor
from src.test_utils import (
    SparkTestFixture,
    DataFrameAssertions,
    TestDataFactory,
    TestSchemas,
    MockLogger
)


class TestExtractor:
    """Test suite for Extractor class"""
    
    def test_extract_from_database(self, spark_fixture: SparkTestFixture, mock_logger: MockLogger):
        """Test extracting data from database"""
        # Setup
        spark_fixture.create_source_table(count=10)
        extractor = Extractor(
            spark=spark_fixture.spark,
            source_type="DATABASE",
            run_id="TEST001",
            logger=mock_logger
        )
        
        # Execute
        df = extractor.extract_data()
        
        # Assert
        assert df is not None
        assert df.count() == 10
        assert "id" in df.columns
        assert "name" in df.columns
        mock_logger.assert_logged("INFO", "EXTRACTOR", "Starting extraction")
    
    def test_extract_with_filter(self, spark_fixture: SparkTestFixture):
        """Test extraction with filter"""
        # Setup
        spark_fixture.create_source_table(count=20, category="PREMIUM", status="ACTIVE")
        extractor = Extractor(
            spark=spark_fixture.spark,
            source_type="DATABASE",
            run_id="TEST002"
        )
        
        # Execute
        df = extractor.extract_data(filter_expr="status = 'ACTIVE'")
        
        # Assert
        assert df.count() == 20
        active_count = df.filter(col("status") == "ACTIVE").count()
        assert active_count == 20
    
    def test_extract_with_max_records(self, spark_fixture: SparkTestFixture):
        """Test extraction with record limit"""
        # Setup
        spark_fixture.create_source_table(count=100)
        extractor = Extractor(
            spark=spark_fixture.spark,
            source_type="DATABASE",
            run_id="TEST003"
        )
        
        # Execute
        df = extractor.extract_data(max_records=50)
        
        # Assert
        assert df.count() == 50
    
    def test_extract_incremental(self, spark_fixture: SparkTestFixture):
        """Test incremental extraction"""
        # Setup
        base_time = datetime.now()
        data = TestDataFactory.create_source_records(count=10)
        # Modify timestamps for half the records
        for i in range(5):
            data[i]["changed_at"] = base_time
        
        df = spark_fixture.create_temp_table(
            "incremental_source",
            data,
            TestSchemas.source_schema()
        )
        
        extractor = Extractor(
            spark=spark_fixture.spark,
            source_type="INCREMENTAL",
            run_id="TEST004"
        )
        
        # Execute
        df_incremental = extractor.extract_incremental(last_run_time=base_time)
        
        # Assert
        assert df_incremental.count() <= 10
    
    def test_extract_empty_source(self, spark_fixture: SparkTestFixture, mock_logger: MockLogger):
        """Test extraction from empty source"""
        # Setup - create empty table
        spark_fixture.create_temp_table(
            "empty_source",
            [],
            TestSchemas.source_schema()
        )
        
        extractor = Extractor(
            spark=spark_fixture.spark,
            source_type="DATABASE",
            run_id="TEST005",
            logger=mock_logger
        )
        
        # Execute
        df = extractor.extract_data()
        
        # Assert
        assert df.count() == 0
        mock_logger.assert_logged("WARNING", "EXTRACTOR", "extracted 0 records")
    
    def test_extract_with_invalid_source_type(self, spark_fixture: SparkTestFixture):
        """Test extraction with invalid source type"""
        # Setup
        extractor = Extractor(
            spark=spark_fixture.spark,
            source_type="INVALID_TYPE",
            run_id="TEST006"
        )
        
        # Execute & Assert
        with pytest.raises(ValueError):
            extractor.extract_data()
    
    def test_extract_schema_validation(
        self,
        spark_fixture: SparkTestFixture,
        df_assertions: DataFrameAssertions
    ):
        """Test extracted data has correct schema"""
        # Setup
        spark_fixture.create_source_table(count=5)
        extractor = Extractor(
            spark=spark_fixture.spark,
            source_type="DATABASE",
            run_id="TEST007"
        )
        
        # Execute
        df = extractor.extract_data()
        
        # Assert
        df_assertions.assert_dataframe_schema(df, TestSchemas.source_schema())
        df_assertions.assert_column_exists(df, "id")
        df_assertions.assert_column_exists(df, "name")
        df_assertions.assert_column_exists(df, "value")