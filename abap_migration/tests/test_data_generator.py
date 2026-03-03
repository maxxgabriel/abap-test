"""
Unit tests for ETL data generator module.
"""

import pytest
from datetime import datetime
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.data_generator import DataGenerator, ConfigGenerator, ScheduleGenerator


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing."""
    spark_session = SparkSession.builder \
        .appName("ETL_Generator_Tests") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    yield spark_session
    
    spark_session.stop()


class TestDataGenerator:
    """Test cases for DataGenerator class."""
    
    def test_generate_source_data_count(self, spark):
        """Test that correct number of records are generated."""
        generator = DataGenerator(spark)
        num_records = 50
        
        df = generator.generate_source_data(num_records=num_records)
        
        assert df.count() == num_records, "Should generate exact number of records"
    
    def test_generate_source_data_schema(self, spark):
        """Test that generated data has correct schema."""
        generator = DataGenerator(spark)
        df = generator.generate_source_data(num_records=10)
        
        expected_columns = [
            "id", "name", "value", "status", "category", 
            "source_system", "created_at", "created_by", 
            "changed_at", "changed_by"
        ]
        
        assert df.columns == expected_columns, "Schema columns should match"
    
    def test_generate_source_data_no_nulls(self, spark):
        """Test that no null values in non-nullable fields."""
        generator = DataGenerator(spark)
        df = generator.generate_source_data(num_records=20)
        
        null_counts = df.select([
            df[col].isNull().cast("int").alias(col) 
            for col in df.columns
        ]).agg(*[f"sum({col})" for col in df.columns]).collect()[0]
        
        assert all(count == 0 for count in null_counts), \
            "Should have no null values in required fields"
    
    def test_generate_source_data_categories(self, spark):
        """Test that generated categories are valid."""
        generator = DataGenerator(spark)
        df = generator.generate_source_data(num_records=100)
        
        categories = df.select("category").distinct().rdd.flatMap(lambda x: x).collect()
        
        for category in categories:
            assert category in generator.CATEGORIES, \
                f"Category {category} should be in valid list"
    
    def test_generate_source_data_value_range(self, spark):
        """Test that generated values are within expected range."""
        generator = DataGenerator(spark)
        df = generator.generate_source_data(num_records=50)
        
        result = df.agg({"value": "min", "value": "max"}).collect()[0]
        min_value = float(result["min(value)"])
        max_value = float(result["max(value)"])
        
        assert 50.0 <= min_value <= 1000.0, "Min value should be in range"
        assert 50.0 <= max_value <= 1000.0, "Max value should be in range"
    
    def test_generate_source_data_reproducibility(self, spark):
        """Test that same seed produces same data."""
        generator = DataGenerator(spark)
        
        df1 = generator.generate_source_data(num_records=10, seed=42)
        df2 = generator.generate_source_data(num_records=10, seed=42)
        
        data1 = df1.collect()
        data2 = df2.collect()
        
        assert data1 == data2, "Same seed should produce identical data"
    
    def test_generate_staging_data(self, spark):
        """Test staging data generation."""
        generator = DataGenerator(spark)
        run_id = "TEST_RUN_001"
        
        df = generator.generate_staging_data(num_records=25, run_id=run_id)
        
        assert df.count() == 25, "Should generate correct number of staging records"
        
        # Check run_id is consistent
        run_ids = df.select("run_id").distinct().collect()
        assert len(run_ids) == 1, "Should have single run_id"
        assert run_ids[0][0] == run_id, "Run ID should match"
    
    def test_generate_staging_data_schema(self, spark):
        """Test staging data schema."""
        generator = DataGenerator(spark)
        df = generator.generate_staging_data(num_records=10)
        
        expected_columns = ["id", "run_id", "status", "raw_data", "created_at"]
        assert df.columns == expected_columns, "Staging schema should match"


class TestConfigGenerator:
    """Test cases for ConfigGenerator class."""
    
    def test_generate_config_count(self, spark):
        """Test that all config entries are generated."""
        generator = ConfigGenerator(spark)
        df = generator.generate_config()
        
        assert df.count() >= 10, "Should generate at least 10 config entries"
    
    def test_generate_config_schema(self, spark):
        """Test config data schema."""
        generator = ConfigGenerator(spark)
        df = generator.generate_config()
        
        expected_columns = [
            "config_key", "config_value", "description", 
            "config_type", "is_active", "changed_at", "changed_by"
        ]
        
        assert df.columns == expected_columns, "Config schema should match"
    
    def test_generate_config_required_keys(self, spark):
        """Test that required config keys are present."""
        generator = ConfigGenerator(spark)
        df = generator.generate_config()
        
        required_keys = [
            "BATCH_SIZE", "MAX_RETRIES", "ALERT_EMAIL", 
            "LOG_RETENTION_DAYS", "ENABLE_RECONCILIATION"
        ]
        
        config_keys = df.select("config_key").rdd.flatMap(lambda x: x).collect()
        
        for key in required_keys:
            assert key in config_keys, f"Config should contain {key}"
    
    def test_generate_config_active_flag(self, spark):
        """Test that config entries have valid active flag."""
        generator = ConfigGenerator(spark)
        df = generator.generate_config()
        
        active_values = df.select("is_active").distinct().rdd.flatMap(lambda x: x).collect()
        
        for value in active_values:
            assert value in ["X", ""], "Active flag should be 'X' or empty"


class TestScheduleGenerator:
    """Test cases for ScheduleGenerator class."""
    
    def test_generate_schedules_count(self, spark):
        """Test that schedules are generated."""
        generator = ScheduleGenerator(spark)
        df = generator.generate_schedules()
        
        assert df.count() >= 5, "Should generate at least 5 schedules"
    
    def test_generate_schedules_schema(self, spark):
        """Test schedule data schema."""
        generator = ScheduleGenerator(spark)
        df = generator.generate_schedules()
        
        expected_columns = [
            "schedule_id", "schedule_name", "etl_type", "frequency",
            "start_date", "start_time", "is_active", "created_by", "created_at"
        ]
        
        assert df.columns == expected_columns, "Schedule schema should match"
    
    def test_generate_schedules_unique_ids(self, spark):
        """Test that schedule IDs are unique."""
        generator = ScheduleGenerator(spark)
        df = generator.generate_schedules()
        
        total_count = df.count()
        unique_count = df.select("schedule_id").distinct().count()
        
        assert total_count == unique_count, "Schedule IDs should be unique"
    
    def test_generate_schedules_etl_types(self, spark):
        """Test that ETL types are valid."""
        generator = ScheduleGenerator(spark)
        df = generator.generate_schedules()
        
        valid_types = ["FULL", "INCREMENTAL", "RECONCILIATION", "ARCHIVE", "STREAMING"]
        etl_types = df.select("etl_type").distinct().rdd.flatMap(lambda x: x).collect()
        
        for etl_type in etl_types:
            assert etl_type in valid_types, f"ETL type {etl_type} should be valid"
    
    def test_generate_schedules_frequencies(self, spark):
        """Test that frequencies are valid."""
        generator = ScheduleGenerator(spark)
        df = generator.generate_schedules()
        
        valid_frequencies = ["DAILY", "HOURLY", "WEEKLY", "MONTHLY", "CONTINUOUS"]
        frequencies = df.select("frequency").distinct().rdd.flatMap(lambda x: x).collect()
        
        for freq in frequencies:
            assert freq in valid_frequencies, f"Frequency {freq} should be valid"


class TestIntegration:
    """Integration tests for complete data generation flow."""
    
    def test_complete_generation_flow(self, spark):
        """Test complete data generation workflow."""
        # Generate all data types
        data_gen = DataGenerator(spark)
        config_gen = ConfigGenerator(spark)
        schedule_gen = ScheduleGenerator(spark)
        
        source_df = data_gen.generate_source_data(num_records=10)
        staging_df = data_gen.generate_staging_data(num_records=5)
        config_df = config_gen.generate_config()
        schedule_df = schedule_gen.generate_schedules()
        
        # Verify all DataFrames created successfully
        assert source_df is not None, "Source data should be created"
        assert staging_df is not None, "Staging data should be created"
        assert config_df is not None, "Config data should be created"
        assert schedule_df is not None, "Schedule data should be created"
        
        # Verify data can be collected
        assert len(source_df.collect()) == 10, "Should collect source records"
        assert len(staging_df.collect()) == 5, "Should collect staging records"
        assert len(config_df.collect()) >= 10, "Should collect config entries"
        assert len(schedule_df.collect()) >= 5, "Should collect schedules"
    
    def test_data_persistence(self, spark, tmp_path):
        """Test that generated data can be saved and loaded."""
        generator = DataGenerator(spark)
        df = generator.generate_source_data(num_records=20)
        
        output_path = str(tmp_path / "test_data.parquet")
        df.write.mode("overwrite").parquet(output_path)
        
        # Load back
        loaded_df = spark.read.parquet(output_path)
        
        assert loaded_df.count() == 20, "Should load same number of records"
        assert loaded_df.columns == df.columns, "Schema should be preserved"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])