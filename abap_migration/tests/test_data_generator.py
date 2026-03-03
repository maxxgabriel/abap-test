"""
Unit tests for ETL Data Generator
"""
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.data_generator import ETLDataGenerator


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    spark_session = SparkSession.builder \
        .appName("ETL Data Generator Tests") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    yield spark_session
    spark_session.stop()


@pytest.fixture
def generator(spark):
    """Create data generator instance"""
    return ETLDataGenerator(spark)


class TestSourceDataGeneration:
    """Test source data generation"""
    
    def test_generate_source_data_creates_records(self, generator):
        """Test that source data generation creates expected number of records"""
        num_records = 50
        df = generator.generate_source_data(num_records)
        
        assert df.count() == num_records, f"Expected {num_records} records"
    
    def test_source_data_has_correct_schema(self, generator):
        """Test that generated data has correct schema"""
        df = generator.generate_source_data(10)
        
        expected_fields = [
            "id", "name", "value", "status", "category", 
            "source_system", "created_at", "created_by", 
            "changed_at", "changed_by"
        ]
        
        actual_fields = df.columns
        assert set(expected_fields) == set(actual_fields), "Schema mismatch"
    
    def test_source_data_has_no_null_ids(self, generator):
        """Test that all IDs are populated"""
        df = generator.generate_source_data(20)
        null_count = df.filter(df.id.isNull()).count()
        
        assert null_count == 0, "Found null IDs"
    
    def test_source_data_categories_are_valid(self, generator):
        """Test that categories are from valid list"""
        df = generator.generate_source_data(30)
        
        categories = df.select("category").distinct().rdd.flatMap(lambda x: x).collect()
        
        for cat in categories:
            assert cat in generator.CATEGORIES, f"Invalid category: {cat}"
    
    def test_source_data_values_in_range(self, generator):
        """Test that values are within expected range"""
        df = generator.generate_source_data(25)
        
        stats = df.select("value").describe().collect()
        min_val = float([row for row in stats if row[0] == 'min'][0][1])
        max_val = float([row for row in stats if row[0] == 'max'][0][1])
        
        assert min_val >= 50, f"Minimum value {min_val} below expected 50"
        assert max_val <= 1000, f"Maximum value {max_val} above expected 1000"
    
    def test_source_data_ids_are_unique(self, generator):
        """Test that generated IDs are unique"""
        df = generator.generate_source_data(40)
        
        total_count = df.count()
        unique_count = df.select("id").distinct().count()
        
        assert total_count == unique_count, "Duplicate IDs found"


class TestConfigDataGeneration:
    """Test configuration data generation"""
    
    def test_generate_config_data_creates_entries(self, generator):
        """Test that config generation creates entries"""
        df = generator.generate_config_data()
        
        assert df.count() > 0, "No config entries generated"
    
    def test_config_data_has_required_keys(self, generator):
        """Test that essential config keys are present"""
        df = generator.generate_config_data()
        
        required_keys = [
            "BATCH_SIZE", "MAX_RETRIES", "ALERT_EMAIL", 
            "LOG_RETENTION_DAYS", "ENABLE_RECONCILIATION"
        ]
        
        config_keys = df.select("config_key").rdd.flatMap(lambda x: x).collect()
        
        for key in required_keys:
            assert key in config_keys, f"Missing required config key: {key}"
    
    def test_config_data_all_active(self, generator):
        """Test that all generated configs are active"""
        df = generator.generate_config_data()
        
        inactive_count = df.filter(df.is_active != "X").count()
        
        assert inactive_count == 0, "Found inactive config entries"
    
    def test_config_data_has_descriptions(self, generator):
        """Test that all configs have descriptions"""
        df = generator.generate_config_data()
        
        null_desc_count = df.filter(df.description.isNull()).count()
        
        assert null_desc_count == 0, "Found configs without descriptions"
    
    def test_config_data_numeric_values_valid(self, generator):
        """Test that numeric config values are valid"""
        df = generator.generate_config_data()
        
        numeric_configs = df.filter(
            df.config_key.isin(["BATCH_SIZE", "MAX_RETRIES", "LOG_RETENTION_DAYS"])
        ).collect()
        
        for row in numeric_configs:
            try:
                int_val = int(row.config_value)
                assert int_val > 0, f"Invalid numeric value for {row.config_key}"
            except ValueError:
                pytest.fail(f"Non-numeric value for {row.config_key}: {row.config_value}")


class TestScheduleDataGeneration:
    """Test schedule data generation"""
    
    def test_generate_schedule_data_creates_schedules(self, generator):
        """Test that schedule generation creates entries"""
        df = generator.generate_schedule_data()
        
        assert df.count() > 0, "No schedule entries generated"
    
    def test_schedule_data_has_unique_ids(self, generator):
        """Test that schedule IDs are unique"""
        df = generator.generate_schedule_data()
        
        total_count = df.count()
        unique_count = df.select("schedule_id").distinct().count()
        
        assert total_count == unique_count, "Duplicate schedule IDs found"
    
    def test_schedule_data_has_valid_frequencies(self, generator):
        """Test that frequencies are valid"""
        df = generator.generate_schedule_data()
        
        valid_frequencies = ["DAILY", "HOURLY", "WEEKLY", "CONTINUOUS"]
        frequencies = df.select("frequency").distinct().rdd.flatMap(lambda x: x).collect()
        
        for freq in frequencies:
            assert freq in valid_frequencies, f"Invalid frequency: {freq}"
    
    def test_schedule_data_times_formatted_correctly(self, generator):
        """Test that times are in correct format"""
        df = generator.generate_schedule_data()
        
        times = df.select("start_time").rdd.flatMap(lambda x: x).collect()
        
        for time_str in times:
            assert len(time_str) == 6, f"Invalid time format: {time_str}"
            assert time_str.isdigit(), f"Time contains non-digits: {time_str}"


class TestStagingDataGeneration:
    """Test staging data generation"""
    
    def test_generate_staging_data_creates_records(self, generator):
        """Test that staging data generation creates records"""
        df = generator.generate_staging_data("TEST_RUN_001", 30)
        
        assert df.count() == 30, "Incorrect number of staging records"
    
    def test_staging_data_has_run_id(self, generator):
        """Test that all staging records have correct run_id"""
        run_id = "TEST_RUN_123"
        df = generator.generate_staging_data(run_id, 20)
        
        distinct_run_ids = df.select("run_id").distinct().collect()
        
        assert len(distinct_run_ids) == 1, "Multiple run_ids found"
        assert distinct_run_ids[0][0] == run_id, f"Incorrect run_id"
    
    def test_staging_data_has_valid_statuses(self, generator):
        """Test that staging statuses are valid"""
        df = generator.generate_staging_data("RUN_001", 25)
        
        valid_statuses = ["READY", "PROCESSING", "COMPLETED"]
        statuses = df.select("status").distinct().rdd.flatMap(lambda x: x).collect()
        
        for status in statuses:
            assert status in valid_statuses, f"Invalid status: {status}"
    
    def test_staging_data_raw_data_not_empty(self, generator):
        """Test that raw_data field is populated"""
        df = generator.generate_staging_data("RUN_001", 15)
        
        null_count = df.filter(df.raw_data.isNull()).count()
        empty_count = df.filter(df.raw_data == "").count()
        
        assert null_count == 0, "Found null raw_data"
        assert empty_count == 0, "Found empty raw_data"


class TestDataGeneratorIntegration:
    """Integration tests for data generator"""
    
    def test_all_data_types_generated_successfully(self, generator):
        """Test that all data types can be generated"""
        source_df = generator.generate_source_data(10)
        config_df = generator.generate_config_data()
        schedule_df = generator.generate_schedule_data()
        staging_df = generator.generate_staging_data("RUN_001", 10)
        
        assert source_df.count() == 10
        assert config_df.count() > 0
        assert schedule_df.count() > 0
        assert staging_df.count() == 10
    
    def test_generated_data_can_be_written_and_read(self, generator, tmp_path, spark):
        """Test that generated data can be persisted and loaded"""
        df = generator.generate_source_data(20)
        
        output_path = str(tmp_path / "test_output")
        df.write.mode("overwrite").parquet(output_path)
        
        loaded_df = spark.read.parquet(output_path)
        
        assert loaded_df.count() == 20, "Data loss during write/read"
        assert set(df.columns) == set(loaded_df.columns), "Schema mismatch"
    
    def test_large_dataset_generation(self, generator):
        """Test generation of larger datasets"""
        df = generator.generate_source_data(1000)
        
        assert df.count() == 1000, "Failed to generate large dataset"
        
        # Verify data quality on large dataset
        null_ids = df.filter(df.id.isNull()).count()
        assert null_ids == 0, "Null IDs in large dataset"


class TestHelperMethods:
    """Test helper methods"""
    
    def test_generate_username_format(self, generator):
        """Test username generation format"""
        username = generator._generate_username()
        
        assert len(username) == 6, "Username wrong length"
        assert username.isupper(), "Username not uppercase"
        assert username.isalpha(), "Username contains non-alpha"
    
    def test_generate_username_uniqueness(self, generator):
        """Test that generated usernames vary"""
        usernames = set()
        
        for _ in range(100):
            usernames.add(generator._generate_username())
        
        # Should have at least some variety in 100 generations
        assert len(usernames) > 10, "Insufficient username variety"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])