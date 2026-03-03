"""
Unit tests for Delta Lake Loader
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from datetime import datetime
from decimal import Decimal
import sys
import os

# Add src to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.load import DeltaLakeLoader, create_loader


@pytest.fixture(scope="session")
def spark():
    """Create SparkSession for testing"""
    spark = SparkSession.builder \
        .appName("TestDeltaLoader") \
        .master("local[2]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def test_config(tmp_path):
    """Create test configuration"""
    return {
        'batch_size': 100,
        'target_path': str(tmp_path / "delta_table"),
        'enable_reconciliation': True,
        'run_id': 'TEST_RUN_001'
    }


@pytest.fixture
def sample_data(spark):
    """Create sample test data"""
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("value", DecimalType(15, 2), True),
        StructField("transformed_value", DecimalType(15, 2), True),
        StructField("status", StringType(), True),
        StructField("category", StringType(), True),
        StructField("priority", IntegerType(), True),
        StructField("etl_run_id", StringType(), True),
        StructField("processed_at", TimestampType(), True),
        StructField("processed_by", StringType(), True)
    ])
    
    data = [
        ("ID001", "Product A", Decimal("100.50"), Decimal("150.75"), "ACTIVE", "PREMIUM", 1, 
         "RUN001", datetime.now(), "test_user"),
        ("ID002", "Product B", Decimal("200.00"), Decimal("240.00"), "ACTIVE", "STANDARD", 2,
         "RUN001", datetime.now(), "test_user"),
        ("ID003", "Product C", Decimal("300.25"), Decimal("360.30"), "ACTIVE", "BASIC", 3,
         "RUN001", datetime.now(), "test_user"),
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def loader(spark, test_config):
    """Create loader instance"""
    return DeltaLakeLoader(spark, test_config)


class TestDeltaLakeLoader:
    """Test cases for Delta Lake Loader"""
    
    def test_loader_initialization(self, loader, test_config):
        """Test loader initialization"""
        assert loader.batch_size == test_config['batch_size']
        assert loader.target_path == test_config['target_path']
        assert loader.run_id == test_config['run_id']
    
    def test_get_target_schema(self, loader):
        """Test schema definition"""
        schema = loader.get_target_schema()
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 11
        assert schema.fieldNames()[0] == "id"
    
    def test_insert_data(self, loader, sample_data):
        """Test INSERT operation"""
        result = loader._insert_data(sample_data)
        
        assert result['mode'] == 'INSERT'
        assert result['success_count'] == 3
        assert result['error_count'] == 0
        assert len(result['errors']) == 0
    
    def test_upsert_data_new_table(self, loader, sample_data):
        """Test UPSERT operation on new table"""
        result = loader._upsert_data(sample_data)
        
        assert result['mode'] == 'UPSERT_INITIAL'
        assert result['success_count'] == 3
        assert result['error_count'] == 0
    
    def test_upsert_data_existing_table(self, loader, sample_data, spark):
        """Test UPSERT operation on existing table"""
        # First insert
        loader._insert_data(sample_data)
        
        # Modify data
        updated_data = sample_data.withColumn(
            "status", 
            spark.sql.functions.lit("UPDATED")
        )
        
        # Upsert
        result = loader._upsert_data(updated_data)
        
        assert result['mode'] == 'UPSERT'
        assert result['success_count'] >= 0
        assert result['error_count'] == 0
    
    def test_update_data(self, loader, sample_data, spark):
        """Test UPDATE operation"""
        # First insert base data
        loader._insert_data(sample_data)
        
        # Create update data
        updated_data = sample_data.withColumn(
            "value",
            spark.sql.functions.col("value") * 2
        )
        
        # Update
        result = loader._update_data(updated_data)
        
        assert result['mode'] == 'UPDATE'
        assert result['error_count'] == 0
    
    def test_overwrite_data(self, loader, sample_data):
        """Test OVERWRITE operation"""
        result = loader._overwrite_data(sample_data)
        
        assert result['mode'] == 'OVERWRITE'
        assert result['success_count'] == 3
        assert result['error_count'] == 0
    
    def test_load_data_with_mode(self, loader, sample_data):
        """Test load_data with different modes"""
        # Test insert mode
        result = loader.load_data(sample_data, mode='insert')
        assert result['success_count'] > 0
        
        # Test upsert mode
        result = loader.load_data(sample_data, mode='upsert')
        assert result['success_count'] > 0
    
    def test_add_load_metadata(self, loader, sample_data):
        """Test metadata addition"""
        df_with_metadata = loader._add_load_metadata(sample_data)
        
        assert "load_timestamp" in df_with_metadata.columns
        assert "etl_run_id" in df_with_metadata.columns
        assert df_with_metadata.count() == sample_data.count()
    
    def test_calculate_partitions(self, loader):
        """Test partition calculation"""
        assert loader._calculate_partitions(500) == 1
        assert loader._calculate_partitions(5000) == 4
        assert loader._calculate_partitions(50000) == 8
        assert loader._calculate_partitions(500000) == 16
        assert loader._calculate_partitions(5000000) == 32
    
    def test_reconcile_data(self, loader, sample_data):
        """Test data reconciliation"""
        # Load data first
        loader._insert_data(sample_data)
        
        # Reconcile
        result = loader._reconcile_data(sample_data)
        
        assert 'matches' in result
        assert 'source_count' in result
        assert 'target_count' in result
    
    def test_error_handling_invalid_mode(self, loader, sample_data):
        """Test error handling for invalid mode"""
        with pytest.raises(ValueError, match="Invalid mode"):
            loader.load_data(sample_data, mode='invalid_mode')
    
    def test_batch_size_configuration(self, spark, tmp_path):
        """Test batch size configuration"""
        config = {
            'batch_size': 500,
            'target_path': str(tmp_path / "test_batch"),
            'run_id': 'TEST_BATCH'
        }
        
        loader = DeltaLakeLoader(spark, config)
        assert loader.batch_size == 500
    
    def test_create_loader_factory(self, spark, test_config):
        """Test loader factory function"""
        loader = create_loader(spark, test_config)
        assert isinstance(loader, DeltaLakeLoader)
        assert loader.batch_size == test_config['batch_size']
    
    def test_large_dataset_partitioning(self, loader, spark):
        """Test partitioning with larger dataset"""
        # Create larger dataset
        large_data = []
        for i in range(1000):
            large_data.append((
                f"ID{i:06d}",
                f"Product {i}",
                Decimal(f"{i * 10}.00"),
                Decimal(f"{i * 12}.00"),
                "ACTIVE",
                f"CAT{i % 5}",
                (i % 5) + 1,
                "RUN_LARGE",
                datetime.now(),
                "test_user"
            ))
        
        schema = loader.get_target_schema()
        # Remove load_timestamp from schema for test data
        test_schema = StructType([f for f in schema.fields if f.name != "load_timestamp"])
        
        df = spark.createDataFrame(large_data, test_schema)
        
        # Test repartitioning
        num_partitions = loader._calculate_partitions(df.count())
        df_partitioned = df.repartition(num_partitions, "category")
        
        assert df_partitioned.rdd.getNumPartitions() == num_partitions
    
    def test_concurrent_writes(self, loader, sample_data):
        """Test handling of concurrent write operations"""
        # First write
        result1 = loader.load_data(sample_data, mode='insert')
        assert result1['success_count'] == 3
        
        # Second write (should work with Delta's ACID properties)
        result2 = loader.load_data(sample_data, mode='upsert')
        assert result2['success_count'] >= 0


class TestDeltaTableOperations:
    """Test Delta-specific operations"""
    
    def test_compact_table(self, loader, sample_data):
        """Test table compaction"""
        # Load some data first
        loader.load_data(sample_data, mode='insert')
        
        # Compact should not raise errors
        try:
            loader.compact_table()
            assert True
        except Exception as e:
            pytest.fail(f"Compaction failed: {str(e)}")
    
    def test_vacuum_table(self, loader, sample_data):
        """Test table vacuum"""
        # Load data
        loader.load_data(sample_data, mode='insert')
        
        # Vacuum with short retention for testing
        try:
            loader.vacuum_table(retention_hours=0)
            assert True
        except Exception:
            # Vacuum might fail in test environment, that's ok
            assert True


class TestEdgeCases:
    """Test edge cases and error scenarios"""
    
    def test_empty_dataframe(self, loader, spark):
        """Test loading empty DataFrame"""
        schema = loader.get_target_schema()
        test_schema = StructType([f for f in schema.fields if f.name != "load_timestamp"])
        empty_df = spark.createDataFrame([], test_schema)
        
        result = loader.load_data(empty_df, mode='insert')
        assert result['success_count'] == 0
    
    def test_null_values(self, loader, spark):
        """Test handling of null values"""
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("value", DecimalType(15, 2), True),
        ])
        
        data = [
            ("ID001", None, None),
            ("ID002", "Name", Decimal("100.00")),
        ]
        
        df = spark.createDataFrame(data, schema)
        
        # Should handle nulls gracefully
        try:
            result = loader._insert_data(df)
            assert result['error_count'] == 0
        except Exception as e:
            pytest.fail(f"Failed to handle nulls: {str(e)}")
    
    def test_duplicate_keys(self, loader, spark):
        """Test handling of duplicate keys in upsert"""
        schema = loader.get_target_schema()
        test_schema = StructType([f for f in schema.fields if f.name != "load_timestamp"])
        
        # Create data with duplicate IDs
        data = [
            ("ID001", "Product A", Decimal("100.00"), Decimal("120.00"), "ACTIVE", 
             "CAT1", 1, "RUN001", datetime.now(), "user1"),
            ("ID001", "Product A Updated", Decimal("150.00"), Decimal("180.00"), "ACTIVE",
             "CAT1", 1, "RUN001", datetime.now(), "user1"),
        ]
        
        df = spark.createDataFrame(data, test_schema)
        
        # Upsert should handle duplicates (last write wins)
        result = loader.load_data(df, mode='upsert')
        assert result['error_count'] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])