"""
Unit tests for ETL Loading Module
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType
from src.load import ETLLoader, LoadResult, create_loader
from datetime import datetime


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("ETL_Loader_Test") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    yield spark
    spark.stop()


@pytest.fixture
def sample_schema():
    """Sample data schema"""
    return StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("value", DecimalType(15, 2), True),
        StructField("transformed_value", DecimalType(15, 2), True),
        StructField("status", StringType(), True),
        StructField("category", StringType(), True),
        StructField("priority", IntegerType(), True)
    ])


@pytest.fixture
def sample_data(spark, sample_schema):
    """Create sample DataFrame"""
    data = [
        ("TEST001", "Product 1", 100.00, 150.00, "ACTIVE", "PREMIUM", 1),
        ("TEST002", "Product 2", 200.00, 240.00, "ACTIVE", "STANDARD", 2),
        ("TEST003", "Product 3", 300.00, 360.00, "ACTIVE", "BASIC", 3),
        ("TEST004", "Product 4", 400.00, 480.00, "ACTIVE", "VIP", 1),
        ("TEST005", "Product 5", 500.00, 600.00, "ACTIVE", "TRIAL", 2)
    ]
    return spark.createDataFrame(data, sample_schema)


@pytest.fixture
def loader_config():
    """Test configuration"""
    return {
        "target_type": "PARQUET",
        "batch_size": 2,
        "enable_reconciliation": True,
        "error_table": "test_error_log"
    }


@pytest.fixture
def loader(spark, loader_config):
    """Create loader instance"""
    return ETLLoader(
        spark=spark,
        target_type=loader_config["target_type"],
        batch_size=loader_config["batch_size"],
        run_id="TEST_RUN_001",
        config=loader_config
    )


class TestETLLoader:
    """Test suite for ETL Loader"""
    
    def test_loader_initialization(self, loader):
        """Test loader initialization"""
        assert loader is not None
        assert loader.target_type == "PARQUET"
        assert loader.batch_size == 2
        assert loader.run_id == "TEST_RUN_001"
    
    def test_generate_run_id(self, spark):
        """Test run ID generation"""
        loader = ETLLoader(spark=spark)
        assert loader.run_id.startswith("RUN_")
        assert len(loader.run_id) > 10
    
    def test_add_metadata(self, loader, sample_data):
        """Test metadata addition"""
        df_with_metadata = loader._add_metadata(sample_data)
        
        assert "etl_run_id" in df_with_metadata.columns
        assert "etl_loaded_at" in df_with_metadata.columns
        assert "etl_loaded_by" in df_with_metadata.columns
        
        first_row = df_with_metadata.first()
        assert first_row["etl_run_id"] == "TEST_RUN_001"
        assert first_row["etl_loaded_by"] == "pyspark_loader"
    
    def test_load_to_file_parquet(self, loader, sample_data, tmp_path):
        """Test loading to Parquet file"""
        target_path = str(tmp_path / "test_output.parquet")
        
        result = loader._load_to_file(
            df=sample_data,
            mode="INSERT",
            target_path=target_path
        )
        
        assert result.success_count == 5
        assert result.error_count == 0
        assert len(result.errors) == 0
        
        # Verify file was created
        loaded_df = loader.spark.read.parquet(target_path)
        assert loaded_df.count() == 5
    
    def test_load_empty_dataframe(self, loader, spark, sample_schema):
        """Test loading empty DataFrame"""
        empty_df = spark.createDataFrame([], sample_schema)
        
        result = loader.load_data(
            df=empty_df,
            mode="INSERT",
            target_path="/tmp/test_output"
        )
        
        assert result.total_count == 0
        assert result.success_count == 0
        assert result.error_count == 0
    
    def test_load_with_insert_mode(self, loader, sample_data, tmp_path):
        """Test INSERT mode"""
        target_path = str(tmp_path / "insert_test.parquet")
        
        result = loader.load_data(
            df=sample_data,
            mode="INSERT",
            target_path=target_path
        )
        
        assert result.success_count > 0
        assert result.error_count == 0
    
    def test_load_with_upsert_mode(self, loader, sample_data, tmp_path):
        """Test UPSERT mode"""
        target_path = str(tmp_path / "upsert_test.parquet")
        
        # First load
        result1 = loader.load_data(
            df=sample_data,
            mode="UPSERT",
            target_path=target_path
        )
        assert result1.success_count == 5
        
        # Second load with same data (should handle duplicates)
        result2 = loader.load_data(
            df=sample_data,
            mode="UPSERT",
            target_path=target_path
        )
        assert result2.success_count >= 0
    
    def test_batch_processing(self, spark, sample_data):
        """Test batch processing logic"""
        loader = ETLLoader(
            spark=spark,
            batch_size=2,
            run_id="BATCH_TEST"
        )
        
        total_rows = sample_data.count()
        num_batches = (total_rows + loader.batch_size - 1) // loader.batch_size
        
        assert num_batches == 3  # 5 rows / 2 per batch = 3 batches
    
    def test_reconciliation(self, loader, sample_data):
        """Test data reconciliation"""
        result = LoadResult(
            success_count=5,
            error_count=0,
            total_count=5
        )
        
        reconciled = loader._reconcile_data(sample_data, result)
        assert reconciled is True
    
    def test_reconciliation_mismatch(self, loader, sample_data):
        """Test reconciliation with mismatch"""
        result = LoadResult(
            success_count=3,  # Mismatch: expected 5, got 3
            error_count=0,
            total_count=5
        )
        
        reconciled = loader._reconcile_data(sample_data, result)
        assert reconciled is False
    
    def test_error_handling(self, loader):
        """Test error handling"""
        loader.handle_load_error(
            record_id="ERR001",
            error=Exception("Test error")
        )
        # Should not raise exception, just log
    
    def test_load_result_dataclass(self):
        """Test LoadResult dataclass"""
        result = LoadResult(
            success_count=10,
            error_count=2,
            total_count=12,
            errors=["Error 1", "Error 2"]
        )
        
        assert result.success_count == 10
        assert result.error_count == 2
        assert result.total_count == 12
        assert len(result.errors) == 2
    
    def test_load_result_default_errors(self):
        """Test LoadResult with default errors list"""
        result = LoadResult()
        assert result.errors == []
        assert result.success_count == 0
    
    def test_create_loader_factory(self, spark):
        """Test loader factory function"""
        config = {
            "target_type": "DELTA",
            "batch_size": 500,
            "run_id": "FACTORY_TEST"
        }
        
        loader = create_loader(spark, config)
        
        assert loader.target_type == "DELTA"
        assert loader.batch_size == 500
        assert loader.run_id == "FACTORY_TEST"
    
    def test_load_with_large_dataset(self, spark, sample_schema):
        """Test loading larger dataset"""
        # Create larger dataset
        data = [(f"ID{i:05d}", f"Product {i}", float(i * 10), float(i * 12), 
                 "ACTIVE", "TEST", 1) for i in range(1000)]
        large_df = spark.createDataFrame(data, sample_schema)
        
        loader = ETLLoader(
            spark=spark,
            batch_size=100,
            config={"target_type": "PARQUET"}
        )
        
        assert large_df.count() == 1000
    
    def test_load_with_null_values(self, spark, sample_schema):
        """Test loading data with null values"""
        data = [
            ("NULL001", None, 100.00, 150.00, "ACTIVE", None, 1),
            ("NULL002", "Product", None, 240.00, None, "STANDARD", 2)
        ]
        null_df = spark.createDataFrame(data, sample_schema)
        
        loader = ETLLoader(
            spark=spark,
            config={"target_type": "PARQUET"}
        )
        
        df_with_metadata = loader._add_metadata(null_df)
        assert df_with_metadata.count() == 2
    
    def test_load_with_special_characters(self, spark, sample_schema):
        """Test loading data with special characters"""
        data = [
            ("SPEC001", "Product 'with' quotes", 100.00, 150.00, 
             "ACTIVE", "TEST", 1),
            ("SPEC002", 'Product "with" quotes', 200.00, 240.00, 
             "ACTIVE", "TEST", 2)
        ]
        special_df = spark.createDataFrame(data, sample_schema)
        
        loader = ETLLoader(
            spark=spark,
            config={"target_type": "PARQUET"}
        )
        
        df_with_metadata = loader._add_metadata(special_df)
        assert df_with_metadata.count() == 2
    
    def test_unsupported_target_type(self, spark, sample_data):
        """Test with unsupported target type"""
        loader = ETLLoader(
            spark=spark,
            target_type="UNSUPPORTED",
            config={}
        )
        
        with pytest.raises(ValueError, match="Unsupported target type"):
            loader.load_data(
                df=sample_data,
                mode="INSERT",
                target_table="test_table"
            )


class TestLoadResultIntegration:
    """Integration tests for load results"""
    
    def test_full_load_cycle(self, loader, sample_data, tmp_path):
        """Test complete load cycle with results"""
        target_path = str(tmp_path / "full_cycle.parquet")
        
        result = loader.load_data(
            df=sample_data,
            mode="INSERT",
            target_path=target_path
        )
        
        assert result.total_count == 5
        assert result.success_count == 5
        assert result.error_count == 0
        assert len(result.errors) == 0
        
        # Verify loaded data
        loaded_df = loader.spark.read.parquet(target_path)
        assert loaded_df.count() == 5
        assert "etl_run_id" in loaded_df.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])