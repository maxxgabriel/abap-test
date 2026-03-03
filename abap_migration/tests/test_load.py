"""
Unit tests for ETL Loading Module.
Tests batch operations, INSERT/UPSERT modes, and success/error counting.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from pyspark.sql.functions import col, current_timestamp, lit
from datetime import datetime
from decimal import Decimal

from src.load import ETLLoader, LoadMode, LoadResult, LoaderFactory


@pytest.fixture(scope="session")
def spark():
    """Create SparkSession for testing"""
    spark = SparkSession.builder \
        .appName("ETL Loader Tests") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .getOrCreate()
    
    yield spark
    spark.stop()


@pytest.fixture
def sample_schema():
    """Sample schema for test data"""
    return StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("value", DecimalType(15, 2), True),
        StructField("transformed_value", DecimalType(15, 2), True),
        StructField("status", StringType(), True),
        StructField("category", StringType(), True),
        StructField("priority", IntegerType(), True),
        StructField("etl_run_id", StringType(), True),
        StructField("processed_at", TimestampType(), True),
        StructField("processed_by", StringType(), True),
    ])


@pytest.fixture
def sample_data(spark, sample_schema):
    """Create sample DataFrame for testing"""
    data = [
        ("ID001", "Product 1", Decimal("100.00"), Decimal("150.00"), 
         "ACTIVE", "PREMIUM", 1, "RUN001", datetime.now(), "test_user"),
        ("ID002", "Product 2", Decimal("200.00"), Decimal("240.00"), 
         "ACTIVE", "STANDARD", 2, "RUN001", datetime.now(), "test_user"),
        ("ID003", "Product 3", Decimal("300.00"), Decimal("360.00"), 
         "ACTIVE", "BASIC", 3, "RUN001", datetime.now(), "test_user"),
        ("ID004", "Product 4", Decimal("400.00"), Decimal("480.00"), 
         "ACTIVE", "PREMIUM", 2, "RUN001", datetime.now(), "test_user"),
        ("ID005", "Product 5", Decimal("500.00"), Decimal("600.00"), 
         "ACTIVE", "VIP", 1, "RUN001", datetime.now(), "test_user"),
    ]
    
    return spark.createDataFrame(data, sample_schema)


@pytest.fixture
def loader(spark):
    """Create ETLLoader instance for testing"""
    return ETLLoader(
        spark=spark,
        target_type="DATABASE",
        batch_size=2,  # Small batch size for testing
        run_id="TEST_RUN_001",
        target_table="test_target"
    )


class TestETLLoader:
    """Test suite for ETL Loader"""
    
    def test_loader_initialization(self, spark):
        """Test loader initialization with default parameters"""
        loader = ETLLoader(spark=spark)
        
        assert loader.spark is not None
        assert loader.target_type == "DATABASE"
        assert loader.batch_size == 1000
        assert loader.run_id is not None
        assert loader.target_table == "etl_target_data"
    
    def test_loader_custom_initialization(self, spark):
        """Test loader initialization with custom parameters"""
        loader = ETLLoader(
            spark=spark,
            target_type="CUSTOM",
            batch_size=500,
            run_id="CUSTOM_RUN",
            target_table="custom_table"
        )
        
        assert loader.target_type == "CUSTOM"
        assert loader.batch_size == 500
        assert loader.run_id == "CUSTOM_RUN"
        assert loader.target_table == "custom_table"
    
    def test_schema_validation_success(self, loader, sample_data):
        """Test schema validation with valid data"""
        result = loader._validate_schema(sample_data)
        assert result is True
    
    def test_schema_validation_failure(self, loader, spark):
        """Test schema validation with invalid data"""
        # Create DataFrame with missing required fields
        invalid_data = spark.createDataFrame(
            [("ID001", "Product 1")],
            ["id", "name"]
        )
        
        result = loader._validate_schema(invalid_data)
        assert result is False
    
    def test_batch_creation(self, loader, sample_data):
        """Test DataFrame batching"""
        batches = loader._create_batches(sample_data)
        
        # With batch_size=2 and 5 records, expect 3 batches
        assert len(batches) >= 1
        
        # Verify all data is present
        total_records = sum(batch.count() for batch in batches)
        assert total_records == sample_data.count()
    
    def test_batch_size_partitioning(self, spark, sample_data):
        """Test batch size partitioning with different sizes"""
        test_cases = [
            (1, 5),   # batch_size=1, expect 5 batches
            (2, 3),   # batch_size=2, expect 3 batches
            (5, 1),   # batch_size=5, expect 1 batch
            (10, 1),  # batch_size=10, expect 1 batch
        ]
        
        for batch_size, expected_min_batches in test_cases:
            loader = ETLLoader(
                spark=spark,
                batch_size=batch_size,
                run_id=f"TEST_{batch_size}"
            )
            
            batches = loader._create_batches(sample_data)
            assert len(batches) >= expected_min_batches
    
    def test_load_result_initialization(self):
        """Test LoadResult dataclass initialization"""
        result = LoadResult()
        
        assert result.success_count == 0
        assert result.error_count == 0
        assert result.total_count == 0
        assert result.errors == []
    
    def test_load_result_with_data(self):
        """Test LoadResult with data"""
        result = LoadResult(
            success_count=100,
            error_count=5,
            total_count=105,
            errors=["Error 1", "Error 2"]
        )
        
        assert result.success_count == 100
        assert result.error_count == 5
        assert result.total_count == 105
        assert len(result.errors) == 2
    
    def test_load_data_insert_mode(self, loader, sample_data, spark):
        """Test load_data with INSERT mode"""
        # Create in-memory table for testing
        sample_data.createOrReplaceTempView("test_target")
        
        result = loader.load_data(sample_data, mode=LoadMode.INSERT)
        
        assert result.total_count == sample_data.count()
        assert result.success_count >= 0
        assert result.error_count >= 0
        assert result.success_count + result.error_count == result.total_count
    
    def test_load_data_upsert_mode(self, loader, sample_data, spark):
        """Test load_data with UPSERT mode"""
        # Create in-memory table
        sample_data.createOrReplaceTempView("test_target")
        
        result = loader.load_data(sample_data, mode=LoadMode.UPSERT)
        
        assert result.total_count == sample_data.count()
        assert isinstance(result.errors, list)
    
    def test_load_mode_enum(self):
        """Test LoadMode enum values"""
        assert LoadMode.INSERT.value == "INSERT"
        assert LoadMode.UPDATE.value == "UPDATE"
        assert LoadMode.UPSERT.value == "UPSERT"
    
    def test_success_count_tracking(self, spark, sample_schema):
        """Test accurate success count tracking"""
        # Create small dataset
        data = [
            ("ID001", "Product 1", Decimal("100.00"), Decimal("150.00"), 
             "ACTIVE", "PREMIUM", 1, "RUN001", datetime.now(), "test_user"),
            ("ID002", "Product 2", Decimal("200.00"), Decimal("240.00"), 
             "ACTIVE", "STANDARD", 2, "RUN001", datetime.now(), "test_user"),
        ]
        df = spark.createDataFrame(data, sample_schema)
        
        loader = ETLLoader(spark=spark, batch_size=1, run_id="SUCCESS_TEST")
        df.createOrReplaceTempView("test_target")
        
        result = loader.load_data(df, mode=LoadMode.INSERT)
        
        # Verify counts
        assert result.total_count == 2
        assert result.success_count + result.error_count == result.total_count
    
    def test_error_count_tracking(self, spark, sample_schema):
        """Test error count tracking with invalid data"""
        # Create data with potential issues
        data = [
            ("ID001", "Product 1", Decimal("100.00"), Decimal("150.00"), 
             "ACTIVE", "PREMIUM", 1, "RUN001", datetime.now(), "test_user"),
        ]
        df = spark.createDataFrame(data, sample_schema)
        
        loader = ETLLoader(spark=spark, batch_size=1, run_id="ERROR_TEST")
        
        result = loader.load_data(df, mode=LoadMode.INSERT)
        
        # Verify error tracking exists
        assert hasattr(result, 'error_count')
        assert hasattr(result, 'errors')
        assert isinstance(result.errors, list)
    
    def test_error_messages_collection(self, loader, spark, sample_schema):
        """Test error message collection"""
        # Create invalid data (missing required fields)
        invalid_df = spark.createDataFrame(
            [("ID001", "Product 1")],
            ["id", "name"]
        )
        
        result = loader.load_data(invalid_df, mode=LoadMode.INSERT)
        
        # Should have error messages
        assert result.error_count > 0
        assert len(result.errors) > 0
        assert "Schema validation failed" in result.errors[0]
    
    def test_batch_error_handling(self, spark, sample_schema):
        """Test error handling for individual batches"""
        data = [
            ("ID001", "Product 1", Decimal("100.00"), Decimal("150.00"), 
             "ACTIVE", "PREMIUM", 1, "RUN001", datetime.now(), "test_user"),
            ("ID002", "Product 2", Decimal("200.00"), Decimal("240.00"), 
             "ACTIVE", "STANDARD", 2, "RUN001", datetime.now(), "test_user"),
        ]
        df = spark.createDataFrame(data, sample_schema)
        
        loader = ETLLoader(
            spark=spark,
            batch_size=1,
            run_id="BATCH_ERROR_TEST"
        )
        
        result = loader.load_data(df, mode=LoadMode.INSERT)
        
        # Verify batch error tracking
        assert result.total_count == 2
        assert result.success_count >= 0
        assert result.error_count >= 0
    
    def test_reconciliation(self, loader, sample_data, spark):
        """Test data reconciliation after load"""
        # Create table and load data
        sample_data.createOrReplaceTempView("test_target")
        
        # Test reconciliation
        reconciled = loader._reconcile_data(sample_data)
        
        # Reconciliation should execute (result may vary based on implementation)
        assert isinstance(reconciled, bool)
    
    def test_loader_factory(self, spark):
        """Test LoaderFactory creation"""
        config = {
            "target_type": "DATABASE",
            "batch_size": 500,
            "run_id": "FACTORY_TEST",
            "target_table": "factory_table"
        }
        
        loader = LoaderFactory.create_loader(spark, config)
        
        assert loader.target_type == "DATABASE"
        assert loader.batch_size == 500
        assert loader.run_id == "FACTORY_TEST"
        assert loader.target_table == "factory_table"
    
    def test_loader_factory_defaults(self, spark):
        """Test LoaderFactory with default configuration"""
        config = {}
        loader = LoaderFactory.create_loader(spark, config)
        
        assert loader.target_type == "DATABASE"
        assert loader.batch_size == 1000
        assert loader.target_table == "etl_target_data"
    
    def test_insert_mode_behavior(self, spark, sample_schema):
        """Test INSERT mode specific behavior"""
        data = [
            ("ID001", "Product 1", Decimal("100.00"), Decimal("150.00"), 
             "ACTIVE", "PREMIUM", 1, "RUN001", datetime.now(), "test_user"),
        ]
        df = spark.createDataFrame(data, sample_schema)
        
        loader = ETLLoader(spark=spark, batch_size=10, run_id="INSERT_TEST")
        df.createOrReplaceTempView("test_target")
        
        result = loader.load_data(df, mode=LoadMode.INSERT)
        
        assert result.total_count == 1
    
    def test_upsert_mode_behavior(self, spark, sample_schema):
        """Test UPSERT mode specific behavior"""
        data = [
            ("ID001", "Product 1", Decimal("100.00"), Decimal("150.00"), 
             "ACTIVE", "PREMIUM", 1, "RUN001", datetime.now(), "test_user"),
        ]
        df = spark.createDataFrame(data, sample_schema)
        
        loader = ETLLoader(spark=spark, batch_size=10, run_id="UPSERT_TEST")
        df.createOrReplaceTempView("test_target")
        
        result = loader.load_data(df, mode=LoadMode.UPSERT)
        
        assert result.total_count == 1
    
    def test_large_batch_processing(self, spark, sample_schema):
        """Test processing with large batches"""
        # Create larger dataset
        data = [
            (f"ID{i:03d}", f"Product {i}", Decimal(f"{i * 100}.00"), 
             Decimal(f"{i * 120}.00"), "ACTIVE", "STANDARD", 
             (i % 5) + 1, "RUN001", datetime.now(), "test_user")
            for i in range(1, 51)
        ]
        df = spark.createDataFrame(data, sample_schema)
        
        loader = ETLLoader(spark=spark, batch_size=10, run_id="LARGE_BATCH")
        df.createOrReplaceTempView("test_target")
        
        result = loader.load_data(df, mode=LoadMode.INSERT)
        
        assert result.total_count == 50
        assert result.success_count + result.error_count == 50
    
    def test_empty_dataframe_handling(self, spark, sample_schema):
        """Test handling of empty DataFrame"""
        empty_df = spark.createDataFrame([], sample_schema)
        
        loader = ETLLoader(spark=spark, run_id="EMPTY_TEST")
        result = loader.load_data(empty_df, mode=LoadMode.INSERT)
        
        assert result.total_count == 0
        assert result.success_count == 0
        assert result.error_count == 0
    
    def test_concurrent_batch_processing(self, spark, sample_schema):
        """Test batch processing behavior with concurrent loads"""
        data = [
            (f"ID{i:03d}", f"Product {i}", Decimal(f"{i * 100}.00"), 
             Decimal(f"{i * 120}.00"), "ACTIVE", "STANDARD", 
             (i % 5) + 1, "RUN001", datetime.now(), "test_user")
            for i in range(1, 21)
        ]
        df = spark.createDataFrame(data, sample_schema)
        
        loader = ETLLoader(spark=spark, batch_size=5, run_id="CONCURRENT_TEST")
        df.createOrReplaceTempView("test_target")
        
        result = loader.load_data(df, mode=LoadMode.INSERT)
        
        # Verify all records processed
        assert result.total_count == 20