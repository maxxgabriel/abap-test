"""
Test framework utilities for ETL test isolation and setup.
Provides reusable fixtures, data generators, and assertion patterns.
"""
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import tempfile
import shutil
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    IntegerType, TimestampType, BooleanType
)
import pytest


@dataclass
class TestDataConfig:
    """Configuration for generating test data."""
    num_records: int = 100
    include_nulls: bool = False
    include_duplicates: bool = False
    include_invalid: bool = False
    categories: List[str] = None
    value_range: tuple = (50, 1000)


@dataclass
class SparkTestContext:
    """Context for Spark testing with isolated resources."""
    spark: SparkSession
    temp_dir: Path
    warehouse_dir: Path
    checkpoint_dir: Path
    
    def cleanup(self):
        """Clean up temporary directories."""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)


class SparkTestSessionBuilder:
    """Builder for creating isolated Spark test sessions."""
    
    @staticmethod
    def create_test_session(
        app_name: str = "test",
        warehouse_location: Optional[str] = None,
        config_overrides: Optional[Dict[str, str]] = None
    ) -> SparkTestContext:
        """
        Create an isolated Spark session for testing.
        
        Args:
            app_name: Application name for the session
            warehouse_location: Optional warehouse location
            config_overrides: Additional Spark configurations
            
        Returns:
            SparkTestContext with session and temp directories
        """
        temp_dir = Path(tempfile.mkdtemp(prefix=f"spark_test_{app_name}_"))
        warehouse_dir = temp_dir / "warehouse"
        checkpoint_dir = temp_dir / "checkpoint"
        
        warehouse_dir.mkdir(parents=True, exist_ok=True)
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
        builder = (
            SparkSession.builder
            .appName(app_name)
            .master("local[2]")
            .config("spark.sql.warehouse.dir", str(warehouse_dir))
            .config("spark.sql.streaming.checkpointLocation", str(checkpoint_dir))
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.default.parallelism", "2")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.driver.memory", "1g")
        )
        
        if config_overrides:
            for key, value in config_overrides.items():
                builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        return SparkTestContext(
            spark=spark,
            temp_dir=temp_dir,
            warehouse_dir=warehouse_dir,
            checkpoint_dir=checkpoint_dir
        )


class TestDataGenerator:
    """Generate test data with various characteristics."""
    
    @staticmethod
    def get_source_schema() -> StructType:
        """Get the schema for source test data."""
        return StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("value", DecimalType(15, 2), True),
            StructField("status", StringType(), True),
            StructField("category", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", StringType(), True),
            StructField("changed_at", TimestampType(), True),
            StructField("changed_by", StringType(), True),
        ])
    
    @staticmethod
    def get_transformed_schema() -> StructType:
        """Get the schema for transformed test data."""
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
    
    @staticmethod
    def generate_source_data(
        spark: SparkSession,
        config: TestDataConfig
    ) -> DataFrame:
        """
        Generate test source data.
        
        Args:
            spark: Spark session
            config: Configuration for data generation
            
        Returns:
            DataFrame with generated test data
        """
        import random
        from datetime import timedelta
        
        categories = config.categories or ["PREMIUM", "STANDARD", "BASIC", "VIP", "TRIAL"]
        min_val, max_val = config.value_range
        now = datetime.now()
        
        data = []
        for i in range(1, config.num_records + 1):
            record_id = f"TEST{i:06d}"
            
            # Handle nulls for testing
            if config.include_nulls and random.random() < 0.1:
                name = None
                value = None
            else:
                name = f"Product {i}"
                value = Decimal(str(random.uniform(min_val, max_val)))
            
            # Handle duplicates for testing
            if config.include_duplicates and i > 1 and random.random() < 0.05:
                record_id = f"TEST{i-1:06d}"
            
            # Handle invalid values for testing
            if config.include_invalid and random.random() < 0.05:
                value = Decimal("-1")
            
            record = {
                "id": record_id,
                "name": name,
                "value": value,
                "status": random.choice(["ACTIVE", "INACTIVE", "PENDING"]),
                "category": random.choice(categories),
                "source_system": "TEST_SYSTEM",
                "created_at": now - timedelta(days=random.randint(1, 365)),
                "created_by": "test_user",
                "changed_at": now - timedelta(hours=random.randint(1, 24)),
                "changed_by": "test_user",
            }
            data.append(record)
        
        return spark.createDataFrame(data, schema=TestDataGenerator.get_source_schema())
    
    @staticmethod
    def generate_transformed_data(
        spark: SparkSession,
        config: TestDataConfig,
        run_id: str = "TEST_RUN_001"
    ) -> DataFrame:
        """
        Generate test transformed data.
        
        Args:
            spark: Spark session
            config: Configuration for data generation
            run_id: ETL run identifier
            
        Returns:
            DataFrame with generated transformed test data
        """
        import random
        
        now = datetime.now()
        data = []
        
        for i in range(1, config.num_records + 1):
            value = Decimal(str(random.uniform(*config.value_range)))
            transformed_value = value * Decimal("1.2")
            
            if value >= 750:
                priority = 1
            elif value >= 300:
                priority = 2
            else:
                priority = 3
            
            record = {
                "id": f"TEST{i:06d}",
                "name": f"PRODUCT {i}",
                "value": value,
                "transformed_value": transformed_value,
                "status": "TRANSFORMED",
                "category": random.choice(config.categories or ["PREMIUM", "STANDARD"]),
                "priority": priority,
                "etl_run_id": run_id,
                "processed_at": now,
                "processed_by": "test_user",
            }
            data.append(record)
        
        return spark.createDataFrame(data, schema=TestDataGenerator.get_transformed_schema())


class DataFrameAssertions:
    """Custom assertions for DataFrame testing."""
    
    @staticmethod
    def assert_row_count(df: DataFrame, expected_count: int, message: str = None):
        """Assert DataFrame has expected row count."""
        actual_count = df.count()
        msg = message or f"Expected {expected_count} rows, got {actual_count}"
        assert actual_count == expected_count, msg
    
    @staticmethod
    def assert_column_exists(df: DataFrame, column_name: str):
        """Assert column exists in DataFrame."""
        assert column_name in df.columns, f"Column '{column_name}' not found"
    
    @staticmethod
    def assert_no_nulls(df: DataFrame, column_name: str):
        """Assert column has no null values."""
        null_count = df.filter(df[column_name].isNull()).count()
        assert null_count == 0, f"Column '{column_name}' has {null_count} null values"
    
    @staticmethod
    def assert_unique_values(df: DataFrame, column_name: str):
        """Assert column has unique values."""
        total_count = df.count()
        distinct_count = df.select(column_name).distinct().count()
        assert total_count == distinct_count, \
            f"Column '{column_name}' has duplicates: {total_count} total, {distinct_count} distinct"
    
    @staticmethod
    def assert_value_range(
        df: DataFrame,
        column_name: str,
        min_value: Any,
        max_value: Any
    ):
        """Assert column values are within range."""
        from pyspark.sql.functions import col, min as spark_min, max as spark_max
        
        stats = df.agg(
            spark_min(col(column_name)).alias("min"),
            spark_max(col(column_name)).alias("max")
        ).collect()[0]
        
        actual_min = stats["min"]
        actual_max = stats["max"]
        
        assert actual_min >= min_value, \
            f"Min value {actual_min} is less than {min_value}"
        assert actual_max <= max_value, \
            f"Max value {actual_max} is greater than {max_value}"
    
    @staticmethod
    def assert_schemas_equal(df1: DataFrame, df2: DataFrame):
        """Assert two DataFrames have the same schema."""
        schema1 = sorted([(f.name, f.dataType) for f in df1.schema.fields])
        schema2 = sorted([(f.name, f.dataType) for f in df2.schema.fields])
        assert schema1 == schema2, "Schemas do not match"
    
    @staticmethod
    def assert_data_equal(df1: DataFrame, df2: DataFrame, order_by: List[str] = None):
        """Assert two DataFrames have the same data."""
        if order_by:
            df1 = df1.orderBy(order_by)
            df2 = df2.orderBy(order_by)
        
        rows1 = df1.collect()
        rows2 = df2.collect()
        
        assert len(rows1) == len(rows2), \
            f"Row counts differ: {len(rows1)} vs {len(rows2)}"
        
        for i, (row1, row2) in enumerate(zip(rows1, rows2)):
            assert row1 == row2, f"Row {i} differs: {row1} vs {row2}"


class TestDataStore:
    """Manage test data storage and retrieval."""
    
    def __init__(self, spark: SparkSession, base_path: Path):
        """
        Initialize test data store.
        
        Args:
            spark: Spark session
            base_path: Base path for storing test data
        """
        self.spark = spark
        self.base_path = base_path
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def save_test_data(
        self,
        df: DataFrame,
        name: str,
        format: str = "parquet",
        mode: str = "overwrite"
    ) -> Path:
        """
        Save test data to storage.
        
        Args:
            df: DataFrame to save
            name: Name for the dataset
            format: Storage format
            mode: Write mode
            
        Returns:
            Path where data was saved
        """
        path = self.base_path / name
        df.write.format(format).mode(mode).save(str(path))
        return path
    
    def load_test_data(
        self,
        name: str,
        format: str = "parquet"
    ) -> DataFrame:
        """
        Load test data from storage.
        
        Args:
            name: Name of the dataset
            format: Storage format
            
        Returns:
            Loaded DataFrame
        """
        path = self.base_path / name
        return self.spark.read.format(format).load(str(path))
    
    def cleanup(self):
        """Remove all test data."""
        if self.base_path.exists():
            shutil.rmtree(self.base_path)


class MockDataSource:
    """Mock data source for testing extraction."""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize mock data source.
        
        Args:
            spark: Spark session
        """
        self.spark = spark
        self.data_store: Dict[str, DataFrame] = {}
    
    def register_table(self, name: str, df: DataFrame):
        """
        Register a table in the mock source.
        
        Args:
            name: Table name
            df: DataFrame to register
        """
        self.data_store[name] = df
        df.createOrReplaceTempView(name)
    
    def query(self, sql: str) -> DataFrame:
        """
        Execute SQL query against mock source.
        
        Args:
            sql: SQL query string
            
        Returns:
            Query result DataFrame
        """
        return self.spark.sql(sql)
    
    def get_table(self, name: str) -> DataFrame:
        """
        Get table from mock source.
        
        Args:
            name: Table name
            
        Returns:
            Table DataFrame
        """
        return self.data_store.get(name)


class TestRunContext:
    """Context manager for test runs with isolation."""
    
    def __init__(
        self,
        run_id: str,
        spark_context: SparkTestContext,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize test run context.
        
        Args:
            run_id: Test run identifier
            spark_context: Spark test context
            config: Test configuration
        """
        self.run_id = run_id
        self.spark_context = spark_context
        self.config = config or {}
        self.data_store = TestDataStore(
            spark_context.spark,
            spark_context.temp_dir / "data"
        )
        self.mock_source = MockDataSource(spark_context.spark)
        self.metrics: Dict[str, Any] = {}
    
    def __enter__(self):
        """Enter test context."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit test context and cleanup."""
        self.cleanup()
    
    def cleanup(self):
        """Clean up test resources."""
        self.data_store.cleanup()
    
    def record_metric(self, name: str, value: Any):
        """Record a test metric."""
        self.metrics[name] = value
    
    def get_metric(self, name: str) -> Any:
        """Get a recorded metric."""
        return self.metrics.get(name)


def create_test_config(overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Create test configuration with defaults.
    
    Args:
        overrides: Configuration overrides
        
    Returns:
        Test configuration dictionary
    """
    config = {
        "batch_size": 100,
        "max_retries": 3,
        "source_type": "DATABASE",
        "target_type": "DATABASE",
        "enable_validation": True,
        "enable_reconciliation": False,
        "log_level": "INFO",
    }
    
    if overrides:
        config.update(overrides)
    
    return config


# Pytest fixtures

@pytest.fixture(scope="function")
def spark_session():
    """Fixture for creating isolated Spark session per test."""
    context = SparkTestSessionBuilder.create_test_session("test")
    yield context.spark
    context.spark.stop()
    context.cleanup()


@pytest.fixture(scope="function")
def spark_context():
    """Fixture for creating full test context per test."""
    context = SparkTestSessionBuilder.create_test_session("test")
    yield context
    context.spark.stop()
    context.cleanup()


@pytest.fixture(scope="function")
def test_data_generator():
    """Fixture for test data generator."""
    return TestDataGenerator()


@pytest.fixture(scope="function")
def test_run_context(spark_context):
    """Fixture for test run context."""
    context = TestRunContext(
        run_id="TEST_RUN_001",
        spark_context=spark_context
    )
    yield context
    context.cleanup()


@pytest.fixture(scope="function")
def sample_source_data(spark_session):
    """Fixture for sample source data."""
    config = TestDataConfig(num_records=10)
    return TestDataGenerator.generate_source_data(spark_session, config)


@pytest.fixture(scope="function")
def sample_transformed_data(spark_session):
    """Fixture for sample transformed data."""
    config = TestDataConfig(num_records=10)
    return TestDataGenerator.generate_transformed_data(spark_session, config)


@pytest.fixture(scope="function")
def df_assertions():
    """Fixture for DataFrame assertions."""
    return DataFrameAssertions()