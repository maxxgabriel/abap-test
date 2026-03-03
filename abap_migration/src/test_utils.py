"""
Test Framework Utilities for ETL Testing
Provides reusable fixtures, assertions, and isolation helpers
"""

from typing import List, Dict, Any, Optional, Callable
from datetime import datetime, timedelta
from decimal import Decimal
from contextlib import contextmanager
import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    IntegerType, TimestampType
)


class TestDataFactory:
    """Factory for creating test data with various patterns"""
    
    @staticmethod
    def create_source_records(
        count: int = 10,
        id_prefix: str = "TEST",
        category: str = "STANDARD",
        status: str = "ACTIVE",
        base_value: Decimal = Decimal("100.00")
    ) -> List[Dict[str, Any]]:
        """Create source data records for testing"""
        records = []
        base_time = datetime.now()
        
        for i in range(count):
            record_id = f"{id_prefix}{str(i+1).zfill(6)}"
            records.append({
                "id": record_id,
                "name": f"Product {i+1}",
                "value": base_value + Decimal(i * 10),
                "status": status,
                "category": category,
                "source_system": "TEST_SYSTEM",
                "created_at": base_time - timedelta(days=i),
                "created_by": "TEST_USER",
                "changed_at": base_time,
                "changed_by": "TEST_USER"
            })
        
        return records
    
    @staticmethod
    def create_transformed_records(
        count: int = 10,
        run_id: str = "TESTRUN001",
        priority: int = 3
    ) -> List[Dict[str, Any]]:
        """Create transformed data records for testing"""
        records = []
        base_time = datetime.now()
        
        for i in range(count):
            record_id = f"TEST{str(i+1).zfill(6)}"
            value = Decimal(100 + i * 10)
            records.append({
                "id": record_id,
                "name": f"PRODUCT {i+1}",
                "value": value,
                "transformed_value": value * Decimal("1.5"),
                "status": "TRANSFORMED",
                "category": "STANDARD",
                "priority": priority,
                "etl_run_id": run_id,
                "processed_at": base_time,
                "processed_by": "TEST_USER"
            })
        
        return records
    
    @staticmethod
    def create_invalid_records(count: int = 5) -> List[Dict[str, Any]]:
        """Create invalid records for validation testing"""
        return [
            {"id": None, "name": "Invalid1", "value": Decimal("100")},
            {"id": "INV001", "name": None, "value": Decimal("200")},
            {"id": "INV002", "name": "Invalid2", "value": None},
            {"id": "INV003", "name": "Invalid3", "value": Decimal("-100")},
            {"id": "", "name": "", "value": Decimal("0")}
        ][:count]


class TestSchemas:
    """Centralized schema definitions for testing"""
    
    @staticmethod
    def source_schema() -> StructType:
        """Schema for source data"""
        return StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("value", DecimalType(15, 2), False),
            StructField("status", StringType(), False),
            StructField("category", StringType(), False),
            StructField("source_system", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", StringType(), True),
            StructField("changed_at", TimestampType(), True),
            StructField("changed_by", StringType(), True)
        ])
    
    @staticmethod
    def transformed_schema() -> StructType:
        """Schema for transformed data"""
        return StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("value", DecimalType(15, 2), False),
            StructField("transformed_value", DecimalType(15, 2), False),
            StructField("status", StringType(), False),
            StructField("category", StringType(), False),
            StructField("priority", IntegerType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("processed_at", TimestampType(), False),
            StructField("processed_by", StringType(), False)
        ])
    
    @staticmethod
    def config_schema() -> StructType:
        """Schema for configuration data"""
        return StructType([
            StructField("config_key", StringType(), False),
            StructField("config_value", StringType(), False),
            StructField("description", StringType(), True),
            StructField("config_type", StringType(), True),
            StructField("is_active", StringType(), True)
        ])


class SparkTestFixture:
    """Test fixture manager for Spark sessions and data"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._temp_tables: List[str] = []
        self._temp_views: List[str] = []
    
    def create_temp_table(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
        schema: StructType
    ) -> DataFrame:
        """Create temporary table for testing"""
        df = self.spark.createDataFrame(data, schema)
        df.createOrReplaceTempView(table_name)
        self._temp_views.append(table_name)
        return df
    
    def create_source_table(
        self,
        count: int = 10,
        table_name: str = "test_source_data",
        **kwargs
    ) -> DataFrame:
        """Create source data table for testing"""
        data = TestDataFactory.create_source_records(count, **kwargs)
        return self.create_temp_table(
            table_name,
            data,
            TestSchemas.source_schema()
        )
    
    def create_transformed_table(
        self,
        count: int = 10,
        table_name: str = "test_transformed_data",
        **kwargs
    ) -> DataFrame:
        """Create transformed data table for testing"""
        data = TestDataFactory.create_transformed_records(count, **kwargs)
        return self.create_temp_table(
            table_name,
            data,
            TestSchemas.transformed_schema()
        )
    
    def create_config_table(
        self,
        table_name: str = "test_config_data"
    ) -> DataFrame:
        """Create configuration table for testing"""
        config_data = [
            {
                "config_key": "BATCH_SIZE",
                "config_value": "1000",
                "description": "Default batch size",
                "config_type": "PERFORMANCE",
                "is_active": "X"
            },
            {
                "config_key": "MAX_RETRIES",
                "config_value": "3",
                "description": "Maximum retries",
                "config_type": "ERROR_HANDLING",
                "is_active": "X"
            },
            {
                "config_key": "PREMIUM_MULTIPLIER",
                "config_value": "1.5",
                "description": "Premium multiplier",
                "config_type": "BUSINESS_RULE",
                "is_active": "X"
            }
        ]
        return self.create_temp_table(
            table_name,
            config_data,
            TestSchemas.config_schema()
        )
    
    def cleanup(self):
        """Clean up all temporary tables and views"""
        for view in self._temp_views:
            try:
                self.spark.catalog.dropTempView(view)
            except Exception:
                pass
        self._temp_views.clear()
    
    @contextmanager
    def isolated_test(self):
        """Context manager for isolated test execution"""
        try:
            yield self
        finally:
            self.cleanup()


class DataFrameAssertions:
    """Custom assertions for DataFrame testing"""
    
    @staticmethod
    def assert_dataframe_not_empty(df: DataFrame, msg: str = "DataFrame should not be empty"):
        """Assert DataFrame is not empty"""
        assert df.count() > 0, msg
    
    @staticmethod
    def assert_dataframe_count(df: DataFrame, expected: int, msg: str = None):
        """Assert DataFrame has expected row count"""
        actual = df.count()
        msg = msg or f"Expected {expected} rows, got {actual}"
        assert actual == expected, msg
    
    @staticmethod
    def assert_dataframe_schema(df: DataFrame, expected_schema: StructType, msg: str = None):
        """Assert DataFrame has expected schema"""
        msg = msg or "Schema mismatch"
        assert df.schema == expected_schema, msg
    
    @staticmethod
    def assert_column_exists(df: DataFrame, column_name: str, msg: str = None):
        """Assert column exists in DataFrame"""
        msg = msg or f"Column '{column_name}' should exist"
        assert column_name in df.columns, msg
    
    @staticmethod
    def assert_no_nulls(df: DataFrame, column_name: str, msg: str = None):
        """Assert column has no null values"""
        from pyspark.sql.functions import col
        null_count = df.filter(col(column_name).isNull()).count()
        msg = msg or f"Column '{column_name}' should have no nulls, found {null_count}"
        assert null_count == 0, msg
    
    @staticmethod
    def assert_all_values_positive(df: DataFrame, column_name: str, msg: str = None):
        """Assert all numeric values are positive"""
        from pyspark.sql.functions import col
        negative_count = df.filter(col(column_name) <= 0).count()
        msg = msg or f"Column '{column_name}' should have all positive values, found {negative_count} non-positive"
        assert negative_count == 0, msg
    
    @staticmethod
    def assert_unique_ids(df: DataFrame, id_column: str = "id", msg: str = None):
        """Assert all IDs are unique"""
        total_count = df.count()
        unique_count = df.select(id_column).distinct().count()
        msg = msg or f"IDs should be unique, found {total_count - unique_count} duplicates"
        assert total_count == unique_count, msg
    
    @staticmethod
    def assert_dataframes_equal(df1: DataFrame, df2: DataFrame, check_order: bool = False):
        """Assert two DataFrames are equal"""
        if check_order:
            assert df1.collect() == df2.collect(), "DataFrames are not equal (order matters)"
        else:
            from pyspark.sql.functions import col
            # Sort both DataFrames by all columns for comparison
            sorted_df1 = df1.sort(*df1.columns).collect()
            sorted_df2 = df2.sort(*df2.columns).collect()
            assert sorted_df1 == sorted_df2, "DataFrames are not equal (order ignored)"
    
    @staticmethod
    def assert_value_in_range(
        df: DataFrame,
        column_name: str,
        min_value: Any,
        max_value: Any,
        msg: str = None
    ):
        """Assert all values in column are within range"""
        from pyspark.sql.functions import col
        out_of_range = df.filter(
            (col(column_name) < min_value) | (col(column_name) > max_value)
        ).count()
        msg = msg or f"Column '{column_name}' should have values between {min_value} and {max_value}, found {out_of_range} out of range"
        assert out_of_range == 0, msg


class TestIsolation:
    """Utilities for test isolation and cleanup"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._cleanup_functions: List[Callable] = []
    
    def register_cleanup(self, cleanup_func: Callable):
        """Register cleanup function to be called after test"""
        self._cleanup_functions.append(cleanup_func)
    
    def cleanup_all(self):
        """Execute all registered cleanup functions"""
        for cleanup_func in reversed(self._cleanup_functions):
            try:
                cleanup_func()
            except Exception as e:
                print(f"Cleanup error: {e}")
        self._cleanup_functions.clear()
    
    @contextmanager
    def table_cleanup(self, table_names: List[str]):
        """Context manager to clean up tables after test"""
        try:
            yield
        finally:
            for table_name in table_names:
                try:
                    self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")
                except Exception:
                    pass
    
    @contextmanager
    def temp_view_cleanup(self, view_names: List[str]):
        """Context manager to clean up temp views after test"""
        try:
            yield
        finally:
            for view_name in view_names:
                try:
                    self.spark.catalog.dropTempView(view_name)
                except Exception:
                    pass


class MockLogger:
    """Mock logger for testing without side effects"""
    
    def __init__(self):
        self.logs = []
    
    def log_info(self, component: str, message: str, details: str = None):
        """Log info message"""
        self.logs.append({
            "level": "INFO",
            "component": component,
            "message": message,
            "details": details
        })
    
    def log_error(self, component: str, message: str, details: str = None):
        """Log error message"""
        self.logs.append({
            "level": "ERROR",
            "component": component,
            "message": message,
            "details": details
        })
    
    def log_warning(self, component: str, message: str, details: str = None):
        """Log warning message"""
        self.logs.append({
            "level": "WARNING",
            "component": component,
            "message": message,
            "details": details
        })
    
    def get_logs(self, level: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all logs or filter by level"""
        if level:
            return [log for log in self.logs if log["level"] == level]
        return self.logs
    
    def clear_logs(self):
        """Clear all logs"""
        self.logs.clear()
    
    def assert_logged(self, level: str, component: str, message_contains: str):
        """Assert a log entry exists"""
        for log in self.logs:
            if (log["level"] == level and 
                log["component"] == component and 
                message_contains in log["message"]):
                return True
        raise AssertionError(
            f"Expected log not found: {level} - {component} - {message_contains}"
        )


class TestMetrics:
    """Collect and assert on test metrics"""
    
    def __init__(self):
        self.metrics = {}
    
    def record_metric(self, name: str, value: Any):
        """Record a test metric"""
        self.metrics[name] = value
    
    def get_metric(self, name: str, default: Any = None) -> Any:
        """Get recorded metric"""
        return self.metrics.get(name, default)
    
    def assert_metric_equals(self, name: str, expected: Any):
        """Assert metric equals expected value"""
        actual = self.get_metric(name)
        assert actual == expected, f"Metric '{name}': expected {expected}, got {actual}"
    
    def assert_metric_greater_than(self, name: str, threshold: Any):
        """Assert metric is greater than threshold"""
        actual = self.get_metric(name)
        assert actual > threshold, f"Metric '{name}': expected > {threshold}, got {actual}"
    
    def clear_metrics(self):
        """Clear all metrics"""
        self.metrics.clear()


def pytest_configure(config):
    """Pytest configuration hook"""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "unit: mark test as unit test"
    )


@pytest.fixture(scope="session")
def spark_session():
    """Create SparkSession for testing"""
    spark = (SparkSession.builder
             .appName("ETL_Test")
             .master("local[2]")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.default.parallelism", "2")
             .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
             .getOrCreate())
    
    yield spark
    
    spark.stop()


@pytest.fixture(scope="function")
def spark_fixture(spark_session):
    """Create SparkTestFixture for each test"""
    fixture = SparkTestFixture(spark_session)
    
    with fixture.isolated_test():
        yield fixture


@pytest.fixture(scope="function")
def test_isolation(spark_session):
    """Create TestIsolation for each test"""
    isolation = TestIsolation(spark_session)
    
    yield isolation
    
    isolation.cleanup_all()


@pytest.fixture(scope="function")
def mock_logger():
    """Create MockLogger for testing"""
    logger = MockLogger()
    yield logger
    logger.clear_logs()


@pytest.fixture(scope="function")
def test_metrics():
    """Create TestMetrics for testing"""
    metrics = TestMetrics()
    yield metrics
    metrics.clear_metrics()


@pytest.fixture(scope="function")
def data_factory():
    """Create TestDataFactory"""
    return TestDataFactory()


@pytest.fixture(scope="function")
def df_assertions():
    """Create DataFrameAssertions"""
    return DataFrameAssertions()