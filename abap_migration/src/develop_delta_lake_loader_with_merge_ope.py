# PySpark Migration: Delta Lake Loader with Merge Operations

===FILE: src/load.py===
"""
Delta Lake Loader Module
Implements PySpark-based data loading with Delta Lake merge operations.
Replaces ABAP batch loops with DataFrame operations and COMMIT WORK with Spark write modes.
"""

from typing import Dict, List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from delta.tables import DeltaTable
import logging
from datetime import datetime

from src.logger import ETLLogger
from src.config import Config


class LoadResult:
    """Data class for load operation results"""
    def __init__(self):
        self.success_count: int = 0
        self.error_count: int = 0
        self.total_count: int = 0
        self.errors: List[str] = []
        self.insert_count: int = 0
        self.update_count: int = 0
        self.delete_count: int = 0


class DeltaLakeLoader:
    """
    PySpark-based Delta Lake loader with merge operations.
    Replaces ABAP batch processing with DataFrame repartitioning and
    INSERT/UPDATE/UPSERT with Delta merge operations.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Config,
        run_id: str,
        target_type: str = "delta",
        batch_size: int = 1000
    ):
        """
        Initialize Delta Lake Loader
        
        Args:
            spark: SparkSession instance
            config: Configuration object
            run_id: Unique run identifier
            target_type: Target storage type (delta, parquet, etc.)
            batch_size: Number of records per partition (used for repartitioning)
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.target_type = target_type
        self.batch_size = batch_size
        self.logger = ETLLogger(component="LOADER")
        
        # Get target paths from config
        self.target_path = config.get("target.delta_table_path")
        self.checkpoint_path = config.get("target.checkpoint_path")
        
    def load_data(
        self,
        df: DataFrame,
        mode: str = "upsert",
        partition_cols: Optional[List[str]] = None
    ) -> LoadResult:
        """
        Load data using Delta Lake operations
        
        Args:
            df: Transformed DataFrame to load
            mode: Load mode (insert, update, upsert)
            partition_cols: Columns to partition by
            
        Returns:
            LoadResult with operation statistics
        """
        self.logger.info(
            f"Starting load - Mode: {mode}, Batch size: {self.batch_size}"
        )
        
        result = LoadResult()
        result.total_count = df.count()
        
        if result.total_count == 0:
            self.logger.warning("No data to load")
            return result
        
        try:
            # Optimize DataFrame with repartitioning (replaces ABAP batch loops)
            num_partitions = max(1, result.total_count // self.batch_size)
            df_optimized = df.repartition(num_partitions)
            
            # Execute load based on mode
            if mode.lower() == "insert":
                result = self._insert_new(df_optimized, partition_cols)
            elif mode.lower() == "update":
                result = self._update_existing(df_optimized)
            elif mode.lower() == "upsert":
                result = self._upsert_data(df_optimized)
            elif mode.lower() == "merge":
                result = self._merge_with_delete(df_optimized)
            else:
                self.logger.error(f"Unknown load mode: {mode}")
                result.error_count = result.total_count
                result.errors.append(f"Unknown load mode: {mode}")
                return result
            
            # Reconcile data (replaces ABAP reconciliation)
            if self.config.get("loader.enable_reconciliation", True):
                is_reconciled = self._reconcile_data(df_optimized)
                if not is_reconciled:
                    self.logger.warning("Data reconciliation failed")
                    result.errors.append("Reconciliation validation failed")
            
            # Log results
            self.logger.info(
                f"Load complete - Success: {result.success_count}, "
                f"Errors: {result.error_count}, "
                f"Inserts: {result.insert_count}, "
                f"Updates: {result.update_count}"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Load operation failed: {str(e)}")
            result.error_count = result.total_count
            result.errors.append(str(e))
            return result
    
    def _insert_new(
        self,
        df: DataFrame,
        partition_cols: Optional[List[str]] = None
    ) -> LoadResult:
        """
        Insert new records using Delta Lake append mode
        Replaces ABAP INSERT with Spark write.mode("append")
        
        Args:
            df: DataFrame to insert
            partition_cols: Columns to partition by
            
        Returns:
            LoadResult with insert statistics
        """
        result = LoadResult()
        
        try:
            # Add ETL metadata
            df_with_metadata = df.withColumn("etl_run_id", lit(self.run_id)) \
                                 .withColumn("processed_at", current_timestamp())
            
            # Write using Delta Lake (replaces COMMIT WORK)
            writer = df_with_metadata.write \
                .format("delta") \
                .mode("append")
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            writer.save(self.target_path)
            
            result.success_count = df.count()
            result.insert_count = result.success_count
            
            self.logger.info(f"Inserted {result.insert_count} records")
            
        except Exception as e:
            self.logger.error(f"Insert operation failed: {str(e)}")
            result.error_count = df.count()
            result.errors.append(str(e))
        
        return result
    
    def _update_existing(self, df: DataFrame) -> LoadResult:
        """
        Update existing records using Delta Lake merge
        Replaces ABAP UPDATE with Delta merge operation
        
        Args:
            df: DataFrame with updates
            
        Returns:
            LoadResult with update statistics
        """
        result = LoadResult()
        
        try:
            if not DeltaTable.isDeltaTable(self.spark, self.target_path):
                self.logger.error("Target is not a Delta table, cannot update")
                result.error_count = df.count()
                result.errors.append("Target is not a Delta table")
                return result
            
            # Load Delta table
            delta_table = DeltaTable.forPath(self.spark, self.target_path)
            
            # Add metadata
            df_with_metadata = df.withColumn("etl_run_id", lit(self.run_id)) \
                                 .withColumn("processed_at", current_timestamp())
            
            # Perform merge with update only
            merge_result = delta_table.alias("target") \
                .merge(
                    df_with_metadata.alias("source"),
                    "target.id = source.id"
                ) \
                .whenMatchedUpdateAll() \
                .execute()
            
            result.success_count = df.count()
            result.update_count = result.success_count
            
            self.logger.info(f"Updated {result.update_count} records")
            
        except Exception as e:
            self.logger.error(f"Update operation failed: {str(e)}")
            result.error_count = df.count()
            result.errors.append(str(e))
        
        return result
    
    def _upsert_data(self, df: DataFrame) -> LoadResult:
        """
        Perform upsert (update + insert) using Delta Lake merge
        Replaces ABAP UPSERT logic with Delta merge whenMatched/whenNotMatched
        
        Args:
            df: DataFrame to upsert
            
        Returns:
            LoadResult with upsert statistics
        """
        result = LoadResult()
        
        try:
            # Create Delta table if it doesn't exist
            if not DeltaTable.isDeltaTable(self.spark, self.target_path):
                self.logger.info("Creating new Delta table")
                self._create_delta_table(df)
                result.insert_count = df.count()
                result.success_count = result.insert_count
                return result
            
            # Load existing Delta table
            delta_table = DeltaTable.forPath(self.spark, self.target_path)
            
            # Add metadata
            df_with_metadata = df.withColumn("etl_run_id", lit(self.run_id)) \
                                 .withColumn("processed_at", current_timestamp())
            
            # Get merge key from config
            merge_keys = self.config.get("loader.merge_keys", ["id"])
            merge_condition = " AND ".join([
                f"target.{key} = source.{key}" for key in merge_keys
            ])
            
            # Perform Delta merge (replaces ABAP COMMIT WORK)
            delta_table.alias("target") \
                .merge(
                    df_with_metadata.alias("source"),
                    merge_condition
                ) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
            
            # Get statistics from Delta history
            history = delta_table.history(1).select("operationMetrics").collect()
            if history:
                metrics = history[0]["operationMetrics"]
                result.update_count = int(metrics.get("numTargetRowsUpdated", 0))
                result.insert_count = int(metrics.get("numTargetRowsInserted", 0))
                result.success_count = result.update_count + result.insert_count
            else:
                result.success_count = df.count()
            
            self.logger.info(
                f"Upsert complete - Inserted: {result.insert_count}, "
                f"Updated: {result.update_count}"
            )
            
        except Exception as e:
            self.logger.error(f"Upsert operation failed: {str(e)}")
            result.error_count = df.count()
            result.errors.append(str(e))
        
        return result
    
    def _merge_with_delete(self, df: DataFrame) -> LoadResult:
        """
        Perform merge with delete operations for CDC
        Handles insert, update, and delete operations
        
        Args:
            df: DataFrame with CDC operations (including delete flag)
            
        Returns:
            LoadResult with merge statistics
        """
        result = LoadResult()
        
        try:
            if not DeltaTable.isDeltaTable(self.spark, self.target_path):
                self.logger.error("Target is not a Delta table")
                result.error_count = df.count()
                return result
            
            delta_table = DeltaTable.forPath(self.spark, self.target_path)
            
            # Add metadata
            df_with_metadata = df.withColumn("etl_run_id", lit(self.run_id)) \
                                 .withColumn("processed_at", current_timestamp())
            
            merge_keys = self.config.get("loader.merge_keys", ["id"])
            merge_condition = " AND ".join([
                f"target.{key} = source.{key}" for key in merge_keys
            ])
            
            # Perform merge with delete
            merge_builder = delta_table.alias("target").merge(
                df_with_metadata.alias("source"),
                merge_condition
            )
            
            # Handle updates
            merge_builder = merge_builder.whenMatchedUpdate(
                condition="source.operation_type != 'DELETE'",
                set={col: f"source.{col}" for col in df.columns if col != "operation_type"}
            )
            
            # Handle deletes
            merge_builder = merge_builder.whenMatchedDelete(
                condition="source.operation_type = 'DELETE'"
            )
            
            # Handle inserts
            merge_builder = merge_builder.whenNotMatchedInsertAll()
            
            merge_builder.execute()
            
            # Get statistics
            history = delta_table.history(1).select("operationMetrics").collect()
            if history:
                metrics = history[0]["operationMetrics"]
                result.update_count = int(metrics.get("numTargetRowsUpdated", 0))
                result.insert_count = int(metrics.get("numTargetRowsInserted", 0))
                result.delete_count = int(metrics.get("numTargetRowsDeleted", 0))
                result.success_count = result.update_count + result.insert_count + result.delete_count
            
            self.logger.info(
                f"Merge complete - Inserted: {result.insert_count}, "
                f"Updated: {result.update_count}, Deleted: {result.delete_count}"
            )
            
        except Exception as e:
            self.logger.error(f"Merge operation failed: {str(e)}")
            result.error_count = df.count()
            result.errors.append(str(e))
        
        return result
    
    def _create_delta_table(self, df: DataFrame):
        """
        Create a new Delta table with initial data
        
        Args:
            df: DataFrame to write as new table
        """
        partition_cols = self.config.get("target.partition_columns", [])
        
        writer = df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        writer.save(self.target_path)
        
        self.logger.info(f"Created new Delta table at {self.target_path}")
    
    def _reconcile_data(self, df: DataFrame) -> bool:
        """
        Reconcile loaded data with source
        Replaces ABAP reconciliation logic
        
        Args:
            df: Source DataFrame
            
        Returns:
            True if reconciliation passes
        """
        try:
            source_count = df.count()
            
            # Read from Delta table
            target_df = self.spark.read.format("delta").load(self.target_path)
            target_count = target_df.filter(col("etl_run_id") == self.run_id).count()
            
            if source_count == target_count:
                self.logger.info(f"Reconciliation passed: {source_count} records")
                return True
            else:
                self.logger.warning(
                    f"Reconciliation failed - Source: {source_count}, "
                    f"Target: {target_count}"
                )
                return False
                
        except Exception as e:
            self.logger.error(f"Reconciliation error: {str(e)}")
            return False
    
    def optimize_table(self):
        """
        Optimize Delta table by running OPTIMIZE and VACUUM
        Should be run periodically for performance
        """
        try:
            if not DeltaTable.isDeltaTable(self.spark, self.target_path):
                return
            
            delta_table = DeltaTable.forPath(self.spark, self.target_path)
            
            # Run OPTIMIZE with Z-ordering
            z_order_cols = self.config.get("target.z_order_columns", [])
            if z_order_cols:
                delta_table.optimize().executeZOrderBy(*z_order_cols)
                self.logger.info(f"Optimized table with Z-order on {z_order_cols}")
            else:
                delta_table.optimize().executeCompaction()
                self.logger.info("Optimized table with compaction")
            
            # Run VACUUM (only if configured)
            retention_hours = self.config.get("target.vacuum_retention_hours", 168)
            if retention_hours > 0:
                delta_table.vacuum(retention_hours)
                self.logger.info(f"Vacuumed table with {retention_hours} hour retention")
                
        except Exception as e:
            self.logger.error(f"Table optimization failed: {str(e)}")
    
    def get_table_statistics(self) -> Dict:
        """
        Get Delta table statistics
        
        Returns:
            Dictionary with table statistics
        """
        try:
            if not DeltaTable.isDeltaTable(self.spark, self.target_path):
                return {}
            
            delta_table = DeltaTable.forPath(self.spark, self.target_path)
            
            # Get table details
            details = delta_table.detail().collect()[0]
            
            # Get history
            history = delta_table.history(10).collect()
            
            return {
                "table_path": self.target_path,
                "num_files": details["numFiles"],
                "size_in_bytes": details["sizeInBytes"],
                "partition_columns": details["partitionColumns"],
                "created_at": details["createdAt"],
                "last_modified": details["lastModified"],
                "recent_operations": len(history)
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get table statistics: {str(e)}")
            return {}


===FILE: src/config.py===
"""
Configuration Management Module
Handles YAML configuration loading and environment-specific settings
"""

import yaml
import os
from typing import Any, Dict, Optional


class Config:
    """Configuration manager for ETL pipeline"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize configuration
        
        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = config_path
        self.config_data = self._load_config()
        self._merge_environment_vars()
    
    def _load_config(self) -> Dict:
        """Load configuration from YAML file"""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _merge_environment_vars(self):
        """Merge environment variables into configuration"""
        # Override with environment variables if present
        env_mappings = {
            "SPARK_APP_NAME": "spark.app_name",
            "DELTA_TABLE_PATH": "target.delta_table_path",
            "CHECKPOINT_PATH": "target.checkpoint_path",
            "LOG_LEVEL": "logging.level",
            "BATCH_SIZE": "loader.batch_size"
        }
        
        for env_var, config_key in env_mappings.items():
            value = os.getenv(env_var)
            if value:
                self.set(config_key, value)
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by dot-notation key
        
        Args:
            key: Configuration key (e.g., 'spark.app_name')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self.config_data
        
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
            
            if value is None:
                return default
        
        return value
    
    def set(self, key: str, value: Any):
        """
        Set configuration value by dot-notation key
        
        Args:
            key: Configuration key
            value: Value to set
        """
        keys = key.split('.')
        config = self.config_data
        
        for k in keys[:-1]:
            config = config.setdefault(k, {})
        
        config[keys[-1]] = value
    
    def get_spark_config(self) -> Dict[str, str]:
        """Get Spark-specific configuration"""
        return self.get("spark", {})
    
    def get_loader_config(self) -> Dict[str, Any]:
        """Get loader-specific configuration"""
        return self.get("loader", {})


===FILE: src/logger.py===
"""
Logging Module
Provides centralized logging for ETL pipeline
"""

import logging
import sys
from datetime import datetime
from typing import Optional


class ETLLogger:
    """Centralized logger for ETL operations"""
    
    _loggers = {}
    
    def __init__(self, component: str = "ETL", level: str = "INFO"):
        """
        Initialize logger for component
        
        Args:
            component: Component name (e.g., 'LOADER', 'TRANSFORMER')
            level: Log level (DEBUG, INFO, WARNING, ERROR)
        """
        self.component = component
        
        if component not in ETLLogger._loggers:
            logger = logging.getLogger(component)
            logger.setLevel(getattr(logging, level.upper()))
            
            # Console handler
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(getattr(logging, level.upper()))
            
            # Formatter
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            
            logger.addHandler(handler)
            ETLLogger._loggers[component] = logger
        
        self.logger = ETLLogger._loggers[component]
    
    def info(self, message: str, details: Optional[str] = None):
        """Log info message"""
        full_message = f"{message}"
        if details:
            full_message += f" | Details: {details}"
        self.logger.info(full_message)
    
    def warning(self, message: str, details: Optional[str] = None):
        """Log warning message"""
        full_message = f"{message}"
        if details:
            full_message += f" | Details: {details}"
        self.logger.warning(full_message)
    
    def error(self, message: str, details: Optional[str] = None):
        """Log error message"""
        full_message = f"{message}"
        if details:
            full_message += f" | Details: {details}"
        self.logger.error(full_message)
    
    def debug(self, message: str, details: Optional[str] = None):
        """Log debug message"""
        full_message = f"{message}"
        if details:
            full_message += f" | Details: {details}"
        self.logger.debug(full_message)


===FILE: config.yaml===
# ETL Pipeline Configuration

spark:
  app_name: "etl_delta_lake_loader"
  master: "local[*]"
  log_level: "WARN"
  
  # Spark SQL configurations
  sql:
    adaptive:
      enabled: true
      coalescePartitions:
        enabled: true
    shuffle:
      partitions: 200
  
  # Delta Lake configurations
  delta:
    logRetentionDuration: "interval 30 days"
    deletedFileRetentionDuration: "interval 7 days"
    autoOptimize:
      optimizeWrite: true
      autoCompact: true

# Source configuration
source:
  database: "default"
  table: "etl_source_data"
  format: "delta"
  path: "/data/source"
  
  # Extraction settings
  extraction:
    mode: "DATABASE"  # DATABASE, STAGING, INCREMENTAL
    batch_size: 10000
    max_records: 0  # 0 = unlimited
    
# Target configuration
target:
  database: "default"
  table: "etl_target_data"
  delta_table_path: "/data/target/etl_target_data"
  checkpoint_path: "/data/checkpoints/etl_loader"
  
  # Partitioning
  partition_columns:
    - "category"
    - "processed_date"
  
  # Z-ordering for query optimization
  z_order_columns:
    - "id"
    - "status"
  
  # Retention settings
  vacuum_retention_hours: 168  # 7 days
  
# Loader configuration
loader:
  batch_size: 1000  # Records per partition
  mode: "upsert"  # insert, update, upsert, merge
  
  # Merge configuration
  merge_keys:
    - "id"
  
  # Reconciliation
  enable_reconciliation: true
  
  # Performance tuning
  repartition: true
  coalesce_output: false
  
  # Error handling
  max_retries: 3
  retry_delay_seconds: 5
  
# Transform configuration
transform:
  # Business rules
  business_rules:
    premium_multiplier: 1.5
    high_value_threshold: 750
    medium_value_threshold: 300
  
  # Validation rules
  validation:
    required_fields:
      - "id"
      - "name"
      - "value"
    
    field_rules:
      value:
        min: 0
        max: 999999
      priority:
        min: 1
        max: 5

# Data quality configuration
data_quality:
  enabled: true
  
  checks:
    - name: "completeness"
      enabled: true
      critical: true
    
    - name: "uniqueness"
      enabled: true
      critical: true
    
    - name: "validity"
      enabled: true
      critical: false
    
    - name: "consistency"
      enabled: true
      critical: false
  
  # Anomaly detection
  anomaly_detection:
    enabled: true
    std_deviation_threshold: 3.0

# Logging configuration
logging:
  level: "INFO"  # DEBUG, INFO, WARNING, ERROR
  
  # Log to file
  file:
    enabled: true
    path: "/logs/etl_pipeline.log"
    max_size_mb: 100
    backup_count: 5
  
  # Component-specific levels
  components:
    LOADER: "INFO"
    TRANSFORMER: "INFO"
    EXTRACTOR: "INFO"
    QUALITY: "INFO"

# Monitoring configuration
monitoring:
  enabled: true
  
  metrics:
    - name: "records_processed"
      type: "counter"
    
    - name: "load_duration"
      type: "histogram"
    
    - name: "error_rate"
      type: "gauge"
  
  # Alerting
  alerts:
    email:
      enabled: false
      recipients:
        - "admin@example.com"
    
    slack:
      enabled: false
      webhook_url: ""

# Execution configuration
execution:
  run_type: "MANUAL"  # MANUAL, SCHEDULED, INCREMENTAL
  parallel_jobs: 4
  timeout_minutes: 60


===FILE: tests/test_load.py===
"""
Unit tests for Delta Lake Loader
Tests load operations, merge strategies, and reconciliation
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from delta.tables import DeltaTable
import tempfile
import shutil
from datetime import datetime

from src.load import DeltaLakeLoader, LoadResult
from src.config import Config
from src.logger import ETLLogger


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("test_delta_loader") \
        .master("local[2]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    yield spark
    
    spark.stop()


@pytest.fixture
def test_config(tmp_path):
    """Create test configuration"""
    config_data = {
        "target": {
            "delta_table_path": str(tmp_path / "target"),
            "checkpoint_path": str(tmp_path / "checkpoint"),
            "partition_columns": ["category"],
            "z_order_columns": ["id"],
            "vacuum_retention_hours": 0
        },
        "loader": {
            "batch_size": 100,
            "merge_keys": ["id"],
            "enable_reconciliation": True,
            "max_retries": 3
        },
        "logging": {
            "level": "INFO"
        }
    }
    
    config = Config.__new__(Config)
    config.config_data = config_data
    return config


@pytest.fixture
def sample_schema():
    """Define sample data schema"""
    return StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("value", DecimalType(15, 2), False),
        StructField("transformed_value", DecimalType(15, 2), False),
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
        ("TEST003", "Product 3", 300.00, 360.00, "ACTIVE", "BASIC", 3)
    ]
    return spark.createDataFrame(data, schema=sample_schema)


@pytest.fixture
def loader(spark, test_config):
    """Create loader instance"""
    return DeltaLakeLoader(
        spark=spark,
        config=test_config,
        run_id="TEST_RUN_001",
        target_type="delta",
        batch_size=100
    )


class TestDeltaLakeLoader:
    """Test suite for Delta Lake Loader"""
    
    def test_loader_initialization(self, loader, test_config):
        """Test loader initialization"""
        assert loader.spark is not None
        assert loader.config == test_config
        assert loader.run_id == "TEST_RUN_001"
        assert loader.batch_size == 100
        assert loader.target_type == "delta"
    
    def test_insert_new_records(self, loader, sample_data):
        """Test inserting new records"""
        result = loader.load_data(sample_data, mode="insert")
        
        assert result.success_count == 3
        assert result.error_count == 0
        assert result.insert_count == 3
        assert len(result.errors) == 0
        
        # Verify data written
        df_read = loader.spark.read.format("delta").load(loader.target_path)
        assert df_read.count() == 3
    
    def test_upsert_new_records(self, loader, sample_data):
        """Test upsert with new records (should insert)"""
        result = loader.load_data(sample_data, mode="upsert")
        
        assert result.success_count == 3
        assert result.insert_count == 3
        
        # Verify Delta table created
        assert D