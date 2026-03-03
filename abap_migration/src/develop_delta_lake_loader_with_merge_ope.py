# PySpark Migration: Delta Lake Loader with Merge Operations

===FILE: src/load.py===
"""
Delta Lake Loader Module
Handles loading data to Delta Lake with merge operations (INSERT/UPDATE/UPSERT)
Replaces ABAP batch loops with DataFrame repartitioning and COMMIT WORK with write modes
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit
from delta.tables import DeltaTable
from typing import Dict, Any, List, Optional
import logging
from dataclasses import dataclass
from datetime import datetime


@dataclass
class LoadResult:
    """Result of a load operation"""
    success_count: int
    error_count: int
    total_count: int
    errors: List[str]
    duration_seconds: float


class DeltaLakeLoader:
    """
    Delta Lake Loader with merge operations
    Replaces ABAP zcl_etl_loader with Spark DataFrame operations
    """
    
    def __init__(
        self,
        spark: SparkSession,
        target_type: str = "delta",
        batch_size: int = 1000,
        run_id: str = None,
        config: Dict[str, Any] = None
    ):
        """
        Initialize Delta Lake Loader
        
        Args:
            spark: SparkSession instance
            target_type: Target storage type (delta, parquet, etc.)
            batch_size: Number of records per partition (replaces ABAP batch loops)
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.target_type = target_type
        self.batch_size = batch_size
        self.run_id = run_id or f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.config = config or {}
        self.logger = logging.getLogger(__name__)
        
    def load_data(
        self,
        data: DataFrame,
        target_path: str,
        mode: str = "UPSERT",
        merge_keys: List[str] = None
    ) -> LoadResult:
        """
        Load data using appropriate mode (INSERT/UPDATE/UPSERT)
        Replaces ABAP load_data method with batch loops
        
        Args:
            data: Transformed DataFrame to load
            target_path: Target Delta table path or name
            mode: Load mode - INSERT, UPDATE, or UPSERT
            merge_keys: Keys for merge operation (default: ['id'])
            
        Returns:
            LoadResult with success/error counts
        """
        start_time = datetime.now()
        
        self.logger.info(
            f"Starting load - Mode: {mode}, Batch size: {self.batch_size}, "
            f"Target: {target_path}"
        )
        
        if merge_keys is None:
            merge_keys = ["id"]
            
        try:
            # Add ETL metadata columns
            data_with_metadata = self._add_metadata(data)
            
            # Validate data before load
            validation_result = self._validate_data(data_with_metadata)
            if not validation_result["is_valid"]:
                return LoadResult(
                    success_count=0,
                    error_count=data_with_metadata.count(),
                    total_count=data_with_metadata.count(),
                    errors=validation_result["errors"],
                    duration_seconds=0.0
                )
            
            # Repartition data (replaces ABAP batch loops)
            num_partitions = max(1, data_with_metadata.count() // self.batch_size)
            data_partitioned = data_with_metadata.repartition(num_partitions)
            
            # Execute load based on mode
            if mode.upper() == "INSERT":
                success_count = self._insert_new(data_partitioned, target_path)
                error_count = 0
                
            elif mode.upper() == "UPDATE":
                success_count = self._update_existing(
                    data_partitioned, target_path, merge_keys
                )
                error_count = 0
                
            elif mode.upper() == "UPSERT":
                success_count = self._upsert_data(
                    data_partitioned, target_path, merge_keys
                )
                error_count = 0
                
            else:
                self.logger.warning(f"Unknown mode {mode}, defaulting to INSERT")
                success_count = self._insert_new(data_partitioned, target_path)
                error_count = 0
            
            # Reconcile data (replaces ABAP reconcile_data)
            reconciled = self._reconcile_data(data_partitioned, target_path, merge_keys)
            if not reconciled:
                self.logger.warning("Data reconciliation failed")
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            total_count = data_with_metadata.count()
            
            result = LoadResult(
                success_count=success_count,
                error_count=error_count,
                total_count=total_count,
                errors=[],
                duration_seconds=duration
            )
            
            self.logger.info(
                f"Load complete - Success: {success_count}, Errors: {error_count}, "
                f"Duration: {duration:.2f}s"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}", exc_info=True)
            duration = (datetime.now() - start_time).total_seconds()
            return LoadResult(
                success_count=0,
                error_count=data.count() if data else 0,
                total_count=data.count() if data else 0,
                errors=[str(e)],
                duration_seconds=duration
            )
    
    def _insert_new(self, data: DataFrame, target_path: str) -> int:
        """
        Insert new records using append mode
        Replaces ABAP insert_new with COMMIT WORK
        
        Args:
            data: DataFrame to insert
            target_path: Target location
            
        Returns:
            Number of records inserted
        """
        self.logger.info(f"Inserting {data.count()} records to {target_path}")
        
        try:
            # Write with append mode (replaces INSERT + COMMIT WORK)
            data.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(target_path)
            
            return data.count()
            
        except Exception as e:
            self.logger.error(f"Insert failed: {str(e)}")
            raise
    
    def _update_existing(
        self,
        data: DataFrame,
        target_path: str,
        merge_keys: List[str]
    ) -> int:
        """
        Update existing records using Delta merge
        Replaces ABAP update_existing with UPDATE + COMMIT WORK
        
        Args:
            data: DataFrame with updates
            target_path: Target Delta table
            merge_keys: Keys for matching records
            
        Returns:
            Number of records updated
        """
        self.logger.info(f"Updating records in {target_path}")
        
        try:
            # Load Delta table
            delta_table = DeltaTable.forPath(self.spark, target_path)
            
            # Build merge condition
            merge_condition = " AND ".join([
                f"target.{key} = source.{key}" for key in merge_keys
            ])
            
            # Build update dict (exclude merge keys)
            update_dict = {
                col_name: f"source.{col_name}"
                for col_name in data.columns
                if col_name not in merge_keys
            }
            
            # Execute merge with update only (replaces UPDATE + COMMIT WORK)
            delta_table.alias("target") \
                .merge(
                    data.alias("source"),
                    merge_condition
                ) \
                .whenMatchedUpdate(set=update_dict) \
                .execute()
            
            return data.count()
            
        except Exception as e:
            self.logger.error(f"Update failed: {str(e)}")
            raise
    
    def _upsert_data(
        self,
        data: DataFrame,
        target_path: str,
        merge_keys: List[str]
    ) -> int:
        """
        Upsert (merge) data using Delta Lake merge operation
        Replaces ABAP UPSERT logic with Delta merge
        
        Args:
            data: DataFrame to upsert
            target_path: Target Delta table
            merge_keys: Keys for matching records
            
        Returns:
            Number of records upserted
        """
        self.logger.info(f"Upserting {data.count()} records to {target_path}")
        
        try:
            # Check if target exists
            if not self._target_exists(target_path):
                # First load - use insert
                return self._insert_new(data, target_path)
            
            # Load Delta table
            delta_table = DeltaTable.forPath(self.spark, target_path)
            
            # Build merge condition
            merge_condition = " AND ".join([
                f"target.{key} = source.{key}" for key in merge_keys
            ])
            
            # Build update and insert dicts
            all_columns = data.columns
            update_dict = {col: f"source.{col}" for col in all_columns}
            insert_dict = {col: f"source.{col}" for col in all_columns}
            
            # Execute Delta merge (replaces ABAP UPSERT + COMMIT WORK)
            delta_table.alias("target") \
                .merge(
                    data.alias("source"),
                    merge_condition
                ) \
                .whenMatchedUpdate(set=update_dict) \
                .whenNotMatchedInsert(values=insert_dict) \
                .execute()
            
            return data.count()
            
        except Exception as e:
            self.logger.error(f"Upsert failed: {str(e)}")
            raise
    
    def _add_metadata(self, data: DataFrame) -> DataFrame:
        """
        Add ETL metadata columns
        Replaces ABAP field mapping in load methods
        
        Args:
            data: Source DataFrame
            
        Returns:
            DataFrame with metadata columns
        """
        return data \
            .withColumn("etl_run_id", lit(self.run_id)) \
            .withColumn("etl_loaded_at", current_timestamp()) \
            .withColumn("etl_loaded_by", lit(self.spark.sparkContext.sparkUser()))
    
    def _validate_data(self, data: DataFrame) -> Dict[str, Any]:
        """
        Validate data before loading
        
        Args:
            data: DataFrame to validate
            
        Returns:
            Dictionary with validation results
        """
        errors = []
        
        # Check required columns
        required_columns = ["id", "name"]
        missing_columns = [col for col in required_columns if col not in data.columns]
        
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")
            
        # Check for null values in key columns
        if "id" in data.columns:
            null_count = data.filter(col("id").isNull()).count()
            if null_count > 0:
                errors.append(f"Found {null_count} records with null ID")
        
        return {
            "is_valid": len(errors) == 0,
            "errors": errors
        }
    
    def _reconcile_data(
        self,
        source_data: DataFrame,
        target_path: str,
        merge_keys: List[str]
    ) -> bool:
        """
        Reconcile loaded data with source
        Replaces ABAP reconcile_data method
        
        Args:
            source_data: Source DataFrame
            target_path: Target location
            merge_keys: Keys for matching
            
        Returns:
            True if reconciliation passes
        """
        try:
            if not self._target_exists(target_path):
                self.logger.warning(f"Target {target_path} does not exist")
                return False
            
            # Load target data
            target_data = self.spark.read.format("delta").load(target_path)
            
            # Filter target data for current run
            target_filtered = target_data.filter(col("etl_run_id") == self.run_id)
            
            # Compare counts
            source_count = source_data.count()
            target_count = target_filtered.count()
            
            if source_count != target_count:
                self.logger.warning(
                    f"Reconciliation mismatch: source={source_count}, "
                    f"target={target_count}"
                )
                return False
            
            # Sample validation - compare keys
            source_keys = source_data.select(*merge_keys).distinct().count()
            target_keys = target_filtered.select(*merge_keys).distinct().count()
            
            if source_keys != target_keys:
                self.logger.warning(
                    f"Key count mismatch: source={source_keys}, target={target_keys}"
                )
                return False
            
            self.logger.info("Data reconciliation passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Reconciliation failed: {str(e)}")
            return False
    
    def _target_exists(self, target_path: str) -> bool:
        """Check if target Delta table exists"""
        try:
            self.spark.read.format("delta").load(target_path)
            return True
        except Exception:
            return False
    
    def optimize_table(self, target_path: str, z_order_cols: List[str] = None):
        """
        Optimize Delta table with compaction and Z-ordering
        
        Args:
            target_path: Target Delta table path
            z_order_cols: Columns for Z-ordering optimization
        """
        try:
            delta_table = DeltaTable.forPath(self.spark, target_path)
            
            # Compact small files
            delta_table.optimize().executeCompaction()
            self.logger.info(f"Optimized table at {target_path}")
            
            # Z-order if columns specified
            if z_order_cols:
                delta_table.optimize().executeZOrderBy(*z_order_cols)
                self.logger.info(f"Applied Z-ordering on {z_order_cols}")
                
        except Exception as e:
            self.logger.error(f"Optimization failed: {str(e)}")
    
    def vacuum_table(self, target_path: str, retention_hours: int = 168):
        """
        Clean up old versions of Delta table
        
        Args:
            target_path: Target Delta table path
            retention_hours: Hours to retain (default 7 days)
        """
        try:
            delta_table = DeltaTable.forPath(self.spark, target_path)
            delta_table.vacuum(retention_hours)
            self.logger.info(
                f"Vacuumed table at {target_path} "
                f"(retention: {retention_hours} hours)"
            )
        except Exception as e:
            self.logger.error(f"Vacuum failed: {str(e)}")


===FILE: src/extract.py===
"""
Data Extraction Module
Extracts data from various sources (database, staging, incremental)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp
from typing import Optional, Dict, Any
import logging
from datetime import datetime


class DataExtractor:
    """
    Data extractor supporting multiple source types
    Replaces ABAP zcl_etl_extractor
    """
    
    def __init__(
        self,
        spark: SparkSession,
        source_type: str = "database",
        run_id: str = None,
        config: Dict[str, Any] = None
    ):
        """
        Initialize extractor
        
        Args:
            spark: SparkSession instance
            source_type: Source type (database, staging, incremental)
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.source_type = source_type.upper()
        self.run_id = run_id or f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.config = config or {}
        self.logger = logging.getLogger(__name__)
    
    def extract_data(
        self,
        source_path: str = None,
        filter_condition: str = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data based on source type
        
        Args:
            source_path: Source location (table name, file path, etc.)
            filter_condition: SQL filter condition
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            Extracted DataFrame
        """
        self.logger.info(f"Starting extraction - Source: {self.source_type}")
        
        try:
            if self.source_type == "DATABASE":
                data = self._extract_from_database(source_path, filter_condition)
            elif self.source_type == "STAGING":
                data = self._extract_from_staging(source_path)
            elif self.source_type == "INCREMENTAL":
                data = self._extract_incremental(source_path)
            else:
                data = self._extract_from_database(source_path, filter_condition)
            
            # Apply record limit if specified
            if max_records > 0:
                data = data.limit(max_records)
            
            record_count = data.count()
            self.logger.info(f"Extracted {record_count} records")
            
            return data
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}", exc_info=True)
            raise
    
    def _extract_from_database(
        self,
        table_name: str,
        filter_condition: str = None
    ) -> DataFrame:
        """Extract from database table"""
        self.logger.info(f"Extracting from database table: {table_name}")
        
        # Read from table
        data = self.spark.read \
            .format("delta") \
            .load(table_name)
        
        # Apply filter if provided
        if filter_condition:
            data = data.filter(filter_condition)
        
        return data
    
    def _extract_from_staging(self, staging_path: str) -> DataFrame:
        """Extract from staging area"""
        self.logger.info(f"Extracting from staging: {staging_path}")
        
        data = self.spark.read \
            .format("delta") \
            .load(staging_path) \
            .filter(
                (col("run_id") == self.run_id) &
                (col("status") == "READY")
            )
        
        return data
    
    def _extract_incremental(self, table_name: str) -> DataFrame:
        """Extract incremental data based on last run timestamp"""
        self.logger.info(f"Extracting incremental data from: {table_name}")
        
        # Get last successful run timestamp
        last_run_time = self._get_last_run_time()
        
        if last_run_time:
            data = self.spark.read \
                .format("delta") \
                .load(table_name) \
                .filter(col("changed_at") > last_run_time)
        else:
            # No previous run, extract all
            data = self.spark.read.format("delta").load(table_name)
        
        return data
    
    def _get_last_run_time(self) -> Optional[datetime]:
        """Get timestamp of last successful run"""
        # In production, this would query run log table
        return None


===FILE: src/transform.py===
"""
Data Transformation Module
Applies business rules and transformations to extracted data
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, trim, when, current_timestamp, lit, regexp_replace
)
from pyspark.sql.types import DecimalType
from typing import Dict, Any, List, Tuple
import logging
from datetime import datetime


class DataTransformer:
    """
    Data transformer with business rules
    Replaces ABAP zcl_etl_transformer
    """
    
    def __init__(
        self,
        spark: SparkSession,
        run_id: str = None,
        config: Dict[str, Any] = None
    ):
        """
        Initialize transformer
        
        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id or f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.config = config or {}
        self.logger = logging.getLogger(__name__)
    
    def transform_data(self, source_data: DataFrame) -> DataFrame:
        """
        Transform extracted data
        
        Args:
            source_data: Extracted DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        try:
            # Apply transformations
            transformed = source_data \
                .withColumn("name", upper(trim(col("name")))) \
                .withColumn(
                    "transformed_value",
                    self._calculate_derived_values(col("value"), col("category"))
                ) \
                .withColumn("status", lit("TRANSFORMED")) \
                .withColumn(
                    "priority",
                    self._calculate_priority(col("value"), col("category"))
                ) \
                .withColumn("etl_run_id", lit(self.run_id)) \
                .withColumn("processed_at", current_timestamp()) \
                .withColumn("processed_by", lit(self.spark.sparkContext.sparkUser()))
            
            # Apply business rules
            transformed = self._apply_business_rules(transformed)
            
            # Enrich data
            transformed = self._enrich_data(transformed)
            
            record_count = transformed.count()
            self.logger.info(f"Transformed {record_count} records")
            
            return transformed
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}", exc_info=True)
            raise
    
    def _calculate_derived_values(self, value_col, category_col):
        """Calculate derived/transformed values"""
        return when(category_col == "PREMIUM", value_col * 1.5) \
            .when(category_col == "STANDARD", value_col * 1.2) \
            .when(category_col == "VIP", value_col * 2.0) \
            .otherwise(value_col)
    
    def _calculate_priority(self, value_col, category_col):
        """Calculate priority based on value and category"""
        return when(value_col >= 1000, 1) \
            .when((value_col >= 750) & (category_col == "PREMIUM"), 1) \
            .when(value_col >= 500, 2) \
            .when(value_col >= 200, 3) \
            .otherwise(4)
    
    def _apply_business_rules(self, data: DataFrame) -> DataFrame:
        """Apply business rules to data"""
        self.logger.info("Applying business rules")
        
        # Rule 1: Set status based on value
        data = data.withColumn(
            "status",
            when(col("value").isNull(), "INVALID")
            .when(col("transformed_value") >= 750, "HIGH_VALUE")
            .when(col("transformed_value") >= 300, "MEDIUM_VALUE")
            .otherwise("LOW_VALUE")
        )
        
        # Rule 2: Priority override for high value items
        data = data.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, 1)
            .otherwise(col("priority"))
        )
        
        # Rule 3: Name normalization (remove multiple spaces)
        data = data.withColumn(
            "name",
            regexp_replace(col("name"), "\\s+", " ")
        )
        
        # Rule 4: Category validation
        data = data.withColumn(
            "category",
            when(col("category").isNull(), "UNCATEGORIZED")
            .otherwise(col("category"))
        )
        
        return data
    
    def _enrich_data(self, data: DataFrame) -> DataFrame:
        """Enrich data with additional information"""
        self.logger.info("Enriching data")
        
        # Apply premium multiplier if configured
        premium_multiplier = self.config.get("premium_multiplier", 1.2)
        
        data = data.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("transformed_value") * premium_multiplier)
            .otherwise(col("transformed_value"))
        )
        
        return data
    
    def validate_data(self, data: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data
        
        Args:
            data: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of errors)
        """
        errors = []
        
        # Check for null IDs
        null_id_count = data.filter(col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with null ID")
        
        # Check for null names
        null_name_count = data.filter(col("name").isNull()).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with null name")
        
        # Check for negative values
        negative_value_count = data.filter(col("value") < 0).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative values")
        
        is_valid = len(errors) == 0
        
        if not is_valid:
            self.logger.warning(f"Validation failed: {errors}")
        
        return is_valid, errors


===FILE: config.yaml===
# ETL Configuration File
# Delta Lake Loader with Merge Operations

# Spark Configuration
spark:
  app_name: "delta_lake_etl_loader"
  master: "local[*]"
  
  # Delta Lake specific configs
  extensions:
    - "io.delta.sql.DeltaSparkSessionExtension"
  catalog:
    - "org.apache.spark.sql.delta.catalog.DeltaCatalog"
  
  # Performance tuning
  shuffle_partitions: 200
  default_parallelism: 100
  adaptive_enabled: true
  
  # Delta configurations
  delta:
    log_cache_size: 1000
    checkpoint_interval: 10
    merge_matched_only: false

# Source Configuration
source:
  type: "database"  # database, staging, incremental
  
  # Database source
  database:
    format: "delta"
    path: "/data/source/etl_source_data"
    table_name: "etl_source_data"
  
  # Staging source
  staging:
    path: "/data/staging/etl_staging"
    status_filter: "READY"
  
  # Incremental load
  incremental:
    enabled: true
    timestamp_column: "changed_at"
    lookback_hours: 24

# Target Configuration
target:
  type: "delta"
  
  # Delta Lake target
  delta:
    path: "/data/target/etl_target_data"
    table_name: "etl_target_data"
    
    # Merge configuration
    merge_keys:
      - "id"
    
    # Optimization
    optimize:
      enabled: true
      z_order_columns:
        - "category"
        - "status"
      auto_compact: true
    
    # Vacuum configuration
    vacuum:
      enabled: true
      retention_hours: 168  # 7 days

# Load Configuration
load:
  mode: "UPSERT"  # INSERT, UPDATE, UPSERT
  batch_size: 1000  # Records per partition (replaces ABAP batch loops)
  
  # Repartition strategy
  repartition:
    enabled: true
    num_partitions: null  # null = auto-calculate
    partition_by:
      - "category"
  
  # Write options
  write:
    mode: "append"
    merge_schema: true
    overwrite_schema: false
  
  # Reconciliation
  reconciliation:
    enabled: true
    sample_size: 1000
    tolerance: 0.0  # Acceptable difference percentage

# Transform Configuration
transform:
  # Business rules
  business_rules:
    enable_value_categorization: true
    enable_priority_calculation: true
    enable_name_normalization: true
  
  # Enrichment
  enrichment:
    premium_multiplier: 1.5
    standard_multiplier: 1.2
    vip_multiplier: 2.0
  
  # Validation
  validation:
    required_fields:
      - "id"
      - "name"
      - "value"
    
    value_constraints:
      min_value: 0
      max_value: 999999
    
    priority_range:
      min: 1
      max: 5

# Data Quality Configuration
data_quality:
  enabled: true
  
  checks:
    completeness: true
    uniqueness: true
    validity: true
    consistency: true
  
  thresholds:
    max_null_percentage: 5.0
    max_duplicate_percentage: 1.0
    max_invalid_percentage: 2.0

# Monitoring Configuration
monitoring:
  enabled: true
  
  metrics:
    - "record_count"
    - "load_duration"
    - "error_count"
    - "throughput"
  
  alerts:
    error_rate_threshold: 10.0  # Percentage
    duration_threshold_minutes: 60
    
  logging:
    level: "INFO"
    format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file: "logs/etl_loader.log"

# Error Handling
error_handling:
  max_retries: 3
  retry_delay_seconds: 30
  
  on_failure:
    action: "rollback"  # rollback, continue, abort
    log_errors: true
    alert: true
  
  bad_records:
    path: "/data/errors/bad_records"
    mode: "PERMISSIVE"  # PERMISSIVE, DROPMALFORMED, FAILFAST

# Performance Configuration
performance:
  cache_intermediate: true
  broadcast_threshold: 10485760  # 10MB
  
  # Memory management
  memory:
    executor_memory: "4g"
    driver_memory: "2g"
    memory_fraction: 0.6
  
  # Compression
  compression:
    enabled: true
    codec: "snappy"

# Run Configuration
run:
  mode: "production"  # development, test, production
  
  dry_run: false
  
  limits:
    max_records: 0  # 0 = unlimited
    timeout_minutes: 120
  
  checkpointing:
    enabled: true
    location: "/data/checkpoints"
    interval: 100


===FILE: tests/test_load.py===
"""
Unit tests for Delta Lake Loader
Tests merge operations (INSERT/UPDATE/UPSERT) and reconciliation
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType
from delta import configure_spark_with_delta_pip
from src.load import DeltaLakeLoader, LoadResult
import