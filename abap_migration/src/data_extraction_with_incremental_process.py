# PySpark ETL Migration - Production Code

===FILE: src/extract.py===
"""
Data extraction module with incremental processing support.
Replaces ABAP zcl_etl_extractor with PySpark DataFrame operations.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional, Dict, Any
import logging

from src.logger import ETLLogger
from src.config import ConfigManager


class DataExtractor:
    """Extracts data from various sources with incremental processing support."""
    
    SOURCE_SCHEMA = StructType([
        StructField("id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("value", DecimalType(15, 2), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("source_system", StringType(), nullable=True),
        StructField("created_at", TimestampType(), nullable=False),
        StructField("created_by", StringType(), nullable=True),
        StructField("changed_at", TimestampType(), nullable=False),
        StructField("changed_by", StringType(), nullable=True),
    ])
    
    def __init__(
        self,
        spark: SparkSession,
        config: ConfigManager,
        source_type: str = "DATABASE",
        run_id: str = None
    ):
        """
        Initialize the data extractor.
        
        Args:
            spark: Active SparkSession
            config: Configuration manager
            source_type: Source type (DATABASE, STAGING, INCREMENTAL)
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.source_type = source_type.upper()
        self.run_id = run_id or self._generate_run_id()
        self.logger = ETLLogger.get_instance()
        
    def _generate_run_id(self) -> str:
        """Generate unique run ID."""
        return f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def extract_data(
        self,
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data based on source type.
        
        Args:
            filter_condition: SQL WHERE clause condition
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}"
        )
        
        try:
            # Route to appropriate extraction method
            if self.source_type == "DATABASE":
                df = self._extract_from_database(filter_condition)
            elif self.source_type == "STAGING":
                df = self._extract_from_staging()
            elif self.source_type == "INCREMENTAL":
                df = self._extract_incremental()
            else:
                self.logger.log_warning(
                    component="EXTRACTOR",
                    message=f"Unknown source type: {self.source_type}, defaulting to DATABASE"
                )
                df = self._extract_from_database(filter_condition)
            
            # Apply row limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.log_info(
                component="EXTRACTOR",
                message=f"Extracted {record_count} records"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="EXTRACTOR",
                message="Extraction failed",
                details=str(e)
            )
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract from source database table.
        
        Args:
            filter_condition: SQL WHERE clause
            
        Returns:
            DataFrame with source data
        """
        source_path = self.config.get("source.database.path")
        source_format = self.config.get("source.database.format", "delta")
        
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Reading from {source_format} table: {source_path}"
        )
        
        # Read source data
        df = self.spark.read.format(source_format).load(source_path)
        
        # Apply filter if provided
        if filter_condition:
            df = df.filter(filter_condition)
            self.logger.log_info(
                component="EXTRACTOR",
                message=f"Applied filter: {filter_condition}"
            )
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """
        Extract from staging area for specific run.
        
        Returns:
            DataFrame with staged data
        """
        staging_path = self.config.get("source.staging.path")
        
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Reading from staging: {staging_path}"
        )
        
        df = (
            self.spark.read
            .format("delta")
            .load(staging_path)
            .filter(F.col("run_id") == self.run_id)
            .filter(F.col("status") == "READY")
        )
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run using Delta Lake time travel.
        
        Returns:
            DataFrame with incremental data
        """
        source_path = self.config.get("source.database.path")
        run_log_path = self.config.get("metadata.run_log.path")
        
        self.logger.log_info(
            component="EXTRACTOR",
            message="Performing incremental extraction"
        )
        
        try:
            # Get last successful run timestamp from run log
            run_log_df = self.spark.read.format("delta").load(run_log_path)
            
            last_success_row = (
                run_log_df
                .filter(F.col("status") == "SUCCESS")
                .orderBy(F.col("end_time").desc())
                .first()
            )
            
            if last_success_row:
                last_run_time = last_success_row["end_time"]
                
                self.logger.log_info(
                    component="EXTRACTOR",
                    message=f"Last successful run: {last_run_time}"
                )
                
                # Extract records changed after last run
                df = (
                    self.spark.read
                    .format("delta")
                    .load(source_path)
                    .filter(F.col("changed_at") > last_run_time)
                )
                
                self.logger.log_info(
                    component="EXTRACTOR",
                    message="Incremental extraction based on changed_at timestamp"
                )
                
            else:
                # No previous successful run - perform full load
                self.logger.log_warning(
                    component="EXTRACTOR",
                    message="No previous successful run found - performing full load"
                )
                df = self.spark.read.format("delta").load(source_path)
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="EXTRACTOR",
                message="Incremental extraction failed",
                details=str(e)
            )
            # Fallback to full load
            self.logger.log_warning(
                component="EXTRACTOR",
                message="Falling back to full load"
            )
            return self.spark.read.format("delta").load(source_path)
    
    def extract_with_delta_time_travel(
        self,
        version: Optional[int] = None,
        timestamp: Optional[str] = None
    ) -> DataFrame:
        """
        Extract data using Delta Lake time travel.
        
        Args:
            version: Delta table version number
            timestamp: Timestamp string (e.g., '2024-01-01 00:00:00')
            
        Returns:
            DataFrame at specified version/timestamp
        """
        source_path = self.config.get("source.database.path")
        
        reader = self.spark.read.format("delta")
        
        if version is not None:
            self.logger.log_info(
                component="EXTRACTOR",
                message=f"Time travel to version: {version}"
            )
            df = reader.option("versionAsOf", version).load(source_path)
        elif timestamp is not None:
            self.logger.log_info(
                component="EXTRACTOR",
                message=f"Time travel to timestamp: {timestamp}"
            )
            df = reader.option("timestampAsOf", timestamp).load(source_path)
        else:
            df = reader.load(source_path)
        
        return df
    
    def get_extraction_metadata(self, df: DataFrame) -> Dict[str, Any]:
        """
        Get metadata about extracted data.
        
        Args:
            df: Extracted DataFrame
            
        Returns:
            Dictionary with metadata
        """
        return {
            "run_id": self.run_id,
            "source_type": self.source_type,
            "record_count": df.count(),
            "schema": df.schema.jsonValue(),
            "extraction_time": datetime.now().isoformat(),
            "partitions": df.rdd.getNumPartitions()
        }


===FILE: src/transform.py===
"""
Data transformation module with business rules and validation.
Replaces ABAP zcl_etl_transformer with PySpark transformations.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from pyspark.sql.window import Window
from typing import List, Tuple
from datetime import datetime

from src.logger import ETLLogger
from src.config import ConfigManager


class DataTransformer:
    """Transforms extracted data with business rules and enrichment."""
    
    TRANSFORMED_SCHEMA = StructType([
        StructField("id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("value", DecimalType(15, 2), nullable=False),
        StructField("transformed_value", DecimalType(15, 2), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("priority", IntegerType(), nullable=False),
        StructField("etl_run_id", StringType(), nullable=False),
        StructField("processed_at", TimestampType(), nullable=False),
        StructField("processed_by", StringType(), nullable=True),
    ])
    
    def __init__(self, spark: SparkSession, config: ConfigManager, run_id: str):
        """
        Initialize transformer.
        
        Args:
            spark: Active SparkSession
            config: Configuration manager
            run_id: ETL run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
        self.processed_at = datetime.now()
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply complete transformation pipeline.
        
        Args:
            source_df: Source DataFrame from extraction
            
        Returns:
            Transformed DataFrame
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Starting transformation"
        )
        
        # Basic transformations
        df = self._apply_basic_transformations(source_df)
        
        # Calculate derived values
        df = self._calculate_derived_values(df)
        
        # Apply business rules
        df = self._apply_business_rules(df)
        
        # Enrich data
        df = self._enrich_data(df)
        
        # Add metadata
        df = self._add_metadata(df)
        
        record_count = df.count()
        self.logger.log_info(
            component="TRANSFORMER",
            message=f"Transformed {record_count} records"
        )
        
        return df
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic data transformations."""
        return (
            df
            # Normalize name - upper case and trim
            .withColumn("name", F.upper(F.trim(F.col("name"))))
            # Remove extra spaces
            .withColumn("name", F.regexp_replace(F.col("name"), r"\s+", " "))
            # Ensure category has value
            .withColumn(
                "category",
                F.when(
                    F.col("category").isNull() | (F.trim(F.col("category")) == ""),
                    F.lit("UNCATEGORIZED")
                ).otherwise(F.col("category"))
            )
        )
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """Calculate transformed values based on category."""
        # Get premium multiplier from config
        premium_multiplier = float(self.config.get("business_rules.premium_multiplier", 1.5))
        
        return (
            df
            .withColumn(
                "transformed_value",
                F.when(F.col("category") == "PREMIUM", F.col("value") * premium_multiplier)
                .when(F.col("category") == "VIP", F.col("value") * 2.0)
                .when(F.col("category") == "STANDARD", F.col("value") * 1.2)
                .otherwise(F.col("value"))
            )
            # Calculate priority based on transformed value
            .withColumn(
                "priority",
                F.when(F.col("transformed_value") >= 1000, F.lit(1))
                .when(F.col("transformed_value") >= 750, F.lit(2))
                .when(F.col("transformed_value") >= 500, F.lit(3))
                .when(F.col("transformed_value") >= 300, F.lit(4))
                .otherwise(F.lit(5))
            )
        )
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to data."""
        self.logger.log_info(
            component="TRANSFORMER",
            message="Applying business rules"
        )
        
        return (
            df
            # Rule 1: Set status based on value
            .withColumn(
                "status",
                F.when(F.col("value").isNull() | (F.col("value") == 0), F.lit("INVALID"))
                .when(F.col("transformed_value") >= 750, F.lit("HIGH_VALUE"))
                .when(F.col("transformed_value") >= 300, F.lit("MEDIUM_VALUE"))
                .otherwise(F.lit("LOW_VALUE"))
            )
            # Rule 2: Override priority for high value items
            .withColumn(
                "priority",
                F.when(F.col("transformed_value") >= 1000, F.lit(1))
                .otherwise(F.col("priority"))
            )
        )
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional information."""
        self.logger.log_info(
            component="TRANSFORMER",
            message="Enriching data"
        )
        
        # Load enrichment config if available
        config_path = self.config.get("enrichment.config_path", None)
        
        if config_path:
            try:
                config_df = self.spark.read.format("delta").load(config_path)
                config_df = config_df.filter(F.col("is_active") == True)
                
                # Join with config for enrichment
                df = df.join(
                    config_df.select("config_key", "config_value"),
                    how="left",
                    on=[]
                )
            except Exception as e:
                self.logger.log_warning(
                    component="TRANSFORMER",
                    message=f"Could not load enrichment config: {e}"
                )
        
        return df
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """Add ETL metadata columns."""
        import getpass
        
        return (
            df
            .withColumn("etl_run_id", F.lit(self.run_id))
            .withColumn("processed_at", F.lit(self.processed_at))
            .withColumn("processed_by", F.lit(getpass.getuser()))
        )
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Validating transformed data"
        )
        
        errors = []
        
        # Validation 1: ID is required
        null_id_count = df.filter(F.col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"Found {null_id_count} records with null ID")
        
        # Validation 2: Name is required
        null_name_count = df.filter(
            F.col("name").isNull() | (F.trim(F.col("name")) == "")
        ).count()
        if null_name_count > 0:
            errors.append(f"Found {null_name_count} records with null/empty name")
        
        # Validation 3: Value must be positive
        invalid_value_count = df.filter(
            F.col("value").isNull() | (F.col("value") < 0)
        ).count()
        if invalid_value_count > 0:
            errors.append(f"Found {invalid_value_count} records with invalid value")
        
        # Validation 4: Priority must be 1-5
        invalid_priority_count = df.filter(
            (F.col("priority") < 1) | (F.col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"Found {invalid_priority_count} records with invalid priority")
        
        # Validation 5: No duplicate IDs
        duplicate_count = (
            df.groupBy("id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        if duplicate_count > 0:
            errors.append(f"Found {duplicate_count} duplicate IDs")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.log_info(
                component="TRANSFORMER",
                message="Validation passed"
            )
        else:
            self.logger.log_error(
                component="TRANSFORMER",
                message="Validation failed",
                details="; ".join(errors)
            )
        
        return is_valid, errors
    
    def apply_category_rules(self, df: DataFrame, category: str) -> DataFrame:
        """
        Apply category-specific transformation rules.
        
        Args:
            df: DataFrame to transform
            category: Category to filter and apply rules
            
        Returns:
            Transformed DataFrame for specific category
        """
        category_df = df.filter(F.col("category") == category)
        
        # Apply category-specific rules
        if category == "PREMIUM":
            category_df = category_df.withColumn(
                "transformed_value",
                F.col("transformed_value") * 1.1
            )
        elif category == "VIP":
            category_df = category_df.withColumn("priority", F.lit(1))
        
        return category_df


===FILE: src/load.py===
"""
Data loading module with batch processing and reconciliation.
Replaces ABAP zcl_etl_loader with PySpark write operations.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict, Any, List
from dataclasses import dataclass
from datetime import datetime

from src.logger import ETLLogger
from src.config import ConfigManager


@dataclass
class LoadResult:
    """Result of load operation."""
    success_count: int
    error_count: int
    total_count: int
    errors: List[str]
    duration_seconds: float


class DataLoader:
    """Loads transformed data to target with batch processing."""
    
    def __init__(
        self,
        spark: SparkSession,
        config: ConfigManager,
        target_type: str = "DATABASE",
        batch_size: int = 1000,
        run_id: str = None
    ):
        """
        Initialize data loader.
        
        Args:
            spark: Active SparkSession
            config: Configuration manager
            target_type: Target type (DATABASE, DELTA, etc.)
            batch_size: Records per batch
            run_id: ETL run identifier
        """
        self.spark = spark
        self.config = config
        self.target_type = target_type.upper()
        self.batch_size = batch_size
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
    
    def load_data(
        self,
        df: DataFrame,
        mode: str = "UPSERT"
    ) -> LoadResult:
        """
        Load data to target.
        
        Args:
            df: Transformed DataFrame to load
            mode: Load mode (INSERT, UPDATE, UPSERT, OVERWRITE)
            
        Returns:
            LoadResult with operation details
        """
        start_time = datetime.now()
        
        self.logger.log_info(
            component="LOADER",
            message=f"Starting load - Mode: {mode}, Batch size: {self.batch_size}"
        )
        
        try:
            total_count = df.count()
            
            # Perform load based on mode
            if mode.upper() == "INSERT":
                success = self._insert_new(df)
            elif mode.upper() == "UPDATE":
                success = self._update_existing(df)
            elif mode.upper() == "UPSERT":
                success = self._upsert_data(df)
            elif mode.upper() == "OVERWRITE":
                success = self._overwrite_data(df)
            else:
                raise ValueError(f"Unknown load mode: {mode}")
            
            # Reconcile data
            if self.config.get("data_quality.enable_reconciliation", True):
                reconciled = self._reconcile_data(df)
                if not reconciled:
                    self.logger.log_warning(
                        component="LOADER",
                        message="Data reconciliation failed"
                    )
            
            duration = (datetime.now() - start_time).total_seconds()
            
            result = LoadResult(
                success_count=total_count if success else 0,
                error_count=0 if success else total_count,
                total_count=total_count,
                errors=[] if success else ["Load operation failed"],
                duration_seconds=duration
            )
            
            self.logger.log_info(
                component="LOADER",
                message=f"Load complete - Success: {result.success_count}, "
                       f"Errors: {result.error_count}, Duration: {duration:.2f}s"
            )
            
            return result
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            
            self.logger.log_error(
                component="LOADER",
                message="Load operation failed",
                details=str(e)
            )
            
            return LoadResult(
                success_count=0,
                error_count=df.count() if df else 0,
                total_count=df.count() if df else 0,
                errors=[str(e)],
                duration_seconds=duration
            )
    
    def _insert_new(self, df: DataFrame) -> bool:
        """Insert new records."""
        try:
            target_path = self.config.get("target.database.path")
            
            self.logger.log_info(
                component="LOADER",
                message=f"Inserting {df.count()} records to {target_path}"
            )
            
            # Write as Delta table with append mode
            (
                df.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(target_path)
            )
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Insert operation failed",
                details=str(e)
            )
            return False
    
    def _update_existing(self, df: DataFrame) -> bool:
        """Update existing records."""
        try:
            from delta.tables import DeltaTable
            
            target_path = self.config.get("target.database.path")
            
            self.logger.log_info(
                component="LOADER",
                message=f"Updating records in {target_path}"
            )
            
            # Load target as Delta table
            target_table = DeltaTable.forPath(self.spark, target_path)
            
            # Prepare update set
            update_set = {
                col: f"source.{col}"
                for col in df.columns
                if col != "id"
            }
            
            # Perform merge update
            (
                target_table.alias("target")
                .merge(
                    df.alias("source"),
                    "target.id = source.id"
                )
                .whenMatchedUpdate(set=update_set)
                .execute()
            )
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Update operation failed",
                details=str(e)
            )
            return False
    
    def _upsert_data(self, df: DataFrame) -> bool:
        """Upsert (merge) data - update existing, insert new."""
        try:
            from delta.tables import DeltaTable
            
            target_path = self.config.get("target.database.path")
            
            self.logger.log_info(
                component="LOADER",
                message=f"Upserting {df.count()} records to {target_path}"
            )
            
            # Check if target exists
            try:
                target_table = DeltaTable.forPath(self.spark, target_path)
                
                # Prepare update/insert set
                update_set = {
                    col: f"source.{col}"
                    for col in df.columns
                    if col != "id"
                }
                
                insert_set = {
                    col: f"source.{col}"
                    for col in df.columns
                }
                
                # Perform merge
                (
                    target_table.alias("target")
                    .merge(
                        df.alias("source"),
                        "target.id = source.id"
                    )
                    .whenMatchedUpdate(set=update_set)
                    .whenNotMatchedInsert(values=insert_set)
                    .execute()
                )
                
            except Exception:
                # Target doesn't exist - create it
                self.logger.log_info(
                    component="LOADER",
                    message="Target table doesn't exist - creating new table"
                )
                (
                    df.write
                    .format("delta")
                    .mode("overwrite")
                    .save(target_path)
                )
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Upsert operation failed",
                details=str(e)
            )
            return False
    
    def _overwrite_data(self, df: DataFrame) -> bool:
        """Overwrite target data."""
        try:
            target_path = self.config.get("target.database.path")
            
            self.logger.log_info(
                component="LOADER",
                message=f"Overwriting data in {target_path}"
            )
            
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(target_path)
            )
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Overwrite operation failed",
                details=str(e)
            )
            return False
    
    def _reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data with source.
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            True if reconciliation matches
        """
        try:
            target_path = self.config.get("target.database.path")
            
            # Read back from target
            target_df = self.spark.read.format("delta").load(target_path)
            
            # Filter to current run
            target_df = target_df.filter(F.col("etl_run_id") == self.run_id)
            
            # Count comparison
            loaded_count = loaded_df.count()
            target_count = target_df.count()
            
            if loaded_count != target_count:
                self.logger.log_warning(
                    component="LOADER",
                    message=f"Reconciliation mismatch - Loaded: {loaded_count}, Target: {target_count}"
                )
                return False
            
            # Hash comparison for data integrity
            loaded_hash = loaded_df.select(
                F.md5(F.concat_ws("|", *loaded_df.columns)).alias("hash")
            ).agg(F.collect_list("hash")).first()[0]
            
            target_hash = target_df.select(
                F.md5(F.concat_ws("|", *target_df.columns)).alias("hash")
            ).agg(F.collect_list("hash")).first()[0]
            
            if set(loaded_hash) != set(target_hash):
                self.logger.log_warning(
                    component="LOADER",
                    message="Reconciliation failed - Data hash mismatch"
                )
                return False
            
            self.logger.log_info(
                component="LOADER",
                message="Data reconciliation successful"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Reconciliation failed",
                details=str(e)
            )
            return False
    
    def optimize_target(self) -> None:
        """Optimize target Delta table."""
        try:
            from delta.tables import DeltaTable
            
            target_path = self.config.get("target.database.path")
            
            self.logger.log_info(
                component="LOADER",
                message=f"Optimizing target table: {target_path}"
            )
            
            target_table = DeltaTable.forPath(self.spark, target_path)
            target_table.optimize().executeCompaction()
            
            # Vacuum old files
            retention_hours = self.config.get("target.vacuum_retention_hours", 168)
            target_table.vacuum(retention_hours)
            
            self.logger.log_info(
                component="LOADER",
                message="Table optimization complete"
            )
            
        except Exception as e:
            self.logger.log_warning(
                component="LOADER",
                message=f"Table optimization failed: {e}"
            )


===FILE: