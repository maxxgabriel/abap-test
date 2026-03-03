===FILE: src/extract.py===
"""
PySpark Data Extraction Module with Incremental Processing
Supports full and incremental loads using Delta Lake time-travel
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, max as spark_max, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional, Dict, Any
import logging

from src.utils.logger import ETLLogger
from src.utils.config import ConfigManager


class DataExtractor:
    """
    Extracts data from various sources with support for incremental processing
    """
    
    def __init__(self, spark: SparkSession, run_id: str, config: ConfigManager):
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = ETLLogger.get_logger("EXTRACTOR")
        
    def get_source_schema(self) -> StructType:
        """Define the source data schema"""
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
    
    def extract_data(
        self, 
        source_type: str = "DATABASE",
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Main extraction method routing to appropriate extractor
        
        Args:
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            filter_condition: Optional filter condition
            max_records: Maximum records to extract (0 = no limit)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.info(f"Starting extraction - Source: {source_type}, Run ID: {self.run_id}")
        
        try:
            if source_type == "DATABASE":
                df = self._extract_from_database(filter_condition)
            elif source_type == "STAGING":
                df = self._extract_from_staging()
            elif source_type == "INCREMENTAL":
                df = self._extract_incremental()
            else:
                self.logger.warning(f"Unknown source type: {source_type}, defaulting to DATABASE")
                df = self._extract_from_database(filter_condition)
            
            # Apply max records limit
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}", exc_info=True)
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract data from database source
        
        Args:
            filter_condition: SQL WHERE clause condition
            
        Returns:
            DataFrame with source data
        """
        source_path = self.config.get("source.database.path")
        source_format = self.config.get("source.database.format", "delta")
        
        self.logger.info(f"Extracting from database: {source_path}")
        
        # Read from Delta Lake or other format
        df = self.spark.read.format(source_format).load(source_path)
        
        # Apply filter if provided
        if filter_condition:
            df = df.filter(filter_condition)
            self.logger.info(f"Applied filter: {filter_condition}")
        
        # Apply default row limit from config if no specific limit
        default_limit = self.config.get("extraction.default_row_limit", 1000)
        if default_limit > 0:
            df = df.limit(default_limit)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """
        Extract data from staging area
        
        Returns:
            DataFrame with staged data
        """
        staging_path = self.config.get("source.staging.path")
        staging_format = self.config.get("source.staging.format", "parquet")
        
        self.logger.info(f"Extracting from staging: {staging_path}")
        
        df = (self.spark.read
              .format(staging_format)
              .load(staging_path)
              .filter(col("run_id") == self.run_id)
              .filter(col("status") == "READY"))
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run
        Uses Delta Lake time-travel for incremental processing
        
        Returns:
            DataFrame with incremental data
        """
        source_path = self.config.get("source.database.path")
        run_log_path = self.config.get("metadata.run_log_path")
        
        self.logger.info("Starting incremental extraction")
        
        try:
            # Get last successful run timestamp from run log
            last_run_time = self._get_last_successful_run_time(run_log_path)
            
            if last_run_time is None:
                self.logger.warning("No previous successful run found, performing full load")
                return self._extract_from_database()
            
            self.logger.info(f"Last successful run: {last_run_time}")
            
            # Use Delta Lake time-travel to get changes since last run
            df = (self.spark.read
                  .format("delta")
                  .load(source_path)
                  .filter(col("changed_at") > lit(last_run_time)))
            
            change_count = df.count()
            self.logger.info(f"Found {change_count} changed records since {last_run_time}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Incremental extraction failed: {str(e)}")
            self.logger.warning("Falling back to full extraction")
            return self._extract_from_database()
    
    def _get_last_successful_run_time(self, run_log_path: str) -> Optional[datetime]:
        """
        Retrieve the end time of the last successful ETL run
        
        Args:
            run_log_path: Path to run log Delta table
            
        Returns:
            Timestamp of last successful run or None
        """
        try:
            run_log_df = (self.spark.read
                         .format("delta")
                         .load(run_log_path)
                         .filter(col("status") == "SUCCESS")
                         .orderBy(col("end_time").desc())
                         .limit(1))
            
            if run_log_df.count() > 0:
                last_run = run_log_df.first()
                return last_run["end_time"]
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Could not retrieve last run time: {str(e)}")
            return None
    
    def extract_with_time_travel(self, version: Optional[int] = None, 
                                 timestamp: Optional[str] = None) -> DataFrame:
        """
        Extract data using Delta Lake time-travel feature
        
        Args:
            version: Specific version number to read
            timestamp: Specific timestamp to read (ISO format)
            
        Returns:
            DataFrame from specified version/timestamp
        """
        source_path = self.config.get("source.database.path")
        
        reader = self.spark.read.format("delta")
        
        if version is not None:
            self.logger.info(f"Reading Delta table version {version}")
            reader = reader.option("versionAsOf", version)
        elif timestamp is not None:
            self.logger.info(f"Reading Delta table as of timestamp {timestamp}")
            reader = reader.option("timestampAsOf", timestamp)
        
        df = reader.load(source_path)
        return df
    
    def get_extraction_metadata(self, df: DataFrame) -> Dict[str, Any]:
        """
        Collect metadata about extracted data
        
        Args:
            df: Extracted DataFrame
            
        Returns:
            Dictionary with metadata
        """
        return {
            "run_id": self.run_id,
            "record_count": df.count(),
            "extraction_time": datetime.now().isoformat(),
            "columns": df.columns,
            "partitions": df.rdd.getNumPartitions()
        }


===FILE: src/transform.py===
"""
PySpark Data Transformation Module
Applies business rules, enrichment, and validation
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, current_timestamp,
    lit, coalesce, round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from typing import List, Tuple
import logging

from src.utils.logger import ETLLogger
from src.utils.config import ConfigManager


class DataTransformer:
    """
    Transforms extracted data according to business rules
    """
    
    def __init__(self, spark: SparkSession, run_id: str, config: ConfigManager):
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = ETLLogger.get_logger("TRANSFORMER")
    
    def get_target_schema(self) -> StructType:
        """Define the transformed data schema"""
        return StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("value", DecimalType(15, 2), True),
            StructField("transformed_value", DecimalType(15, 2), True),
            StructField("status", StringType(), True),
            StructField("category", StringType(), True),
            StructField("priority", IntegerType(), True),
            StructField("etl_run_id", StringType(), False),
            StructField("processed_at", TimestampType(), False),
            StructField("processed_by", StringType(), False),
        ])
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Main transformation pipeline
        
        Args:
            source_df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting data transformation")
        
        try:
            # Step 1: Apply basic transformations
            df = self._apply_basic_transformations(source_df)
            
            # Step 2: Calculate derived values
            df = self._calculate_derived_values(df)
            
            # Step 3: Calculate priority
            df = self._calculate_priority(df)
            
            # Step 4: Apply category-specific rules
            df = self._apply_category_rules(df)
            
            # Step 5: Apply business rules
            df = self._apply_business_rules(df)
            
            # Step 6: Enrich data
            df = self._enrich_data(df)
            
            # Step 7: Add metadata
            df = self._add_metadata(df)
            
            record_count = df.count()
            self.logger.info(f"Transformed {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}", exc_info=True)
            raise
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic data cleaning and normalization"""
        self.logger.info("Applying basic transformations")
        
        df = (df
              .withColumn("name", upper(trim(col("name"))))
              .withColumn("name", regexp_replace(col("name"), r'\s+', ' '))
              .withColumn("category", coalesce(col("category"), lit("UNCATEGORIZED")))
              .withColumn("status", upper(trim(col("status")))))
        
        return df
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """Calculate transformed values based on category"""
        self.logger.info("Calculating derived values")
        
        # Get multipliers from config
        premium_multiplier = float(self.config.get("transformation.premium_multiplier", 1.5))
        standard_multiplier = float(self.config.get("transformation.standard_multiplier", 1.2))
        
        df = (df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("value") * premium_multiplier)
            .when(col("category") == "VIP", col("value") * 2.0)
            .when(col("category") == "STANDARD", col("value") * standard_multiplier)
            .otherwise(col("value"))
        ))
        
        # Round to 2 decimal places
        df = df.withColumn("transformed_value", spark_round(col("transformed_value"), 2))
        
        return df
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """Calculate priority based on value and category"""
        self.logger.info("Calculating priority")
        
        df = (df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, lit(1))
            .when(col("transformed_value") >= 750, lit(2))
            .when(col("transformed_value") >= 300, lit(3))
            .when(col("transformed_value") >= 100, lit(4))
            .otherwise(lit(5))
        ))
        
        # Override priority for VIP category
        df = df.withColumn(
            "priority",
            when(col("category") == "VIP", lit(1))
            .otherwise(col("priority"))
        )
        
        return df
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """Apply category-specific transformation rules"""
        self.logger.info("Applying category rules")
        
        # Premium category gets additional boost
        df = df.withColumn(
            "transformed_value",
            when(
                (col("category") == "PREMIUM") & (col("transformed_value") < 500),
                col("transformed_value") * 1.1
            ).otherwise(col("transformed_value"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to set status and perform validations"""
        self.logger.info("Applying business rules")
        
        # Rule 1: Set status based on transformed value
        df = (df.withColumn(
            "status",
            when(col("value").isNull() | (col("value") == 0), lit("INVALID"))
            .when(col("transformed_value") >= 750, lit("HIGH_VALUE"))
            .when(col("transformed_value") >= 300, lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        ))
        
        # Rule 2: Validate name is not empty
        df = df.withColumn(
            "status",
            when(trim(col("name")) == "", lit("INVALID"))
            .otherwise(col("status"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional information"""
        self.logger.info("Enriching data")
        
        # Load enrichment config if available
        enable_enrichment = self.config.get("transformation.enable_enrichment", True)
        
        if not enable_enrichment:
            return df
        
        # Example: Add calculated fields or lookup data
        # In production, this might join with reference tables
        
        return df
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """Add ETL metadata columns"""
        self.logger.info("Adding metadata")
        
        df = (df
              .withColumn("etl_run_id", lit(self.run_id))
              .withColumn("processed_at", current_timestamp())
              .withColumn("processed_by", lit(self.config.get("system.user", "etl_system"))))
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        self.logger.info("Validating transformed data")
        
        errors = []
        
        # Validation 1: Check for null IDs
        null_id_count = df.filter(col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with null ID")
        
        # Validation 2: Check for null names
        null_name_count = df.filter(col("name").isNull() | (trim(col("name")) == "")).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with null or empty name")
        
        # Validation 3: Check for negative values
        negative_value_count = df.filter(col("value") < 0).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative values")
        
        # Validation 4: Check priority range
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        # Validation 5: Check for invalid status
        invalid_status = df.filter(col("status") == "INVALID").count()
        if invalid_status > 0:
            self.logger.warning(f"{invalid_status} records marked as INVALID")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Validation passed")
        else:
            self.logger.error(f"Validation failed with {len(errors)} errors")
            for error in errors:
                self.logger.error(f"  - {error}")
        
        return is_valid, errors


===FILE: src/load.py===
"""
PySpark Data Loading Module
Loads transformed data to Delta Lake target with upsert capability
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable
from typing import Dict, Any, List
import logging

from src.utils.logger import ETLLogger
from src.utils.config import ConfigManager


class DataLoader:
    """
    Loads transformed data to target Delta Lake tables
    """
    
    def __init__(self, spark: SparkSession, run_id: str, config: ConfigManager):
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = ETLLogger.get_logger("LOADER")
        self.batch_size = config.get("loading.batch_size", 1000)
    
    def load_data(self, df: DataFrame, mode: str = "UPSERT") -> Dict[str, Any]:
        """
        Main loading method
        
        Args:
            df: Transformed DataFrame to load
            mode: Loading mode (INSERT, UPDATE, UPSERT)
            
        Returns:
            Dictionary with load results
        """
        self.logger.info(f"Starting data load - Mode: {mode}, Batch size: {self.batch_size}")
        
        target_path = self.config.get("target.database.path")
        target_format = self.config.get("target.database.format", "delta")
        
        try:
            if mode.upper() == "INSERT":
                result = self._insert_new(df, target_path, target_format)
            elif mode.upper() == "UPDATE":
                result = self._update_existing(df, target_path)
            elif mode.upper() == "UPSERT":
                result = self._upsert_data(df, target_path)
            else:
                self.logger.warning(f"Unknown mode {mode}, defaulting to UPSERT")
                result = self._upsert_data(df, target_path)
            
            # Perform reconciliation if enabled
            if self.config.get("loading.enable_reconciliation", True):
                self._reconcile_data(df, target_path)
            
            self.logger.info(
                f"Load complete - Success: {result['success_count']}, "
                f"Errors: {result['error_count']}"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Data load failed: {str(e)}", exc_info=True)
            return {
                "success_count": 0,
                "error_count": df.count(),
                "total_count": df.count(),
                "errors": [str(e)]
            }
    
    def _insert_new(self, df: DataFrame, target_path: str, target_format: str) -> Dict[str, Any]:
        """
        Insert new records (append mode)
        
        Args:
            df: DataFrame to insert
            target_path: Target table path
            target_format: Target format (delta)
            
        Returns:
            Load result dictionary
        """
        self.logger.info(f"Inserting {df.count()} records")
        
        try:
            (df.write
             .format(target_format)
             .mode("append")
             .save(target_path))
            
            record_count = df.count()
            
            return {
                "success_count": record_count,
                "error_count": 0,
                "total_count": record_count,
                "errors": []
            }
            
        except Exception as e:
            self.logger.error(f"Insert failed: {str(e)}")
            return {
                "success_count": 0,
                "error_count": df.count(),
                "total_count": df.count(),
                "errors": [str(e)]
            }
    
    def _update_existing(self, df: DataFrame, target_path: str) -> Dict[str, Any]:
        """
        Update existing records using Delta merge
        
        Args:
            df: DataFrame with updates
            target_path: Target Delta table path
            
        Returns:
            Load result dictionary
        """
        self.logger.info(f"Updating {df.count()} records")
        
        try:
            # Load Delta table
            delta_table = DeltaTable.forPath(self.spark, target_path)
            
            # Perform merge (update only)
            merge_result = (delta_table.alias("target")
                           .merge(
                               df.alias("source"),
                               "target.id = source.id"
                           )
                           .whenMatchedUpdateAll()
                           .execute())
            
            # Get metrics
            updated_count = df.count()  # Approximation
            
            return {
                "success_count": updated_count,
                "error_count": 0,
                "total_count": updated_count,
                "errors": []
            }
            
        except Exception as e:
            self.logger.error(f"Update failed: {str(e)}")
            return {
                "success_count": 0,
                "error_count": df.count(),
                "total_count": df.count(),
                "errors": [str(e)]
            }
    
    def _upsert_data(self, df: DataFrame, target_path: str) -> Dict[str, Any]:
        """
        Upsert (insert or update) records using Delta merge
        
        Args:
            df: DataFrame to upsert
            target_path: Target Delta table path
            
        Returns:
            Load result dictionary
        """
        self.logger.info(f"Upserting {df.count()} records to {target_path}")
        
        try:
            # Check if table exists
            try:
                delta_table = DeltaTable.forPath(self.spark, target_path)
                table_exists = True
            except:
                table_exists = False
                self.logger.info("Target table does not exist, will create")
            
            if not table_exists:
                # Create table with first write
                (df.write
                 .format("delta")
                 .mode("overwrite")
                 .save(target_path))
                
                return {
                    "success_count": df.count(),
                    "error_count": 0,
                    "total_count": df.count(),
                    "errors": []
                }
            
            # Perform merge (upsert)
            (delta_table.alias("target")
             .merge(
                 df.alias("source"),
                 "target.id = source.id"
             )
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())
            
            record_count = df.count()
            
            return {
                "success_count": record_count,
                "error_count": 0,
                "total_count": record_count,
                "errors": []
            }
            
        except Exception as e:
            self.logger.error(f"Upsert failed: {str(e)}")
            return {
                "success_count": 0,
                "error_count": df.count(),
                "total_count": df.count(),
                "errors": [str(e)]
            }
    
    def _reconcile_data(self, source_df: DataFrame, target_path: str) -> bool:
        """
        Reconcile loaded data with source
        
        Args:
            source_df: Source DataFrame
            target_path: Target table path
            
        Returns:
            True if reconciliation passes
        """
        self.logger.info("Performing data reconciliation")
        
        try:
            # Load target data for this run
            target_df = (self.spark.read
                        .format("delta")
                        .load(target_path)
                        .filter(col("etl_run_id") == self.run_id))
            
            source_count = source_df.count()
            target_count = target_df.count()
            
            if source_count == target_count:
                self.logger.info(f"Reconciliation passed: {source_count} records")
                return True
            else:
                self.logger.warning(
                    f"Reconciliation mismatch - Source: {source_count}, Target: {target_count}"
                )
                return False
                
        except Exception as e:
            self.logger.warning(f"Reconciliation failed: {str(e)}")
            return False
    
    def optimize_table(self, target_path: str):
        """
        Optimize Delta table (compact small files, Z-order)
        
        Args:
            target_path: Path to Delta table
        """
        self.logger.info("Optimizing Delta table")
        
        try:
            delta_table = DeltaTable.forPath(self.spark, target_path)
            
            # Compact files
            delta_table.optimize().executeCompaction()
            
            # Z-order by commonly filtered columns
            z_order_columns = self.config.get("loading.z_order_columns", ["id", "category"])
            if z_order_columns:
                delta_table.optimize().executeZOrderBy(*z_order_columns)
            
            self.logger.info("Table optimization complete")
            
        except Exception as e:
            self.logger.warning(f"Table optimization failed: {str(e)}")
    
    def vacuum_table(self, target_path: str, retention_hours: int = 168):
        """
        Vacuum old versions from Delta table
        
        Args:
            target_path: Path to Delta table
            retention_hours: Retention period in hours (default 7 days)
        """
        self.logger.info(f"Vacuuming Delta table (retention: {retention_hours}h)")
        
        try:
            delta_table = DeltaTable.forPath(self.spark, target_path)
            delta_table.vacuum(retention_hours)
            
            self.logger.info("Table vacuum complete")
            
        except Exception as e:
            self.logger.warning(f"Table vacuum failed: {str(e)}")


===FILE: src/orchestrator.py===
"""
ETL Orchestrator - Main pipeline coordinator
"""

from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict, Any
import uuid

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.utils.logger import ETLLogger
from src.utils.config import ConfigManager
from src.utils.monitoring import ETLMonitor


class ETLOrchestrator:
    """
    Orchestrates the complete ETL pipeline
    """
    
    def __init__(self, spark: SparkSession, config: ConfigManager, run_type: str = "MANUAL"):
        self.spark = spark
        self.config = config
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        self.logger = ETLLogger.get_logger("ORCHESTRATOR")
        self.monitor = ETLMonitor(spark, config)
    
    def _generate_run_id(self) -> str:
        """Generate unique run ID"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"RUN_{timestamp}_{unique_id}"
    
    def execute_etl(
        self,
        source_type: str = "DATABASE",
        target_type: str = "DATABASE",
        filter_condition: str = None,
        max_records: int = 0
    ) -> Dict[str, Any]:
        """
        Execute complete ETL pipeline
        
        Args:
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            target_type: Type of target
            filter_condition: Optional filter
            max_records: Max records to process
            
        Returns:
            ETL execution results
        """
        self.logger.info(f"ETL execution started - Run ID: {self.run_id}")
        start_time = datetime.now()
        
        result = {
            "run_id": self.run_id,
            "status": "RUNNING",
            "start_time": start_time.isoformat(),
            "records_extracted": 0,
            "records_transformed": 0,
            "records_loaded": 0,
            "records_failed": 0,
            "error_count": 0,
            "warnings": []
        }
        
        try:
            # Log run start
            self.monitor.log_run_start(self.run_id, self.run_type)
            
            # Step 1: Extract
            self.logger.info("=" * 60)
            self.logger.info("STEP 1: EXTRACTION")
            self.logger.info("=" * 60)
            
            extractor = DataExtractor(self.spark, self.run_id, self.config)
            source_df = extractor.extract_data(source_type, filter_condition, max_records)
            
            result["records_extracted"] = source_df.count()
            
            if result["records_extracted"] == 0:
                self.logger.warning("No data extracted - ETL stopping")
                result["status"] = "NO_DATA"
                return result
            
            # Step 2: Transform
            self.logger.info("=" * 60)
            self.logger.info("STEP 2: TRANSFORMATION")