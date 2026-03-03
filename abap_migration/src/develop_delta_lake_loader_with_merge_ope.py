# PySpark Delta Lake Loader Migration

===FILE: src/extract.py===
"""
Data extraction module for Delta Lake ETL pipeline.
Supports multiple source types and incremental loading patterns.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional, Dict, Any
import logging


class DataExtractor:
    """Handles data extraction from various sources with configurable filters."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize extractor with Spark session and configuration.
        
        Args:
            spark: Active SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def get_source_schema(self) -> StructType:
        """Define source data schema."""
        return StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("value", DecimalType(15, 2), False),
            StructField("status", StringType(), True),
            StructField("category", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", StringType(), True),
            StructField("changed_at", TimestampType(), True),
            StructField("changed_by", StringType(), True)
        ])
    
    def extract_data(
        self,
        source_type: str = "database",
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data from configured source.
        
        Args:
            source_type: Type of source (database, staging, incremental)
            filter_condition: Optional filter SQL WHERE clause
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame containing extracted data
        """
        self.logger.info(f"Starting extraction - Source: {source_type}, Run ID: {self.run_id}")
        
        if source_type.lower() == "database":
            df = self._extract_from_database(filter_condition)
        elif source_type.lower() == "staging":
            df = self._extract_from_staging()
        elif source_type.lower() == "incremental":
            df = self._extract_incremental()
        else:
            self.logger.warning(f"Unknown source type {source_type}, defaulting to database")
            df = self._extract_from_database(filter_condition)
        
        if df.isEmpty():
            self.logger.warning("No data extracted")
            return df
        
        # Apply max records limit
        if max_records > 0:
            df = df.limit(max_records)
        
        record_count = df.count()
        self.logger.info(f"Extracted {record_count} records")
        
        return df
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract from primary database source."""
        source_config = self.config["sources"]["database"]
        
        read_options = {
            "url": source_config["jdbc_url"],
            "dbtable": source_config["table"],
            "user": source_config.get("user", ""),
            "password": source_config.get("password", ""),
            "driver": source_config.get("driver", "org.postgresql.Driver")
        }
        
        # Build query with filter
        if filter_condition:
            query = f"(SELECT * FROM {source_config['table']} WHERE {filter_condition}) AS filtered_data"
            read_options["dbtable"] = query
        
        df = self.spark.read \
            .format("jdbc") \
            .options(**read_options) \
            .load()
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract from staging area."""
        staging_config = self.config["sources"]["staging"]
        staging_path = staging_config["path"]
        
        self.logger.info(f"Reading from staging: {staging_path}")
        
        df = self.spark.read \
            .format(staging_config.get("format", "parquet")) \
            .load(staging_path) \
            .filter(f"run_id = '{self.run_id}' AND status = 'READY'")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last successful run."""
        source_config = self.config["sources"]["database"]
        
        # Get last successful run timestamp
        last_run_time = self._get_last_run_timestamp()
        
        if last_run_time:
            self.logger.info(f"Incremental load from: {last_run_time}")
            filter_clause = f"changed_at > '{last_run_time}'"
        else:
            self.logger.info("No previous run found, performing full extraction")
            filter_clause = None
        
        return self._extract_from_database(filter_clause)
    
    def _get_last_run_timestamp(self) -> Optional[str]:
        """Retrieve timestamp of last successful ETL run."""
        try:
            run_log_config = self.config.get("run_log", {})
            if not run_log_config:
                return None
            
            df = self.spark.read \
                .format("jdbc") \
                .option("url", run_log_config["jdbc_url"]) \
                .option("dbtable", run_log_config["table"]) \
                .option("user", run_log_config.get("user", "")) \
                .option("password", run_log_config.get("password", "")) \
                .load() \
                .filter("status = 'SUCCESS'") \
                .orderBy("end_time", ascending=False) \
                .limit(1)
            
            if df.isEmpty():
                return None
            
            return df.select("end_time").collect()[0][0]
        except Exception as e:
            self.logger.warning(f"Could not retrieve last run timestamp: {e}")
            return None


===FILE: src/transform.py===
"""
Data transformation module for Delta Lake ETL pipeline.
Implements business rules, data enrichment, and validation.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, current_timestamp,
    current_user, lit, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import Dict, Any, List, Tuple
import logging


class DataTransformer:
    """Handles data transformation, enrichment, and validation."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize transformer with Spark session and configuration.
        
        Args:
            spark: Active SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def get_target_schema(self) -> StructType:
        """Define target data schema."""
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
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply complete transformation pipeline.
        
        Args:
            df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting data transformation")
        
        # Basic transformations
        df_transformed = self._apply_basic_transformations(df)
        
        # Calculate derived values
        df_transformed = self._calculate_derived_values(df_transformed)
        
        # Calculate priority
        df_transformed = self._calculate_priority(df_transformed)
        
        # Apply business rules
        df_transformed = self._apply_business_rules(df_transformed)
        
        # Enrich data
        df_transformed = self._enrich_data(df_transformed)
        
        # Add metadata
        df_transformed = self._add_metadata(df_transformed)
        
        record_count = df_transformed.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return df_transformed
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic data cleansing and normalization."""
        return df.select(
            col("id"),
            upper(trim(regexp_replace(col("name"), r"\s+", " "))).alias("name"),
            col("value"),
            col("status"),
            coalesce(col("category"), lit("UNCATEGORIZED")).alias("category"),
            col("source_system"),
            col("created_at"),
            col("created_by"),
            col("changed_at"),
            col("changed_by")
        )
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """Calculate transformed values based on category rules."""
        transformation_rules = self.config.get("transformation", {}).get("rules", {})
        
        # Get category multipliers
        premium_mult = float(transformation_rules.get("premium_multiplier", 1.5))
        vip_mult = float(transformation_rules.get("vip_multiplier", 1.8))
        standard_mult = float(transformation_rules.get("standard_multiplier", 1.0))
        
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("value") * premium_mult)
            .when(col("category") == "VIP", col("value") * vip_mult)
            .when(col("category") == "STANDARD", col("value") * standard_mult)
            .otherwise(col("value"))
        )
        
        return df
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """Calculate priority based on value and category."""
        priority_rules = self.config.get("transformation", {}).get("priority_rules", {})
        
        high_threshold = float(priority_rules.get("high_threshold", 1000))
        medium_threshold = float(priority_rules.get("medium_threshold", 500))
        
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= high_threshold, lit(1))
            .when(col("transformed_value") >= medium_threshold, lit(2))
            .when(col("category") == "VIP", lit(2))
            .when(col("category") == "PREMIUM", lit(3))
            .otherwise(lit(4))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to set status and other derived fields."""
        df = df.withColumn(
            "status",
            when(col("value").isNull() | (col("value") == 0), lit("INVALID"))
            .when(col("transformed_value") >= 750, lit("HIGH_VALUE"))
            .when(col("transformed_value") >= 300, lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )
        
        # Override priority for high value items
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, lit(1))
            .otherwise(col("priority"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional calculated fields."""
        enrichment_config = self.config.get("transformation", {}).get("enrichment", {})
        
        if enrichment_config.get("apply_premium_boost", False):
            df = df.withColumn(
                "transformed_value",
                when(col("category") == "PREMIUM", 
                     col("transformed_value") * 1.2)
                .otherwise(col("transformed_value"))
            )
        
        return df
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """Add ETL metadata fields."""
        return df.withColumn("etl_run_id", lit(self.run_id)) \
                 .withColumn("processed_at", current_timestamp()) \
                 .withColumn("processed_by", current_user())
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data against business rules.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        self.logger.info("Starting data validation")
        errors = []
        
        # Check for null IDs
        null_id_count = df.filter(col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with null ID")
        
        # Check for null names
        null_name_count = df.filter(col("name").isNull()).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with null name")
        
        # Check for invalid values
        invalid_value_count = df.filter(
            (col("value") < 0) | (col("transformed_value") < 0)
        ).count()
        if invalid_value_count > 0:
            errors.append(f"{invalid_value_count} records with negative values")
        
        # Check for invalid priorities
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        # Check for duplicate IDs
        total_count = df.count()
        unique_count = df.select("id").distinct().count()
        if total_count != unique_count:
            errors.append(f"{total_count - unique_count} duplicate IDs found")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Data validation passed")
        else:
            self.logger.error(f"Data validation failed with {len(errors)} errors")
            for error in errors:
                self.logger.error(f"  - {error}")
        
        return is_valid, errors


===FILE: src/load.py===
"""
Delta Lake loader module with merge/upsert operations.
Supports multiple write modes and optimized batch processing.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable
from typing import Dict, Any, Optional
import logging


class DeltaLakeLoader:
    """Handles loading data to Delta Lake with merge operations."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize loader with Spark session and configuration.
        
        Args:
            spark: Active SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def load_data(
        self,
        df: DataFrame,
        mode: str = "upsert",
        partition_cols: Optional[list] = None
    ) -> Dict[str, int]:
        """
        Load data to Delta Lake target.
        
        Args:
            df: DataFrame to load
            mode: Write mode (insert, update, upsert, overwrite)
            partition_cols: Optional partition columns
            
        Returns:
            Dictionary with load statistics
        """
        self.logger.info(f"Starting data load - Mode: {mode}, Run ID: {self.run_id}")
        
        target_config = self.config["target"]
        target_path = target_config["path"]
        
        # Get partition columns from config if not provided
        if partition_cols is None:
            partition_cols = target_config.get("partition_by", [])
        
        # Execute load based on mode
        if mode.lower() == "insert":
            result = self._insert_data(df, target_path, partition_cols)
        elif mode.lower() == "update":
            result = self._update_data(df, target_path)
        elif mode.lower() == "upsert":
            result = self._upsert_data(df, target_path, partition_cols)
        elif mode.lower() == "overwrite":
            result = self._overwrite_data(df, target_path, partition_cols)
        else:
            self.logger.warning(f"Unknown mode {mode}, defaulting to upsert")
            result = self._upsert_data(df, target_path, partition_cols)
        
        # Perform reconciliation if enabled
        if self.config.get("load", {}).get("enable_reconciliation", True):
            self._reconcile_data(df, target_path)
        
        # Optimize table if enabled
        if self.config.get("load", {}).get("enable_optimization", True):
            self._optimize_table(target_path)
        
        self.logger.info(
            f"Load complete - Success: {result['success_count']}, "
            f"Failed: {result['error_count']}"
        )
        
        return result
    
    def _insert_data(
        self,
        df: DataFrame,
        target_path: str,
        partition_cols: list
    ) -> Dict[str, int]:
        """Insert new records only (append mode)."""
        try:
            writer = df.write.format("delta").mode("append")
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            writer.save(target_path)
            
            record_count = df.count()
            return {
                "success_count": record_count,
                "error_count": 0,
                "total_count": record_count
            }
        except Exception as e:
            self.logger.error(f"Insert failed: {str(e)}")
            return {
                "success_count": 0,
                "error_count": df.count(),
                "total_count": df.count()
            }
    
    def _update_data(self, df: DataFrame, target_path: str) -> Dict[str, int]:
        """Update existing records only."""
        try:
            if not DeltaTable.isDeltaTable(self.spark, target_path):
                self.logger.error("Target is not a Delta table")
                return {
                    "success_count": 0,
                    "error_count": df.count(),
                    "total_count": df.count()
                }
            
            delta_table = DeltaTable.forPath(self.spark, target_path)
            merge_key = self.config["target"].get("merge_key", "id")
            
            # Update only matching records
            delta_table.alias("target") \
                .merge(
                    df.alias("source"),
                    f"target.{merge_key} = source.{merge_key}"
                ) \
                .whenMatchedUpdateAll() \
                .execute()
            
            record_count = df.count()
            return {
                "success_count": record_count,
                "error_count": 0,
                "total_count": record_count
            }
        except Exception as e:
            self.logger.error(f"Update failed: {str(e)}")
            return {
                "success_count": 0,
                "error_count": df.count(),
                "total_count": df.count()
            }
    
    def _upsert_data(
        self,
        df: DataFrame,
        target_path: str,
        partition_cols: list
    ) -> Dict[str, int]:
        """Upsert (merge) data - update existing and insert new."""
        try:
            merge_key = self.config["target"].get("merge_key", "id")
            
            # Check if table exists
            if not DeltaTable.isDeltaTable(self.spark, target_path):
                self.logger.info("Target table does not exist, creating new table")
                return self._insert_data(df, target_path, partition_cols)
            
            delta_table = DeltaTable.forPath(self.spark, target_path)
            
            # Add update timestamp
            df = df.withColumn("updated_at", current_timestamp())
            
            # Perform merge operation
            merge_builder = delta_table.alias("target").merge(
                df.alias("source"),
                f"target.{merge_key} = source.{merge_key}"
            )
            
            # Define update columns (exclude merge key)
            update_cols = {col_name: f"source.{col_name}" 
                          for col_name in df.columns 
                          if col_name != merge_key}
            
            # Execute merge with update and insert
            merge_builder \
                .whenMatchedUpdate(set=update_cols) \
                .whenNotMatchedInsertAll() \
                .execute()
            
            record_count = df.count()
            return {
                "success_count": record_count,
                "error_count": 0,
                "total_count": record_count
            }
        except Exception as e:
            self.logger.error(f"Upsert failed: {str(e)}")
            return {
                "success_count": 0,
                "error_count": df.count(),
                "total_count": df.count()
            }
    
    def _overwrite_data(
        self,
        df: DataFrame,
        target_path: str,
        partition_cols: list
    ) -> Dict[str, int]:
        """Overwrite entire table or partitions."""
        try:
            writer = df.write.format("delta").mode("overwrite")
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
                writer = writer.option("replaceWhere", 
                                      self._build_partition_filter(df, partition_cols))
            
            writer.save(target_path)
            
            record_count = df.count()
            return {
                "success_count": record_count,
                "error_count": 0,
                "total_count": record_count
            }
        except Exception as e:
            self.logger.error(f"Overwrite failed: {str(e)}")
            return {
                "success_count": 0,
                "error_count": df.count(),
                "total_count": df.count()
            }
    
    def _build_partition_filter(self, df: DataFrame, partition_cols: list) -> str:
        """Build partition filter for dynamic overwrite."""
        if not partition_cols:
            return ""
        
        # Get distinct partition values
        partition_values = df.select(*partition_cols).distinct().collect()
        
        filters = []
        for row in partition_values:
            conditions = [f"{col}='{row[col]}'" for col in partition_cols]
            filters.append(" AND ".join(conditions))
        
        return " OR ".join(f"({f})" for f in filters)
    
    def _reconcile_data(self, source_df: DataFrame, target_path: str):
        """Verify data reconciliation between source and target."""
        try:
            target_df = self.spark.read.format("delta").load(target_path)
            
            source_count = source_df.count()
            merge_key = self.config["target"].get("merge_key", "id")
            
            # Count matching records in target
            matched_count = target_df.join(
                source_df.select(merge_key),
                merge_key,
                "inner"
            ).count()
            
            if matched_count == source_count:
                self.logger.info("Data reconciliation successful")
            else:
                self.logger.warning(
                    f"Reconciliation mismatch - Source: {source_count}, "
                    f"Matched: {matched_count}"
                )
        except Exception as e:
            self.logger.error(f"Reconciliation failed: {str(e)}")
    
    def _optimize_table(self, target_path: str):
        """Optimize Delta table with compaction and Z-ordering."""
        try:
            delta_table = DeltaTable.forPath(self.spark, target_path)
            
            # Optimize with compaction
            delta_table.optimize().executeCompaction()
            
            # Apply Z-ordering if configured
            zorder_cols = self.config.get("load", {}).get("zorder_by", [])
            if zorder_cols:
                self.spark.sql(
                    f"OPTIMIZE delta.`{target_path}` ZORDER BY ({','.join(zorder_cols)})"
                )
            
            self.logger.info("Table optimization completed")
        except Exception as e:
            self.logger.warning(f"Table optimization failed: {str(e)}")


===FILE: src/orchestrator.py===
"""
ETL orchestrator for coordinating extract, transform, and load operations.
"""

from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict, Any, Optional
import logging
import uuid

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DeltaLakeLoader


class ETLOrchestrator:
    """Orchestrates the complete ETL pipeline execution."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize orchestrator.
        
        Args:
            spark: Active SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
        
    def execute_etl(
        self,
        source_type: str = "database",
        target_mode: str = "upsert",
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> Dict[str, Any]:
        """
        Execute complete ETL pipeline.
        
        Args:
            source_type: Type of data source
            target_mode: Write mode for target
            filter_condition: Optional filter for extraction
            max_records: Maximum records to process
            
        Returns:
            Dictionary containing execution results
        """
        run_id = self._generate_run_id()
        start_time = datetime.now()
        
        result = {
            "run_id": run_id,
            "status": "RUNNING",
            "start_time": start_time,
            "records_extracted": 0,
            "records_transformed": 0,
            "records_loaded": 0,
            "records_failed": 0,
            "errors": []
        }
        
        self.logger.info(f"Starting ETL execution - Run ID: {run_id}")
        
        try:
            # Step 1: Extract
            self.logger.info("Phase 1: Extraction")
            extractor = DataExtractor(self.spark, self.config, run_id)
            source_df = extractor.extract_data(
                source_type=source_type,
                filter_condition=filter_condition,
                max_records=max_records
            )
            
            result["records_extracted"] = source_df.count()
            
            if result["records_extracted"] == 0:
                self.logger.warning("No data extracted, stopping pipeline")
                result["status"] = "NO_DATA"
                result["end_time"] = datetime.now()
                return result
            
            # Step 2: Transform
            self.logger.info("Phase 2: Transformation")
            transformer = DataTransformer(self.spark, self.config, run_id)
            transformed_df = transformer.transform_data(source_df)
            
            result["records_transformed"] = transformed_df.count()
            
            # Step 3: Validate
            self.logger.info("Phase 3: Validation")
            is_valid, validation_errors = transformer.validate_data(transformed_df)
            
            if not is_valid:
                self.logger.error(f"Validation failed with {len(validation_errors)} errors")
                result["status"] = "VALIDATION_FAILED"
                result["errors"] = validation_errors
                result["end_time"] = datetime.now()
                return result
            
            # Step 4: Load
            self.logger.info("Phase 4: Loading")
            loader = DeltaLakeLoader(self.spark, self.config, run_id)
            
            partition_cols = self.config.get("target", {}).get("partition_by", [])
            load_result = loader.load_data(
                df=transformed_df,
                mode=target_mode,
                partition_cols=partition_cols
            )
            
            result["records_loaded"] = load_result["success_count"]
            result["records_failed"] = load_result["error_count"]
            
            # Determine final status
            if load_result["error_count"] == 0:
                result["status"] = "SUCCESS"
            elif load_result["success_count"] > 0:
                result["status"] = "PARTIAL_SUCCESS"
            else:
                result["status"] = "FAILED"
            
        except Exception as e:
            self.logger.error(f"ETL execution failed: {str(e)}", exc_info=True)
            result["status"] = "ERROR"
            result["errors"].append(str(e))
        
        finally:
            result["end_time"] = datetime.now()
            result["duration_seconds"] = (
                result["end_time"] - result["start_time"]
            ).total_seconds()
            
            self._log_run_completion(result)
        
        return result
    
    def _generate_run_id(self) -> str:
        """Generate unique run identifier."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"ETL_{timestamp}_{unique_id}"
    
    def _log_run_completion(self, result: Dict[str, Any]):
        """Log execution completion summary."""
        self.logger.info("="*60)
        self.logger.info("ETL Execution Summary")
        self.logger.info("="*60)
        self.logger.info(f"Run ID: {result['run_id']}")
        self.logger.info(f"Status: {result['status']}")
        self.logger.info(f"Duration: {result['duration_seconds']:.2f} seconds")
        self.logger.info(f"Records Extracted: {result['records_extracted']}")
        self.logger.info(f"Records Transformed: {result['records_transformed']}")
        self.logger.info(f"Records Loaded: {result['records_loaded']}")
        self.logger.info(f"Records Failed: {result['records_failed']}")
        
        if result["errors"]:
            self.logger.error(f"Errors ({len(result['errors'])}):")
            for error in result["errors"]:
                self.logger.error(f"  - {error}")
        
        self.logger.info("="*60)


===