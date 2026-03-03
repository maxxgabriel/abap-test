===FILE: src/extract.py===
"""
Delta Lake Loader - Extract Module
Extracts data from various sources with incremental support
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


class DataExtractor:
    """Handles data extraction from various sources"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.source_schema = self._get_source_schema()
        
    def _get_source_schema(self) -> StructType:
        """Define source data schema"""
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
            StructField("changed_by", StringType(), True)
        ])
    
    def extract_data(
        self,
        source_type: str = "DATABASE",
        filter_expr: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Main extraction method
        
        Args:
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            filter_expr: Optional filter expression
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        logger.info(f"Starting extraction - Source: {source_type}, Run ID: {self.run_id}")
        
        if source_type == "DATABASE":
            df = self._extract_from_database(filter_expr)
        elif source_type == "STAGING":
            df = self._extract_from_staging()
        elif source_type == "INCREMENTAL":
            df = self._extract_incremental()
        else:
            df = self._extract_from_database(filter_expr)
        
        # Apply max records limit
        if max_records > 0:
            df = df.limit(max_records)
        
        record_count = df.count()
        logger.info(f"Extracted {record_count} records")
        
        return df
    
    def _extract_from_database(self, filter_expr: Optional[str] = None) -> DataFrame:
        """Extract from database source"""
        source_path = self.config["source"]["database_path"]
        
        logger.info(f"Extracting from database: {source_path}")
        
        df = self.spark.read \
            .format(self.config["source"]["format"]) \
            .schema(self.source_schema) \
            .load(source_path)
        
        # Apply filter if provided
        if filter_expr:
            df = df.filter(filter_expr)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract from staging area"""
        staging_path = self.config["source"]["staging_path"]
        
        logger.info(f"Extracting from staging: {staging_path}")
        
        df = self.spark.read \
            .format(self.config["source"]["format"]) \
            .schema(self.source_schema) \
            .load(staging_path) \
            .filter(col("run_id") == self.run_id) \
            .filter(col("status") == "READY")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last run"""
        source_path = self.config["source"]["database_path"]
        
        logger.info("Extracting incremental data")
        
        # Get last successful run timestamp
        last_run_time = self._get_last_run_timestamp()
        
        df = self.spark.read \
            .format(self.config["source"]["format"]) \
            .schema(self.source_schema) \
            .load(source_path)
        
        if last_run_time:
            logger.info(f"Filtering records changed after: {last_run_time}")
            df = df.filter(col("changed_at") > lit(last_run_time))
        else:
            logger.warning("No last run timestamp found, performing full extract")
        
        return df
    
    def _get_last_run_timestamp(self) -> Optional[datetime]:
        """Get timestamp of last successful run"""
        try:
            run_log_path = self.config["metadata"]["run_log_path"]
            
            run_log_df = self.spark.read \
                .format("delta") \
                .load(run_log_path) \
                .filter(col("status") == "SUCCESS") \
                .orderBy(col("end_time").desc()) \
                .limit(1)
            
            if run_log_df.count() > 0:
                return run_log_df.first()["end_time"]
            
        except Exception as e:
            logger.warning(f"Could not retrieve last run timestamp: {e}")
        
        return None


===FILE: src/transform.py===
"""
Delta Lake Loader - Transform Module
Applies business rules and transformations to extracted data
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit,
    current_timestamp, current_user, round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from typing import Dict, Any, Tuple, List
import logging

logger = logging.getLogger(__name__)


class DataTransformer:
    """Handles data transformation and business rule application"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.target_schema = self._get_target_schema()
        
    def _get_target_schema(self) -> StructType:
        """Define target data schema"""
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
            StructField("processed_by", StringType(), True)
        ])
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Main transformation method
        
        Args:
            source_df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Starting data transformation")
        
        # Base transformations
        df = self._apply_base_transformations(source_df)
        
        # Calculate derived values
        df = self._calculate_derived_values(df)
        
        # Apply business rules
        df = self._apply_business_rules(df)
        
        # Enrich data
        df = self._enrich_data(df)
        
        # Add metadata
        df = self._add_metadata(df)
        
        record_count = df.count()
        logger.info(f"Transformed {record_count} records")
        
        return df
    
    def _apply_base_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic data transformations"""
        logger.info("Applying base transformations")
        
        df = df.withColumn(
            "name",
            trim(upper(regexp_replace(col("name"), "\\s+", " ")))
        )
        
        # Handle null categories
        df = df.withColumn(
            "category",
            when(col("category").isNull() | (col("category") == ""), 
                 lit("UNCATEGORIZED")
            ).otherwise(col("category"))
        )
        
        return df
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """Calculate derived and transformed values"""
        logger.info("Calculating derived values")
        
        # Get category multipliers from config
        multipliers = self.config.get("transformations", {}).get("category_multipliers", {})
        
        # Apply category-specific transformations
        transformed_value_expr = col("value")
        for category, multiplier in multipliers.items():
            transformed_value_expr = when(
                col("category") == category,
                spark_round(col("value") * multiplier, 2)
            ).otherwise(transformed_value_expr)
        
        df = df.withColumn("transformed_value", transformed_value_expr)
        
        # Calculate priority based on value
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, 1)
            .when(col("transformed_value") >= 750, 2)
            .when(col("transformed_value") >= 500, 3)
            .when(col("transformed_value") >= 250, 4)
            .otherwise(5)
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules"""
        logger.info("Applying business rules")
        
        # Rule 1: Set status based on transformed value
        df = df.withColumn(
            "status",
            when(col("value").isNull(), "INVALID")
            .when(col("transformed_value") >= 750, "HIGH_VALUE")
            .when(col("transformed_value") >= 300, "MEDIUM_VALUE")
            .otherwise("LOW_VALUE")
        )
        
        # Rule 2: Override priority for very high values
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, 1)
            .otherwise(col("priority"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional information"""
        logger.info("Enriching data")
        
        # Could join with reference data, add calculated fields, etc.
        # For now, apply premium category bonus
        premium_multiplier = self.config.get("transformations", {}).get(
            "premium_multiplier", 1.2
        )
        
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM",
                 spark_round(col("transformed_value") * premium_multiplier, 2)
            ).otherwise(col("transformed_value"))
        )
        
        return df
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """Add ETL metadata fields"""
        df = df.withColumn("etl_run_id", lit(self.run_id)) \
               .withColumn("processed_at", current_timestamp()) \
               .withColumn("processed_by", lit(current_user()))
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        logger.info("Validating transformed data")
        
        errors = []
        
        # Check for null IDs
        null_id_count = df.filter(col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with null ID")
        
        # Check for null names
        null_name_count = df.filter(col("name").isNull()).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with null name")
        
        # Check for negative values
        negative_value_count = df.filter(
            (col("value") < 0) | (col("transformed_value") < 0)
        ).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative values")
        
        # Check priority range
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            logger.info("Data validation passed")
        else:
            logger.error(f"Data validation failed with {len(errors)} errors")
            for error in errors:
                logger.error(f"  - {error}")
        
        return is_valid, errors


===FILE: src/load.py===
"""
Delta Lake Loader - Load Module
Loads transformed data to Delta Lake with merge/upsert operations
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from delta.tables import DeltaTable
from typing import Dict, Any, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class LoadResult:
    """Result of load operation"""
    success_count: int
    error_count: int
    total_count: int
    inserted_count: int = 0
    updated_count: int = 0
    deleted_count: int = 0
    errors: list = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class DeltaLoader:
    """Handles loading data to Delta Lake with merge operations"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.batch_size = config.get("load", {}).get("batch_size", 1000)
        
    def load_data(
        self,
        df: DataFrame,
        mode: str = "merge",
        merge_keys: Optional[list] = None
    ) -> LoadResult:
        """
        Main load method with multiple write modes
        
        Args:
            df: DataFrame to load
            mode: Write mode (merge, append, overwrite, insert)
            merge_keys: Keys for merge operation (default: ["id"])
            
        Returns:
            LoadResult with operation statistics
        """
        logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        target_path = self.config["target"]["delta_path"]
        total_count = df.count()
        
        try:
            if mode == "merge":
                result = self._merge_data(df, target_path, merge_keys or ["id"])
            elif mode == "append":
                result = self._append_data(df, target_path)
            elif mode == "overwrite":
                result = self._overwrite_data(df, target_path)
            elif mode == "insert":
                result = self._insert_data(df, target_path)
            else:
                raise ValueError(f"Unsupported load mode: {mode}")
            
            logger.info(
                f"Load complete - Success: {result.success_count}, "
                f"Errors: {result.error_count}, Total: {result.total_count}"
            )
            
            # Perform reconciliation if enabled
            if self.config.get("load", {}).get("enable_reconciliation", True):
                self._reconcile_data(df, target_path)
            
            return result
            
        except Exception as e:
            logger.error(f"Load failed: {str(e)}")
            return LoadResult(
                success_count=0,
                error_count=total_count,
                total_count=total_count,
                errors=[str(e)]
            )
    
    def _merge_data(
        self,
        df: DataFrame,
        target_path: str,
        merge_keys: list
    ) -> LoadResult:
        """
        Merge (upsert) data into Delta table
        
        Performs update for existing records and insert for new ones
        """
        logger.info(f"Performing merge on keys: {merge_keys}")
        
        try:
            # Check if Delta table exists
            if DeltaTable.isDeltaTable(self.spark, target_path):
                delta_table = DeltaTable.forPath(self.spark, target_path)
                
                # Build merge condition
                merge_condition = " AND ".join([
                    f"target.{key} = source.{key}" for key in merge_keys
                ])
                
                # Get all columns except merge keys for update
                update_cols = {
                    col_name: f"source.{col_name}"
                    for col_name in df.columns
                    if col_name not in merge_keys
                }
                
                # Perform merge
                merge_builder = delta_table.alias("target").merge(
                    df.alias("source"),
                    merge_condition
                )
                
                merge_builder = merge_builder.whenMatchedUpdate(set=update_cols)
                merge_builder = merge_builder.whenNotMatchedInsertAll()
                
                merge_result = merge_builder.execute()
                
                # Get operation metrics from Delta transaction log
                history = delta_table.history(1).select("operationMetrics").first()
                metrics = history["operationMetrics"] if history else {}
                
                inserted = int(metrics.get("numTargetRowsInserted", 0))
                updated = int(metrics.get("numTargetRowsUpdated", 0))
                total = inserted + updated
                
                logger.info(f"Merge complete - Inserted: {inserted}, Updated: {updated}")
                
                return LoadResult(
                    success_count=total,
                    error_count=0,
                    total_count=df.count(),
                    inserted_count=inserted,
                    updated_count=updated
                )
            else:
                # First load - create table
                logger.info("Delta table doesn't exist, creating new table")
                return self._create_delta_table(df, target_path)
                
        except Exception as e:
            logger.error(f"Merge operation failed: {str(e)}")
            raise
    
    def _append_data(self, df: DataFrame, target_path: str) -> LoadResult:
        """Append data to Delta table"""
        logger.info("Appending data to Delta table")
        
        try:
            df.write \
                .format("delta") \
                .mode("append") \
                .save(target_path)
            
            count = df.count()
            
            return LoadResult(
                success_count=count,
                error_count=0,
                total_count=count,
                inserted_count=count
            )
            
        except Exception as e:
            logger.error(f"Append operation failed: {str(e)}")
            raise
    
    def _overwrite_data(self, df: DataFrame, target_path: str) -> LoadResult:
        """Overwrite Delta table"""
        logger.info("Overwriting Delta table")
        
        try:
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save(target_path)
            
            count = df.count()
            
            return LoadResult(
                success_count=count,
                error_count=0,
                total_count=count,
                inserted_count=count
            )
            
        except Exception as e:
            logger.error(f"Overwrite operation failed: {str(e)}")
            raise
    
    def _insert_data(self, df: DataFrame, target_path: str) -> LoadResult:
        """Insert new records only (fails if duplicates exist)"""
        logger.info("Inserting new records")
        
        try:
            if DeltaTable.isDeltaTable(self.spark, target_path):
                # Check for duplicates
                delta_table = DeltaTable.forPath(self.spark, target_path)
                existing_ids = delta_table.toDF().select("id").distinct()
                
                duplicates = df.join(existing_ids, "id", "inner").count()
                
                if duplicates > 0:
                    raise ValueError(f"Found {duplicates} duplicate records")
            
            return self._append_data(df, target_path)
            
        except Exception as e:
            logger.error(f"Insert operation failed: {str(e)}")
            raise
    
    def _create_delta_table(self, df: DataFrame, target_path: str) -> LoadResult:
        """Create new Delta table"""
        logger.info(f"Creating Delta table at {target_path}")
        
        partition_cols = self.config.get("target", {}).get("partition_columns", [])
        
        writer = df.write.format("delta")
        
        if partition_cols:
            logger.info(f"Partitioning by: {partition_cols}")
            writer = writer.partitionBy(*partition_cols)
        
        writer.mode("overwrite").save(target_path)
        
        count = df.count()
        
        return LoadResult(
            success_count=count,
            error_count=0,
            total_count=count,
            inserted_count=count
        )
    
    def _reconcile_data(self, source_df: DataFrame, target_path: str) -> bool:
        """
        Reconcile loaded data against source
        
        Args:
            source_df: Source DataFrame
            target_path: Path to Delta table
            
        Returns:
            True if reconciliation passes
        """
        logger.info("Performing data reconciliation")
        
        try:
            delta_table = DeltaTable.forPath(self.spark, target_path)
            target_df = delta_table.toDF().filter(col("etl_run_id") == self.run_id)
            
            source_count = source_df.count()
            target_count = target_df.count()
            
            if source_count == target_count:
                logger.info(f"Reconciliation passed - {target_count} records match")
                return True
            else:
                logger.warning(
                    f"Reconciliation mismatch - Source: {source_count}, "
                    f"Target: {target_count}"
                )
                return False
                
        except Exception as e:
            logger.error(f"Reconciliation failed: {str(e)}")
            return False
    
    def optimize_table(self, target_path: Optional[str] = None):
        """Optimize Delta table (compaction and Z-ordering)"""
        target_path = target_path or self.config["target"]["delta_path"]
        
        logger.info(f"Optimizing Delta table: {target_path}")
        
        try:
            delta_table = DeltaTable.forPath(self.spark, target_path)
            
            # Compact files
            delta_table.optimize().executeCompaction()
            logger.info("Table compaction complete")
            
            # Z-order if configured
            z_order_cols = self.config.get("target", {}).get("z_order_columns", [])
            if z_order_cols:
                logger.info(f"Applying Z-order on: {z_order_cols}")
                delta_table.optimize().executeZOrderBy(*z_order_cols)
            
            logger.info("Table optimization complete")
            
        except Exception as e:
            logger.error(f"Table optimization failed: {str(e)}")


===FILE: src/orchestrator.py===
"""
Delta Lake Loader - Orchestrator Module
Coordinates the entire ETL pipeline execution
"""

from pyspark.sql import SparkSession
from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DeltaLoader, LoadResult
from src.quality import DataQualityChecker
from datetime import datetime
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
import logging
import yaml

logger = logging.getLogger(__name__)


@dataclass
class ETLResult:
    """Result of ETL execution"""
    run_id: str
    status: str
    start_time: datetime
    end_time: datetime
    duration_seconds: float
    records_extracted: int
    records_transformed: int
    records_loaded: int
    records_failed: int
    error_count: int
    warning_count: int
    load_result: Optional[LoadResult] = None


class ETLOrchestrator:
    """Orchestrates the complete ETL pipeline"""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config = self._load_config(config_path)
        self.spark = self._create_spark_session()
        self.run_id = self._generate_run_id()
        
        # Initialize components
        self.extractor = DataExtractor(self.spark, self.config, self.run_id)
        self.transformer = DataTransformer(self.spark, self.config, self.run_id)
        self.loader = DeltaLoader(self.spark, self.config, self.run_id)
        self.quality_checker = DataQualityChecker(self.spark, self.config, self.run_id)
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        spark_config = self.config.get("spark", {})
        
        builder = SparkSession.builder \
            .appName(spark_config.get("app_name", "DeltaLakeLoader"))
        
        # Add Spark configurations
        for key, value in spark_config.get("configs", {}).items():
            builder = builder.config(key, value)
        
        # Add Delta Lake configurations
        builder = builder.config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        
        return builder.getOrCreate()
    
    def _generate_run_id(self) -> str:
        """Generate unique run ID"""
        return f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def execute_etl(
        self,
        source_type: str = "DATABASE",
        filter_expr: Optional[str] = None,
        max_records: int = 0,
        load_mode: str = "merge",
        skip_validation: bool = False
    ) -> ETLResult:
        """
        Execute complete ETL pipeline
        
        Args:
            source_type: Type of source to extract from
            filter_expr: Optional filter expression
            max_records: Maximum records to process
            load_mode: Load mode (merge, append, overwrite, insert)
            skip_validation: Skip data quality validation
            
        Returns:
            ETLResult with execution details
        """
        start_time = datetime.now()
        
        logger.info(f"="*60)
        logger.info(f"Starting ETL execution - Run ID: {self.run_id}")
        logger.info(f"="*60)
        
        try:
            # Step 1: Extract
            logger.info("Phase 1: Extraction")
            source_df = self.extractor.extract_data(
                source_type=source_type,
                filter_expr=filter_expr,
                max_records=max_records
            )
            records_extracted = source_df.count()
            
            if records_extracted == 0:
                logger.warning("No data extracted - ETL process stopping")
                return self._create_result(
                    start_time=start_time,
                    status="NO_DATA",
                    records_extracted=0
                )
            
            # Step 2: Transform
            logger.info("Phase 2: Transformation")
            transformed_df = self.transformer.transform_data(source_df)
            records_transformed = transformed_df.count()
            
            # Step 3: Validate
            if not skip_validation:
                logger.info("Phase 3: Validation")
                is_valid, errors = self.transformer.validate_data(transformed_df)
                
                if not is_valid:
                    logger.error(f"Validation failed with {len(errors)} errors")
                    for error in errors:
                        logger.error(f"  - {error}")
                    
                    return self._create_result(
                        start_time=start_time,
                        status="VALIDATION_FAILED",
                        records_extracted=records_extracted,
                        records_transformed=records_transformed,
                        error_count=len(errors)
                    )
            
            # Step 4: Quality Checks
            logger.info("Phase 4: Data Quality Checks")
            quality_checks = self.quality_checker.perform_quality_checks(transformed_df)
            failed_checks = [c for c in quality_checks if not c["passed"]]
            
            if failed_checks:
                logger.warning(f"{len(failed_checks)} quality checks failed")
                for check in failed_checks:
                    logger.warning(f"  - {check['check_name']}: {check['message']}")
            
            # Step 5: Load
            logger.info(f"Phase 5: Load (mode: {load_mode})")
            load_result = self.loader.load_data(
                df=transformed_df,
                mode=load_mode
            )
            
            # Step 6: Optimize (if configured)
            if self.config.get("target", {}).get("optimize_after_load", False):
                logger.info("Phase 6: Table Optimization")
                self.loader.optimize_table()
            
            # Determine final status
            if load_result.error_count == 0:
                status = "SUCCESS"
            elif load_result.success_count > 0:
                status = "PARTIAL_SUCCESS"
            else:
                status = "FAILED"
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result = ETLResult(
                run_id=self.run_id,
                status=status,
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration,
                records_extracted=records_extracted,
                records_transformed=records_transformed,
                records_loaded=load_result.success_count,
                records_failed=load_result.error_count,
                error_count=load_result.error_count + len(failed_checks),
                warning_count=len(failed_checks),
                load_result=load_result
            )
            
            self._log_run_completion(result)
            
            return result
            
        except Exception as e:
            logger.error(f"ETL execution failed: {str(e)}", exc_info=True)
            
            end_time = datetime.now()
            
            return ETLResult(
                run_id=self.run_id,
                status="ERROR",
                start_time=start_time,
                end_time=end_time,
                duration_seconds=(end_