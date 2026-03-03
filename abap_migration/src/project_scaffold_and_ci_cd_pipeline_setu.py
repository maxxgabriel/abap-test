===FILE: src/extract.py===
"""
Extract module for ETL pipeline
Handles data extraction from various sources with incremental and full load support
"""
from typing import List, Dict, Optional
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
import logging
from config import Config

logger = logging.getLogger(__name__)


class Extractor:
    """Data extraction handler with support for multiple source types"""
    
    SCHEMA = StructType([
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
    
    def __init__(self, spark: SparkSession, config: Config, run_id: str):
        """
        Initialize extractor
        
        Args:
            spark: SparkSession instance
            config: Configuration object
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.source_type = config.source_type
        
        logger.info(f"Extractor initialized - Source: {self.source_type}, Run ID: {run_id}")
    
    def extract_data(self, filter_condition: Optional[str] = None, 
                     max_records: int = 0) -> DataFrame:
        """
        Extract data based on source type
        
        Args:
            filter_condition: Optional SQL filter condition
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        logger.info(f"Starting extraction - Source: {self.source_type}")
        
        try:
            if self.source_type == "DATABASE":
                df = self._extract_from_database(filter_condition)
            elif self.source_type == "STAGING":
                df = self._extract_from_staging()
            elif self.source_type == "INCREMENTAL":
                df = self._extract_incremental()
            else:
                df = self._extract_from_database(filter_condition)
            
            # Apply max records limit
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract from database source"""
        logger.info("Extracting from database")
        
        jdbc_url = self.config.jdbc_url
        table_name = self.config.source_table
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", self.config.db_user) \
            .option("password", self.config.db_password) \
            .option("driver", self.config.jdbc_driver) \
            .load()
        
        # Apply filter if provided
        if filter_condition:
            df = df.filter(filter_condition)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract from staging area"""
        logger.info(f"Extracting from staging - Run ID: {self.run_id}")
        
        staging_path = self.config.staging_path
        
        df = self.spark.read \
            .format("parquet") \
            .load(f"{staging_path}/run_id={self.run_id}") \
            .filter("status = 'READY'")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last run"""
        logger.info("Extracting incremental data")
        
        # Get last successful run timestamp
        last_run_time = self._get_last_run_time()
        
        if last_run_time:
            logger.info(f"Loading data changed after {last_run_time}")
            df = self._extract_from_database(
                filter_condition=f"changed_at > '{last_run_time}'"
            )
        else:
            logger.warning("No previous run found, performing full load")
            df = self._extract_from_database()
        
        return df
    
    def _get_last_run_time(self) -> Optional[str]:
        """Get timestamp of last successful run"""
        try:
            log_table = self.config.run_log_table
            
            last_run_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.config.jdbc_url) \
                .option("dbtable", f"(SELECT MAX(end_time) as last_run FROM {log_table} WHERE status = 'SUCCESS') as subq") \
                .option("user", self.config.db_user) \
                .option("password", self.config.db_password) \
                .option("driver", self.config.jdbc_driver) \
                .load()
            
            result = last_run_df.collect()
            if result and result[0]['last_run']:
                return result[0]['last_run'].isoformat()
            
            return None
            
        except Exception as e:
            logger.warning(f"Could not retrieve last run time: {str(e)}")
            return None


===FILE: src/transform.py===
"""
Transform module for ETL pipeline
Applies business rules, data enrichment, and validation
"""
from typing import List, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, upper, trim, when, current_timestamp, 
    lit, regexp_replace, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
import logging
from config import Config

logger = logging.getLogger(__name__)


class Transformer:
    """Data transformation handler with business rules and validation"""
    
    SCHEMA = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("value", DecimalType(15, 2), True),
        StructField("transformed_value", DecimalType(15, 2), True),
        StructField("status", StringType(), True),
        StructField("category", StringType(), True),
        StructField("priority", IntegerType(), True),
        StructField("etl_run_id", StringType(), True),
        StructField("processed_at", TimestampType(), True),
        StructField("processed_by", StringType(), True),
    ])
    
    def __init__(self, spark: SparkSession, config: Config, run_id: str):
        """
        Initialize transformer
        
        Args:
            spark: SparkSession instance
            config: Configuration object
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        
        logger.info(f"Transformer initialized - Run ID: {run_id}")
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply complete transformation pipeline
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Starting transformation")
        
        try:
            # Basic transformations
            df_transformed = df.select(
                col("id"),
                upper(trim(col("name"))).alias("name"),
                col("value"),
                self._calculate_derived_values(col("value"), col("category")).alias("transformed_value"),
                lit("TRANSFORMED").alias("status"),
                col("category"),
                self._calculate_priority(col("value"), col("category")).alias("priority"),
                lit(self.run_id).alias("etl_run_id"),
                current_timestamp().alias("processed_at"),
                lit(self.config.processed_by).alias("processed_by")
            )
            
            # Apply category-specific rules
            df_transformed = self._apply_category_rules(df_transformed)
            
            # Apply business rules
            df_transformed = self._apply_business_rules(df_transformed)
            
            # Enrich data
            df_transformed = self._enrich_data(df_transformed)
            
            # Clean up
            df_transformed = self._cleanup_data(df_transformed)
            
            record_count = df_transformed.count()
            logger.info(f"Transformed {record_count} records")
            
            return df_transformed
            
        except Exception as e:
            logger.error(f"Transformation failed: {str(e)}")
            raise
    
    def _calculate_derived_values(self, value_col, category_col):
        """Calculate derived/transformed values based on category"""
        return when(category_col == "PREMIUM", value_col * 1.5) \
            .when(category_col == "VIP", value_col * 1.8) \
            .when(category_col == "STANDARD", value_col * 1.2) \
            .otherwise(value_col)
    
    def _calculate_priority(self, value_col, category_col):
        """Calculate priority based on value and category"""
        return when(value_col >= 1000, 1) \
            .when((value_col >= 750) | (category_col == "VIP"), 2) \
            .when((value_col >= 500) | (category_col == "PREMIUM"), 3) \
            .when(value_col >= 250, 4) \
            .otherwise(5)
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """Apply category-specific business rules"""
        logger.info("Applying category rules")
        
        df = df.withColumn(
            "category",
            coalesce(col("category"), lit("UNCATEGORIZED"))
        )
        
        # Premium category gets additional boost
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", 
                 col("transformed_value") * 1.2)
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply core business rules"""
        logger.info("Applying business rules")
        
        # Rule 1: Set status based on transformed value
        df = df.withColumn(
            "status",
            when(col("transformed_value").isNull() | (col("value") == 0), "INVALID")
            .when(col("transformed_value") >= 750, "HIGH_VALUE")
            .when(col("transformed_value") >= 300, "MEDIUM_VALUE")
            .otherwise("LOW_VALUE")
        )
        
        # Rule 2: Priority override for high-value items
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, 1)
            .otherwise(col("priority"))
        )
        
        # Rule 3: Name normalization - remove extra spaces
        df = df.withColumn(
            "name",
            regexp_replace(trim(col("name")), "\\s+", " ")
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional attributes"""
        logger.info("Enriching data")
        
        # Add value tier classification
        df = df.withColumn(
            "value_tier",
            when(col("transformed_value") >= 1000, "TIER_1")
            .when(col("transformed_value") >= 500, "TIER_2")
            .when(col("transformed_value") >= 250, "TIER_3")
            .otherwise("TIER_4")
        )
        
        return df
    
    def _cleanup_data(self, df: DataFrame) -> DataFrame:
        """Final cleanup and formatting"""
        # Drop temporary columns if any
        if "value_tier" in df.columns:
            df = df.drop("value_tier")
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        logger.info("Validating data")
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
        
        # Check for invalid priorities
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            logger.info("Validation passed")
        else:
            logger.warning(f"Validation failed with {len(errors)} errors")
            for error in errors:
                logger.warning(f"  - {error}")
        
        return is_valid, errors


===FILE: src/load.py===
"""
Load module for ETL pipeline
Handles data loading to target with batch processing and reconciliation
"""
from typing import Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
import logging
from config import Config

logger = logging.getLogger(__name__)


class Loader:
    """Data loading handler with batch processing and reconciliation"""
    
    def __init__(self, spark: SparkSession, config: Config, run_id: str):
        """
        Initialize loader
        
        Args:
            spark: SparkSession instance
            config: Configuration object
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.target_type = config.target_type
        self.batch_size = config.batch_size
        
        logger.info(f"Loader initialized - Target: {self.target_type}, Batch size: {self.batch_size}")
    
    def load_data(self, df: DataFrame, mode: str = "overwrite") -> Dict:
        """
        Load data to target
        
        Args:
            df: DataFrame to load
            mode: Load mode (append, overwrite, upsert)
            
        Returns:
            Dictionary with load results
        """
        logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        result = {
            "success_count": 0,
            "error_count": 0,
            "total_count": 0,
            "errors": []
        }
        
        try:
            total_records = df.count()
            result["total_count"] = total_records
            
            if total_records == 0:
                logger.warning("No data to load")
                return result
            
            # Load based on target type
            if self.target_type == "DATABASE":
                success = self._load_to_database(df, mode)
            elif self.target_type == "PARQUET":
                success = self._load_to_parquet(df, mode)
            elif self.target_type == "DELTA":
                success = self._load_to_delta(df, mode)
            else:
                success = self._load_to_database(df, mode)
            
            if success:
                result["success_count"] = total_records
                logger.info(f"Successfully loaded {total_records} records")
                
                # Reconcile if enabled
                if self.config.enable_reconciliation:
                    self._reconcile_data(df)
            else:
                result["error_count"] = total_records
                result["errors"].append("Load operation failed")
                logger.error("Load operation failed")
            
        except Exception as e:
            result["error_count"] = result["total_count"] - result["success_count"]
            result["errors"].append(str(e))
            logger.error(f"Load failed: {str(e)}")
        
        return result
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """Load data to database via JDBC"""
        try:
            logger.info("Loading to database")
            
            # Convert mode
            jdbc_mode = "append" if mode in ["append", "upsert"] else "overwrite"
            
            df.write \
                .format("jdbc") \
                .option("url", self.config.jdbc_url) \
                .option("dbtable", self.config.target_table) \
                .option("user", self.config.db_user) \
                .option("password", self.config.db_password) \
                .option("driver", self.config.jdbc_driver) \
                .option("batchsize", self.batch_size) \
                .mode(jdbc_mode) \
                .save()
            
            return True
            
        except Exception as e:
            logger.error(f"Database load failed: {str(e)}")
            return False
    
    def _load_to_parquet(self, df: DataFrame, mode: str) -> bool:
        """Load data to Parquet files"""
        try:
            logger.info("Loading to Parquet")
            
            output_path = f"{self.config.output_path}/data"
            
            df.write \
                .format("parquet") \
                .mode(mode) \
                .partitionBy("category") \
                .save(output_path)
            
            return True
            
        except Exception as e:
            logger.error(f"Parquet load failed: {str(e)}")
            return False
    
    def _load_to_delta(self, df: DataFrame, mode: str) -> bool:
        """Load data to Delta Lake"""
        try:
            logger.info("Loading to Delta Lake")
            
            output_path = f"{self.config.output_path}/delta"
            
            if mode == "upsert":
                # Perform merge operation
                from delta.tables import DeltaTable
                
                if DeltaTable.isDeltaTable(self.spark, output_path):
                    delta_table = DeltaTable.forPath(self.spark, output_path)
                    
                    delta_table.alias("target") \
                        .merge(
                            df.alias("source"),
                            "target.id = source.id"
                        ) \
                        .whenMatchedUpdateAll() \
                        .whenNotMatchedInsertAll() \
                        .execute()
                else:
                    df.write.format("delta").mode("overwrite").save(output_path)
            else:
                df.write \
                    .format("delta") \
                    .mode(mode) \
                    .save(output_path)
            
            return True
            
        except Exception as e:
            logger.error(f"Delta load failed: {str(e)}")
            return False
    
    def _reconcile_data(self, source_df: DataFrame):
        """Reconcile loaded data with source"""
        logger.info("Performing data reconciliation")
        
        try:
            # Read back loaded data
            if self.target_type == "DATABASE":
                target_df = self.spark.read \
                    .format("jdbc") \
                    .option("url", self.config.jdbc_url) \
                    .option("dbtable", self.config.target_table) \
                    .option("user", self.config.db_user) \
                    .option("password", self.config.db_password) \
                    .option("driver", self.config.jdbc_driver) \
                    .load() \
                    .filter(col("etl_run_id") == self.run_id)
            else:
                output_path = f"{self.config.output_path}/data"
                target_df = self.spark.read.parquet(output_path)
            
            source_count = source_df.count()
            target_count = target_df.count()
            
            if source_count == target_count:
                logger.info(f"Reconciliation passed: {source_count} records match")
            else:
                logger.warning(
                    f"Reconciliation mismatch: Source={source_count}, Target={target_count}"
                )
                
        except Exception as e:
            logger.error(f"Reconciliation failed: {str(e)}")


===FILE: src/orchestrator.py===
"""
Orchestrator module for ETL pipeline
Coordinates extract, transform, load operations with error handling
"""
from datetime import datetime
from typing import Dict, Optional
from pyspark.sql import SparkSession
import logging
from extract import Extractor
from transform import Transformer
from load import Loader
from data_quality import DataQuality
from config import Config

logger = logging.getLogger(__name__)


class ETLOrchestrator:
    """Main orchestrator for ETL pipeline execution"""
    
    def __init__(self, spark: SparkSession, config: Config):
        """
        Initialize orchestrator
        
        Args:
            spark: SparkSession instance
            config: Configuration object
        """
        self.spark = spark
        self.config = config
        self.run_id = self._generate_run_id()
        
        logger.info(f"ETL Orchestrator initialized - Run ID: {self.run_id}")
    
    def execute_etl(self, 
                    filter_condition: Optional[str] = None,
                    max_records: int = 0) -> Dict:
        """
        Execute complete ETL pipeline
        
        Args:
            filter_condition: Optional SQL filter
            max_records: Maximum records to process
            
        Returns:
            Dictionary with execution results
        """
        start_time = datetime.now()
        
        result = {
            "run_id": self.run_id,
            "status": "RUNNING",
            "start_time": start_time.isoformat(),
            "end_time": None,
            "duration_seconds": 0,
            "records_extracted": 0,
            "records_transformed": 0,
            "records_loaded": 0,
            "records_failed": 0,
            "error_count": 0,
            "warning_count": 0,
            "errors": []
        }
        
        logger.info(f"Starting ETL execution - Run ID: {self.run_id}")
        
        try:
            # Step 1: Extract
            logger.info("=== EXTRACT PHASE ===")
            extractor = Extractor(self.spark, self.config, self.run_id)
            source_df = extractor.extract_data(filter_condition, max_records)
            result["records_extracted"] = source_df.count()
            
            if result["records_extracted"] == 0:
                logger.warning("No data extracted - stopping ETL")
                result["status"] = "NO_DATA"
                return result
            
            # Step 2: Transform
            logger.info("=== TRANSFORM PHASE ===")
            transformer = Transformer(self.spark, self.config, self.run_id)
            transformed_df = transformer.transform_data(source_df)
            result["records_transformed"] = transformed_df.count()
            
            # Step 3: Validate
            logger.info("=== VALIDATION PHASE ===")
            is_valid, validation_errors = transformer.validate_data(transformed_df)
            
            if not is_valid:
                logger.error(f"Validation failed with {len(validation_errors)} errors")
                result["status"] = "VALIDATION_FAILED"
                result["error_count"] = len(validation_errors)
                result["errors"] = validation_errors
                return result
            
            # Step 4: Quality Checks
            if self.config.enable_data_quality:
                logger.info("=== DATA QUALITY PHASE ===")
                quality_checker = DataQuality(self.spark, self.config, self.run_id)
                quality_checks = quality_checker.perform_quality_checks(transformed_df)
                
                failed_checks = [c for c in quality_checks if not c["passed"]]
                if failed_checks:
                    result["warning_count"] = len(failed_checks)
                    logger.warning(f"{len(failed_checks)} quality checks failed")
            
            # Step 5: Load
            logger.info("=== LOAD PHASE ===")
            loader = Loader(self.spark, self.config, self.run_id)
            load_result = loader.load_data(transformed_df, mode=self.config.load_mode)
            
            result["records_loaded"] = load_result["success_count"]
            result["records_failed"] = load_result["error_count"]
            result["error_count"] += load_result["error_count"]
            
            if load_result["errors"]:
                result["errors"].extend(load_result["errors"])
            
            # Determine final status
            if result["error_count"] == 0:
                result["status"] = "SUCCESS"
            elif result["records_loaded"] > 0:
                result["status"] = "PARTIAL_SUCCESS"
            else:
                result["status"] = "FAILED"
            
        except Exception as e:
            logger.error(f"ETL execution failed: {str(e)}", exc_info=True)
            result["status"] = "ERROR"
            result["errors"].append(str(e))
        
        finally:
            end_time = datetime.now()
            result["end_time"] = end_time.isoformat()
            result["duration_seconds"] = (end_time - start_time).total_seconds()
            
            # Log to run log table
            self._log_run_results(result)
            
            logger.info(f"ETL execution completed - Status: {result['status']}")
            logger.info(f"Records: Extracted={result['records_extracted']}, "
                       f"Transformed={result['records_transformed']}, "
                       f"Loaded={result['records_loaded']}, "
                       f"Failed={result['records_failed']}")
        
        return result
    
    def _generate_run_id(self) -> str:
        """Generate unique run ID"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"RUN_{timestamp}"
    
    def _log_run_results(self, result: Dict):
        """Log execution results to database"""
        try:
            log_data = [(
                result["run_id"],
                result["status"],
                result["start_time"],
                result["end_time"],
                result["duration_seconds"],
                result["records_extracted"],
                result["records_transformed"],
                result["records_loaded"],
                result["records_failed"],
                result["error_count"]
            )]
            
            log_df = self.spark.createDataFrame(
                log_data,
                ["run_id", "status", "start_time", "end_time", "duration", 
                 "records_extracted", "records_transformed", "records_loaded", 
                 "records_failed", "error_count"]
            )
            
            log_df.write \
                .format("jdbc") \
                .option("url", self.config.jdbc_url) \
                .option("dbtable", self.config.run_log_table) \
                .option("user", self.config.db_user) \
                .option("password", self.config.db_password) \
                .option("driver", self.config.jdbc_driver) \
                .mode("append") \
                .save()
            
            logger.info("Run results logged to database")
            
        except Exception as e:
            logger.warning(f"Failed to log run results: {str(e)}")


===FILE: src/data_quality.py===
"""
Data Quality module for ETL pipeline
Performs data quality checks and profiling
"""
from typing import List, Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, countDistinct, avg, stddev, min as spark_min, max as spark_max
import logging
from config import Config

logger = logging.getLogger(__name__)


class DataQuality:
    """Data quality checker with multiple validation rules"""
    
    def __init__(self, spark: SparkSession, config: Config, run_id: str):
        """
        Initialize data quality checker
        
        Args:
            spark: SparkSession instance
            config: Configuration object
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        
        logger.info(f"DataQuality initialized - Run ID: {run_id}")
    
    def perform_quality_checks(self, df: DataFrame) -> List[Dict]:
        """
        Perform all quality checks
        
        Args:
            df: DataFrame to check
            
        Returns:
            List of quality check results
        """
        logger.info("Starting data quality checks")
        
        checks = []
        
        # Run all checks
        checks.append(self._check_completeness(df))
        checks.append(self._check_uniqueness(df))
        checks.append(self._check_validity(df))
        checks.append(self._check_consistency(df))
        
        # Count results
        passed = sum(1 for c in checks if c["passed"])
        failed = len(checks) - passed
        
        logger.info(f"Quality checks complete: {passed} passed, {failed} failed")
        
        return checks
    
    def _check_completeness(self, df: DataFrame) -> Dict:
        """Check for null/empty values in critical fields"""
        check = {
            "check_name": "Completeness Check",
            "check_type": "COMPLETENESS",
            "passed": False,
            "failed_count": 0,
            "message": ""
        }
        
        null_count = df.filter(
            col("id").isNull() | 
            col("name").isNull() | 
            col("value").isNull()
        ).count()
        
        check["failed_count"] = null_count
        check["passed"] = null_count == 0
        check["message"] = (
            "All required fields are complete" if null_count == 0
            else f"{null_count} records with incomplete data"
        )
        
        return check
    
    def _check_uniqueness(self, df: DataFrame) -> Dict:
        """Check for duplicate IDs"""
        check = {
            "check_name": "Uniqueness Check",
            "check_type": "UNIQUENESS",
            "passed": False,
            "failed_count": 0,
            "message": ""
        }
        
        total_count = df.count()
        unique_count = df.select("id").distinct().count()
        duplicate_count = total_count - unique_count
        
        check["failed_count"] = duplicate_count
        check["passed"] = duplicate_count == 0
        check["message"] =