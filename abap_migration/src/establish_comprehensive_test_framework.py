===FILE: src/extract.py===
"""
ETL Data Extraction Module
Extracts data from various sources with filtering and incremental load support.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from pyspark.sql import functions as F
from datetime import datetime
from typing import Optional, Dict
import logging


class ETLExtractor:
    """Handles data extraction from various sources."""
    
    def __init__(self, spark: SparkSession, config: Dict, run_id: str):
        """
        Initialize the extractor.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def get_source_schema(self) -> StructType:
        """Define schema for source data."""
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
        source_type: str = "database",
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data from specified source.
        
        Args:
            source_type: Type of source (database, staging, incremental)
            filter_condition: Optional filter condition
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.info(f"Starting extraction - Source: {source_type}, Run ID: {self.run_id}")
        
        try:
            if source_type.lower() == "database":
                df = self._extract_from_database(filter_condition)
            elif source_type.lower() == "staging":
                df = self._extract_from_staging()
            elif source_type.lower() == "incremental":
                df = self._extract_incremental()
            else:
                df = self._extract_from_database(filter_condition)
            
            # Apply max records limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract data from database source."""
        source_path = self.config["source"]["database"]["path"]
        source_format = self.config["source"]["database"]["format"]
        
        self.logger.info(f"Reading from {source_format} at {source_path}")
        
        df = (self.spark.read
              .format(source_format)
              .schema(self.get_source_schema())
              .load(source_path))
        
        # Apply filter if provided
        if filter_condition:
            df = df.filter(filter_condition)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract data from staging area."""
        staging_path = self.config["source"]["staging"]["path"]
        
        self.logger.info(f"Reading from staging at {staging_path}")
        
        df = (self.spark.read
              .format("parquet")
              .schema(self.get_source_schema())
              .load(staging_path)
              .filter(F.col("run_id") == self.run_id)
              .filter(F.col("status") == "READY"))
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last run."""
        source_path = self.config["source"]["database"]["path"]
        
        # Get last successful run time
        last_run_time = self._get_last_run_time()
        
        self.logger.info(f"Extracting incremental data since {last_run_time}")
        
        df = (self.spark.read
              .format(self.config["source"]["database"]["format"])
              .schema(self.get_source_schema())
              .load(source_path))
        
        if last_run_time:
            df = df.filter(F.col("changed_at") > last_run_time)
        
        return df
    
    def _get_last_run_time(self) -> Optional[datetime]:
        """Get timestamp of last successful run."""
        try:
            log_path = self.config["metadata"]["run_log_path"]
            
            if self.spark._jsc.hadoopConfiguration().get("fs.defaultFS"):
                log_df = (self.spark.read
                         .format("parquet")
                         .load(log_path)
                         .filter(F.col("status") == "SUCCESS")
                         .orderBy(F.col("end_time").desc())
                         .limit(1))
                
                if log_df.count() > 0:
                    return log_df.select("end_time").collect()[0][0]
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Could not get last run time: {str(e)}")
            return None


===FILE: src/transform.py===
"""
ETL Data Transformation Module
Applies business rules, enrichment, and validation to extracted data.
"""

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
from typing import Dict, List, Tuple
import logging


class ETLTransformer:
    """Handles data transformation and business rule application."""
    
    def __init__(self, config: Dict, run_id: str):
        """
        Initialize the transformer.
        
        Args:
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def get_transformed_schema(self) -> StructType:
        """Define schema for transformed data."""
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
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply all transformations to source data.
        
        Args:
            source_df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting data transformation")
        
        # Basic transformations
        df = self._apply_basic_transformations(source_df)
        
        # Calculate derived values
        df = self._calculate_derived_values(df)
        
        # Apply business rules
        df = self._apply_business_rules(df)
        
        # Calculate priority
        df = self._calculate_priority(df)
        
        # Apply category-specific rules
        df = self._apply_category_rules(df)
        
        # Enrich data
        df = self._enrich_data(df)
        
        record_count = df.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return df
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic field transformations."""
        processed_time = datetime.now()
        user = self.config["metadata"]["processed_by"]
        
        return (df
                .withColumn("name", F.upper(F.trim(F.col("name"))))
                .withColumn("status", F.lit("TRANSFORMED"))
                .withColumn("etl_run_id", F.lit(self.run_id))
                .withColumn("processed_at", F.lit(processed_time))
                .withColumn("processed_by", F.lit(user)))
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """Calculate transformed values based on category and value."""
        rules = self.config["transformation"]["value_rules"]
        
        # Apply multipliers based on category
        df = df.withColumn(
            "transformed_value",
            F.when(F.col("category") == "PREMIUM", F.col("value") * rules["premium_multiplier"])
            .when(F.col("category") == "STANDARD", F.col("value") * rules["standard_multiplier"])
            .when(F.col("category") == "VIP", F.col("value") * rules["vip_multiplier"])
            .otherwise(F.col("value") * rules["default_multiplier"])
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to determine status."""
        thresholds = self.config["transformation"]["thresholds"]
        
        df = df.withColumn(
            "status",
            F.when(F.col("value").isNull(), F.lit("INVALID"))
            .when(F.col("transformed_value") >= thresholds["high_value"], F.lit("HIGH_VALUE"))
            .when(F.col("transformed_value") >= thresholds["medium_value"], F.lit("MEDIUM_VALUE"))
            .otherwise(F.lit("LOW_VALUE"))
        )
        
        # Fill null categories
        df = df.withColumn(
            "category",
            F.when(F.col("category").isNull(), F.lit("UNCATEGORIZED"))
            .otherwise(F.col("category"))
        )
        
        # Clean name field - remove extra spaces
        df = df.withColumn("name", F.regexp_replace(F.col("name"), "\\s+", " "))
        
        return df
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """Calculate priority based on value and category."""
        priority_rules = self.config["transformation"]["priority_rules"]
        
        df = df.withColumn(
            "priority",
            F.when(F.col("transformed_value") >= priority_rules["critical_threshold"], F.lit(1))
            .when(F.col("transformed_value") >= priority_rules["high_threshold"], F.lit(2))
            .when(F.col("transformed_value") >= priority_rules["medium_threshold"], F.lit(3))
            .when(F.col("transformed_value") >= priority_rules["low_threshold"], F.lit(4))
            .otherwise(F.lit(5))
        )
        
        # Override priority for high value items
        df = df.withColumn(
            "priority",
            F.when(F.col("transformed_value") >= 1000, F.lit(1))
            .otherwise(F.col("priority"))
        )
        
        return df
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """Apply category-specific transformation rules."""
        # Premium category gets additional boost
        df = df.withColumn(
            "transformed_value",
            F.when(F.col("category") == "PREMIUM", 
                   F.col("transformed_value") * 1.2)
            .otherwise(F.col("transformed_value"))
        )
        
        # VIP category always gets priority 1
        df = df.withColumn(
            "priority",
            F.when(F.col("category") == "VIP", F.lit(1))
            .otherwise(F.col("priority"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional calculated fields."""
        # Add value rank within category
        window_spec = Window.partitionBy("category").orderBy(F.col("transformed_value").desc())
        df = df.withColumn("value_rank", F.row_number().over(window_spec))
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        self.logger.info("Starting data validation")
        errors = []
        
        # Check for required fields
        null_ids = df.filter(F.col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null ID")
        
        null_names = df.filter(F.col("name").isNull()).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null name")
        
        # Check for negative values
        negative_values = df.filter(F.col("value") < 0).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative values")
        
        # Check priority range
        invalid_priority = df.filter((F.col("priority") < 1) | (F.col("priority") > 5)).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        # Check for duplicate IDs
        duplicate_ids = df.groupBy("id").count().filter(F.col("count") > 1).count()
        if duplicate_ids > 0:
            errors.append(f"{duplicate_ids} duplicate IDs found")
        
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
ETL Data Loading Module
Loads transformed data into target systems with batch processing and reconciliation.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, NamedTuple
import logging


class LoadResult(NamedTuple):
    """Result of load operation."""
    success_count: int
    error_count: int
    total_count: int
    errors: list


class ETLLoader:
    """Handles data loading to target systems."""
    
    def __init__(self, config: Dict, run_id: str):
        """
        Initialize the loader.
        
        Args:
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.config = config
        self.run_id = run_id
        self.batch_size = config["loading"]["batch_size"]
        self.logger = logging.getLogger(__name__)
        
    def load_data(
        self,
        df: DataFrame,
        mode: str = "upsert"
    ) -> LoadResult:
        """
        Load data to target system.
        
        Args:
            df: DataFrame to load
            mode: Load mode (insert, update, upsert)
            
        Returns:
            LoadResult with operation statistics
        """
        self.logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        total_count = df.count()
        errors = []
        
        try:
            # Load to target
            success = self._load_to_target(df, mode)
            
            if success:
                # Perform reconciliation
                if self.config["loading"]["enable_reconciliation"]:
                    reconciled = self._reconcile_data(df)
                    if not reconciled:
                        self.logger.warning("Data reconciliation failed")
                        errors.append("Reconciliation check failed")
                
                success_count = total_count
                error_count = 0
            else:
                success_count = 0
                error_count = total_count
                errors.append("Load operation failed")
            
            result = LoadResult(
                success_count=success_count,
                error_count=error_count,
                total_count=total_count,
                errors=errors
            )
            
            self.logger.info(
                f"Load complete - Success: {success_count}, "
                f"Errors: {error_count}"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            return LoadResult(
                success_count=0,
                error_count=total_count,
                total_count=total_count,
                errors=[str(e)]
            )
    
    def _load_to_target(self, df: DataFrame, mode: str) -> bool:
        """Load data to target system."""
        target_path = self.config["target"]["path"]
        target_format = self.config["target"]["format"]
        
        try:
            self.logger.info(f"Writing to {target_format} at {target_path}")
            
            # Map mode to Spark write mode
            write_mode = {
                "insert": "append",
                "update": "overwrite",
                "upsert": "merge"
            }.get(mode.lower(), "append")
            
            if write_mode == "merge" and target_format in ["delta", "iceberg"]:
                # Use merge for upsert
                self._perform_merge(df, target_path, target_format)
            else:
                # Standard write
                (df.write
                 .format(target_format)
                 .mode(write_mode if write_mode != "merge" else "append")
                 .save(target_path))
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load to target: {str(e)}")
            return False
    
    def _perform_merge(self, df: DataFrame, target_path: str, target_format: str):
        """Perform merge/upsert operation."""
        # For Delta Lake
        if target_format == "delta":
            from delta.tables import DeltaTable
            
            # Check if table exists
            if DeltaTable.isDeltaTable(df.sparkSession, target_path):
                delta_table = DeltaTable.forPath(df.sparkSession, target_path)
                
                # Perform merge
                (delta_table.alias("target")
                 .merge(
                     df.alias("source"),
                     "target.id = source.id"
                 )
                 .whenMatchedUpdateAll()
                 .whenNotMatchedInsertAll()
                 .execute())
            else:
                # First load - just write
                df.write.format("delta").save(target_path)
        else:
            # Fallback to append
            df.write.format(target_format).mode("append").save(target_path)
    
    def _reconcile_data(self, df: DataFrame) -> bool:
        """
        Reconcile loaded data with source.
        
        Args:
            df: Source DataFrame
            
        Returns:
            True if reconciliation passes
        """
        self.logger.info("Starting data reconciliation")
        
        try:
            target_path = self.config["target"]["path"]
            target_format = self.config["target"]["format"]
            
            # Read back loaded data
            loaded_df = (df.sparkSession.read
                        .format(target_format)
                        .load(target_path)
                        .filter(F.col("etl_run_id") == self.run_id))
            
            source_count = df.count()
            loaded_count = loaded_df.count()
            
            self.logger.info(
                f"Reconciliation - Source: {source_count}, "
                f"Loaded: {loaded_count}"
            )
            
            # Check record counts match
            if source_count != loaded_count:
                self.logger.error(
                    f"Record count mismatch: {source_count} vs {loaded_count}"
                )
                return False
            
            # Check value sums match
            source_sum = df.agg(F.sum("transformed_value")).collect()[0][0]
            loaded_sum = loaded_df.agg(F.sum("transformed_value")).collect()[0][0]
            
            if abs(float(source_sum) - float(loaded_sum)) > 0.01:
                self.logger.error(
                    f"Value sum mismatch: {source_sum} vs {loaded_sum}"
                )
                return False
            
            self.logger.info("Reconciliation passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Reconciliation failed: {str(e)}")
            return False


===FILE: src/orchestrator.py===
"""
ETL Orchestration Module
Coordinates the complete ETL pipeline execution.
"""

from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict, NamedTuple
import logging

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.data_quality import DataQualityChecker


class ETLResult(NamedTuple):
    """Result of ETL execution."""
    run_id: str
    status: str
    start_time: datetime
    end_time: datetime
    duration: float
    records_extracted: int
    records_transformed: int
    records_loaded: int
    records_failed: int
    error_count: int
    warning_count: int


class ETLOrchestrator:
    """Orchestrates the complete ETL pipeline."""
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize the orchestrator.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
        
    def execute_etl(
        self,
        source_type: str = "database",
        filter_condition: str = None,
        max_records: int = 0
    ) -> ETLResult:
        """
        Execute complete ETL pipeline.
        
        Args:
            source_type: Source type (database, staging, incremental)
            filter_condition: Optional filter condition
            max_records: Maximum records to process
            
        Returns:
            ETLResult with execution statistics
        """
        run_id = self._generate_run_id()
        start_time = datetime.now()
        
        self.logger.info(f"ETL execution started - Run ID: {run_id}")
        
        try:
            # Step 1: Extract
            extractor = ETLExtractor(self.spark, self.config, run_id)
            source_df = extractor.extract_data(
                source_type=source_type,
                filter_condition=filter_condition,
                max_records=max_records
            )
            
            records_extracted = source_df.count()
            
            if records_extracted == 0:
                self.logger.warning("No data extracted - ETL process stopping")
                return self._create_result(
                    run_id, "NO_DATA", start_time, datetime.now(), 0, 0, 0, 0
                )
            
            # Step 2: Transform
            transformer = ETLTransformer(self.config, run_id)
            transformed_df = transformer.transform_data(source_df)
            
            # Step 3: Validate
            is_valid, validation_errors = transformer.validate_data(transformed_df)
            
            if not is_valid:
                self.logger.error(f"Validation failed with {len(validation_errors)} errors")
                return self._create_result(
                    run_id, "VALIDATION_FAILED", start_time, datetime.now(),
                    records_extracted, 0, 0, len(validation_errors)
                )
            
            records_transformed = transformed_df.count()
            
            # Step 4: Data Quality Checks
            if self.config["data_quality"]["enabled"]:
                quality_checker = DataQualityChecker(self.config, run_id)
                quality_results = quality_checker.perform_quality_checks(transformed_df)
                
                failed_checks = [r for r in quality_results if not r.passed]
                if failed_checks and not self.config["data_quality"]["allow_failures"]:
                    self.logger.error(f"Data quality checks failed: {len(failed_checks)} failures")
                    return self._create_result(
                        run_id, "QUALITY_FAILED", start_time, datetime.now(),
                        records_extracted, records_transformed, 0, len(failed_checks)
                    )
            
            # Step 5: Load
            loader = ETLLoader(self.config, run_id)
            load_result = loader.load_data(
                transformed_df,
                mode=self.config["loading"]["mode"]
            )
            
            # Determine final status
            if load_result.error_count == 0:
                status = "SUCCESS"
            elif load_result.success_count > 0:
                status = "PARTIAL_SUCCESS"
            else:
                status = "FAILED"
            
            end_time = datetime.now()
            
            # Log completion
            self._log_run_completion(
                run_id, status, start_time, end_time,
                records_extracted, records_transformed,
                load_result.success_count, load_result.error_count
            )
            
            result = self._create_result(
                run_id, status, start_time, end_time,
                records_extracted, records_transformed,
                load_result.success_count, load_result.error_count
            )
            
            self.logger.info(f"ETL execution completed - Status: {status}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"ETL execution failed: {str(e)}")
            end_time = datetime.now()
            
            self._log_run_completion(
                run_id, "ERROR", start_time, end_time, 0, 0, 0, 1
            )
            
            return self._create_result(
                run_id, "ERROR", start_time, end_time, 0, 0, 0, 1
            )
    
    def _generate_run_id(self) -> str:
        """Generate unique run ID."""
        return f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def _create_result(
        self,
        run_id: str,
        status: str,
        start_time: datetime,
        end_time: datetime,
        extracted: int,
        transformed: int,
        loaded: int,
        errors: int
    ) -> ETLResult:
        """Create ETL result object."""
        duration = (end_time - start_time).total_seconds()
        
        return ETLResult(
            run_id=run_id,
            status=status,
            start_time=start_time,
            end_time=end_time,
            duration=duration,
            records_extracted=extracted,
            records_transformed=transformed,
            records_loaded=loaded,
            records_failed=errors,
            error_count=errors,
            warning_count=0
        )
    
    def _log_run_completion(
        self,
        run_id: str,
        status: str,
        start_time: datetime,
        end_time: datetime,
        extracted: int,
        transformed: int,
        loaded: int,
        failed: int
    ):
        """Log run completion to metadata store."""
        duration = (end_time - start_time).total_seconds()
        
        log_data = [(
            run_id, status, start_time, end_time, duration,
            extracted, transformed, loaded, failed
        )]
        
        schema = ["run_id", "status", "start_time", "end_time", "duration",
                 "records_extracted", "records_transformed", "records_loaded", "records_failed"]
        
        log_df = self.spark.createDataFrame(log_data, schema)
        
        log_path = self.config["metadata"]["run_log_path"]
        
        try:
            log_df.write.mode("append").parquet(log_path)
        except Exception as e:
            self.logger.error(f"Failed to write run log: {str(e)}")


===FILE: src/data_quality.py===
"""
Data Quality Module
Performs comprehensive data quality checks and profiling.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, List, NamedTuple
import logging


class QualityCheck(NamedTuple):
    """Result of a quality check."""
    check_name: str
    check_type: str
    passed: bool
    failed_count: int
    message: str


class DataProfile(NamedTuple):
    """Data profile statistics."""
    total_records: int
    null_count: int
    duplicate_count: int
    min_value: float
    max_value: float
    avg_value: float
    std_deviation: float
    unique_categories: int


class DataQualityChecker:
    """Performs data quality checks and profiling."""
    
    def __init__(self, config: Dict, run_id: str):
        """
        Initialize the quality checker.
        
        Args:
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def perform_quality_checks(self, df: DataFrame) -> List[QualityCheck]:
        """
        Perform all quality checks.
        
        Args:
            df: DataFrame to check
            
        Returns:
            List of quality check results
        """
        self.logger.info("Starting data quality checks")
        
        checks = [
            self._check_completeness(df),
            self._check_uniqueness(df),
            self._check_validity(df),
            self._check_consistency(df)
        ]
        
        passed = sum(1 for c in checks if c.passed)
        failed = len(checks) - passed
        
        self.logger.info(f"Quality checks complete: {passed} passed, {failed} failed")
        
        return checks
    
    def _check_completeness(self, df: DataFrame) -> QualityCheck:
        """Check for null/empty values in critical fields."""
        null_count = (df.filter(
            F.col("id").isNull() |
            F.col("name").isNull() |
            F.col("value").isNull()
        ).count())
        
        passed = null_count == 0
        message = "All required fields are complete" if passed else f"{null_count} records with incomplete data"
        
        return QualityCheck(
            check_name="Completeness Check",
            check_type="COMPLETENESS",
            passed=passed,
            failed_count=null_count,
            message=message
        )
    
    def _check_uniqueness(self, df: DataFrame) -> QualityCheck:
        """Check for duplicate