===FILE: src/extract.py===
"""
ETL Data Extraction Module
Extracts data from various sources with incremental and full load support.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from datetime import datetime
from typing import Optional, Dict, Any
import logging


class ETLExtractor:
    """Handles data extraction from various sources."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
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
        """Define the source data schema."""
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
        Extract data based on source type.
        
        Args:
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            filter_condition: Optional filter condition
            max_records: Maximum records to extract (0 for unlimited)
            
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
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract data from database source."""
        jdbc_config = self.config['source']['jdbc']
        
        query = f"(SELECT * FROM {jdbc_config['table']}"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        query += " LIMIT 10000) AS source_data"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", query) \
            .option("user", jdbc_config.get('user', '')) \
            .option("password", jdbc_config.get('password', '')) \
            .option("driver", jdbc_config.get('driver', 'org.postgresql.Driver')) \
            .load()
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract data from staging area."""
        staging_path = self.config['source']['staging_path']
        
        df = self.spark.read \
            .format(self.config['source'].get('staging_format', 'parquet')) \
            .option("header", "true") \
            .load(f"{staging_path}/run_id={self.run_id}")
        
        return df.filter("status = 'READY'")
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last run."""
        jdbc_config = self.config['source']['jdbc']
        
        # Get last successful run timestamp
        last_run_query = """
            (SELECT MAX(end_time) as last_run 
             FROM etl_run_log 
             WHERE status = 'SUCCESS') AS last_run_time
        """
        
        last_run_df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", last_run_query) \
            .option("user", jdbc_config.get('user', '')) \
            .option("password", jdbc_config.get('password', '')) \
            .load()
        
        last_run_time = last_run_df.first()['last_run']
        
        if last_run_time:
            incremental_query = f"""
                (SELECT * FROM {jdbc_config['table']} 
                 WHERE changed_at > '{last_run_time}') AS incremental_data
            """
        else:
            # First run - extract all data
            incremental_query = f"(SELECT * FROM {jdbc_config['table']}) AS incremental_data"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", incremental_query) \
            .option("user", jdbc_config.get('user', '')) \
            .option("password", jdbc_config.get('password', '')) \
            .load()
        
        return df


===FILE: src/transform.py===
"""
ETL Data Transformation Module
Applies business rules, calculations, and data enrichment.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, current_timestamp,
    current_user, concat_ws, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import Dict, Any, Tuple, List
import logging


class ETLTransformer:
    """Handles data transformation and business rule application."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the transformer.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
    
    def get_transformed_schema(self) -> StructType:
        """Define the transformed data schema."""
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
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply all transformations to source data.
        
        Args:
            source_df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        # Basic transformations
        df = self._apply_basic_transformations(source_df)
        
        # Calculate derived values
        df = self._calculate_derived_values(df)
        
        # Calculate priority
        df = self._calculate_priority(df)
        
        # Apply business rules
        df = self._apply_business_rules(df)
        
        # Enrich data
        df = self._enrich_data(df)
        
        # Add ETL metadata
        df = self._add_etl_metadata(df)
        
        record_count = df.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return df
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic data cleaning transformations."""
        return df.select(
            col("id"),
            upper(trim(regexp_replace(col("name"), "\\s+", " "))).alias("name"),
            col("value"),
            col("status"),
            coalesce(col("category"), lit("UNCATEGORIZED")).alias("category")
        )
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """Calculate transformed values based on business logic."""
        # Get multipliers from config
        premium_mult = float(self.config['transformation']['premium_multiplier'])
        standard_mult = float(self.config['transformation']['standard_multiplier'])
        
        return df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("value") * premium_mult)
            .when(col("category") == "VIP", col("value") * 2.0)
            .when(col("category") == "STANDARD", col("value") * standard_mult)
            .otherwise(col("value"))
        )
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """Calculate priority based on value and category."""
        return df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, lit(1))
            .when(col("transformed_value") >= 750, lit(2))
            .when(col("transformed_value") >= 500, lit(3))
            .when(col("transformed_value") >= 300, lit(4))
            .otherwise(lit(5))
        )
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to the data."""
        # Rule 1: Set status based on transformed value
        df = df.withColumn(
            "status",
            when(col("value").isNull(), lit("INVALID"))
            .when(col("transformed_value") >= 750, lit("HIGH_VALUE"))
            .when(col("transformed_value") >= 300, lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )
        
        # Rule 2: Priority override for high value items
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, lit(1))
            .otherwise(col("priority"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional information."""
        # Apply category-specific enrichment
        enable_enrichment = self.config['transformation'].get('enable_enrichment', True)
        
        if enable_enrichment:
            df = df.withColumn(
                "transformed_value",
                when(col("category") == "PREMIUM", col("transformed_value") * 1.2)
                .otherwise(col("transformed_value"))
            )
        
        return df
    
    def _add_etl_metadata(self, df: DataFrame) -> DataFrame:
        """Add ETL processing metadata."""
        return df.withColumn("etl_run_id", lit(self.run_id)) \
                 .withColumn("processed_at", current_timestamp()) \
                 .withColumn("processed_by", lit(current_user()))
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        errors = []
        
        # Check for required fields
        null_ids = df.filter(col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null ID")
        
        null_names = df.filter(col("name").isNull()).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null name")
        
        # Check for valid values
        negative_values = df.filter(
            (col("value") < 0) | (col("transformed_value") < 0)
        ).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative values")
        
        # Check priority range
        invalid_priority = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Data validation passed")
        else:
            self.logger.warning(f"Data validation failed: {errors}")
        
        return is_valid, errors


===FILE: src/load.py===
"""
ETL Data Loading Module
Loads transformed data into target systems with batch processing and reconciliation.
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any, Tuple
import logging


class ETLLoader:
    """Handles data loading to target systems."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the loader.
        
        Args:
            spark: SparkSession instance
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
        mode: str = "upsert"
    ) -> Dict[str, Any]:
        """
        Load data to target system.
        
        Args:
            df: DataFrame to load
            mode: Load mode (insert, update, upsert)
            
        Returns:
            Dictionary with load results
        """
        batch_size = self.config['load']['batch_size']
        
        self.logger.info(
            f"Starting load - Mode: {mode}, Batch size: {batch_size}, Run ID: {self.run_id}"
        )
        
        total_count = df.count()
        success_count = 0
        error_count = 0
        errors = []
        
        try:
            if mode.lower() == "insert":
                success_count = self._insert_data(df)
            elif mode.lower() == "update":
                success_count = self._update_data(df)
            elif mode.lower() == "upsert":
                success_count = self._upsert_data(df)
            else:
                self.logger.warning(f"Unknown mode: {mode}, defaulting to insert")
                success_count = self._insert_data(df)
            
            error_count = total_count - success_count
            
            # Reconcile data if enabled
            if self.config['load'].get('enable_reconciliation', True):
                reconciled = self._reconcile_data(df)
                if not reconciled:
                    self.logger.warning("Data reconciliation failed")
                    errors.append("Reconciliation check failed")
            
            self.logger.info(
                f"Load complete - Success: {success_count}, Errors: {error_count}"
            )
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            error_count = total_count
            errors.append(str(e))
        
        return {
            "success_count": success_count,
            "error_count": error_count,
            "total_count": total_count,
            "errors": errors
        }
    
    def _insert_data(self, df: DataFrame) -> int:
        """Insert new records."""
        jdbc_config = self.config['target']['jdbc']
        
        df.write \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", jdbc_config['table']) \
            .option("user", jdbc_config.get('user', '')) \
            .option("password", jdbc_config.get('password', '')) \
            .option("driver", jdbc_config.get('driver', 'org.postgresql.Driver')) \
            .mode("append") \
            .save()
        
        return df.count()
    
    def _update_data(self, df: DataFrame) -> int:
        """Update existing records."""
        # For update, we would typically use JDBC with SQL UPDATE statements
        # or use Delta Lake merge functionality
        jdbc_config = self.config['target']['jdbc']
        
        # Create temporary view
        df.createOrReplaceTempView("updates")
        
        # Execute update via JDBC
        # This is a simplified version - production would use proper update logic
        df.write \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", jdbc_config['table']) \
            .option("user", jdbc_config.get('user', '')) \
            .option("password", jdbc_config.get('password', '')) \
            .mode("overwrite") \
            .save()
        
        return df.count()
    
    def _upsert_data(self, df: DataFrame) -> int:
        """Upsert (insert or update) records."""
        target_format = self.config['target'].get('format', 'jdbc')
        
        if target_format == 'delta':
            return self._upsert_delta(df)
        else:
            return self._upsert_jdbc(df)
    
    def _upsert_delta(self, df: DataFrame) -> int:
        """Upsert using Delta Lake merge."""
        from delta.tables import DeltaTable
        
        target_path = self.config['target']['delta_path']
        
        # Check if target table exists
        if DeltaTable.isDeltaTable(self.spark, target_path):
            delta_table = DeltaTable.forPath(self.spark, target_path)
            
            delta_table.alias("target").merge(
                df.alias("source"),
                "target.id = source.id"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
        else:
            # First time - just write
            df.write.format("delta").mode("overwrite").save(target_path)
        
        return df.count()
    
    def _upsert_jdbc(self, df: DataFrame) -> int:
        """Upsert using JDBC (simplified version)."""
        # Try update first, then insert
        try:
            return self._update_data(df)
        except Exception:
            return self._insert_data(df)
    
    def _reconcile_data(self, df: DataFrame) -> bool:
        """
        Reconcile loaded data with source.
        
        Args:
            df: Source DataFrame
            
        Returns:
            True if reconciliation passes
        """
        try:
            jdbc_config = self.config['target']['jdbc']
            
            # Read back loaded data
            loaded_df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_config['url']) \
                .option("dbtable", jdbc_config['table']) \
                .option("user", jdbc_config.get('user', '')) \
                .option("password", jdbc_config.get('password', '')) \
                .load() \
                .filter(f"etl_run_id = '{self.run_id}'")
            
            source_count = df.count()
            loaded_count = loaded_df.count()
            
            self.logger.info(
                f"Reconciliation - Source: {source_count}, Loaded: {loaded_count}"
            )
            
            return source_count == loaded_count
            
        except Exception as e:
            self.logger.error(f"Reconciliation failed: {str(e)}")
            return False


===FILE: src/orchestrator.py===
"""
ETL Orchestration Module
Coordinates the end-to-end ETL process execution.
"""

from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict, Any, Optional
import logging
import uuid

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.quality import DataQualityChecker


class ETLOrchestrator:
    """Orchestrates the complete ETL pipeline."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_type: str = "MANUAL"):
        """
        Initialize the orchestrator.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_type: Type of run (MANUAL, SCHEDULED, etc.)
        """
        self.spark = spark
        self.config = config
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        self.logger = logging.getLogger(__name__)
    
    def execute_etl(
        self,
        source_type: str = "DATABASE",
        target_type: str = "DATABASE",
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> Dict[str, Any]:
        """
        Execute the complete ETL process.
        
        Args:
            source_type: Source system type
            target_type: Target system type
            filter_condition: Optional filter
            max_records: Maximum records to process
            
        Returns:
            Dictionary with execution results
        """
        result = {
            "run_id": self.run_id,
            "status": "RUNNING",
            "start_time": datetime.now(),
            "records_extracted": 0,
            "records_transformed": 0,
            "records_loaded": 0,
            "records_failed": 0,
            "error_count": 0,
            "warning_count": 0
        }
        
        self.logger.info(f"ETL execution started - Run ID: {self.run_id}")
        
        try:
            # Step 1: Extract
            extractor = ETLExtractor(self.spark, self.config, self.run_id)
            source_df = extractor.extract_data(source_type, filter_condition, max_records)
            result["records_extracted"] = source_df.count()
            
            if result["records_extracted"] == 0:
                self.logger.warning("No data extracted - stopping ETL process")
                result["status"] = "NO_DATA"
                return result
            
            # Step 2: Transform
            transformer = ETLTransformer(self.spark, self.config, self.run_id)
            transformed_df = transformer.transform_data(source_df)
            result["records_transformed"] = transformed_df.count()
            
            # Step 3: Validate
            is_valid, validation_errors = transformer.validate_data(transformed_df)
            if not is_valid:
                self.logger.error(f"Validation failed: {validation_errors}")
                result["status"] = "VALIDATION_FAILED"
                result["error_count"] = len(validation_errors)
                return result
            
            # Step 4: Data Quality Checks
            if self.config.get('quality', {}).get('enabled', True):
                quality_checker = DataQualityChecker(self.spark, self.config, self.run_id)
                quality_results = quality_checker.perform_quality_checks(transformed_df)
                
                failed_checks = [c for c in quality_results if not c['passed']]
                if failed_checks:
                    self.logger.warning(f"Quality checks failed: {len(failed_checks)}")
                    result["warning_count"] += len(failed_checks)
            
            # Step 5: Load
            loader = ETLLoader(self.spark, self.config, self.run_id)
            load_result = loader.load_data(transformed_df, mode="upsert")
            
            result["records_loaded"] = load_result["success_count"]
            result["records_failed"] = load_result["error_count"]
            result["error_count"] += load_result["error_count"]
            
            # Determine final status
            if load_result["error_count"] == 0:
                result["status"] = "SUCCESS"
            elif load_result["success_count"] > 0:
                result["status"] = "PARTIAL_SUCCESS"
            else:
                result["status"] = "FAILED"
            
            result["end_time"] = datetime.now()
            result["duration"] = (result["end_time"] - result["start_time"]).total_seconds()
            
            # Log run completion
            self._log_run_completion(result)
            
            self.logger.info(f"ETL execution completed - Status: {result['status']}")
            
        except Exception as e:
            self.logger.error(f"ETL execution failed: {str(e)}", exc_info=True)
            result["status"] = "ERROR"
            result["end_time"] = datetime.now()
            result["error_count"] += 1
            self._log_run_completion(result)
            raise
        
        return result
    
    def _generate_run_id(self) -> str:
        """Generate unique run ID."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"RUN_{timestamp}_{unique_id}"
    
    def _log_run_completion(self, result: Dict[str, Any]):
        """Log run completion to tracking table."""
        try:
            # Create DataFrame with run log
            run_log_df = self.spark.createDataFrame([{
                "run_id": result["run_id"],
                "run_type": self.run_type,
                "status": result["status"],
                "start_time": result["start_time"],
                "end_time": result.get("end_time"),
                "duration": result.get("duration", 0),
                "records_extracted": result["records_extracted"],
                "records_transformed": result["records_transformed"],
                "records_loaded": result["records_loaded"],
                "records_failed": result["records_failed"],
                "error_count": result["error_count"],
                "warning_count": result["warning_count"]
            }])
            
            # Write to run log table
            jdbc_config = self.config['target']['jdbc']
            run_log_df.write \
                .format("jdbc") \
                .option("url", jdbc_config['url']) \
                .option("dbtable", "etl_run_log") \
                .option("user", jdbc_config.get('user', '')) \
                .option("password", jdbc_config.get('password', '')) \
                .mode("append") \
                .save()
                
        except Exception as e:
            self.logger.error(f"Failed to log run completion: {str(e)}")


===FILE: src/quality.py===
"""
Data Quality Module
Performs comprehensive data quality checks.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, countDistinct, avg, stddev, min as spark_min, max as spark_max
from typing import Dict, Any, List
import logging


class DataQualityChecker:
    """Performs data quality validation checks."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the quality checker.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
    
    def perform_quality_checks(self, df: DataFrame) -> List[Dict[str, Any]]:
        """
        Perform all quality checks.
        
        Args:
            df: DataFrame to check
            
        Returns:
            List of quality check results
        """
        self.logger.info("Starting data quality checks")
        
        checks = []
        checks.append(self._check_completeness(df))
        checks.append(self._check_uniqueness(df))
        checks.append(self._check_validity(df))
        checks.append(self._check_consistency(df))
        
        passed = sum(1 for c in checks if c['passed'])
        failed = len(checks) - passed
        
        self.logger.info(f"Quality checks complete: {passed} passed, {failed} failed")
        
        return checks
    
    def _check_completeness(self, df: DataFrame) -> Dict[str, Any]:
        """Check for completeness of required fields."""
        null_count = df.filter(
            col("id").isNull() | 
            col("name").isNull() | 
            col("value").isNull()
        ).count()
        
        return {
            "check_name": "Completeness Check",
            "check_type": "COMPLETENESS",
            "passed": null_count == 0,
            "failed_count": null_count,
            "message": "All required fields complete" if null_count == 0 
                      else f"{null_count} records with incomplete data"
        }
    
    def _check_uniqueness(self, df: DataFrame) -> Dict[str, Any]:
        """Check for duplicate IDs."""
        total_count = df.count()
        unique_count = df.select("id").distinct().count()
        duplicate_count = total_count - unique_count
        
        return {
            "check_name": "Uniqueness Check",
            "check_type": "UNIQUENESS",
            "passed": duplicate_count == 0,
            "failed_count": duplicate_count,
            "message": "All IDs are unique" if duplicate_count == 0 
                      else f"{duplicate_count} duplicate IDs found"
        }
    
    def _check_validity(self, df: DataFrame) -> Dict[str, Any]:
        """Check for valid values."""
        invalid_count = df.filter(
            (col("value") < 0) | 
            (col("transformed_value") < 0) |
            (col("priority") < 1) | 
            (col("priority") > 5) |
            col("category").isNull()
        ).count()
        
        return {
            "check_name": "Validity Check",
            "check_type": "VALIDITY",
            "passed": invalid_count == 0,
            "failed_count": invalid_count,
            "message": "All values are valid" if invalid_count == 0 
                      else f"{invalid_count} records with invalid values"
        }
    
    def _check_consistency(self, df: DataFrame) -> Dict[str, Any]:
        """Check for data consistency."""
        inconsistent_count = df.filter(
            col("transformed_value") < col("value")
        ).count()
        
        return {
            "check_name": "Consistency Check",
            "check_type": "CONSISTENCY",
            "passed": inconsistent_count == 0,
            "failed_count": inconsistent_count,
            "message": "Data is consistent" if inconsistent_count == 0 
                      else f"{inconsistent_count} records with inconsistent values"
        }
    
    def profile_data(self