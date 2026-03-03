===FILE: src/extract.py===
"""
ETL Extractor Module
Handles data extraction from various sources with support for full, incremental, and staging modes.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional, Dict, Any
import logging


class ETLExtractor:
    """Extracts data from source systems"""
    
    SOURCE_SCHEMA = StructType([
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
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize extractor
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        self.source_type = config.get('source_type', 'database')
        
    def extract_data(self, filter_condition: Optional[str] = None, 
                     max_records: int = 0) -> DataFrame:
        """
        Extract data based on source type
        
        Args:
            filter_condition: Optional SQL filter condition
            max_records: Maximum number of records to extract (0 = no limit)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.info(f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}")
        
        try:
            if self.source_type.upper() == 'DATABASE':
                df = self._extract_from_database(filter_condition)
            elif self.source_type.upper() == 'STAGING':
                df = self._extract_from_staging()
            elif self.source_type.upper() == 'INCREMENTAL':
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
        """Extract from source database table"""
        self.logger.info("Extracting from database")
        
        jdbc_config = self.config['source']['database']
        
        # Build query
        query = f"(SELECT * FROM {jdbc_config['table']}"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        query += ") AS source_data"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", query) \
            .option("user", jdbc_config['user']) \
            .option("password", jdbc_config['password']) \
            .option("driver", jdbc_config['driver']) \
            .load()
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract from staging area"""
        self.logger.info(f"Extracting from staging - Run ID: {self.run_id}")
        
        staging_config = self.config['source']['staging']
        
        query = f"""(
            SELECT 
                id, name, value, status, category, source_system,
                created_at, created_by, changed_at, changed_by
            FROM {staging_config['table']}
            WHERE run_id = '{self.run_id}' AND status = 'READY'
        ) AS staging_data"""
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", staging_config['url']) \
            .option("dbtable", query) \
            .option("user", staging_config['user']) \
            .option("password", staging_config['password']) \
            .option("driver", staging_config['driver']) \
            .load()
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last successful run"""
        self.logger.info("Extracting incremental data")
        
        # Get last successful run timestamp
        last_run_time = self._get_last_run_time()
        
        if last_run_time:
            self.logger.info(f"Extracting changes since: {last_run_time}")
            filter_condition = f"changed_at > TIMESTAMP'{last_run_time}'"
        else:
            self.logger.warning("No previous successful run found, performing full extraction")
            filter_condition = None
        
        return self._extract_from_database(filter_condition)
    
    def _get_last_run_time(self) -> Optional[str]:
        """Get timestamp of last successful ETL run"""
        jdbc_config = self.config['source']['database']
        
        query = """(
            SELECT MAX(end_time) as last_run_time
            FROM etl_run_log
            WHERE status = 'SUCCESS'
        ) AS last_run"""
        
        try:
            df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_config['url']) \
                .option("dbtable", query) \
                .option("user", jdbc_config['user']) \
                .option("password", jdbc_config['password']) \
                .option("driver", jdbc_config['driver']) \
                .load()
            
            result = df.first()
            return result['last_run_time'] if result and result['last_run_time'] else None
            
        except Exception as e:
            self.logger.warning(f"Could not retrieve last run time: {str(e)}")
            return None


===FILE: src/transform.py===
"""
ETL Transformer Module
Handles data transformation, business rules, enrichment and validation.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, 
    current_timestamp, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import Dict, Any, List, Tuple
import logging


class ETLTransformer:
    """Transforms extracted data according to business rules"""
    
    TRANSFORMED_SCHEMA = StructType([
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
    
    def __init__(self, spark, config: Dict[str, Any], run_id: str):
        """
        Initialize transformer
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply all transformations to source data
        
        Args:
            source_df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        # Basic transformations
        df = source_df \
            .withColumn("name", upper(trim(col("name")))) \
            .withColumn("name", regexp_replace(col("name"), "\\s+", " ")) \
            .withColumn("transformed_value", self._calculate_derived_values()) \
            .withColumn("priority", self._calculate_priority()) \
            .withColumn("etl_run_id", lit(self.run_id)) \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("processed_by", lit(self.config.get('user', 'etl_system'))) \
            .withColumn("status", lit("TRANSFORMED"))
        
        # Apply business rules
        df = self._apply_business_rules(df)
        
        # Apply category-specific rules
        df = self._apply_category_rules(df)
        
        # Enrich data
        df = self._enrich_data(df)
        
        record_count = df.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return df
    
    def _calculate_derived_values(self):
        """Calculate transformed value based on category and value"""
        return when(col("category") == "PREMIUM", col("value") * 1.5) \
               .when(col("category") == "STANDARD", col("value") * 1.2) \
               .when(col("category") == "VIP", col("value") * 2.0) \
               .otherwise(col("value"))
    
    def _calculate_priority(self):
        """Calculate priority based on value and category"""
        return when(col("value") >= 1000, lit(1)) \
               .when(col("value") >= 750, lit(2)) \
               .when(col("value") >= 300, lit(3)) \
               .when(col("value") >= 100, lit(4)) \
               .otherwise(lit(5))
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to transformed data"""
        self.logger.info("Applying business rules")
        
        # Rule 1: Set status based on transformed value
        df = df.withColumn("status",
            when(col("value").isNull(), lit("INVALID"))
            .when(col("transformed_value") >= 750, lit("HIGH_VALUE"))
            .when(col("transformed_value") >= 300, lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )
        
        # Rule 2: Priority override for high value items
        df = df.withColumn("priority",
            when(col("transformed_value") >= 1000, lit(1))
            .otherwise(col("priority"))
        )
        
        # Rule 3: Category validation
        df = df.withColumn("category",
            when(col("category").isNull() | (col("category") == ""), lit("UNCATEGORIZED"))
            .otherwise(col("category"))
        )
        
        return df
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """Apply category-specific transformation rules"""
        self.logger.info("Applying category rules")
        
        # Get category multipliers from config
        category_rules = self.config.get('transformation', {}).get('category_rules', {})
        
        for category, multiplier in category_rules.items():
            df = df.withColumn("transformed_value",
                when(col("category") == category, col("transformed_value") * multiplier)
                .otherwise(col("transformed_value"))
            )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional calculated fields"""
        self.logger.info("Enriching data")
        
        # Add value tier
        df = df.withColumn("value_tier",
            when(col("transformed_value") >= 1000, lit("TIER_1"))
            .when(col("transformed_value") >= 500, lit("TIER_2"))
            .when(col("transformed_value") >= 100, lit("TIER_3"))
            .otherwise(lit("TIER_4"))
        )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        self.logger.info("Validating data")
        errors = []
        
        # Validation 1: Required fields
        null_ids = df.filter(col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null ID")
        
        null_names = df.filter(col("name").isNull() | (col("name") == "")).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null/empty name")
        
        # Validation 2: Value constraints
        negative_values = df.filter(col("value") < 0).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative values")
        
        # Validation 3: Priority range
        invalid_priority = df.filter((col("priority") < 1) | (col("priority") > 5)).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        # Validation 4: Category validation
        null_categories = df.filter(col("category").isNull() | (col("category") == "")).count()
        if null_categories > 0:
            errors.append(f"{null_categories} records with null/empty category")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Data validation passed")
        else:
            self.logger.warning(f"Data validation failed with {len(errors)} errors")
            for error in errors:
                self.logger.warning(f"  - {error}")
        
        return is_valid, errors


===FILE: src/load.py===
"""
ETL Loader Module
Handles data loading to target systems with batch processing and reconciliation.
"""

from pyspark.sql import DataFrame
from typing import Dict, Any, List
import logging


class LoadResult:
    """Container for load operation results"""
    def __init__(self):
        self.success_count = 0
        self.error_count = 0
        self.total_count = 0
        self.errors: List[str] = []


class ETLLoader:
    """Loads transformed data to target system"""
    
    def __init__(self, spark, config: Dict[str, Any], run_id: str):
        """
        Initialize loader
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        self.batch_size = config.get('load', {}).get('batch_size', 1000)
        self.target_type = config.get('target_type', 'database')
        
    def load_data(self, df: DataFrame, mode: str = 'append') -> LoadResult:
        """
        Load data to target system
        
        Args:
            df: DataFrame to load
            mode: Load mode ('append', 'overwrite', 'upsert')
            
        Returns:
            LoadResult with success/error counts
        """
        self.logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        result = LoadResult()
        
        try:
            result.total_count = df.count()
            
            if result.total_count == 0:
                self.logger.warning("No data to load")
                return result
            
            # Load based on target type
            if self.target_type.upper() == 'DATABASE':
                success = self._load_to_database(df, mode)
            else:
                success = self._load_to_database(df, mode)
            
            if success:
                result.success_count = result.total_count
                self.logger.info(f"Successfully loaded {result.success_count} records")
                
                # Perform reconciliation
                if self.config.get('load', {}).get('enable_reconciliation', True):
                    self._reconcile_data(df)
            else:
                result.error_count = result.total_count
                result.errors.append("Load operation failed")
                
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            result.error_count = result.total_count
            result.errors.append(str(e))
        
        return result
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """Load data to database target"""
        try:
            jdbc_config = self.config['target']['database']
            
            # Map mode to JDBC save mode
            save_mode = 'append'
            if mode.lower() == 'overwrite':
                save_mode = 'overwrite'
            elif mode.lower() == 'upsert':
                # For upsert, we need to handle it differently
                # This is a simplified approach - in production you'd use merge
                save_mode = 'append'
                self.logger.warning("UPSERT mode simplified to APPEND - use database MERGE for true upsert")
            
            df.write \
                .format("jdbc") \
                .option("url", jdbc_config['url']) \
                .option("dbtable", jdbc_config['table']) \
                .option("user", jdbc_config['user']) \
                .option("password", jdbc_config['password']) \
                .option("driver", jdbc_config['driver']) \
                .option("batchsize", self.batch_size) \
                .mode(save_mode) \
                .save()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Database load error: {str(e)}")
            return False
    
    def _reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data against source
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passes
        """
        self.logger.info("Performing data reconciliation")
        
        try:
            jdbc_config = self.config['target']['database']
            
            # Read back loaded data
            query = f"""(
                SELECT COUNT(*) as loaded_count
                FROM {jdbc_config['table']}
                WHERE etl_run_id = '{self.run_id}'
            ) AS recon_check"""
            
            target_df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_config['url']) \
                .option("dbtable", query) \
                .option("user", jdbc_config['user']) \
                .option("password", jdbc_config['password']) \
                .option("driver", jdbc_config['driver']) \
                .load()
            
            loaded_count = target_df.first()['loaded_count']
            expected_count = loaded_df.count()
            
            if loaded_count == expected_count:
                self.logger.info(f"Reconciliation passed: {loaded_count} records")
                return True
            else:
                self.logger.warning(
                    f"Reconciliation mismatch: Expected {expected_count}, Found {loaded_count}"
                )
                return False
                
        except Exception as e:
            self.logger.error(f"Reconciliation failed: {str(e)}")
            return False


===FILE: src/orchestrator.py===
"""
ETL Orchestrator Module
Coordinates the end-to-end ETL process.
"""

from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict, Any, Optional
import logging
import yaml

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.data_quality import DataQualityChecker


class ETLResult:
    """Container for ETL execution results"""
    def __init__(self, run_id: str):
        self.run_id = run_id
        self.status = "RUNNING"
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.duration: Optional[int] = None
        self.records_extracted = 0
        self.records_transformed = 0
        self.records_loaded = 0
        self.records_failed = 0
        self.error_count = 0
        self.warning_count = 0
        self.errors = []


class ETLOrchestrator:
    """Orchestrates the complete ETL pipeline"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize orchestrator
        
        Args:
            config_path: Path to configuration file
        """
        self.logger = logging.getLogger(__name__)
        self.config = self._load_config(config_path)
        self.spark = self._create_spark_session()
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure SparkSession"""
        spark_config = self.config.get('spark', {})
        
        builder = SparkSession.builder \
            .appName(spark_config.get('app_name', 'ETL_Pipeline'))
        
        # Apply Spark configurations
        for key, value in spark_config.get('config', {}).items():
            builder = builder.config(key, value)
        
        return builder.getOrCreate()
    
    def execute_etl(self, 
                    source_type: str = 'database',
                    target_type: str = 'database',
                    filter_condition: Optional[str] = None,
                    max_records: int = 0,
                    run_quality_checks: bool = True) -> ETLResult:
        """
        Execute complete ETL pipeline
        
        Args:
            source_type: Type of source ('database', 'staging', 'incremental')
            target_type: Type of target ('database')
            filter_condition: Optional filter for extraction
            max_records: Maximum records to process (0 = no limit)
            run_quality_checks: Whether to run data quality checks
            
        Returns:
            ETLResult with execution statistics
        """
        run_id = self._generate_run_id()
        result = ETLResult(run_id)
        result.start_time = datetime.now()
        
        self.logger.info(f"=== ETL Execution Started - Run ID: {run_id} ===")
        
        try:
            # Update config with runtime parameters
            self.config['source_type'] = source_type
            self.config['target_type'] = target_type
            
            # Step 1: Extract
            self.logger.info("Step 1/4: Extraction")
            extractor = ETLExtractor(self.spark, self.config, run_id)
            source_df = extractor.extract_data(filter_condition, max_records)
            result.records_extracted = source_df.count()
            
            if result.records_extracted == 0:
                self.logger.warning("No data extracted - stopping ETL")
                result.status = "NO_DATA"
                return result
            
            # Step 2: Transform
            self.logger.info("Step 2/4: Transformation")
            transformer = ETLTransformer(self.spark, self.config, run_id)
            transformed_df = transformer.transform_data(source_df)
            result.records_transformed = transformed_df.count()
            
            # Step 3: Validate
            self.logger.info("Step 3/4: Validation")
            is_valid, validation_errors = transformer.validate_data(transformed_df)
            
            if not is_valid:
                self.logger.error(f"Validation failed: {len(validation_errors)} errors")
                result.status = "VALIDATION_FAILED"
                result.errors = validation_errors
                result.error_count = len(validation_errors)
                return result
            
            # Optional: Data Quality Checks
            if run_quality_checks:
                self.logger.info("Running data quality checks")
                quality_checker = DataQualityChecker(self.spark, self.config, run_id)
                quality_results = quality_checker.perform_quality_checks(transformed_df)
                
                failed_checks = [c for c in quality_results if not c['passed']]
                if failed_checks:
                    result.warning_count += len(failed_checks)
                    for check in failed_checks:
                        self.logger.warning(f"Quality check failed: {check['message']}")
            
            # Step 4: Load
            self.logger.info("Step 4/4: Loading")
            loader = ETLLoader(self.spark, self.config, run_id)
            load_result = loader.load_data(transformed_df, mode='append')
            
            result.records_loaded = load_result.success_count
            result.records_failed = load_result.error_count
            result.error_count += load_result.error_count
            result.errors.extend(load_result.errors)
            
            # Determine final status
            if result.error_count == 0:
                result.status = "SUCCESS"
            elif result.records_loaded > 0:
                result.status = "PARTIAL_SUCCESS"
            else:
                result.status = "FAILED"
            
        except Exception as e:
            self.logger.error(f"ETL execution failed: {str(e)}", exc_info=True)
            result.status = "ERROR"
            result.errors.append(str(e))
            result.error_count += 1
        
        finally:
            result.end_time = datetime.now()
            result.duration = int((result.end_time - result.start_time).total_seconds())
            self._log_run_completion(result)
        
        return result
    
    def _generate_run_id(self) -> str:
        """Generate unique run identifier"""
        return f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def _log_run_completion(self, result: ETLResult):
        """Log ETL run completion details"""
        self.logger.info("=" * 60)
        self.logger.info(f"ETL Run Completed - {result.status}")
        self.logger.info(f"Run ID: {result.run_id}")
        self.logger.info(f"Duration: {result.duration}s")
        self.logger.info(f"Records Extracted: {result.records_extracted}")
        self.logger.info(f"Records Transformed: {result.records_transformed}")
        self.logger.info(f"Records Loaded: {result.records_loaded}")
        self.logger.info(f"Records Failed: {result.records_failed}")
        self.logger.info(f"Errors: {result.error_count}")
        self.logger.info(f"Warnings: {result.warning_count}")
        if result.errors:
            self.logger.error("Errors encountered:")
            for error in result.errors:
                self.logger.error(f"  - {error}")
        self.logger.info("=" * 60)
    
    def cleanup(self):
        """Clean up resources"""
        if self.spark:
            self.spark.stop()


===FILE: src/data_quality.py===
"""
Data Quality Module
Performs comprehensive quality checks on transformed data.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, countDistinct, avg, stddev, min as _min, max as _max
from typing import Dict, Any, List
import logging


class DataQualityChecker:
    """Performs data quality validation"""
    
    def __init__(self, spark, config: Dict[str, Any], run_id: str):
        """
        Initialize quality checker
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def perform_quality_checks(self, df: DataFrame) -> List[Dict[str, Any]]:
        """
        Perform all quality checks
        
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
            self._check_consistency(df),
        ]
        
        passed = sum(1 for c in checks if c['passed'])
        failed = len(checks) - passed
        
        self.logger.info(f"Quality checks complete: {passed} passed, {failed} failed")
        
        return checks
    
    def _check_completeness(self, df: DataFrame) -> Dict[str, Any]:
        """Check for null/empty values in critical fields"""
        check = {
            'check_name': 'Completeness Check',
            'check_type': 'COMPLETENESS',
            'passed': True,
            'failed_count': 0,
            'message': ''
        }
        
        # Count nulls in critical fields
        null_counts = df.select([
            count(when(col('id').isNull(), 1)).alias('null_ids'),
            count(when(col('name').isNull() | (col('name') == ''), 1)).alias('null_names'),
            count(when(col('value').isNull(), 1)).alias('null_values'),
        ]).first()
        
        total_nulls = sum(null_counts.asDict().values())
        check['failed_count'] = total_nulls
        
        if total_nulls > 0:
            check['passed'] = False
            check['message'] = f"{total_nulls} records with incomplete data"
        else:
            check['message'] = "All required fields are complete"
        
        return check
    
    def _check_uniqueness(self, df: DataFrame) -> Dict[str, Any]:
        """Check for duplicate IDs"""
        check = {
            'check_name': 'Uniqueness Check',
            'check_type': 'UNIQUENESS',
            'passed': True,
            'failed_count': 0,
            'message': ''
        }
        
        total_count = df.count()
        unique_count = df.select('id').distinct().count()
        duplicates = total_count - unique_count
        
        check['failed_count'] = duplicates
        
        if duplicates > 0:
            check['passed'] = False
            check['message'] = f"{duplicates} duplicate IDs found"
        else:
            check['message'] = "All IDs are unique"
        
        return check
    
    def _check_validity(self, df: DataFrame) -> Dict[str, Any]:
        """Check for invalid values"""
        check = {
            'check_name': 'Validity Check',
            'check_type': 'VALIDITY',
            'passe