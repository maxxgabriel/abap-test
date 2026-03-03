===FILE: src/extract.py===
"""
Data extraction module for PySpark ETL pipeline.
Extracts data from various source types with support for full and incremental loads.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from typing import Optional
import yaml
from datetime import datetime
import logging


class DataExtractor:
    """Handles data extraction from multiple source types."""
    
    def __init__(self, spark: SparkSession, config: dict, run_id: str):
        """
        Initialize the extractor.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def get_source_schema(self) -> StructType:
        """Define the schema for source data."""
        return StructType([
            StructField("id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("value", DecimalType(15, 2), nullable=True),
            StructField("status", StringType(), nullable=True),
            StructField("category", StringType(), nullable=True),
            StructField("source_system", StringType(), nullable=True),
            StructField("created_at", TimestampType(), nullable=True),
            StructField("created_by", StringType(), nullable=True),
            StructField("changed_at", TimestampType(), nullable=True),
            StructField("changed_by", StringType(), nullable=True)
        ])
    
    def extract_data(
        self,
        source_type: Optional[str] = None,
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data based on source type.
        
        Args:
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            filter_condition: Optional SQL WHERE clause
            max_records: Maximum number of records to extract (0 = unlimited)
            
        Returns:
            DataFrame containing extracted data
        """
        source_type = source_type or self.config.get('source_type', 'DATABASE')
        
        self.logger.info(f"Starting extraction - Source: {source_type}, Run ID: {self.run_id}")
        
        if source_type == 'DATABASE':
            df = self._extract_from_database(filter_condition)
        elif source_type == 'STAGING':
            df = self._extract_from_staging()
        elif source_type == 'INCREMENTAL':
            df = self._extract_incremental()
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
        
        # Apply max records limit if specified
        if max_records > 0:
            df = df.limit(max_records)
        
        record_count = df.count()
        self.logger.info(f"Extracted {record_count} records")
        
        return df
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract data from database source.
        
        Args:
            filter_condition: Optional WHERE clause
            
        Returns:
            DataFrame with source data
        """
        source_config = self.config['source']['database']
        
        # Build JDBC connection properties
        jdbc_url = source_config['jdbc_url']
        connection_properties = {
            "user": source_config.get('user', ''),
            "password": source_config.get('password', ''),
            "driver": source_config.get('driver', 'org.postgresql.Driver')
        }
        
        table_name = source_config['table']
        
        # Build query with optional filter
        if filter_condition:
            query = f"(SELECT * FROM {table_name} WHERE {filter_condition}) as filtered_data"
        else:
            query = table_name
        
        df = self.spark.read.jdbc(
            url=jdbc_url,
            table=query,
            properties=connection_properties
        )
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """
        Extract data from staging area.
        
        Returns:
            DataFrame with staged data
        """
        staging_config = self.config['source']['staging']
        staging_path = staging_config['path']
        
        # Read from staging (could be parquet, csv, etc.)
        file_format = staging_config.get('format', 'parquet')
        
        df = self.spark.read.format(file_format).load(staging_path)
        
        # Filter by run_id if specified
        if 'run_id_column' in staging_config:
            df = df.filter(f"{staging_config['run_id_column']} = '{self.run_id}'")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run.
        
        Returns:
            DataFrame with incremental data
        """
        # Get last successful run timestamp
        last_run_time = self._get_last_run_timestamp()
        
        if last_run_time:
            self.logger.info(f"Extracting incremental data since: {last_run_time}")
            filter_condition = f"changed_at > '{last_run_time}'"
        else:
            self.logger.warning("No previous run found, performing full extraction")
            filter_condition = None
        
        return self._extract_from_database(filter_condition)
    
    def _get_last_run_timestamp(self) -> Optional[str]:
        """
        Get timestamp of last successful ETL run.
        
        Returns:
            Timestamp string or None
        """
        try:
            run_log_config = self.config.get('run_log', {})
            if not run_log_config:
                return None
            
            # Read run log
            df = self.spark.read.jdbc(
                url=run_log_config['jdbc_url'],
                table=run_log_config['table'],
                properties={
                    "user": run_log_config.get('user', ''),
                    "password": run_log_config.get('password', '')
                }
            )
            
            # Get last successful run
            last_run = df.filter("status = 'SUCCESS'") \
                        .orderBy("end_time", ascending=False) \
                        .select("end_time") \
                        .first()
            
            if last_run:
                return str(last_run['end_time'])
            
            return None
        except Exception as e:
            self.logger.error(f"Error getting last run timestamp: {str(e)}")
            return None


===FILE: src/transform.py===
"""
Data transformation module for PySpark ETL pipeline.
Applies business rules, enrichment, and data quality validation.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, current_timestamp,
    current_user, concat_ws, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from typing import Tuple, List
import logging


class DataTransformer:
    """Handles data transformation and business rule application."""
    
    def __init__(self, spark: SparkSession, config: dict, run_id: str):
        """
        Initialize the transformer.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
    
    def get_target_schema(self) -> StructType:
        """Define the schema for transformed data."""
        return StructType([
            StructField("id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("value", DecimalType(15, 2), nullable=True),
            StructField("transformed_value", DecimalType(15, 2), nullable=True),
            StructField("status", StringType(), nullable=True),
            StructField("category", StringType(), nullable=True),
            StructField("priority", IntegerType(), nullable=True),
            StructField("etl_run_id", StringType(), nullable=True),
            StructField("processed_at", TimestampType(), nullable=True),
            StructField("processed_by", StringType(), nullable=True)
        ])
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply all transformations to the data.
        
        Args:
            df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        # Basic transformations
        df_transformed = self._apply_basic_transformations(df)
        
        # Calculate derived values
        df_transformed = self._calculate_derived_values(df_transformed)
        
        # Apply business rules
        df_transformed = self._apply_business_rules(df_transformed)
        
        # Enrich data
        df_transformed = self._enrich_data(df_transformed)
        
        # Add audit columns
        df_transformed = self._add_audit_columns(df_transformed)
        
        record_count = df_transformed.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return df_transformed
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply basic data cleansing transformations.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with basic transformations
        """
        return df.select(
            col("id"),
            upper(trim(regexp_replace(col("name"), r'\s+', ' '))).alias("name"),
            col("value"),
            col("status"),
            coalesce(col("category"), lit("UNCATEGORIZED")).alias("category"),
            col("source_system"),
            col("created_at"),
            col("changed_at")
        )
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate derived values and transformations.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with calculated columns
        """
        transformation_rules = self.config.get('transformation', {})
        premium_multiplier = float(transformation_rules.get('premium_multiplier', 1.5))
        standard_multiplier = float(transformation_rules.get('standard_multiplier', 1.2))
        
        # Calculate transformed value based on category
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("value") * premium_multiplier)
            .when(col("category") == "STANDARD", col("value") * standard_multiplier)
            .otherwise(col("value"))
        )
        
        # Calculate priority based on value and category
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, lit(1))
            .when(col("transformed_value") >= 750, lit(2))
            .when(col("transformed_value") >= 300, lit(3))
            .when(col("transformed_value") >= 100, lit(4))
            .otherwise(lit(5))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to the data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with business rules applied
        """
        self.logger.info("Applying business rules")
        
        # Rule 1: Set status based on value
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
        """
        Enrich data with additional information.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Enriched DataFrame
        """
        self.logger.info("Enriching data")
        
        enrichment_config = self.config.get('enrichment', {})
        
        if enrichment_config.get('enabled', False):
            # Apply additional enrichment based on configuration
            # For example: add region, department, etc.
            pass
        
        return df
    
    def _add_audit_columns(self, df: DataFrame) -> DataFrame:
        """
        Add audit columns for tracking.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with audit columns
        """
        return df.withColumn("etl_run_id", lit(self.run_id)) \
                 .withColumn("processed_at", current_timestamp()) \
                 .withColumn("processed_by", lit("etl_user"))
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
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
        null_name_count = df.filter(col("name").isNull()).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with null name")
        
        # Validation 3: Check for negative values
        negative_value_count = df.filter(
            (col("value") < 0) | (col("transformed_value") < 0)
        ).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative values")
        
        # Validation 4: Check priority range
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Data validation passed")
        else:
            self.logger.warning(f"Data validation failed with {len(errors)} errors")
        
        return is_valid, errors


===FILE: src/load.py===
"""
Data loading module for PySpark ETL pipeline.
Handles writing transformed data to target systems with reconciliation.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from typing import Dict, Any
import logging


class DataLoader:
    """Handles data loading to target systems."""
    
    def __init__(self, spark: SparkSession, config: dict, run_id: str):
        """
        Initialize the loader.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
    
    def load_data(
        self,
        df: DataFrame,
        mode: str = "append",
        target_type: str = None
    ) -> Dict[str, Any]:
        """
        Load data to target system.
        
        Args:
            df: DataFrame to load
            mode: Write mode (append, overwrite, upsert)
            target_type: Type of target (DATABASE, FILE, etc.)
            
        Returns:
            Dictionary with load results
        """
        target_type = target_type or self.config.get('target_type', 'DATABASE')
        batch_size = self.config.get('batch_size', 1000)
        
        self.logger.info(f"Starting load - Mode: {mode}, Target: {target_type}, Batch size: {batch_size}")
        
        try:
            total_count = df.count()
            
            if target_type == 'DATABASE':
                success = self._load_to_database(df, mode)
            elif target_type == 'FILE':
                success = self._load_to_file(df, mode)
            else:
                raise ValueError(f"Unsupported target type: {target_type}")
            
            if success:
                success_count = total_count
                error_count = 0
                
                # Perform reconciliation
                if self.config.get('reconciliation', {}).get('enabled', True):
                    reconciled = self._reconcile_data(df)
                    if not reconciled:
                        self.logger.warning("Data reconciliation failed")
            else:
                success_count = 0
                error_count = total_count
            
            result = {
                'success_count': success_count,
                'error_count': error_count,
                'total_count': total_count,
                'errors': []
            }
            
            self.logger.info(f"Load complete - Success: {success_count}, Errors: {error_count}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            return {
                'success_count': 0,
                'error_count': df.count() if df else 0,
                'total_count': df.count() if df else 0,
                'errors': [str(e)]
            }
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to database target.
        
        Args:
            df: DataFrame to load
            mode: Write mode
            
        Returns:
            Success status
        """
        try:
            target_config = self.config['target']['database']
            
            jdbc_url = target_config['jdbc_url']
            table_name = target_config['table']
            
            connection_properties = {
                "user": target_config.get('user', ''),
                "password": target_config.get('password', ''),
                "driver": target_config.get('driver', 'org.postgresql.Driver')
            }
            
            # Map mode
            write_mode = self._get_write_mode(mode)
            
            # Write to database
            df.write.jdbc(
                url=jdbc_url,
                table=table_name,
                mode=write_mode,
                properties=connection_properties
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Database load error: {str(e)}")
            return False
    
    def _load_to_file(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to file system.
        
        Args:
            df: DataFrame to load
            mode: Write mode
            
        Returns:
            Success status
        """
        try:
            target_config = self.config['target']['file']
            
            output_path = target_config['path']
            file_format = target_config.get('format', 'parquet')
            
            write_mode = self._get_write_mode(mode)
            
            # Write to file
            writer = df.write.mode(write_mode)
            
            if file_format == 'parquet':
                writer.parquet(output_path)
            elif file_format == 'csv':
                writer.option("header", "true").csv(output_path)
            elif file_format == 'json':
                writer.json(output_path)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"File load error: {str(e)}")
            return False
    
    def _get_write_mode(self, mode: str) -> str:
        """
        Map ETL mode to Spark write mode.
        
        Args:
            mode: ETL mode (INSERT, UPDATE, UPSERT)
            
        Returns:
            Spark write mode
        """
        mode_mapping = {
            'INSERT': 'append',
            'APPEND': 'append',
            'UPDATE': 'overwrite',
            'OVERWRITE': 'overwrite',
            'UPSERT': 'append'  # Upsert requires additional logic
        }
        
        return mode_mapping.get(mode.upper(), 'append')
    
    def _reconcile_data(self, df: DataFrame) -> bool:
        """
        Reconcile loaded data with source.
        
        Args:
            df: DataFrame that was loaded
            
        Returns:
            Reconciliation success status
        """
        try:
            self.logger.info("Starting data reconciliation")
            
            source_count = df.count()
            
            # Read back from target to verify
            target_config = self.config['target']['database']
            
            loaded_df = self.spark.read.jdbc(
                url=target_config['jdbc_url'],
                table=target_config['table'],
                properties={
                    "user": target_config.get('user', ''),
                    "password": target_config.get('password', '')
                }
            )
            
            # Filter by run_id
            target_count = loaded_df.filter(col("etl_run_id") == self.run_id).count()
            
            if source_count == target_count:
                self.logger.info(f"Reconciliation passed: {source_count} records")
                return True
            else:
                self.logger.warning(
                    f"Reconciliation failed: Source={source_count}, Target={target_count}"
                )
                return False
                
        except Exception as e:
            self.logger.error(f"Reconciliation error: {str(e)}")
            return False


===FILE: src/quality.py===
"""
Data quality validation module for PySpark ETL pipeline.
Executes validation rules, generates profiling statistics, and checks thresholds.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, stddev, min as _min, max as _max,
    countDistinct, when, isnan, isnull
)
from typing import List, Dict, Any
from dataclasses import dataclass, asdict
import logging


@dataclass
class QualityCheck:
    """Data quality check result."""
    check_name: str
    check_type: str
    passed: bool
    failed_count: int
    message: str


@dataclass
class DataProfile:
    """Data profiling statistics."""
    total_records: int
    null_count: int
    duplicate_count: int
    min_value: float
    max_value: float
    avg_value: float
    std_deviation: float
    unique_categories: int


class DataQualityValidator:
    """Handles data quality checks and profiling."""
    
    def __init__(self, spark: SparkSession, config: dict, run_id: str):
        """
        Initialize the quality validator.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
    
    def perform_quality_checks(self, df: DataFrame) -> List[QualityCheck]:
        """
        Perform all data quality checks.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            List of quality check results
        """
        self.logger.info("Starting data quality checks")
        
        checks = []
        
        # Execute all checks
        checks.append(self._check_completeness(df))
        checks.append(self._check_uniqueness(df))
        checks.append(self._check_validity(df))
        checks.append(self._check_consistency(df))
        checks.append(self._check_record_count_threshold(df))
        
        # Count passed/failed
        passed = sum(1 for c in checks if c.passed)
        failed = len(checks) - passed
        
        self.logger.info(f"Quality checks complete: {passed} passed, {failed} failed")
        
        return checks
    
    def _check_completeness(self, df: DataFrame) -> QualityCheck:
        """
        Check for completeness (null values in critical fields).
        
        Args:
            df: DataFrame to check
            
        Returns:
            QualityCheck result
        """
        # Count records with null values in critical fields
        null_count = df.filter(
            col("id").isNull() | 
            col("name").isNull() | 
            col("value").isNull()
        ).count()
        
        passed = null_count == 0
        message = "All required fields are complete" if passed else \
                  f"{null_count} records with incomplete data"
        
        return QualityCheck(
            check_name="Completeness Check",
            check_type="COMPLETENESS",
            passed=passed,
            failed_count=null_count,
            message=message
        )
    
    def _check_uniqueness(self, df: DataFrame) -> QualityCheck:
        """
        Check for duplicate records.
        
        Args:
            df: DataFrame to check
            
        Returns:
            QualityCheck result
        """
        total_count = df.count()
        unique_count = df.select("id").distinct().count()
        duplicate_count = total_count - unique_count
        
        passed = duplicate_count == 0
        message = "All IDs are unique" if passed else \
                  f"{duplicate_count} duplicate IDs found"
        
        return QualityCheck(
            check_name="Uniqueness Check",
            check_type="UNIQUENESS",
            passed=passed,
            failed_count=duplicate_count,
            message=message
        )
    
    def _check_validity(self, df: DataFrame) -> QualityCheck:
        """
        Check for invalid values.
        
        Args:
            df: DataFrame to check
            
        Returns:
            QualityCheck result
        """
        # Check for negative values
        invalid_count = df.filter(
            (col("value") < 0) | 
            (col("transformed_value") < 0) |
            (col("priority") < 1) | 
            (col("priority") > 5) |
            col("category").isNull()
        ).count()
        
        passed = invalid_count == 0
        message = "All values are valid" if passed else \
                  f"{invalid_count} records with invalid values"
        
        return QualityCheck(
            check_name="Validity Check",
            check_type="VALIDITY",
            passed=passed,
            failed_count=invalid_count,
            message=message
        )
    
    def _check_consistency(self, df: DataFrame) -> QualityCheck:
        """
        Check for data consistency.
        
        Args:
            df: DataFrame to check
            
        Returns:
            QualityCheck result
        """
        # Check that transformed_value is consistent with value
        inconsistent_count = df.filter(
            col("transformed_value") < col("value")
        ).count()
        
        passed = inconsistent_count == 0
        message = "Data is consistent" if passed else \
                  f"{inconsistent_count} records with inconsistent values"
        
        return QualityCheck(
            check_name="Consistency Check",
            check_type="CONSISTENCY",
            passed=passed,
            failed_count=inconsistent_count,
            message=message
        )
    
    def _check_record_count_threshold(self, df: DataFrame) -> QualityCheck:
        """
        Check if record count meets expected thresholds.
        
        Args:
            df: DataFrame to check
            
        Returns:
            QualityCheck result
        """
        thresholds = self.config.get('quality_checks', {}).get('thresholds', {})
        min_records = thresholds.get('min_records', 0)
        max_records = thresholds.get('max_records', float('inf'))
        
        record_count = df.count()
        
        passed = min_records <= record_count <= max_records
        
        if record_count < min_records:
            message = f"Record count {record_count} below minimum threshold {min_records}"
        elif record_count > max_records:
            message = f"Record count {record_count} exceeds maximum threshold {max_records}"
        else:
            message = f"Record count {record_count} within expected range"
        
        failed_count = 0 if passed else 1
        
        return QualityCheck(
            check_name="Record Count Threshold",
            check_type="THRESHOLD",
            passed=passed,
            failed_count=failed_count,
            message=message
        )
    
    def profile_data(self, df: DataFrame) -> DataProfile:
        """
        Generate data profiling statistics.
        
        Args:
            df: DataFrame to profile
            
        Returns:
            DataProfile with statistics
        """
        self.logger.info("Generating data profile")
        
        # Calculate statistics
        stats = df.agg(
            count("*").alias("total_records"),
            _sum(when(col("value").isNull(), 1).otherwise(0)).alias("null_count"),
            _min("value").alias("min_value"),
            _max("value").alias("max_value"),
            avg("value").alias("avg_value"),
            stddev("value").alias("std_deviation"),
            countDistinct("category").alias("unique_categories")
        ).collect()[0]
        
        # Calculate duplicates
        total_count = df.count()
        unique_count = df.select("id").distinct().count()
        duplicate_count = total_count - unique_count
        
        profile = DataProfile(
            total_records=stats["total_records"],
            null_count=stats["null_count"],
            duplicate_count=duplicate_count,
            min_value=float(stats["min_value"]) if stats["min_value"] else 0.0,
            max_value=float(stats["max_value"]) if stats["max_value"] else 0.0,
            avg_value=float(stats["avg_value"]) if stats["avg_value"] else 0.0,
            std_deviation=float(stats["std_deviation"]) if stats["std_deviation"] else 0.0,
            unique_categories=stats["unique_categories"]
        )
        
        self.logger.info(f"Profile complete: {profile.total_records} records")
        
        return profile
    
    def detect_anomalies(self, df: DataFrame) -> List[str]:
        """
        Detect anomalies in the data.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            List of anomaly descriptions
        """
        self.logger.info("Detecting anomalies")
        
        anomalies = []
        
        # Calculate statistics for anomaly detection
        stats = df.agg(
            avg("value").alias("avg_value"),
            stddev("value").alias("std_dev")
        ).collect()[0