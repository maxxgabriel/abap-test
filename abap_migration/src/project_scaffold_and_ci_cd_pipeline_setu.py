# PySpark ETL Project - Production Code

===FILE: src/extract.py===
"""
Data extraction module for PySpark ETL pipeline.
Handles extraction from various source types including database, staging, and incremental loads.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional, Dict, Any
import logging

from src.logger import ETLLogger


class DataExtractor:
    """Handles data extraction from various sources."""
    
    def __init__(self, spark: SparkSession, source_type: str = "DATABASE", run_id: str = None):
        """
        Initialize the data extractor.
        
        Args:
            spark: SparkSession instance
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.source_type = source_type.upper()
        self.run_id = run_id or self._generate_run_id()
        self.logger = ETLLogger.get_instance()
        
    @staticmethod
    def _generate_run_id() -> str:
        """Generate a unique run ID."""
        return f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    @staticmethod
    def get_source_schema() -> StructType:
        """Define the schema for source data."""
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
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data based on source type.
        
        Args:
            filter_condition: Optional SQL filter condition
            max_records: Maximum number of records to extract (0 = no limit)
            
        Returns:
            DataFrame containing extracted data
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Starting extraction - Source: {self.source_type}"
        )
        
        try:
            # Route to appropriate extraction method
            if self.source_type == "DATABASE":
                df = self.extract_from_database(filter_condition)
            elif self.source_type == "STAGING":
                df = self.extract_from_staging(self.run_id)
            elif self.source_type == "INCREMENTAL":
                df = self.extract_incremental()
            else:
                self.logger.log_warning(
                    component="EXTRACTOR",
                    message=f"Unknown source type: {self.source_type}, defaulting to DATABASE"
                )
                df = self.extract_from_database(filter_condition)
            
            # Apply max records limit if specified
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
    
    def extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract data from database source.
        
        Args:
            filter_condition: Optional WHERE clause filter
            
        Returns:
            DataFrame with extracted data
        """
        jdbc_url = self.spark.conf.get("spark.etl.source.jdbc.url")
        table_name = self.spark.conf.get("spark.etl.source.table", "source_data")
        
        query = f"(SELECT * FROM {table_name}"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        query += ") AS source"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", query) \
            .option("user", self.spark.conf.get("spark.etl.source.jdbc.user")) \
            .option("password", self.spark.conf.get("spark.etl.source.jdbc.password")) \
            .option("driver", self.spark.conf.get("spark.etl.source.jdbc.driver")) \
            .schema(self.get_source_schema()) \
            .load()
        
        return df
    
    def extract_from_staging(self, run_id: str) -> DataFrame:
        """
        Extract data from staging area.
        
        Args:
            run_id: Run ID to filter staging data
            
        Returns:
            DataFrame with staged data
        """
        staging_path = self.spark.conf.get("spark.etl.staging.path")
        
        df = self.spark.read \
            .format("parquet") \
            .schema(self.get_source_schema()) \
            .load(staging_path) \
            .filter(f"run_id = '{run_id}' AND status = 'READY'")
        
        return df
    
    def extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run.
        
        Returns:
            DataFrame with incremental data
        """
        # Get last successful run timestamp
        last_run_time = self._get_last_run_timestamp()
        
        if last_run_time:
            self.logger.log_info(
                component="EXTRACTOR",
                message=f"Performing incremental extraction since {last_run_time}"
            )
            filter_condition = f"changed_at > '{last_run_time}'"
        else:
            self.logger.log_warning(
                component="EXTRACTOR",
                message="No previous successful run found, performing full extraction"
            )
            filter_condition = None
        
        return self.extract_from_database(filter_condition)
    
    def _get_last_run_timestamp(self) -> Optional[str]:
        """Get timestamp of last successful ETL run."""
        try:
            jdbc_url = self.spark.conf.get("spark.etl.source.jdbc.url")
            
            df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "(SELECT MAX(end_time) as last_run FROM run_log WHERE status = 'SUCCESS') AS last") \
                .option("user", self.spark.conf.get("spark.etl.source.jdbc.user")) \
                .option("password", self.spark.conf.get("spark.etl.source.jdbc.password")) \
                .load()
            
            result = df.first()
            return result["last_run"] if result and result["last_run"] else None
            
        except Exception as e:
            self.logger.log_warning(
                component="EXTRACTOR",
                message=f"Could not retrieve last run timestamp: {str(e)}"
            )
            return None


===FILE: src/transform.py===
"""
Data transformation module for PySpark ETL pipeline.
Applies business rules, enrichments, and validations to extracted data.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, current_timestamp,
    current_user, concat, round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import List, Tuple, Dict, Any
import logging

from src.logger import ETLLogger


class DataTransformer:
    """Handles data transformation and business rule application."""
    
    def __init__(self, spark: SparkSession, run_id: str):
        """
        Initialize the data transformer.
        
        Args:
            spark: SparkSession instance
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
    
    @staticmethod
    def get_transformed_schema() -> StructType:
        """Define the schema for transformed data."""
        return StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("value", DecimalType(15, 2), True),
            StructField("transformed_value", DecimalType(15, 2), True),
            StructField("status", StringType(), True),
            StructField("category", StringType(), True),
            StructField("priority", IntegerType(), True),
            StructField("etl_run_id", StringType(), True),
            StructField("processed_at", TimestampType(), True),
            StructField("processed_by", StringType(), True)
        ])
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply all transformations to source data.
        
        Args:
            source_df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Starting transformation"
        )
        
        try:
            # Initial transformation
            df = self._apply_initial_transformation(source_df)
            
            # Apply business rules
            df = self.apply_business_rules(df)
            
            # Enrich data
            df = self.enrich_data(df)
            
            record_count = df.count()
            self.logger.log_info(
                component="TRANSFORMER",
                message=f"Transformed {record_count} records"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="TRANSFORMER",
                message="Transformation failed",
                details=str(e)
            )
            raise
    
    def _apply_initial_transformation(self, df: DataFrame) -> DataFrame:
        """
        Apply initial field transformations.
        
        Args:
            df: Source DataFrame
            
        Returns:
            Initially transformed DataFrame
        """
        # Calculate transformed value based on category
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("value") * 1.5)
            .when(col("category") == "VIP", col("value") * 2.0)
            .when(col("category") == "STANDARD", col("value") * 1.2)
            .otherwise(col("value"))
        )
        
        # Calculate priority
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, lit(1))
            .when(col("transformed_value") >= 750, lit(2))
            .when(col("transformed_value") >= 500, lit(3))
            .when(col("transformed_value") >= 250, lit(4))
            .otherwise(lit(5))
        )
        
        # Normalize name field
        df = df.withColumn(
            "name",
            upper(trim(regexp_replace(col("name"), "\\s+", " ")))
        )
        
        # Add ETL metadata
        df = df.withColumn("etl_run_id", lit(self.run_id)) \
               .withColumn("processed_at", current_timestamp()) \
               .withColumn("processed_by", lit(current_user()))
        
        # Set initial status
        df = df.withColumn("status", lit("TRANSFORMED"))
        
        return df
    
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to transformed data.
        
        Args:
            df: DataFrame with initial transformations
            
        Returns:
            DataFrame with business rules applied
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Applying business rules"
        )
        
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
        
        # Rule 3: Default category if missing
        df = df.withColumn(
            "category",
            when(col("category").isNull() | (col("category") == ""), lit("UNCATEGORIZED"))
            .otherwise(col("category"))
        )
        
        # Rule 4: Round transformed values
        df = df.withColumn(
            "transformed_value",
            spark_round(col("transformed_value"), 2)
        )
        
        return df
    
    def enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional calculated fields and lookups.
        
        Args:
            df: DataFrame to enrich
            
        Returns:
            Enriched DataFrame
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Enriching data"
        )
        
        # Load enrichment configuration
        config = self._load_enrichment_config()
        
        # Apply category-specific multipliers
        for category, multiplier in config.get("category_multipliers", {}).items():
            df = df.withColumn(
                "transformed_value",
                when(
                    col("category") == category,
                    col("transformed_value") * multiplier
                ).otherwise(col("transformed_value"))
            )
        
        return df
    
    def _load_enrichment_config(self) -> Dict[str, Any]:
        """Load enrichment configuration from config table or file."""
        try:
            jdbc_url = self.spark.conf.get("spark.etl.source.jdbc.url")
            
            config_df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "(SELECT config_key, config_value FROM config WHERE is_active = 'Y') AS cfg") \
                .option("user", self.spark.conf.get("spark.etl.source.jdbc.user")) \
                .option("password", self.spark.conf.get("spark.etl.source.jdbc.password")) \
                .load()
            
            config_dict = {row["config_key"]: row["config_value"] for row in config_df.collect()}
            
            # Parse specific configuration items
            return {
                "category_multipliers": {
                    "PREMIUM": float(config_dict.get("PREMIUM_MULTIPLIER", "1.5"))
                }
            }
            
        except Exception as e:
            self.logger.log_warning(
                component="TRANSFORMER",
                message=f"Could not load enrichment config: {str(e)}"
            )
            return {"category_multipliers": {}}
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        errors = []
        
        # Rule 1: ID is required
        null_id_count = df.filter(col("id").isNull() | (col("id") == "")).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with missing ID")
        
        # Rule 2: Name is required
        null_name_count = df.filter(col("name").isNull() | (col("name") == "")).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with missing name")
        
        # Rule 3: Value must be positive
        negative_value_count = df.filter(col("value") < 0).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative value")
        
        # Rule 4: Priority must be 1-5
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        # Rule 5: Category is required
        null_category_count = df.filter(col("category").isNull() | (col("category") == "")).count()
        if null_category_count > 0:
            errors.append(f"{null_category_count} records with missing category")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.log_info(
                component="TRANSFORMER",
                message="Data validation passed"
            )
        else:
            self.logger.log_error(
                component="TRANSFORMER",
                message="Data validation failed",
                details="; ".join(errors)
            )
        
        return is_valid, errors
    
    def calculate_priority(self, value: float, category: str) -> int:
        """
        Calculate priority based on value and category.
        
        Args:
            value: Numeric value
            category: Category string
            
        Returns:
            Priority level (1-5)
        """
        if category == "VIP":
            return 1
        elif value >= 1000:
            return 1
        elif value >= 750:
            return 2
        elif value >= 500:
            return 3
        elif value >= 250:
            return 4
        else:
            return 5


===FILE: src/load.py===
"""
Data loading module for PySpark ETL pipeline.
Handles loading transformed data to target systems with batch processing.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from typing import Dict, Any, List
from dataclasses import dataclass
import logging

from src.logger import ETLLogger


@dataclass
class LoadResult:
    """Result of data loading operation."""
    success_count: int
    error_count: int
    total_count: int
    errors: List[str]


class DataLoader:
    """Handles data loading to target systems."""
    
    def __init__(
        self,
        spark: SparkSession,
        target_type: str = "DATABASE",
        batch_size: int = 1000,
        run_id: str = None
    ):
        """
        Initialize the data loader.
        
        Args:
            spark: SparkSession instance
            target_type: Type of target (DATABASE, PARQUET, DELTA)
            batch_size: Number of records per batch
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.target_type = target_type.upper()
        self.batch_size = batch_size
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
    
    def load_data(self, df: DataFrame, mode: str = "INSERT") -> LoadResult:
        """
        Load data to target system.
        
        Args:
            df: DataFrame to load
            mode: Load mode (INSERT, UPDATE, UPSERT)
            
        Returns:
            LoadResult with statistics
        """
        self.logger.log_info(
            component="LOADER",
            message=f"Starting load - Mode: {mode}, Batch size: {self.batch_size}"
        )
        
        errors = []
        total_count = df.count()
        
        try:
            if self.target_type == "DATABASE":
                success = self._load_to_database(df, mode)
            elif self.target_type == "PARQUET":
                success = self._load_to_parquet(df, mode)
            elif self.target_type == "DELTA":
                success = self._load_to_delta(df, mode)
            else:
                self.logger.log_warning(
                    component="LOADER",
                    message=f"Unknown target type: {self.target_type}, defaulting to DATABASE"
                )
                success = self._load_to_database(df, mode)
            
            if success:
                success_count = total_count
                error_count = 0
                
                # Perform reconciliation
                if self.spark.conf.get("spark.etl.reconciliation.enabled", "true") == "true":
                    reconciled = self.reconcile_data(df)
                    if not reconciled:
                        self.logger.log_warning(
                            component="LOADER",
                            message="Data reconciliation failed"
                        )
                        errors.append("Reconciliation mismatch detected")
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
            
            self.logger.log_info(
                component="LOADER",
                message=f"Load complete - Success: {success_count}, Errors: {error_count}"
            )
            
            return result
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Load failed",
                details=str(e)
            )
            return LoadResult(
                success_count=0,
                error_count=total_count,
                total_count=total_count,
                errors=[str(e)]
            )
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to database target.
        
        Args:
            df: DataFrame to load
            mode: Load mode (INSERT, UPDATE, UPSERT)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            jdbc_url = self.spark.conf.get("spark.etl.target.jdbc.url")
            table_name = self.spark.conf.get("spark.etl.target.table", "target_data")
            
            # Map mode to JDBC save mode
            if mode == "INSERT":
                save_mode = "append"
            elif mode == "UPDATE":
                save_mode = "overwrite"
            elif mode == "UPSERT":
                # For upsert, we'll use overwrite with truncate=false
                save_mode = "append"
            else:
                save_mode = "append"
            
            df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", self.spark.conf.get("spark.etl.target.jdbc.user")) \
                .option("password", self.spark.conf.get("spark.etl.target.jdbc.password")) \
                .option("driver", self.spark.conf.get("spark.etl.target.jdbc.driver")) \
                .option("batchsize", self.batch_size) \
                .mode(save_mode) \
                .save()
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Database load error",
                details=str(e)
            )
            return False
    
    def _load_to_parquet(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to Parquet files.
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            True if successful
        """
        try:
            target_path = self.spark.conf.get("spark.etl.target.parquet.path")
            
            save_mode = "overwrite" if mode == "UPDATE" else "append"
            
            df.write \
                .format("parquet") \
                .mode(save_mode) \
                .partitionBy("category") \
                .save(target_path)
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Parquet load error",
                details=str(e)
            )
            return False
    
    def _load_to_delta(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to Delta Lake.
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            True if successful
        """
        try:
            target_path = self.spark.conf.get("spark.etl.target.delta.path")
            
            if mode == "UPSERT":
                # Delta Lake supports merge operations
                df.write \
                    .format("delta") \
                    .mode("append") \
                    .option("mergeSchema", "true") \
                    .save(target_path)
            else:
                save_mode = "overwrite" if mode == "UPDATE" else "append"
                df.write \
                    .format("delta") \
                    .mode(save_mode) \
                    .save(target_path)
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Delta load error",
                details=str(e)
            )
            return False
    
    def reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data with source.
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passes
        """
        try:
            source_count = loaded_df.count()
            
            # Read back from target
            jdbc_url = self.spark.conf.get("spark.etl.target.jdbc.url")
            table_name = self.spark.conf.get("spark.etl.target.table", "target_data")
            
            target_df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", f"(SELECT COUNT(*) as cnt FROM {table_name} WHERE etl_run_id = '{self.run_id}') AS recon") \
                .option("user", self.spark.conf.get("spark.etl.target.jdbc.user")) \
                .option("password", self.spark.conf.get("spark.etl.target.jdbc.password")) \
                .load()
            
            target_count = target_df.first()["cnt"]
            
            matches = source_count == target_count
            
            if matches:
                self.logger.log_info(
                    component="LOADER",
                    message=f"Reconciliation passed - {source_count} records verified"
                )
            else:
                self.logger.log_warning(
                    component="LOADER",
                    message=f"Reconciliation failed - Source: {source_count}, Target: {target_count}"
                )
            
            return matches
            
        except Exception as e:
            self.logger.log_warning(
                component="LOADER",
                message=f"Reconciliation error: {str(e)}"
            )
            return False


===FILE: src/orchestrator.py===
"""
ETL orchestration module for PySpark pipeline.
Coordinates extraction, transformation, and loading phases with error handling.
"""

from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict, Any, Optional
from dataclasses import dataclass
import time

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader, LoadResult
from src.logger import ETLLogger


@dataclass
class ETLResult:
    """Result of ETL execution."""
    run_id: str
    status: str
    start_time: datetime
    end_time: datetime
    duration: int
    records_extracted: int
    records_transformed: int
    records_loaded: int
    records_failed: int
    error_count: int
    warning_count: int


class ETLOrchestrator:
    """Orchestrates the complete ETL pipeline."""
    
    def __init__(self, spark: SparkSession, run_type: str = "MANUAL"):
        """
        Initialize the ETL orchestrator.
        
        Args:
            spark: SparkSession instance
            run_type: Type of run (MANUAL, SCHEDULED, INCREMENTAL)
        """
        self.spark = spark
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        self.logger = ETLLogger.get_instance()
    
    def _generate_run_id(self) -> str:
        """Generate a unique run ID."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"RUN_{timestamp}_{self.run_type}"
    
    def execute_etl(
        self,
        source_type: str = "DATABASE",
        target_type: str = "DATABASE",
        filter_condition: Optional[str] = None,
        batch_size: int = 1000,
        max_records: int = 0
    ) -> ETLResult:
        """
        Execute the complete ETL pipeline.
        
        Args:
            source_type: Source system type
            target_type: Target system type
            filter_condition: Optional filter for extraction
            batch_size: Batch size for loading
            max_records: Maximum records to process
            
        Returns:
            ETLResult with execution statistics
        """
        self.logger.log_info(
            component="ORCHESTRATOR",
            message=f"ETL execution started - Run ID: {self.run_id}"
        )
        
        start_time = datetime.now()
        start_timestamp = time.time()
        
        result = ETLResult(
            run_id=self.run_id,
            status="RUNNING",
            start_time=start_time,
            end_time=start_time,
            duration=0,
            records_extracted=0,
            records_transformed=0,
            records_loaded=0,
            records_failed=0,
            error_count=0,
            warning_count=0
        )
        
        try:
            # Log run start
            self._log_run_start(start_time)
            
            # Step 1: Extract
            extractor = DataExtractor(self.spark, source_type, self.run_id)
            source_df = extractor.extract_data(filter_condition, max_records)
            result.records_extracted = source_df.count()
            
            if result.records_extracted == 0:
                self.logger.log_warning(
                    component="ORCHESTRATOR",
                    message="No data extracted - ETL process stopping"
                )
                result.status = "NO_DATA"
                result.end_time = datetime.now()
                result.duration = int(time.time() - start_timestamp)
                self._log_run_end(result)
                return result
            
            # Step 2: Transform
            transformer = DataTransformer(self.spark, self.run_id)
            transformed_df = transformer.transform_data(source_df)
            
            # Validate
            is_valid, validation_errors = transformer.validate_data(transformed_df)
            if not is_valid:
                self.logger.log_error(
                    component="ORCHESTRATOR",
                    message=f"Validation failed - {len(validation_errors)} errors",
                    details="; ".