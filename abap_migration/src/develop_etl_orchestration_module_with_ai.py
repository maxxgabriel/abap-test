===FILE: src/extract.py===
"""
ETL Data Extraction Module
Migrated from ABAP extractor (zcl_etl_extractor)
"""
from typing import Optional, List, Dict, Any
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
import logging


class ETLExtractor:
    """
    Extract data from various sources with comprehensive error handling.
    Supports full, incremental, and staging-based extraction modes.
    """
    
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
    
    def __init__(
        self,
        spark: SparkSession,
        run_id: str,
        source_type: str = "DATABASE",
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize extractor.
        
        Args:
            spark: Active SparkSession
            run_id: Unique run identifier
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.source_type = source_type.upper()
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def extract_data(
        self,
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Main extraction method with routing logic.
        
        Args:
            filter_condition: Optional SQL filter condition
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
            
        Raises:
            ValueError: If source type is invalid
            RuntimeError: If extraction fails
        """
        self.logger.info(
            f"Starting extraction - Run ID: {self.run_id}, "
            f"Source: {self.source_type}, Filter: {filter_condition}"
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
                raise ValueError(f"Invalid source type: {self.source_type}")
            
            # Apply record limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.info(f"Extracted {record_count} records successfully")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}", exc_info=True)
            raise RuntimeError(f"Extraction failed: {str(e)}") from e
    
    def _extract_from_database(
        self,
        filter_condition: Optional[str] = None
    ) -> DataFrame:
        """
        Extract from source database table.
        
        Args:
            filter_condition: Optional WHERE clause condition
            
        Returns:
            DataFrame with source data
        """
        source_table = self.config.get("source_table", "source_data")
        jdbc_url = self.config.get("jdbc_url")
        
        if jdbc_url:
            # JDBC extraction
            query = f"(SELECT * FROM {source_table}"
            if filter_condition:
                query += f" WHERE {filter_condition}"
            query += " LIMIT 1000) AS source"
            
            df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", query) \
                .option("user", self.config.get("db_user", "")) \
                .option("password", self.config.get("db_password", "")) \
                .load()
        else:
            # File-based extraction
            source_path = self.config.get("source_path", f"data/input/{source_table}")
            df = self.spark.read \
                .format(self.config.get("source_format", "parquet")) \
                .schema(self.SCHEMA) \
                .load(source_path)
            
            if filter_condition:
                df = df.filter(filter_condition)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """
        Extract from staging area by run ID.
        
        Returns:
            DataFrame with staged data
        """
        staging_path = self.config.get("staging_path", "data/staging")
        
        df = self.spark.read \
            .format("parquet") \
            .schema(self.SCHEMA) \
            .load(f"{staging_path}/run_id={self.run_id}") \
            .filter("status = 'READY'")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run.
        
        Returns:
            DataFrame with incremental data
        """
        # Get last successful run timestamp
        last_run_time = self._get_last_run_timestamp()
        
        if last_run_time is None:
            self.logger.warning("No previous run found, performing full extract")
            return self._extract_from_database()
        
        # Extract records changed since last run
        source_table = self.config.get("source_table", "source_data")
        source_path = self.config.get("source_path", f"data/input/{source_table}")
        
        df = self.spark.read \
            .format(self.config.get("source_format", "parquet")) \
            .schema(self.SCHEMA) \
            .load(source_path) \
            .filter(f"changed_at > timestamp('{last_run_time}')")
        
        return df
    
    def _get_last_run_timestamp(self) -> Optional[str]:
        """
        Get timestamp of last successful ETL run.
        
        Returns:
            Timestamp string or None if no previous run
        """
        try:
            log_path = self.config.get("log_path", "data/logs/run_log")
            
            df = self.spark.read \
                .format("parquet") \
                .load(log_path) \
                .filter("status = 'SUCCESS'") \
                .orderBy("end_time", ascending=False) \
                .limit(1)
            
            if df.count() > 0:
                return df.select("end_time").first()["end_time"]
            return None
            
        except Exception as e:
            self.logger.warning(f"Could not retrieve last run time: {e}")
            return None


===FILE: src/transform.py===
"""
ETL Data Transformation Module
Migrated from ABAP transformer (zcl_etl_transformer)
"""
from typing import Tuple, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, current_timestamp,
    current_user, lit, udf
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    TimestampType, IntegerType
)
import logging


class ETLTransformer:
    """
    Transform data with business rules, enrichment, and validation.
    """
    
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
    
    def __init__(self, spark: SparkSession, run_id: str, config: dict = None):
        """
        Initialize transformer.
        
        Args:
            spark: Active SparkSession
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply all transformations to source data.
        
        Args:
            source_df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info(f"Starting transformation for run {self.run_id}")
        
        # Basic transformations
        df = source_df.select(
            col("id"),
            upper(trim(regexp_replace(col("name"), r"\s+", " "))).alias("name"),
            col("value"),
            col("category"),
            col("status")
        )
        
        # Calculate derived values
        df = self._calculate_derived_values(df)
        
        # Calculate priority
        df = self._calculate_priority(df)
        
        # Apply category-specific rules
        df = self._apply_category_rules(df)
        
        # Add metadata
        df = df.withColumn("etl_run_id", lit(self.run_id)) \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("processed_by", lit(current_user())) \
            .withColumn("status", lit("TRANSFORMED"))
        
        # Apply business rules
        df = self._apply_business_rules(df)
        
        # Enrich data
        df = self._enrich_data(df)
        
        record_count = df.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return df
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate transformed values based on category.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with transformed_value column
        """
        # Premium category gets 1.5x multiplier, others 1.2x
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("value") * 1.5)
            .when(col("category") == "VIP", col("value") * 1.8)
            .otherwise(col("value") * 1.2)
        )
        
        return df
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """
        Calculate priority based on value and category.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with priority column
        """
        df = df.withColumn(
            "priority",
            when(col("value") >= 1000, 1)
            .when(col("value") >= 750, 2)
            .when(col("value") >= 500, 3)
            .when(col("value") >= 300, 4)
            .otherwise(5)
        )
        
        # Override for VIP category
        df = df.withColumn(
            "priority",
            when(col("category") == "VIP", 1)
            .otherwise(col("priority"))
        )
        
        return df
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply category-specific transformation rules.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        # Set default category for null values
        df = df.withColumn(
            "category",
            when(col("category").isNull(), lit("UNCATEGORIZED"))
            .otherwise(col("category"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to transformed data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with business rules applied
        """
        # Rule 1: Set status based on transformed value
        df = df.withColumn(
            "status",
            when(col("value").isNull(), "INVALID")
            .when(col("transformed_value") >= 750, "HIGH_VALUE")
            .when(col("transformed_value") >= 300, "MEDIUM_VALUE")
            .otherwise("LOW_VALUE")
        )
        
        # Rule 2: Override priority for high value items
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, 1)
            .otherwise(col("priority"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional calculated fields.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Enriched DataFrame
        """
        # Load enrichment config if available
        premium_multiplier = self.config.get("premium_multiplier", 1.2)
        
        # Apply premium enrichment
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM",
                 col("transformed_value") * premium_multiplier)
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def validate_data(
        self,
        df: DataFrame
    ) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of error messages)
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
        negative_values = df.filter(col("value") < 0).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative values")
        
        # Check priority range
        invalid_priority = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        # Check category
        null_category = df.filter(col("category").isNull()).count()
        if null_category > 0:
            errors.append(f"{null_category} records with null category")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Data validation passed")
        else:
            self.logger.error(f"Data validation failed: {errors}")
        
        return is_valid, errors


===FILE: src/load.py===
"""
ETL Data Loading Module
Migrated from ABAP loader (zcl_etl_loader)
"""
from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import logging


class LoadResult:
    """Result of load operation."""
    
    def __init__(
        self,
        success_count: int = 0,
        error_count: int = 0,
        total_count: int = 0,
        errors: List[str] = None
    ):
        self.success_count = success_count
        self.error_count = error_count
        self.total_count = total_count
        self.errors = errors or []


class ETLLoader:
    """
    Load transformed data to target with batch processing and reconciliation.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        run_id: str,
        target_type: str = "DATABASE",
        batch_size: int = 1000,
        config: Dict[str, Any] = None
    ):
        """
        Initialize loader.
        
        Args:
            spark: Active SparkSession
            run_id: Unique run identifier
            target_type: Type of target (DATABASE, FILE)
            batch_size: Records per batch
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.target_type = target_type.upper()
        self.batch_size = batch_size
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def load_data(
        self,
        df: DataFrame,
        mode: str = "INSERT"
    ) -> LoadResult:
        """
        Load data to target.
        
        Args:
            df: DataFrame to load
            mode: Load mode (INSERT, UPDATE, UPSERT)
            
        Returns:
            LoadResult with statistics
        """
        self.logger.info(
            f"Starting load - Run ID: {self.run_id}, "
            f"Mode: {mode}, Batch size: {self.batch_size}"
        )
        
        total_count = df.count()
        success_count = 0
        error_count = 0
        errors = []
        
        try:
            if self.target_type == "DATABASE":
                success = self._load_to_database(df, mode)
                if success:
                    success_count = total_count
                else:
                    error_count = total_count
                    errors.append("Database load failed")
            else:
                success = self._load_to_file(df, mode)
                if success:
                    success_count = total_count
                else:
                    error_count = total_count
                    errors.append("File write failed")
            
            # Reconciliation
            if self.config.get("enable_reconciliation", True):
                reconciled = self._reconcile_data(df)
                if not reconciled:
                    self.logger.warning("Data reconciliation failed")
            
            self.logger.info(
                f"Load complete - Success: {success_count}, "
                f"Errors: {error_count}"
            )
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}", exc_info=True)
            error_count = total_count
            errors.append(str(e))
        
        return LoadResult(
            success_count=success_count,
            error_count=error_count,
            total_count=total_count,
            errors=errors
        )
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to database via JDBC.
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            True if successful
        """
        try:
            jdbc_url = self.config.get("jdbc_url")
            target_table = self.config.get("target_table", "target_data")
            
            # Map mode to Spark save mode
            save_mode = {
                "INSERT": "append",
                "UPDATE": "overwrite",
                "UPSERT": "append"  # Handled by merge logic
            }.get(mode.upper(), "append")
            
            if jdbc_url:
                df.write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", target_table) \
                    .option("user", self.config.get("db_user", "")) \
                    .option("password", self.config.get("db_password", "")) \
                    .option("batchsize", self.batch_size) \
                    .mode(save_mode) \
                    .save()
            else:
                # Delta Lake upsert
                self._delta_upsert(df, target_table, mode)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Database load error: {str(e)}")
            return False
    
    def _delta_upsert(self, df: DataFrame, target_table: str, mode: str):
        """
        Perform Delta Lake upsert operation.
        
        Args:
            df: Source DataFrame
            target_table: Target table path
            mode: Load mode
        """
        target_path = self.config.get("target_path", f"data/output/{target_table}")
        
        if mode.upper() == "UPSERT":
            # Use Delta merge for upsert
            from delta.tables import DeltaTable
            
            if DeltaTable.isDeltaTable(self.spark, target_path):
                delta_table = DeltaTable.forPath(self.spark, target_path)
                
                delta_table.alias("target").merge(
                    df.alias("source"),
                    "target.id = source.id"
                ).whenMatchedUpdateAll() \
                 .whenNotMatchedInsertAll() \
                 .execute()
            else:
                # First load - create table
                df.write.format("delta").mode("overwrite").save(target_path)
        else:
            # Standard write
            save_mode = "append" if mode.upper() == "INSERT" else "overwrite"
            df.write.format("delta").mode(save_mode).save(target_path)
    
    def _load_to_file(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to file system.
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            True if successful
        """
        try:
            target_path = self.config.get(
                "target_path",
                f"data/output/run_id={self.run_id}"
            )
            target_format = self.config.get("target_format", "parquet")
            
            save_mode = "append" if mode.upper() == "INSERT" else "overwrite"
            
            df.write \
                .format(target_format) \
                .mode(save_mode) \
                .option("compression", "snappy") \
                .save(target_path)
            
            return True
            
        except Exception as e:
            self.logger.error(f"File write error: {str(e)}")
            return False
    
    def _reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data with source.
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passes
        """
        try:
            source_count = loaded_df.count()
            
            target_path = self.config.get("target_path", "data/output")
            target_df = self.spark.read.format("delta").load(target_path)
            
            # Filter to this run's data
            target_count = target_df.filter(
                col("etl_run_id") == self.run_id
            ).count()
            
            matches = source_count == target_count
            
            if matches:
                self.logger.info(
                    f"Reconciliation passed: {source_count} records match"
                )
            else:
                self.logger.warning(
                    f"Reconciliation mismatch - "
                    f"Source: {source_count}, Target: {target_count}"
                )
            
            return matches
            
        except Exception as e:
            self.logger.error(f"Reconciliation error: {str(e)}")
            return False


===FILE: src/orchestrator.py===
"""
ETL Orchestration Module
Main coordinator for extract, transform, load pipeline
Migrated from ABAP orchestrator (zcl_etl_orchestrator)
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
import logging
import uuid

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader, LoadResult
from src.quality import DataQualityChecker


@dataclass
class ETLResult:
    """Result of ETL execution."""
    run_id: str
    status: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: int = 0
    records_extracted: int = 0
    records_transformed: int = 0
    records_loaded: int = 0
    records_failed: int = 0
    error_count: int = 0
    warning_count: int = 0
    errors: list = field(default_factory=list)


class ETLOrchestrator:
    """
    Orchestrate complete ETL pipeline with comprehensive error handling.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Dict[str, Any],
        run_type: str = "MANUAL"
    ):
        """
        Initialize orchestrator.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            run_type: Type of run (MANUAL, SCHEDULED, TEST)
        """
        self.spark = spark
        self.config = config
        self.run_type = run_type.upper()
        self.run_id = self._generate_run_id()
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def _generate_run_id(self) -> str:
        """Generate unique run identifier."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"RUN_{timestamp}_{unique_id}"
    
    def execute_etl(
        self,
        source_type: str = "DATABASE",
        target_type: str = "DATABASE",
        filter_condition: Optional[str] = None,
        batch_size: int = 1000,
        max_records: int = 0
    ) -> ETLResult:
        """
        Execute complete ETL pipeline.
        
        Args:
            source_type: Type of data source
            target_type: Type of target
            filter_condition: Optional filter for extraction
            batch_size: Batch size for loading
            max_records: Maximum records to process (0 = unlimited)
            
        Returns:
            ETLResult with execution statistics
        """
        start_time = datetime.now()
        result = ETLResult(
            run_id=self.run_id,
            status="RUNNING",
            start_time=start_time
        )
        
        self.logger.info(
            f"ETL execution started - Run ID: {self.run_id}, "
            f"Type: {self.run_type}"
        )
        
        try:
            # Log run start
            self._log_run_start(start_time)
            
            # Step 1: Extract
            extractor = ETLExtractor(
                spark=self.spark,
                run_id=self.run_id,
                source_type=source_type,
                config=self.config
            )
            
            source_df = extractor.extract_data(
                filter_condition=filter_condition,
                max_records=max_records
            )
            
            result.records_extracted = source_df.count()
            
            if result.records_extracted == 0:
                self.logger.warning("No data extracted - stopping ETL process")
                result.status = "NO_DATA"
                result.end_time = datetime.now()
                result.duration_seconds = (
                    result.end_time - start_time
                ).seconds
                return result
            
            # Step 2: Transform
            transformer = ETLTransformer(
                spark=self.spark,
                run_id=self.run_id,
                config=self.config
            )
            
            transformed_df = transformer.transform_data(source_df)
            result.records_transformed = transformed_df.count()
            
            # Validate
            is_valid, validation_errors = transformer.validate_data(
                transformed_df
            )
            
            if not is_valid:
                self.logger.error(
                    f"Validation failed - {len(validation_errors)} errors"
                )
                result.status = "VALIDATION_FAILED"
                result.error_count = len(validation_errors)
                result.errors = validation_errors
                result.end_time = datetime.now()
                result.duration_seconds = (
                    result.end_time - start_time
                ).seconds
                return result
            
            # Data Quality Checks (optional)
            if self.config.get("enable_quality_checks", True):
                quality_checker = DataQualityChecker(
                    spark=self.spark,
                    run_id=self.run_id,
                    config=self.config
                )
                
                quality_passed, quality_report = (
                    quality_checker.perform_quality_checks(transformed_df)
                )
                
                if not quality_passed:
                    result.warning_count += 1
                    self.logger.warning(
                        f"Quality checks failed: {quality_report}"
                    )
            
            # Step 3: Load
            loader = ETLLoader(
                spark=self.spark,
                run_id=self.run_id,
                target_type=target_type,
                batch_size=batch_size,
                config=self.config
            )
            
            load_result = loader.load_data(
                df=transformed_df,
                mode="UPSERT"
            )
            
            result.records_loaded = load_result.success_count
            result.records_failed = load_result.error_count
            result.error_count += load_result.error_count
            result.errors.extend(load_result.errors)
            
            # Determine final status
            if load_result.error_count == 0:
                result.status = "SUCCESS"
            elif load_result.success_count > 0:
                result.status = "PARTIAL_SUCCESS"
            else:
                result.status = "FAILED"
            
            # Calculate duration
            result.end_time = datetime.now()
            result.duration_seconds = (
                result.end_time - start_time
            ).seconds
            
            # Log run end
            self._log_run_end(result)
            
            self.logger.info(
                f"ETL execution completed - "
                f"Status: {result.status}, "
                f"Duration: {result.duration_seconds}s"
            )
            
        except Exception as e:
            self.logger.error(
                f"ETL execution failed: {str(e)}",
                exc_info=True
            )
            result.status = "ERROR"
            result.error_count += 1
            result.errors.append(str(e))
            result.end_time = datetime.now()
            result.duration_seconds = (
                result.end_time - start_time
            ).seconds
            
            self._log_run_end(result)
        
        return result
    
    def _log_run_start(self, start_time: datetime):
        """Log ETL run start to persistence."""
        try:
            log_data = [{
                "run_id": self.run_id,
                "run_type": self.run_type,
                "status": "RUNNING",
                "start_time": start_time,
                "end_time": None