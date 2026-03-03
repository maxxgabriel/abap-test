===FILE: src/extract.py===
"""
Delta Lake ETL - Extract Module
Extracts data from source systems with incremental and full load support
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


class DataExtractor:
    """Handles data extraction from various source systems"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.source_schema = self._define_source_schema()
    
    def _define_source_schema(self) -> StructType:
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
        filter_condition: Optional[str] = None,
        max_records: int = 0,
        run_id: Optional[str] = None
    ) -> DataFrame:
        """
        Main extraction method supporting multiple source types
        
        Args:
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            filter_condition: Optional filter to apply
            max_records: Maximum records to extract (0 = no limit)
            run_id: ETL run identifier
            
        Returns:
            DataFrame with extracted data
        """
        logger.info(f"Starting extraction - Source: {source_type}, Run ID: {run_id}")
        
        try:
            if source_type == "DATABASE":
                df = self._extract_from_database(filter_condition)
            elif source_type == "STAGING":
                df = self._extract_from_staging(run_id)
            elif source_type == "INCREMENTAL":
                df = self._extract_incremental()
            else:
                df = self._extract_from_database(filter_condition)
            
            # Apply max records limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract data from source database"""
        source_config = self.config['sources']['database']
        
        # Read from JDBC source
        df = (self.spark.read
              .format("jdbc")
              .option("url", source_config['jdbc_url'])
              .option("dbtable", source_config['table_name'])
              .option("user", source_config.get('username', ''))
              .option("password", source_config.get('password', ''))
              .option("driver", source_config.get('driver', 'org.postgresql.Driver'))
              .load())
        
        # Apply filter if provided
        if filter_condition:
            df = df.filter(filter_condition)
        
        return df
    
    def _extract_from_staging(self, run_id: str) -> DataFrame:
        """Extract data from staging area"""
        staging_config = self.config['sources']['staging']
        staging_path = f"{staging_config['base_path']}/run_id={run_id}"
        
        df = (self.spark.read
              .format(staging_config.get('format', 'parquet'))
              .schema(self.source_schema)
              .load(staging_path))
        
        # Filter for ready records
        df = df.filter(col("status") == "READY")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last run"""
        # Get last successful run timestamp
        last_run_time = self._get_last_successful_run_time()
        
        if last_run_time:
            logger.info(f"Extracting incremental data since {last_run_time}")
            df = self._extract_from_database()
            df = df.filter(col("changed_at") > lit(last_run_time))
        else:
            logger.warning("No previous run found, performing full extraction")
            df = self._extract_from_database()
        
        return df
    
    def _get_last_successful_run_time(self) -> Optional[datetime]:
        """Get timestamp of last successful ETL run"""
        try:
            run_log_path = self.config['metadata']['run_log_path']
            
            run_log_df = (self.spark.read
                         .format("delta")
                         .load(run_log_path))
            
            last_run = (run_log_df
                       .filter(col("status") == "SUCCESS")
                       .orderBy(col("end_time").desc())
                       .select("end_time")
                       .first())
            
            if last_run:
                return last_run['end_time']
            return None
            
        except Exception as e:
            logger.warning(f"Could not retrieve last run time: {str(e)}")
            return None
    
    def extract_from_csv(self, file_path: str) -> DataFrame:
        """Extract data from CSV files"""
        return (self.spark.read
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "false")
                .schema(self.source_schema)
                .load(file_path))
    
    def extract_from_json(self, file_path: str) -> DataFrame:
        """Extract data from JSON files"""
        return (self.spark.read
                .format("json")
                .schema(self.source_schema)
                .load(file_path))


===FILE: src/transform.py===
"""
Delta Lake ETL - Transform Module
Applies business rules, enrichment, and validation to extracted data
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, upper, trim, when, lit, current_timestamp,
    regexp_replace, concat_ws, coalesce, round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from typing import Dict, Any, List, Tuple
import logging

logger = logging.getLogger(__name__)


class DataTransformer:
    """Handles data transformation and business rule application"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.target_schema = self._define_target_schema()
    
    def _define_target_schema(self) -> StructType:
        """Define transformed data schema"""
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
            StructField("processed_by", StringType(), True)
        ])
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Main transformation method applying all business rules
        
        Args:
            source_df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame ready for loading
        """
        logger.info("Starting data transformation")
        
        try:
            # Initial transformations
            df = self._apply_basic_transformations(source_df)
            
            # Calculate derived values
            df = self._calculate_derived_values(df)
            
            # Apply business rules
            df = self._apply_business_rules(df)
            
            # Enrich data
            df = self._enrich_data(df)
            
            # Apply category-specific rules
            df = self._apply_category_rules(df)
            
            # Add metadata
            df = self._add_metadata(df)
            
            record_count = df.count()
            logger.info(f"Transformed {record_count} records")
            
            return df
            
        except Exception as e:
            logger.error(f"Transformation failed: {str(e)}")
            raise
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic data cleaning and normalization"""
        return (df
                # Normalize name: uppercase, trim, remove extra spaces
                .withColumn("name", upper(trim(col("name"))))
                .withColumn("name", regexp_replace(col("name"), "\\s+", " "))
                # Ensure category has a value
                .withColumn("category", coalesce(col("category"), lit("UNCATEGORIZED")))
                # Convert status to uppercase
                .withColumn("status", upper(coalesce(col("status"), lit("ACTIVE"))))
        )
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """Calculate transformed values based on business logic"""
        # Get multipliers from config
        multipliers = self.config['transformation']['value_multipliers']
        
        # Apply category-based transformation
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("value") * multipliers['premium'])
            .when(col("category") == "VIP", col("value") * multipliers['vip'])
            .when(col("category") == "STANDARD", col("value") * multipliers['standard'])
            .when(col("category") == "BASIC", col("value") * multipliers['basic'])
            .otherwise(col("value") * multipliers['default'])
        )
        
        # Round to 2 decimal places
        df = df.withColumn("transformed_value", spark_round(col("transformed_value"), 2))
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to categorize and prioritize data"""
        thresholds = self.config['transformation']['value_thresholds']
        
        # Rule 1: Set status based on transformed value
        df = df.withColumn(
            "status",
            when(col("value").isNull(), lit("INVALID"))
            .when(col("transformed_value") >= thresholds['high'], lit("HIGH_VALUE"))
            .when(col("transformed_value") >= thresholds['medium'], lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )
        
        # Rule 2: Calculate priority (1-5, 1 is highest)
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, lit(1))
            .when(col("transformed_value") >= thresholds['high'], lit(2))
            .when(col("transformed_value") >= thresholds['medium'], lit(3))
            .when(col("transformed_value") >= thresholds['low'], lit(4))
            .otherwise(lit(5))
        )
        
        # Rule 3: Priority override for VIP category
        df = df.withColumn(
            "priority",
            when(col("category") == "VIP", lit(1))
            .otherwise(col("priority"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional attributes"""
        # Load enrichment configuration if available
        enrichment_config = self.config.get('enrichment', {})
        
        if enrichment_config.get('enabled', False):
            # Apply premium multiplier adjustment
            premium_multiplier = enrichment_config.get('premium_multiplier', 1.2)
            df = df.withColumn(
                "transformed_value",
                when(col("category") == "PREMIUM", 
                     col("transformed_value") * premium_multiplier)
                .otherwise(col("transformed_value"))
            )
        
        return df
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """Apply category-specific business rules"""
        # Category-specific adjustments
        df = (df
              # TRIAL category gets minimum priority
              .withColumn(
                  "priority",
                  when(col("category") == "TRIAL", lit(5))
                  .otherwise(col("priority"))
              )
              # VIP category status override
              .withColumn(
                  "status",
                  when(col("category") == "VIP", lit("VIP_PRIORITY"))
                  .otherwise(col("status"))
              )
        )
        
        return df
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """Add ETL metadata columns"""
        return (df
                .withColumn("etl_run_id", lit(self.run_id))
                .withColumn("processed_at", current_timestamp())
                .withColumn("processed_by", lit(self.config.get('user', 'etl_system')))
        )
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data against business rules
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        logger.info("Starting data validation")
        errors = []
        
        # Validation 1: Required fields
        null_ids = df.filter(col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null ID")
        
        null_names = df.filter(col("name").isNull() | (col("name") == "")).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null/empty name")
        
        # Validation 2: Value ranges
        negative_values = df.filter(col("value") < 0).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative value")
        
        # Validation 3: Priority range
        invalid_priority = df.filter((col("priority") < 1) | (col("priority") > 5)).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority (must be 1-5)")
        
        # Validation 4: Valid categories
        valid_categories = self.config['transformation']['valid_categories']
        invalid_categories = df.filter(~col("category").isin(valid_categories)).count()
        if invalid_categories > 0:
            errors.append(f"{invalid_categories} records with invalid category")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            logger.info("Data validation passed")
        else:
            logger.warning(f"Data validation failed with {len(errors)} errors")
            for error in errors:
                logger.warning(f"  - {error}")
        
        return is_valid, errors


===FILE: src/load.py===
"""
Delta Lake ETL - Load Module
Loads data into Delta Lake with merge/upsert operations
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit
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
    errors: list
    mode: str
    duration_seconds: float = 0.0


class DeltaLakeLoader:
    """Handles loading data into Delta Lake with merge operations"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.target_path = config['target']['delta_table_path']
        self.batch_size = config['load']['batch_size']
    
    def load_data(
        self,
        df: DataFrame,
        mode: str = "UPSERT",
        partition_cols: Optional[list] = None
    ) -> LoadResult:
        """
        Main load method supporting multiple write modes
        
        Args:
            df: DataFrame to load
            mode: Load mode (INSERT, UPDATE, UPSERT, APPEND, OVERWRITE)
            partition_cols: Optional list of columns to partition by
            
        Returns:
            LoadResult with operation statistics
        """
        logger.info(f"Starting load - Mode: {mode}, Target: {self.target_path}")
        
        start_time = current_timestamp()
        total_count = df.count()
        errors = []
        
        try:
            if mode == "UPSERT":
                success_count = self._upsert_data(df, partition_cols)
            elif mode == "INSERT":
                success_count = self._insert_data(df, partition_cols)
            elif mode == "UPDATE":
                success_count = self._update_data(df)
            elif mode == "APPEND":
                success_count = self._append_data(df, partition_cols)
            elif mode == "OVERWRITE":
                success_count = self._overwrite_data(df, partition_cols)
            else:
                raise ValueError(f"Unsupported load mode: {mode}")
            
            error_count = total_count - success_count
            
            # Reconcile data
            if self.config['load'].get('enable_reconciliation', True):
                reconcile_success = self._reconcile_data(df)
                if not reconcile_success:
                    logger.warning("Data reconciliation check failed")
                    errors.append("Reconciliation mismatch detected")
            
            # Optimize Delta table
            if self.config['load'].get('optimize_after_load', True):
                self._optimize_table()
            
            result = LoadResult(
                success_count=success_count,
                error_count=error_count,
                total_count=total_count,
                errors=errors,
                mode=mode
            )
            
            logger.info(f"Load complete - Success: {success_count}, Errors: {error_count}")
            return result
            
        except Exception as e:
            logger.error(f"Load failed: {str(e)}")
            return LoadResult(
                success_count=0,
                error_count=total_count,
                total_count=total_count,
                errors=[str(e)],
                mode=mode
            )
    
    def _upsert_data(self, df: DataFrame, partition_cols: Optional[list]) -> int:
        """Perform merge/upsert operation"""
        logger.info("Performing UPSERT operation")
        
        # Check if table exists
        if not self._table_exists():
            logger.info("Target table does not exist, creating with initial data")
            return self._create_initial_table(df, partition_cols)
        
        # Load existing Delta table
        delta_table = DeltaTable.forPath(self.spark, self.target_path)
        
        # Define merge condition
        merge_condition = "target.id = source.id"
        
        # Perform merge
        (delta_table.alias("target")
         .merge(
             df.alias("source"),
             merge_condition
         )
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
        
        return df.count()
    
    def _insert_data(self, df: DataFrame, partition_cols: Optional[list]) -> int:
        """Insert new records only (fail on duplicates)"""
        logger.info("Performing INSERT operation")
        
        if not self._table_exists():
            return self._create_initial_table(df, partition_cols)
        
        # Check for duplicates
        delta_table = DeltaTable.forPath(self.spark, self.target_path)
        existing_ids = delta_table.toDF().select("id").distinct()
        
        # Filter out existing IDs
        new_records = df.join(existing_ids, "id", "left_anti")
        
        if new_records.count() == 0:
            logger.warning("No new records to insert (all IDs already exist)")
            return 0
        
        # Append new records
        (new_records.write
         .format("delta")
         .mode("append")
         .save(self.target_path))
        
        return new_records.count()
    
    def _update_data(self, df: DataFrame) -> int:
        """Update existing records only"""
        logger.info("Performing UPDATE operation")
        
        if not self._table_exists():
            raise ValueError("Cannot UPDATE: target table does not exist")
        
        delta_table = DeltaTable.forPath(self.spark, self.target_path)
        
        # Only update matching records
        (delta_table.alias("target")
         .merge(
             df.alias("source"),
             "target.id = source.id"
         )
         .whenMatchedUpdateAll()
         .execute())
        
        return df.count()
    
    def _append_data(self, df: DataFrame, partition_cols: Optional[list]) -> int:
        """Append data without checking for duplicates"""
        logger.info("Performing APPEND operation")
        
        writer = df.write.format("delta").mode("append")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        writer.save(self.target_path)
        
        return df.count()
    
    def _overwrite_data(self, df: DataFrame, partition_cols: Optional[list]) -> int:
        """Overwrite entire table"""
        logger.info("Performing OVERWRITE operation")
        
        writer = df.write.format("delta").mode("overwrite")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        writer.save(self.target_path)
        
        return df.count()
    
    def _create_initial_table(self, df: DataFrame, partition_cols: Optional[list]) -> int:
        """Create Delta table with initial data"""
        logger.info(f"Creating new Delta table at {self.target_path}")
        
        writer = (df.write
                  .format("delta")
                  .mode("overwrite"))
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        writer.save(self.target_path)
        
        return df.count()
    
    def _table_exists(self) -> bool:
        """Check if Delta table exists"""
        try:
            DeltaTable.forPath(self.spark, self.target_path)
            return True
        except:
            return False
    
    def _reconcile_data(self, df: DataFrame) -> bool:
        """Reconcile loaded data with source"""
        logger.info("Performing data reconciliation")
        
        try:
            # Load target table
            target_df = self.spark.read.format("delta").load(self.target_path)
            
            # Filter for current run
            target_run_df = target_df.filter(col("etl_run_id") == self.run_id)
            
            # Compare counts
            source_count = df.count()
            target_count = target_run_df.count()
            
            if source_count != target_count:
                logger.warning(f"Reconciliation mismatch: source={source_count}, target={target_count}")
                return False
            
            # Sample comparison of key fields
            source_ids = set(df.select("id").rdd.flatMap(lambda x: x).collect())
            target_ids = set(target_run_df.select("id").rdd.flatMap(lambda x: x).collect())
            
            if source_ids != target_ids:
                logger.warning("Reconciliation mismatch: ID sets differ")
                return False
            
            logger.info("Reconciliation successful")
            return True
            
        except Exception as e:
            logger.error(f"Reconciliation failed: {str(e)}")
            return False
    
    def _optimize_table(self):
        """Optimize Delta table (compaction, Z-ordering)"""
        logger.info("Optimizing Delta table")
        
        try:
            delta_table = DeltaTable.forPath(self.spark, self.target_path)
            
            # Compact small files
            delta_table.optimize().executeCompaction()
            
            # Z-order by frequently filtered columns if configured
            zorder_cols = self.config['load'].get('zorder_columns', [])
            if zorder_cols:
                delta_table.optimize().executeZOrderBy(*zorder_cols)
            
            logger.info("Table optimization complete")
            
        except Exception as e:
            logger.warning(f"Table optimization failed: {str(e)}")
    
    def vacuum_table(self, retention_hours: int = 168):
        """Remove old versions of Delta table files"""
        logger.info(f"Vacuuming Delta table (retention: {retention_hours} hours)")
        
        try:
            delta_table = DeltaTable.forPath(self.spark, self.target_path)
            delta_table.vacuum(retention_hours)
            logger.info("Vacuum complete")
        except Exception as e:
            logger.error(f"Vacuum failed: {str(e)}")


===FILE: src/orchestrator.py===
"""
Delta Lake ETL - Orchestrator Module
Coordinates the complete ETL pipeline execution
"""

from pyspark.sql import SparkSession
from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DeltaLakeLoader, LoadResult
from datetime import datetime
from typing import Dict, Any, Optional
from dataclasses import dataclass
import logging
import uuid

logger = logging.getLogger(__name__)


@dataclass
class ETLResult:
    """Complete ETL execution result"""
    run_id: str
    status: str
    start_time: datetime
    end_time: Optional[datetime]
    duration_seconds: float
    records_extracted: int
    records_transformed: int
    records_loaded: int
    records_failed: int
    error_count: int
    warning_count: int
    errors: list


class ETLOrchestrator:
    """Orchestrates the complete ETL pipeline"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.run_id = self._generate_run_id()
        self.extractor = DataExtractor(spark, config)
        self.transformer = DataTransformer(spark, config, self.run_id)
        self.loader = DeltaLakeLoader(spark, config, self.run_id)
    
    def _generate_run_id(self) -> str:
        """Generate unique run ID"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"RUN_{timestamp}_{unique_id}"
    
    def execute_etl(
        self,
        source_type: str = "DATABASE",
        load_mode: str = "UPSERT",
        filter_condition: Optional[str] = None,
        max_records: int = 0,
        partition_cols: Optional[list] = None
    ) -> ETLResult:
        """
        Execute complete ETL pipeline
        
        Args:
            source_type: Type of data source
            load_mode: Delta Lake write mode
            filter_condition: Optional filter for extraction
            max_records: Maximum records to process
            partition_cols: Columns to partition Delta table by
            
        Returns:
            ETLResult with execution statistics
        """
        start_time = datetime.now()
        logger.info(f"=== ETL Pipeline Started === Run ID: {self.run_id}")
        
        result = ETLResult(
            run_id=self.run_id,
            status="RUNNING",
            start_time=start_time,
            end_time=None,
            duration_seconds=0.0,
            records_extracted=0,
            records_transformed=0,
            records_loaded=0,
            records_failed=0,
            error_count=0,
            warning_count=0,
            errors=[]
        )
        
        try:
            # Step 1: Extract
            logger.info("Step 1/3: Extracting data")
            source_df = self.extractor.extract_data(
                source_type=source_type,
                filter_condition=filter_condition,
                max_records=max_records,
                run_id=self.run_id
            )
            
            result.records_extracted = source_df.count()
            
            if result.records_extracted == 0:
                logger.warning("No data extracted - pipeline stopping")
                result.status = "NO_DATA"
                result.end_time = datetime.now()
                result.duration_seconds = (result.end_time - start_time).total_seconds()
                return result
            
            # Step 2: Transform
            logger.info("Step 2/3: Transforming data")
            transformed_df = self.transformer.transform_data(source_df)
            result.records_transformed = transformed_df.count()
            
            # Validate
            is_valid, validation_errors = self.transformer.validate_data(transformed_df)
            if not is_valid:
                logger.error(f"Validation failed with {len(validation_errors)} errors")
                result.status = "VALIDATION_FAILED"
                result.error_count = len(validation_errors)
                result.errors = validation_errors
                result.end_time = datetime.now()
                result.duration_seconds = (result.end_time - start_time).total_seconds()
                return result
            
            # Step 3: Load
            logger.info("Step 3/3: Loading data to Delta Lake")
            load_result = self.loader.load_data(
                df=transformed_df,
                mode=load_mode,
                partition_cols=partition_cols or self.config['load'].get('partition_columns', [])
            )
            
            result.records_loaded = load_result.success_count
            result.records_failed = load_result.error_count
            result.error_count = load_result.error_count
            result.errors = load_result.errors
            
            # Determine final status
            if load_result.error_count == 0:
                result.status = "SUCCESS"
            elif load_result.success_count > 0:
                result.status = "PARTIAL_SUCCESS"
            else:
                result.status = "FAILED"
            
            result.end_time = datetime.now()
            result.duration_seconds = (result.end_time - start_time).total_seconds()
            
            # Log run to metadata
            self._log_run(result)
            
            logger.info(f"=== ETL Pipeline Completed === Status: {result.status}")
            logger.info(f"Duration: {result.duration_seconds:.2f}s, "
                       f"Loaded: {result.records_loaded}, Failed: {result.records_failed}")
            
            return result