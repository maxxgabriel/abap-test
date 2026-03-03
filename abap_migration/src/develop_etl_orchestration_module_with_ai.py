===FILE: src/extract.py===
"""
ETL Extractor Module - PySpark Implementation
Extracts data from various sources with comprehensive error handling
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from pyspark.sql import functions as F
from typing import Optional, Dict, Any
import logging
from datetime import datetime
from src.logger import ETLLogger


class ETLExtractor:
    """
    Handles data extraction from multiple sources
    Supports: Database, Staging, Incremental modes
    """
    
    def __init__(self, spark: SparkSession, source_type: str = "DATABASE", 
                 run_id: str = None, config: Dict[str, Any] = None):
        """
        Initialize extractor with Spark session and configuration
        
        Args:
            spark: Active SparkSession
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            run_id: Unique identifier for this ETL run
            config: Configuration dictionary
        """
        self.spark = spark
        self.source_type = source_type.upper()
        self.run_id = run_id or self._generate_run_id()
        self.config = config or {}
        self.logger = ETLLogger.get_instance()
        
        # Define schema for source data
        self.source_schema = StructType([
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
    
    def _generate_run_id(self) -> str:
        """Generate unique run ID with timestamp"""
        return f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def extract_data(self, filter_condition: Optional[str] = None, 
                    max_records: int = 0) -> DataFrame:
        """
        Main extraction method - routes to appropriate extraction strategy
        
        Args:
            filter_condition: SQL WHERE clause for filtering
            max_records: Maximum number of records to extract (0 = unlimited)
            
        Returns:
            Extracted DataFrame
            
        Raises:
            Exception: For extraction failures
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}"
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
                self.logger.log_warning(
                    component="EXTRACTOR",
                    message=f"Unknown source type {self.source_type}, defaulting to DATABASE"
                )
                df = self._extract_from_database(filter_condition)
            
            # Apply record limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.log_info(
                component="EXTRACTOR",
                message=f"Extracted {record_count} records successfully"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="EXTRACTOR",
                message="Extraction failed",
                details=str(e)
            )
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract data from database source
        
        Args:
            filter_condition: Optional SQL WHERE clause
            
        Returns:
            DataFrame with extracted data
        """
        source_table = self.config.get("source_table", "etl_source_data")
        jdbc_url = self.config.get("jdbc_url")
        
        if jdbc_url:
            # JDBC connection
            df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", source_table) \
                .option("user", self.config.get("db_user")) \
                .option("password", self.config.get("db_password")) \
                .option("driver", self.config.get("jdbc_driver", "org.postgresql.Driver")) \
                .load()
        else:
            # Parquet or other file source
            source_path = self.config.get("source_path", f"data/source/{source_table}")
            df = self.spark.read.parquet(source_path)
        
        # Apply filter if provided
        if filter_condition:
            df = df.filter(filter_condition)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """
        Extract data from staging area for this run
        
        Returns:
            DataFrame with staged data
        """
        staging_path = self.config.get("staging_path", "data/staging")
        
        df = self.spark.read.parquet(f"{staging_path}/run_id={self.run_id}") \
            .filter(F.col("status") == "READY")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run
        
        Returns:
            DataFrame with incremental data
        """
        # Get last successful run timestamp
        last_run_time = self._get_last_run_timestamp()
        
        if last_run_time is None:
            self.logger.log_warning(
                component="EXTRACTOR",
                message="No previous run found, performing full extraction"
            )
            return self._extract_from_database()
        
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Extracting incremental data since {last_run_time}"
        )
        
        # Extract only changed records
        df = self._extract_from_database()
        df = df.filter(F.col("changed_at") > F.lit(last_run_time))
        
        return df
    
    def _get_last_run_timestamp(self) -> Optional[datetime]:
        """
        Retrieve timestamp of last successful ETL run
        
        Returns:
            Timestamp of last run or None if no previous run
        """
        try:
            run_log_path = self.config.get("run_log_path", "data/run_log")
            
            df = self.spark.read.parquet(run_log_path) \
                .filter(F.col("status") == "SUCCESS") \
                .orderBy(F.col("end_time").desc()) \
                .limit(1)
            
            if df.count() > 0:
                return df.select("end_time").first()["end_time"]
            
            return None
            
        except Exception as e:
            self.logger.log_warning(
                component="EXTRACTOR",
                message=f"Could not retrieve last run timestamp: {str(e)}"
            )
            return None
    
    def extract_with_metadata(self, filter_condition: Optional[str] = None, 
                             max_records: int = 0) -> DataFrame:
        """
        Extract data with additional metadata columns
        
        Args:
            filter_condition: Optional filter
            max_records: Record limit
            
        Returns:
            DataFrame with metadata columns added
        """
        df = self.extract_data(filter_condition, max_records)
        
        # Add metadata columns
        df = df.withColumn("extraction_timestamp", F.current_timestamp()) \
               .withColumn("etl_run_id", F.lit(self.run_id)) \
               .withColumn("source_type", F.lit(self.source_type))
        
        return df


===FILE: src/transform.py===
"""
ETL Transformer Module - PySpark Implementation
Applies business rules, data quality checks, and transformations
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Dict, Any, Tuple
from datetime import datetime
from src.logger import ETLLogger


class ETLTransformer:
    """
    Handles all data transformations and business rule applications
    """
    
    def __init__(self, spark: SparkSession, run_id: str, config: Dict[str, Any] = None):
        """
        Initialize transformer
        
        Args:
            spark: Active SparkSession
            run_id: ETL run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config or {}
        self.logger = ETLLogger.get_instance()
        
        # Define transformed data schema
        self.transformed_schema = StructType([
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
        Main transformation pipeline
        
        Args:
            source_df: Source DataFrame from extraction
            
        Returns:
            Transformed DataFrame
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message=f"Starting transformation for {source_df.count()} records"
        )
        
        try:
            # Step 1: Basic transformations
            df = self._apply_basic_transformations(source_df)
            
            # Step 2: Calculate derived values
            df = self._calculate_derived_values(df)
            
            # Step 3: Calculate priority
            df = self._calculate_priority(df)
            
            # Step 4: Apply category-specific rules
            df = self._apply_category_rules(df)
            
            # Step 5: Apply business rules
            df = self._apply_business_rules(df)
            
            # Step 6: Enrich data
            df = self._enrich_data(df)
            
            # Step 7: Add metadata
            df = self._add_metadata(df)
            
            self.logger.log_info(
                component="TRANSFORMER",
                message=f"Transformation complete: {df.count()} records"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="TRANSFORMER",
                message="Transformation failed",
                details=str(e)
            )
            raise
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic data cleansing and normalization"""
        
        # Normalize name: uppercase and trim
        df = df.withColumn("name", 
                          F.upper(F.trim(F.regexp_replace(F.col("name"), "\\s+", " "))))
        
        # Ensure non-null values for critical fields
        df = df.fillna({
            "status": "UNKNOWN",
            "category": "UNCATEGORIZED",
            "value": 0.0
        })
        
        return df
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate transformed values based on business logic
        """
        # Get multiplier from config
        premium_multiplier = float(self.config.get("premium_multiplier", 1.5))
        standard_multiplier = float(self.config.get("standard_multiplier", 1.2))
        
        # Apply category-based transformation
        df = df.withColumn(
            "transformed_value",
            F.when(F.col("category") == "PREMIUM", F.col("value") * premium_multiplier)
             .when(F.col("category") == "STANDARD", F.col("value") * standard_multiplier)
             .otherwise(F.col("value"))
        )
        
        return df
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """
        Calculate priority based on value and category
        Priority: 1 (highest) to 5 (lowest)
        """
        df = df.withColumn(
            "priority",
            F.when((F.col("transformed_value") >= 1000) | (F.col("category") == "VIP"), 1)
             .when((F.col("transformed_value") >= 750) | (F.col("category") == "PREMIUM"), 2)
             .when(F.col("transformed_value") >= 500, 3)
             .when(F.col("transformed_value") >= 250, 4)
             .otherwise(5)
        )
        
        return df
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """Apply category-specific business rules"""
        
        # VIP category gets automatic upgrade
        df = df.withColumn(
            "status",
            F.when(F.col("category") == "VIP", "HIGH_VALUE")
             .otherwise(F.col("status"))
        )
        
        # Trial category has value cap
        df = df.withColumn(
            "transformed_value",
            F.when((F.col("category") == "TRIAL") & (F.col("transformed_value") > 100), 100.0)
             .otherwise(F.col("transformed_value"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply comprehensive business rules
        """
        # Rule 1: Set status based on transformed value
        df = df.withColumn(
            "status",
            F.when(F.col("value").isNull() | (F.col("value") == 0), "INVALID")
             .when(F.col("transformed_value") >= 750, "HIGH_VALUE")
             .when(F.col("transformed_value") >= 300, "MEDIUM_VALUE")
             .otherwise("LOW_VALUE")
        )
        
        # Rule 2: Priority override for very high value items
        df = df.withColumn(
            "priority",
            F.when(F.col("transformed_value") >= 1000, 1)
             .otherwise(F.col("priority"))
        )
        
        # Rule 3: Validate category values
        valid_categories = ["PREMIUM", "STANDARD", "BASIC", "VIP", "TRIAL", "UNCATEGORIZED"]
        df = df.withColumn(
            "category",
            F.when(~F.col("category").isin(valid_categories), "UNCATEGORIZED")
             .otherwise(F.col("category"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional calculated fields and lookups
        """
        # Add value bands
        df = df.withColumn(
            "value_band",
            F.when(F.col("transformed_value") >= 1000, "ULTRA_HIGH")
             .when(F.col("transformed_value") >= 500, "HIGH")
             .when(F.col("transformed_value") >= 250, "MEDIUM")
             .otherwise("LOW")
        )
        
        # Calculate percentile rank within category
        window_spec = Window.partitionBy("category").orderBy(F.col("transformed_value").desc())
        df = df.withColumn("category_rank", F.row_number().over(window_spec))
        
        return df
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """Add ETL metadata columns"""
        
        current_user = self.config.get("user", "etl_system")
        
        df = df.withColumn("etl_run_id", F.lit(self.run_id)) \
               .withColumn("processed_at", F.current_timestamp()) \
               .withColumn("processed_by", F.lit(current_user))
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data against business rules
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Starting data validation"
        )
        
        errors = []
        
        # Validation 1: Required fields are not null
        null_check = df.filter(
            F.col("id").isNull() | 
            F.col("name").isNull() | 
            F.col("transformed_value").isNull()
        ).count()
        
        if null_check > 0:
            errors.append(f"Found {null_check} records with null required fields")
        
        # Validation 2: Values must be non-negative
        negative_check = df.filter(
            (F.col("value") < 0) | 
            (F.col("transformed_value") < 0)
        ).count()
        
        if negative_check > 0:
            errors.append(f"Found {negative_check} records with negative values")
        
        # Validation 3: Priority must be 1-5
        priority_check = df.filter(
            (F.col("priority") < 1) | 
            (F.col("priority") > 5)
        ).count()
        
        if priority_check > 0:
            errors.append(f"Found {priority_check} records with invalid priority")
        
        # Validation 4: Check for duplicates
        duplicate_check = df.groupBy("id").count().filter(F.col("count") > 1).count()
        
        if duplicate_check > 0:
            errors.append(f"Found {duplicate_check} duplicate IDs")
        
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


===FILE: src/load.py===
"""
ETL Loader Module - PySpark Implementation
Loads transformed data to target with reconciliation
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict, Any, Tuple
from dataclasses import dataclass
from src.logger import ETLLogger


@dataclass
class LoadResult:
    """Container for load operation results"""
    success_count: int
    error_count: int
    total_count: int
    errors: list


class ETLLoader:
    """
    Handles loading data to target destinations
    Supports batch processing and multiple write modes
    """
    
    def __init__(self, spark: SparkSession, target_type: str = "DATABASE",
                 batch_size: int = 1000, run_id: str = None, 
                 config: Dict[str, Any] = None):
        """
        Initialize loader
        
        Args:
            spark: Active SparkSession
            target_type: Target destination type
            batch_size: Records per batch
            run_id: ETL run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.target_type = target_type.upper()
        self.batch_size = batch_size
        self.run_id = run_id
        self.config = config or {}
        self.logger = ETLLogger.get_instance()
    
    def load_data(self, df: DataFrame, mode: str = "INSERT") -> LoadResult:
        """
        Main loading method with error handling
        
        Args:
            df: Transformed DataFrame to load
            mode: Write mode (INSERT, UPDATE, UPSERT, OVERWRITE)
            
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
            # Execute load based on mode
            if mode.upper() == "INSERT":
                success = self._insert_data(df)
            elif mode.upper() == "UPDATE":
                success = self._update_data(df)
            elif mode.upper() == "UPSERT":
                success = self._upsert_data(df)
            elif mode.upper() == "OVERWRITE":
                success = self._overwrite_data(df)
            else:
                self.logger.log_warning(
                    component="LOADER",
                    message=f"Unknown mode {mode}, defaulting to INSERT"
                )
                success = self._insert_data(df)
            
            if success:
                success_count = total_count
                error_count = 0
                
                # Perform reconciliation
                if self.config.get("enable_reconciliation", True):
                    recon_success = self._reconcile_data(df)
                    if not recon_success:
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
                message="Load operation failed",
                details=str(e)
            )
            
            return LoadResult(
                success_count=0,
                error_count=total_count,
                total_count=total_count,
                errors=[str(e)]
            )
    
    def _insert_data(self, df: DataFrame) -> bool:
        """Insert new records into target"""
        try:
            target_path = self._get_target_path()
            
            if self.config.get("jdbc_url"):
                # Write to database via JDBC
                df.write \
                    .format("jdbc") \
                    .option("url", self.config["jdbc_url"]) \
                    .option("dbtable", self.config.get("target_table", "etl_target_data")) \
                    .option("user", self.config.get("db_user")) \
                    .option("password", self.config.get("db_password")) \
                    .option("driver", self.config.get("jdbc_driver", "org.postgresql.Driver")) \
                    .option("batchsize", self.batch_size) \
                    .mode("append") \
                    .save()
            else:
                # Write to parquet
                df.write \
                    .mode("append") \
                    .partitionBy("category") \
                    .parquet(target_path)
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Insert failed",
                details=str(e)
            )
            return False
    
    def _update_data(self, df: DataFrame) -> bool:
        """Update existing records in target"""
        try:
            # For file-based targets, this requires reading, merging, and rewriting
            # For database targets, use appropriate update logic
            
            if self.config.get("jdbc_url"):
                # Database update would typically be done via merge/upsert
                # This is a simplified version
                return self._upsert_data(df)
            else:
                # File-based update
                target_path = self._get_target_path()
                
                # Read existing data
                try:
                    existing_df = self.spark.read.parquet(target_path)
                except:
                    # No existing data, treat as insert
                    return self._insert_data(df)
                
                # Remove records that will be updated
                updated_df = existing_df.join(
                    df.select("id"), 
                    on="id", 
                    how="left_anti"
                ).union(df)
                
                # Write back
                updated_df.write \
                    .mode("overwrite") \
                    .partitionBy("category") \
                    .parquet(target_path)
                
                return True
                
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Update failed",
                details=str(e)
            )
            return False
    
    def _upsert_data(self, df: DataFrame) -> bool:
        """
        Perform upsert (insert or update)
        Uses Delta Lake if available, otherwise falls back to merge logic
        """
        try:
            target_path = self._get_target_path()
            
            # Try Delta Lake first
            if self._is_delta_available():
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
                    # Create new Delta table
                    df.write \
                        .format("delta") \
                        .mode("overwrite") \
                        .partitionBy("category") \
                        .save(target_path)
                
                return True
            else:
                # Fallback to manual merge
                return self._update_data(df)
                
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Upsert failed",
                details=str(e)
            )
            return False
    
    def _overwrite_data(self, df: DataFrame) -> bool:
        """Completely overwrite target data"""
        try:
            target_path = self._get_target_path()
            
            if self.config.get("jdbc_url"):
                df.write \
                    .format("jdbc") \
                    .option("url", self.config["jdbc_url"]) \
                    .option("dbtable", self.config.get("target_table", "etl_target_data")) \
                    .option("user", self.config.get("db_user")) \
                    .option("password", self.config.get("db_password")) \
                    .option("driver", self.config.get("jdbc_driver")) \
                    .option("batchsize", self.batch_size) \
                    .mode("overwrite") \
                    .save()
            else:
                df.write \
                    .mode("overwrite") \
                    .partitionBy("category") \
                    .parquet(target_path)
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Overwrite failed",
                details=str(e)
            )
            return False
    
    def _reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Verify loaded data matches source
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passes
        """
        try:
            target_path = self._get_target_path()
            
            # Read back from target
            if self.config.get("jdbc_url"):
                target_df = self.spark.read \
                    .format("jdbc") \
                    .option("url", self.config["jdbc_url"]) \
                    .option("dbtable", self.config.get("target_table")) \
                    .option("user", self.config.get("db_user")) \
                    .option("password", self.config.get("db_password")) \
                    .load()
            else:
                target_df = self.spark.read.parquet(target_path)
            
            # Filter to this run's data
            target_df = target_df.filter(F.col("etl_run_id") == self.run_id)
            
            # Compare counts
            loaded_count = loaded_df.count()
            target_count = target_df.count()
            
            if loaded_count != target_count:
                self.logger.log_error(
                    component="LOADER",
                    message=f"Reconciliation failed: loaded {loaded_count} but found {target_count}"
                )
                return False
            
            # Compare checksums
            loaded_checksum = loaded_df.select(
                F.sum(F.col("transformed_value")).alias("checksum")
            ).first()["checksum"]
            
            target_checksum = target_df.select(
                F.sum(F.col("transformed_value")).alias("checksum")
            ).first()["checksum"]
            
            if loaded_checksum != target_checksum:
                self.logger.log_error(
                    component="LOADER",
                    message=f"Checksum mismatch: {loaded_checksum} vs {target_checksum}"
                )
                return False
            
            self.logger.log_info(
                component="LOADER",
                message="Data reconciliation passed"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Reconciliation error",
                details=str(e)
            )
            return False
    
    def _get_target_path(self) -> str:
        """Get target path from configuration"""
        return self.config.get("target_path", "data/target")
    
    def _is_delta_available(self) -> bool:
        """Check if Delta Lake is available"""
        try:
            import delta
            return True
        except ImportError:
            return False


===FILE: src/orchestrator.py===
"""
ETL Orchestrator - Main pipeline controller
Coordinates extract, transform, load phases with comprehensive error handling
"""

from pyspark.sql import SparkSession
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime
import traceback

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader, LoadResult
from src.logger import ETLLogger
from src.data_quality import DataQualityChecker


@dataclass
class ETLResult:
    """Container for ETL execution results"""
    run_id: str
    status: str
    start_time: datetime
    end_time: Optional[datetime]
    duration_seconds: int
    records_extracted: int
    records_transformed: int
    records_loaded: int
    records_failed: int
    error_count: int
    warning_count: int
    error_message: Optional[str] = None


class