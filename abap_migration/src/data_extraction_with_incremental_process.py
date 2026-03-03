# PySpark Data Extraction with Incremental Processing

===FILE: src/extract.py===
"""
Data extraction module with incremental processing and Delta Lake time-travel support.
Replaces ABAP extractor with PySpark DataFrame operations.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, TimestampType
)
from datetime import datetime
from typing import Optional, Dict, Any
import logging

from src.logger import ETLLogger
from src.config import Config


class DataExtractor:
    """
    Handles data extraction with support for:
    - Full loads from database/files
    - Incremental loads using timestamp filtering
    - Delta Lake time-travel for change data capture
    - Row limiting and filtering
    """
    
    # Define source schema matching ABAP ty_source_data
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
    
    def __init__(
        self, 
        spark: SparkSession,
        config: Config,
        run_id: str,
        source_type: str = "DATABASE"
    ):
        """
        Initialize the extractor.
        
        Args:
            spark: Active SparkSession
            config: Configuration object
            run_id: Unique identifier for this ETL run
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.source_type = source_type.upper()
        self.logger = ETLLogger.get_instance()
        
    def extract_data(
        self,
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Main extraction method - routes to appropriate extraction strategy.
        
        Args:
            filter_condition: Optional SQL WHERE clause condition
            max_records: Maximum number of records to extract (0 = unlimited)
            
        Returns:
            DataFrame containing extracted data
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
                    message=f"Unknown source type '{self.source_type}', defaulting to DATABASE"
                )
                df = self._extract_from_database(filter_condition)
            
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
    
    def _extract_from_database(
        self, 
        filter_condition: Optional[str] = None
    ) -> DataFrame:
        """
        Extract data from database source (JDBC or file-based).
        
        Args:
            filter_condition: Optional WHERE clause filter
            
        Returns:
            DataFrame with source data
        """
        source_path = self.config.get("source.database.path")
        source_format = self.config.get("source.database.format", "parquet")
        
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Extracting from database: {source_path}"
        )
        
        # Read from source
        df = self.spark.read \
            .format(source_format) \
            .schema(self.SOURCE_SCHEMA) \
            .load(source_path)
        
        # Apply filter if provided
        if filter_condition:
            df = df.filter(filter_condition)
            self.logger.log_info(
                component="EXTRACTOR",
                message=f"Applied filter: {filter_condition}"
            )
        
        # Apply default limit from config if no filter specified
        if not filter_condition:
            default_limit = self.config.get("source.database.default_limit", 1000)
            df = df.limit(default_limit)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """
        Extract data from staging area filtered by run_id.
        
        Returns:
            DataFrame with staged data ready for processing
        """
        staging_path = self.config.get("source.staging.path")
        
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Extracting from staging for run_id: {self.run_id}"
        )
        
        # Read staging data
        df = self.spark.read \
            .format("delta") \
            .load(staging_path)
        
        # Filter by run_id and status
        df = df.filter(
            (F.col("run_id") == self.run_id) & 
            (F.col("status") == "READY")
        )
        
        # Parse and extract actual data fields
        # In real scenario, might need to parse JSON/XML from raw_data column
        df = df.select(
            F.col("id"),
            F.lit("STAGED").alias("status"),
            F.col("created_at"),
            # Add other field mappings as needed
        )
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run.
        Uses Delta Lake time-travel to capture changes.
        
        Returns:
            DataFrame with incremental changes
        """
        source_path = self.config.get("source.database.path")
        
        # Get last successful run timestamp
        last_run_time = self._get_last_successful_run_time()
        
        if last_run_time:
            self.logger.log_info(
                component="EXTRACTOR",
                message=f"Extracting incremental changes since {last_run_time}"
            )
            
            # Read Delta table with time-travel
            df = self.spark.read \
                .format("delta") \
                .load(source_path)
            
            # Filter by changed_at timestamp
            df = df.filter(F.col("changed_at") > F.lit(last_run_time))
            
        else:
            self.logger.log_warning(
                component="EXTRACTOR",
                message="No previous successful run found, performing full extraction"
            )
            df = self._extract_from_database()
        
        return df
    
    def _get_last_successful_run_time(self) -> Optional[datetime]:
        """
        Query run log to find the timestamp of the last successful run.
        
        Returns:
            Timestamp of last successful run, or None if not found
        """
        run_log_path = self.config.get("metadata.run_log_path")
        
        try:
            run_log_df = self.spark.read \
                .format("delta") \
                .load(run_log_path)
            
            # Get the most recent successful run
            last_run = run_log_df \
                .filter(F.col("status") == "SUCCESS") \
                .orderBy(F.col("end_time").desc()) \
                .select("end_time") \
                .first()
            
            if last_run:
                return last_run["end_time"]
            else:
                return None
                
        except Exception as e:
            self.logger.log_warning(
                component="EXTRACTOR",
                message=f"Could not retrieve last run time: {str(e)}"
            )
            return None
    
    def extract_with_cdc(
        self,
        from_version: Optional[int] = None,
        from_timestamp: Optional[str] = None
    ) -> DataFrame:
        """
        Extract changes using Delta Lake Change Data Feed (CDC).
        Provides more granular change tracking than timestamp-based filtering.
        
        Args:
            from_version: Delta table version to read changes from
            from_timestamp: Timestamp string to read changes from
            
        Returns:
            DataFrame with change data including _change_type column
        """
        source_path = self.config.get("source.database.path")
        
        self.logger.log_info(
            component="EXTRACTOR",
            message="Extracting changes using Delta Lake CDC"
        )
        
        # Enable change data feed reading
        reader = self.spark.read \
            .format("delta") \
            .option("readChangeFeed", "true")
        
        # Specify version or timestamp
        if from_version is not None:
            reader = reader.option("startingVersion", from_version)
        elif from_timestamp is not None:
            reader = reader.option("startingTimestamp", from_timestamp)
        
        # Read changes
        df = reader.load(source_path)
        
        # The DataFrame will include:
        # - All data columns
        # - _change_type: insert, update_preimage, update_postimage, delete
        # - _commit_version: Delta version of the change
        # - _commit_timestamp: Timestamp of the change
        
        return df
    
    def validate_schema(self, df: DataFrame) -> bool:
        """
        Validate that extracted data matches expected schema.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            True if schema is valid, False otherwise
        """
        expected_fields = set(field.name for field in self.SOURCE_SCHEMA.fields)
        actual_fields = set(df.schema.fieldNames())
        
        missing_fields = expected_fields - actual_fields
        extra_fields = actual_fields - expected_fields
        
        if missing_fields:
            self.logger.log_error(
                component="EXTRACTOR",
                message=f"Missing required fields: {missing_fields}"
            )
            return False
        
        if extra_fields:
            self.logger.log_warning(
                component="EXTRACTOR",
                message=f"Extra fields found: {extra_fields}"
            )
        
        return True
    
    def get_extraction_stats(self, df: DataFrame) -> Dict[str, Any]:
        """
        Gather statistics about extracted data.
        
        Args:
            df: Extracted DataFrame
            
        Returns:
            Dictionary containing extraction statistics
        """
        stats = {
            "total_records": df.count(),
            "distinct_ids": df.select("id").distinct().count(),
            "null_values": {},
            "categories": {}
        }
        
        # Count null values per column
        for col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            if null_count > 0:
                stats["null_values"][col] = null_count
        
        # Count records by category
        if "category" in df.columns:
            category_counts = df.groupBy("category").count().collect()
            stats["categories"] = {
                row["category"]: row["count"] 
                for row in category_counts
            }
        
        return stats


===FILE: src/transform.py===
"""
Data transformation module implementing business rules and data quality checks.
Replaces ABAP transformer with PySpark DataFrame transformations.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, 
    TimestampType, IntegerType
)
from typing import List, Tuple
import logging

from src.logger import ETLLogger
from src.config import Config


class DataTransformer:
    """
    Handles data transformation including:
    - Business rule application
    - Data enrichment
    - Validation
    - Derived field calculation
    """
    
    # Define transformed data schema matching ABAP ty_transformed_data
    TRANSFORMED_SCHEMA = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("value", DecimalType(15, 2), False),
        StructField("transformed_value", DecimalType(15, 2), False),
        StructField("status", StringType(), False),
        StructField("category", StringType(), False),
        StructField("priority", IntegerType(), False),
        StructField("etl_run_id", StringType(), False),
        StructField("processed_at", TimestampType(), False),
        StructField("processed_by", StringType(), False),
    ])
    
    def __init__(
        self,
        spark: SparkSession,
        config: Config,
        run_id: str
    ):
        """
        Initialize the transformer.
        
        Args:
            spark: Active SparkSession
            config: Configuration object
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
        self.current_timestamp = F.current_timestamp()
        self.current_user = self.config.get("runtime.user", "spark_etl")
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Main transformation method applying all transformation steps.
        
        Args:
            source_df: Source DataFrame from extraction
            
        Returns:
            Transformed DataFrame
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Starting transformation"
        )
        
        try:
            # Step 1: Basic field mapping and cleansing
            df = self._map_and_cleanse(source_df)
            
            # Step 2: Calculate derived values
            df = self._calculate_derived_values(df)
            
            # Step 3: Apply category-specific rules
            df = self._apply_category_rules(df)
            
            # Step 4: Calculate priority
            df = self._calculate_priority(df)
            
            # Step 5: Apply business rules
            df = self._apply_business_rules(df)
            
            # Step 6: Enrich data
            df = self._enrich_data(df)
            
            # Step 7: Add metadata fields
            df = self._add_metadata(df)
            
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
    
    def _map_and_cleanse(self, df: DataFrame) -> DataFrame:
        """
        Map source fields to target schema and cleanse data.
        
        Args:
            df: Source DataFrame
            
        Returns:
            Cleansed DataFrame with mapped fields
        """
        return df.select(
            F.col("id"),
            # Normalize name: trim, upper case, remove extra spaces
            F.upper(F.trim(F.regexp_replace(F.col("name"), r'\s+', ' '))).alias("name"),
            F.col("value"),
            F.col("status"),
            F.col("category")
        )
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate transformed_value based on business logic.
        
        Args:
            df: DataFrame with base values
            
        Returns:
            DataFrame with transformed_value column added
        """
        # Get multipliers from config
        premium_multiplier = self.config.get("business_rules.premium_multiplier", 1.5)
        standard_multiplier = self.config.get("business_rules.standard_multiplier", 1.0)
        
        # Calculate transformed value based on category
        df = df.withColumn(
            "transformed_value",
            F.when(F.col("category") == "PREMIUM", F.col("value") * premium_multiplier)
             .when(F.col("category") == "STANDARD", F.col("value") * standard_multiplier)
             .otherwise(F.col("value"))
        )
        
        return df
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply category-specific transformation rules.
        
        Args:
            df: DataFrame to transform
            
        Returns:
            DataFrame with category rules applied
        """
        # Set default category if empty
        df = df.withColumn(
            "category",
            F.when(F.col("category").isNull() | (F.col("category") == ""), "UNCATEGORIZED")
             .otherwise(F.col("category"))
        )
        
        # Apply category-specific adjustments
        df = df.withColumn(
            "transformed_value",
            F.when(F.col("category") == "VIP", F.col("transformed_value") * 1.3)
             .when(F.col("category") == "TRIAL", F.col("transformed_value") * 0.8)
             .otherwise(F.col("transformed_value"))
        )
        
        return df
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """
        Calculate priority based on value and category.
        Priority scale: 1 (highest) to 5 (lowest)
        
        Args:
            df: DataFrame with value and category
            
        Returns:
            DataFrame with priority column
        """
        df = df.withColumn(
            "priority",
            F.when(F.col("transformed_value") >= 1000, 1)
             .when(F.col("transformed_value") >= 750, 2)
             .when(F.col("transformed_value") >= 500, 3)
             .when(F.col("transformed_value") >= 250, 4)
             .otherwise(5)
        )
        
        # Override priority for VIP category
        df = df.withColumn(
            "priority",
            F.when(F.col("category") == "VIP", 1)
             .otherwise(F.col("priority"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to set status and other derived fields.
        
        Args:
            df: DataFrame to apply rules to
            
        Returns:
            DataFrame with business rules applied
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Applying business rules"
        )
        
        # Rule 1: Set status based on value
        df = df.withColumn(
            "status",
            F.when(F.col("value").isNull() | (F.col("value") == 0), "INVALID")
             .when(F.col("transformed_value") >= 750, "HIGH_VALUE")
             .when(F.col("transformed_value") >= 300, "MEDIUM_VALUE")
             .otherwise("LOW_VALUE")
        )
        
        # Rule 2: Override priority for high value items
        df = df.withColumn(
            "priority",
            F.when(F.col("transformed_value") >= 1000, 1)
             .otherwise(F.col("priority"))
        )
        
        # Rule 3: Additional name normalization
        df = df.withColumn(
            "name",
            F.regexp_replace(F.col("name"), r'\s+', ' ')
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional information from config or reference tables.
        
        Args:
            df: DataFrame to enrich
            
        Returns:
            Enriched DataFrame
        """
        # Load enrichment configuration
        enrichment_enabled = self.config.get("transformation.enable_enrichment", True)
        
        if not enrichment_enabled:
            return df
        
        # Apply premium category boost
        df = df.withColumn(
            "transformed_value",
            F.when(F.col("category") == "PREMIUM", F.col("transformed_value") * 1.2)
             .otherwise(F.col("transformed_value"))
        )
        
        # Could join with reference tables here for additional enrichment
        # Example:
        # category_ref = self.spark.read.parquet("path/to/category_reference")
        # df = df.join(category_ref, "category", "left")
        
        return df
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add ETL metadata fields to transformed data.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            DataFrame with metadata columns added
        """
        return df.withColumn("etl_run_id", F.lit(self.run_id)) \
                 .withColumn("processed_at", self.current_timestamp) \
                 .withColumn("processed_by", F.lit(self.current_user))
    
    def validate_data(
        self, 
        df: DataFrame
    ) -> Tuple[bool, List[str]]:
        """
        Validate transformed data against business rules.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        validation_errors = []
        is_valid = True
        
        # Rule 1: ID is required
        null_id_count = df.filter(F.col("id").isNull()).count()
        if null_id_count > 0:
            validation_errors.append(f"{null_id_count} records with null ID")
            is_valid = False
        
        # Rule 2: Name is required
        null_name_count = df.filter(
            F.col("name").isNull() | (F.col("name") == "")
        ).count()
        if null_name_count > 0:
            validation_errors.append(f"{null_name_count} records with null/empty name")
            is_valid = False
        
        # Rule 3: Value must be positive
        negative_value_count = df.filter(F.col("value") < 0).count()
        if negative_value_count > 0:
            validation_errors.append(f"{negative_value_count} records with negative value")
            is_valid = False
        
        # Rule 4: Priority must be 1-5
        invalid_priority_count = df.filter(
            (F.col("priority") < 1) | (F.col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            validation_errors.append(f"{invalid_priority_count} records with invalid priority")
            is_valid = False
        
        # Rule 5: Category must not be empty
        invalid_category_count = df.filter(
            F.col("category").isNull() | (F.col("category") == "")
        ).count()
        if invalid_category_count > 0:
            validation_errors.append(f"{invalid_category_count} records with invalid category")
            is_valid = False
        
        if is_valid:
            self.logger.log_info(
                component="TRANSFORMER",
                message="Data validation passed"
            )
        else:
            self.logger.log_error(
                component="TRANSFORMER",
                message=f"Data validation failed with {len(validation_errors)} errors",
                details="; ".join(validation_errors)
            )
        
        return is_valid, validation_errors


===FILE: src/load.py===
"""
Data loading module with batch processing and reconciliation.
Replaces ABAP loader with PySpark DataFrame write operations.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict, Any, List
from dataclasses import dataclass
import logging

from src.logger import ETLLogger
from src.config import Config


@dataclass
class LoadResult:
    """Result of data loading operation."""
    success_count: int
    error_count: int
    total_count: int
    errors: List[str]


class DataLoader:
    """
    Handles data loading with support for:
    - Batch processing
    - Multiple write modes (insert, update, upsert)
    - Data reconciliation
    - Delta Lake ACID transactions
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Config,
        run_id: str,
        target_type: str = "DATABASE",
        batch_size: int = 1000
    ):
        """
        Initialize the loader.
        
        Args:
            spark: Active SparkSession
            config: Configuration object
            run_id: Unique identifier for this ETL run
            target_type: Type of target (DATABASE, DELTA, etc.)
            batch_size: Number of records per batch
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.target_type = target_type.upper()
        self.batch_size = batch_size
        self.logger = ETLLogger.get_instance()
    
    def load_data(
        self,
        df: DataFrame,
        mode: str = "upsert"
    ) -> LoadResult:
        """
        Main loading method with batch processing.
        
        Args:
            df: Transformed DataFrame to load
            mode: Write mode - 'insert', 'update', 'upsert', 'overwrite'
            
        Returns:
            LoadResult with success/error counts
        """
        mode = mode.lower()
        
        self.logger.log_info(
            component="LOADER",
            message=f"Starting load - Mode: {mode}, Batch size: {self.batch_size}"
        )
        
        total_count = df.count()
        errors = []
        
        try:
            # Execute load based on mode
            if mode == "insert":
                success = self._insert_new(df)
            elif mode == "update":
                success = self._update_existing(df)
            elif mode == "upsert":
                success = self._upsert_data(df)
            elif mode == "overwrite":
                success = self._overwrite_data(df)
            else:
                raise ValueError(f"Unknown load mode: {mode}")
            
            if success:
                success_count = total_count
                error_count = 0
                
                # Perform reconciliation if enabled
                if self.config.get("load.enable_reconciliation", True):
                    reconciled = self._reconcile_data(df)
                    if not reconciled:
                        self.logger.log_warning(
                            component="LOADER",
                            message="Data reconciliation failed"
                        )
            else:
                success_count = 0
                error_count = total_count
                errors.append("Load operation failed")
            
            self.logger.log_info(
                component="LOADER",
                message=f"Load complete - Success: {success_count}, Errors: {error_count}"
            )
            
            return LoadResult(
                success_count=success_count,
                error_count=error_count,
                total_count=total_count,
                errors=errors
            )
            
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
    
    def _insert_new(self, df: DataFrame) -> bool:
        """
        Insert new records into target.
        
        Args:
            df: DataFrame to insert
            
        Returns:
            True if successful, False otherwise
        """
        target_path = self.config.get("target.database.path")
        target_format = self.config.get("target.database.format", "delta")
        
        try:
            df.write \
                .format(target_format) \
                .mode("append") \
                .save(target_path)
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Insert operation failed",
                details=str(e)
            )
            return False
    
    def _update_existing(self, df: DataFrame) -> bool:
        """
        Update existing records in target.
        For Delta Lake, uses merge operation.
        
        Args:
            df: DataFrame with updates
            
        Returns:
            True if successful, False otherwise
        """
        target_path = self.config.get("target.database.path")
        
        try:
            from delta.tables import DeltaTable
            
            # Load existing target table
            target_table = DeltaTable.forPath(self.spark, target_path)
            
            # Perform merge (update only)
            target_table.alias("target") \
                .merge(
                    df.alias("source"),
                    "target.id = source.id"
                ) \
                .whenMatchedUpdateAll() \
                .execute()
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Update operation failed",
                details=str(e)
            )
            return False
    
    def _upsert_data(self, df: DataFrame) -> bool:
        """
        Upsert data - update existing records, insert new ones.
        Uses Delta Lake merge for ACID compliance.
        
        Args:
            df: DataFrame to upsert
            
        Returns:
            True if successful, False otherwise
        """
        target_path = self.config.get("target.database.path")
        
        try:
            from delta.tables import DeltaTable
            
            # Check if target exists
            try:
                target_table = DeltaTable.forPath(self.spark, target_path)
                table_exists = True
            except:
                table_exists = False
            
            if not table_exists:
                # First load - just write
                df.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .save(target_path)
            else:
                # Perform merge (upsert)
                target_table.alias("target") \
                    .merge(
                        df.alias("source"),
                        "target.id = source.id"
                    ) \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Upsert operation failed",
                details=str(e)
            )
            return False
    
    def _overwrite_data(self, df: DataFrame) -> bool:
        """
        Overwrite entire target table.
        
        Args:
            df: DataFrame to write
            
        Returns:
            True if successful, False otherwise
        """
        target_path = self.config.get("target.database.path")
        target_format = self.config.get("target.database.format", "delta")
        
        try:
            df.write \
                .format(target_format) \
                .mode("over