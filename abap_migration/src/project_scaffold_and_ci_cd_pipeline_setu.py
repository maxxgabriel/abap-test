# PySpark ETL Project Migration

===FILE: src/extract.py===
"""
Data extraction module for PySpark ETL pipeline.
Handles extraction from various data sources with incremental and full load support.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from typing import Optional, Dict, Any
from datetime import datetime
import logging

from src.utils.logger import ETLLogger
from src.utils.exceptions import ExtractionError


class DataExtractor:
    """Handles data extraction from various sources."""
    
    # Define source data schema
    SOURCE_SCHEMA = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("value", DecimalType(15, 2), True),
        StructField("status", StringType(), True),
        StructField("category", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("created_by", StringType(), True),
        StructField("changed_at", TimestampType(), True),
        StructField("changed_by", StringType(), True)
    ])
    
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
        self.logger = ETLLogger.get_logger("EXTRACTOR")
        self.source_type = config.get("source_type", "database")
        
    def extract_data(
        self,
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data based on configured source type.
        
        Args:
            filter_condition: Optional SQL filter condition
            max_records: Maximum number of records to extract (0 = unlimited)
            
        Returns:
            Extracted DataFrame
            
        Raises:
            ExtractionError: If extraction fails
        """
        self.logger.info(
            f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}"
        )
        
        try:
            if self.source_type == "database":
                df = self._extract_from_database(filter_condition)
            elif self.source_type == "staging":
                df = self._extract_from_staging()
            elif self.source_type == "incremental":
                df = self._extract_incremental()
            elif self.source_type == "file":
                df = self._extract_from_file()
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
            raise ExtractionError(f"Failed to extract data: {str(e)}") from e
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract data from database source.
        
        Args:
            filter_condition: Optional SQL WHERE clause
            
        Returns:
            DataFrame with extracted data
        """
        db_config = self.config["source"]["database"]
        
        query = f"""
            SELECT 
                id,
                name,
                value,
                status,
                category,
                source_system,
                created_at,
                created_by,
                changed_at,
                changed_by
            FROM {db_config['table']}
        """
        
        if filter_condition:
            query += f" WHERE {filter_condition}"
        
        query += f" LIMIT {db_config.get('max_fetch_size', 10000)}"
        
        self.logger.info(f"Executing query: {query}")
        
        df = (self.spark.read
              .format("jdbc")
              .option("url", db_config["jdbc_url"])
              .option("dbtable", f"({query}) as src")
              .option("user", db_config.get("user", ""))
              .option("password", db_config.get("password", ""))
              .option("driver", db_config.get("driver", "org.postgresql.Driver"))
              .load())
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """
        Extract data from staging area.
        
        Returns:
            DataFrame with staged data
        """
        staging_config = self.config["source"]["staging"]
        staging_path = staging_config["path"]
        
        self.logger.info(f"Reading from staging: {staging_path}")
        
        df = (self.spark.read
              .format(staging_config.get("format", "parquet"))
              .schema(self.SOURCE_SCHEMA)
              .option("path", staging_path)
              .load()
              .filter(col("run_id") == self.run_id)
              .filter(col("status") == "READY"))
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run.
        
        Returns:
            DataFrame with incremental data
        """
        self.logger.info("Performing incremental extraction")
        
        # Get last successful run timestamp
        last_run_time = self._get_last_run_time()
        
        if last_run_time is None:
            self.logger.warning("No previous run found, performing full extract")
            return self._extract_from_database()
        
        self.logger.info(f"Extracting changes since: {last_run_time}")
        
        filter_condition = f"changed_at > '{last_run_time}'"
        return self._extract_from_database(filter_condition)
    
    def _extract_from_file(self) -> DataFrame:
        """
        Extract data from file source (CSV, JSON, Parquet, etc.).
        
        Returns:
            DataFrame with file data
        """
        file_config = self.config["source"]["file"]
        file_path = file_config["path"]
        file_format = file_config.get("format", "parquet")
        
        self.logger.info(f"Reading from file: {file_path} (format: {file_format})")
        
        reader = self.spark.read.format(file_format)
        
        # Apply format-specific options
        if file_format == "csv":
            reader = (reader
                     .option("header", file_config.get("header", "true"))
                     .option("inferSchema", file_config.get("infer_schema", "false"))
                     .option("delimiter", file_config.get("delimiter", ",")))
        
        if file_config.get("use_schema", True):
            reader = reader.schema(self.SOURCE_SCHEMA)
        
        df = reader.load(file_path)
        
        return df
    
    def _get_last_run_time(self) -> Optional[datetime]:
        """
        Get timestamp of last successful ETL run.
        
        Returns:
            Timestamp of last run or None if no previous run
        """
        try:
            run_log_config = self.config["metadata"]["run_log"]
            
            last_run_df = (self.spark.read
                          .format("jdbc")
                          .option("url", run_log_config["jdbc_url"])
                          .option("dbtable", f"""
                              (SELECT MAX(end_time) as last_run_time 
                               FROM {run_log_config['table']}
                               WHERE status = 'SUCCESS') as last_run
                          """)
                          .option("user", run_log_config.get("user", ""))
                          .option("password", run_log_config.get("password", ""))
                          .load())
            
            result = last_run_df.collect()
            if result and result[0]["last_run_time"]:
                return result[0]["last_run_time"]
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Could not retrieve last run time: {str(e)}")
            return None
    
    def extract_with_metadata(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract data and add extraction metadata.
        
        Args:
            filter_condition: Optional filter condition
            
        Returns:
            DataFrame with metadata columns added
        """
        df = self.extract_data(filter_condition)
        
        # Add extraction metadata
        df = (df
              .withColumn("etl_run_id", lit(self.run_id))
              .withColumn("extracted_at", current_timestamp())
              .withColumn("source_type", lit(self.source_type)))
        
        return df


===FILE: src/transform.py===
"""
Data transformation module for PySpark ETL pipeline.
Implements business rules, data enrichment, and validation logic.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, current_timestamp,
    udf, coalesce, round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from typing import Dict, Any, Tuple, List
import logging

from src.utils.logger import ETLLogger
from src.utils.exceptions import TransformationError, ValidationError


class DataTransformer:
    """Handles data transformation and business rule application."""
    
    # Define transformed data schema
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
        StructField("processed_by", StringType(), False)
    ])
    
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
        self.logger = ETLLogger.get_logger("TRANSFORMER")
        self.user = config.get("runtime", {}).get("user", "system")
        
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply full transformation pipeline to source data.
        
        Args:
            source_df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame
            
        Raises:
            TransformationError: If transformation fails
        """
        self.logger.info("Starting data transformation")
        
        try:
            # Step 1: Clean and normalize
            df = self._clean_data(source_df)
            
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
            
            record_count = df.count()
            self.logger.info(f"Transformed {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}")
            raise TransformationError(f"Failed to transform data: {str(e)}") from e
    
    def _clean_data(self, df: DataFrame) -> DataFrame:
        """
        Clean and normalize data fields.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        self.logger.info("Cleaning data")
        
        df = (df
              # Normalize name: uppercase and trim
              .withColumn("name", upper(trim(col("name"))))
              # Remove multiple spaces
              .withColumn("name", regexp_replace(col("name"), "\\s+", " "))
              # Handle null categories
              .withColumn("category", 
                         coalesce(col("category"), lit("UNCATEGORIZED")))
              # Ensure positive values
              .withColumn("value", 
                         when(col("value") < 0, lit(0)).otherwise(col("value")))
        )
        
        return df
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate transformed values based on category and value.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with transformed_value column
        """
        self.logger.info("Calculating derived values")
        
        # Get multipliers from config
        multipliers = self.config.get("transformation", {}).get("multipliers", {})
        premium_mult = multipliers.get("premium", 1.5)
        standard_mult = multipliers.get("standard", 1.0)
        basic_mult = multipliers.get("basic", 0.8)
        vip_mult = multipliers.get("vip", 2.0)
        
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("value") * premium_mult)
            .when(col("category") == "VIP", col("value") * vip_mult)
            .when(col("category") == "BASIC", col("value") * basic_mult)
            .otherwise(col("value") * standard_mult)
        )
        
        # Round to 2 decimal places
        df = df.withColumn("transformed_value", 
                          spark_round(col("transformed_value"), 2))
        
        return df
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """
        Calculate priority based on value and category.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with priority column
        """
        self.logger.info("Calculating priority")
        
        thresholds = self.config.get("transformation", {}).get("priority_thresholds", {})
        high_threshold = thresholds.get("high", 1000)
        medium_threshold = thresholds.get("medium", 500)
        
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= high_threshold, lit(1))
            .when(col("transformed_value") >= medium_threshold, lit(2))
            .when(col("category") == "VIP", lit(2))
            .when(col("category") == "PREMIUM", lit(3))
            .otherwise(lit(4))
        )
        
        return df
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply category-specific transformation rules.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with category rules applied
        """
        self.logger.info("Applying category rules")
        
        # Premium category gets 20% bonus
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM",
                 col("transformed_value") * 1.2)
            .otherwise(col("transformed_value"))
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
        self.logger.info("Applying business rules")
        
        # Rule 1: Set status based on transformed value
        df = df.withColumn(
            "status",
            when(col("transformed_value").isNull(), lit("INVALID"))
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
        Enrich data with additional attributes from configuration.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Enriched DataFrame
        """
        self.logger.info("Enriching data")
        
        enrichment_config = self.config.get("transformation", {}).get("enrichment", {})
        
        if enrichment_config.get("enabled", True):
            # Load enrichment lookup if configured
            if "lookup_table" in enrichment_config:
                lookup_df = self._load_enrichment_lookup()
                if lookup_df:
                    df = df.join(lookup_df, on="category", how="left")
        
        return df
    
    def _load_enrichment_lookup(self) -> DataFrame:
        """
        Load enrichment lookup data.
        
        Returns:
            Lookup DataFrame or None
        """
        try:
            enrichment_config = self.config["transformation"]["enrichment"]
            lookup_path = enrichment_config["lookup_table"]
            
            lookup_df = (self.spark.read
                        .format("parquet")
                        .load(lookup_path))
            
            return lookup_df
        except Exception as e:
            self.logger.warning(f"Could not load enrichment data: {str(e)}")
            return None
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add processing metadata to transformed data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with metadata columns
        """
        df = (df
              .withColumn("etl_run_id", lit(self.run_id))
              .withColumn("processed_at", current_timestamp())
              .withColumn("processed_by", lit(self.user))
        )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data against business rules.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        self.logger.info("Validating transformed data")
        
        errors = []
        
        # Rule 1: ID is required
        null_id_count = df.filter(col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with null ID")
        
        # Rule 2: Name is required
        null_name_count = df.filter(col("name").isNull() | (col("name") == "")).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with null/empty name")
        
        # Rule 3: Value must be non-negative
        negative_value_count = df.filter(col("value") < 0).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative value")
        
        # Rule 4: Priority must be 1-5
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        # Rule 5: Category must not be null
        null_category_count = df.filter(col("category").isNull()).count()
        if null_category_count > 0:
            errors.append(f"{null_category_count} records with null category")
        
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
Data loading module for PySpark ETL pipeline.
Handles writing transformed data to target systems with reconciliation.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from typing import Dict, Any, Optional
import logging

from src.utils.logger import ETLLogger
from src.utils.exceptions import LoadError


class DataLoader:
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
        self.logger = ETLLogger.get_logger("LOADER")
        self.target_type = config.get("target_type", "database")
        self.batch_size = config.get("load", {}).get("batch_size", 1000)
        
    def load_data(
        self,
        df: DataFrame,
        mode: str = "overwrite",
        partition_cols: Optional[list] = None
    ) -> Dict[str, Any]:
        """
        Load transformed data to target.
        
        Args:
            df: Transformed DataFrame to load
            mode: Write mode (overwrite, append, upsert)
            partition_cols: Optional list of columns to partition by
            
        Returns:
            Dictionary with load results
            
        Raises:
            LoadError: If loading fails
        """
        self.logger.info(
            f"Starting data load - Target: {self.target_type}, Mode: {mode}"
        )
        
        try:
            total_count = df.count()
            
            if mode == "upsert":
                success_count, error_count = self._load_upsert(df)
            elif self.target_type == "database":
                success_count, error_count = self._load_to_database(df, mode)
            elif self.target_type == "file":
                success_count, error_count = self._load_to_file(df, mode, partition_cols)
            elif self.target_type == "warehouse":
                success_count, error_count = self._load_to_warehouse(df, mode)
            else:
                success_count, error_count = self._load_to_database(df, mode)
            
            # Reconcile if enabled
            reconciled = False
            if self.config.get("load", {}).get("enable_reconciliation", True):
                reconciled = self._reconcile_data(df, success_count)
            
            result = {
                "success_count": success_count,
                "error_count": error_count,
                "total_count": total_count,
                "reconciled": reconciled
            }
            
            self.logger.info(
                f"Load complete - Success: {success_count}, "
                f"Errors: {error_count}, Reconciled: {reconciled}"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            raise LoadError(f"Failed to load data: {str(e)}") from e
    
    def _load_to_database(self, df: DataFrame, mode: str) -> tuple:
        """
        Load data to database target.
        
        Args:
            df: DataFrame to load
            mode: Write mode
            
        Returns:
            Tuple of (success_count, error_count)
        """
        db_config = self.config["target"]["database"]
        
        try:
            # Configure JDBC write options
            write_options = {
                "url": db_config["jdbc_url"],
                "dbtable": db_config["table"],
                "user": db_config.get("user", ""),
                "password": db_config.get("password", ""),
                "driver": db_config.get("driver", "org.postgresql.Driver"),
                "batchsize": str(self.batch_size),
                "isolationLevel": "READ_COMMITTED"
            }
            
            # Write to database
            (df.write
               .format("jdbc")
               .options(**write_options)
               .mode(mode)
               .save())
            
            success_count = df.count()
            error_count = 0
            
            self.logger.info(f"Loaded {success_count} records to database")
            
            return success_count, error_count
            
        except Exception as e:
            self.logger.error(f"Database load error: {str(e)}")
            return 0, df.count()
    
    def _load_to_file(
        self,
        df: DataFrame,
        mode: str,
        partition_cols: Optional[list] = None
    ) -> tuple:
        """
        Load data to file target.
        
        Args:
            df: DataFrame to load
            mode: Write mode
            partition_cols: Optional partition columns
            
        Returns:
            Tuple of (success_count, error_count)
        """
        file_config = self.config["target"]["file"]
        target_path = file_config["path"]
        file_format = file_config.get("format", "parquet")
        
        try:
            writer = df.write.format(file_format).mode(mode)
            
            # Add partitioning if specified
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            # Add compression if configured
            if file_config.get("compression"):
                writer = writer.option("compression", file_config["compression"])
            
            # Write to file
            writer.save(target_path)
            
            success_count = df.count()
            error_count = 0
            
            self.logger.info(
                f"Loaded {success_count} records to {target_path} ({file_format})"
            )
            
            return success_count, error_count
            
        except Exception as e:
            self.logger.error(f"File load error: {str(e)}")
            return 0, df.count()
    
    def _load_to_warehouse(self, df: DataFrame, mode: str) -> tuple:
        """
        Load data to data warehouse (e.g., Snowflake, Redshift).
        
        Args:
            df: DataFrame to load
            mode: Write mode
            
        Returns:
            Tuple of (success_count, error_count)
        """
        warehouse_config = self.config["target"]["warehouse"]
        warehouse_type = warehouse_config.get("type", "snowflake")
        
        try:
            if warehouse_type == "snowflake":
                return self._load_to_snowflake(df, mode, warehouse_config)
            elif warehouse_type == "redshift":
                return self._load_to_redshift(df, mode, warehouse_config)
            else:
                raise LoadError(f"Unsupported warehouse type: {warehouse_type}")
                
        except Exception as e:
            self.logger.error(f"Warehouse load error: {str(e)}")
            return 0, df.count()
    
    def _load_to_snowflake(
        self,
        df: DataFrame,
        mode: str,
        config: Dict[str, Any]
    ) -> tuple:
        """Load data to Snowflake."""
        snowflake_options = {
            "sfURL": config["url"],
            "sfUser": config["user"],
            "sfPassword": config["password"],
            "sfDatabase": config["database"],
            "sfSchema": config["schema"],
            "sfWarehouse": config["warehouse"],
            "dbtable": config["table"]
        }
        
        (df.write
           .format("snowflake")
           .options(**snowflake_options)
           .mode(mode)
           .save())
        
        return df.count(), 0
    
    def _load_to_redshift(
        self,
        df: DataFrame,
        mode: str,
        config: Dict[str, Any]
    ) -> tuple:
        """Load data to Redshift."""
        redshift_options = {
            "url": config["jdbc_url"],
            "dbtable": config["table"],
            "user": config["user"],
            "password": config["password"],
            "tempdir": config["temp_s3_path"]
        }
        
        (df.write
           .format("io.github.spark_redshift_community.spark.redshift")
           .options(**redshift_options)
           .mode(mode)
           .save())
        
        return df.count(), 0
    
    def _load_upsert(self, df: DataFrame) -> tuple:
        """
        Perform upsert operation (update existing, insert new).
        
        Args:
            df: DataFrame to upsert
            
        Returns:
            Tuple of (success_count, error_count)
        """
        self.logger.info("Performing upsert operation")
        
        db_config = self.config["target"]["database"]
        target_table = db_config["table"]
        primary_key = db_config.get("primary_key", "id")
        
        try:
            # Create temp view
            df.createOrReplaceTempView("source_data")
            
            # Perform merge using Delta Lake or database-specific upsert
            if self.config.get("use_delta", False):
                return self._delta_upsert(df, target_table, primary_key)
            else:
                return self._jdbc_upsert(df, target_table, primary_key)
                
        except Exception as e:
            self.logger.error(f"Upsert failed: {str(e)}")
            return 0, df.count()
    
    def _delta_upsert(self, df: DataFrame, target_table: str, key: str) -> tuple:
        """Perform Delta Lake merge/upsert."""
        from delta.tables import DeltaTable
        
        # Get existing table
        delta_table = DeltaTable.forName(self.spark, target_table)
        
        # Perform merge
        (delta_table.alias("target")
         .merge(
             df.alias("source"),
             f"target.{key} = source.{key}"
         )
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
        
        return df.count(), 0
    
    def _jdbc_upsert(self, df: DataFrame, target_table: str, key: str) -> tuple:
        """Perform JDBC-based upsert."""
        # This is a simplified version - production code would need
        # database-specific upsert logic (ON CONFLICT, MERGE, etc.)
        
        # First try update, then insert remaining
        try:
            # Update existing
            self._load_to_database(df, mode="