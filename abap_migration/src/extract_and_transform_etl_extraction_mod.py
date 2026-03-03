# PySpark ETL Migration - Production Code

===FILE: src/extract.py===
"""
PySpark-based ETL Extractor Module
Handles database extraction, incremental loads, and data type transformations
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, 
    TimestampType, IntegerType
)
from pyspark.sql import functions as F
from typing import Optional, Dict, Any
from datetime import datetime
import logging

from src.logger import ETLLogger


class ETLExtractor:
    """
    PySpark-based data extraction with database-to-DataFrame conversion,
    incremental load filtering, and data type transformations.
    """
    
    # Source data schema definition
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
        source_type: str = "DATABASE",
        run_id: str = None,
        config: Dict[str, Any] = None
    ):
        """
        Initialize ETL Extractor.
        
        Args:
            spark: Active SparkSession
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.source_type = source_type.upper()
        self.run_id = run_id or self._generate_run_id()
        self.config = config or {}
        self.logger = ETLLogger.get_instance()
        
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Initialized extractor - Source: {self.source_type}, Run ID: {self.run_id}"
        )
    
    def extract_data(
        self,
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Main extraction method with routing logic.
        
        Args:
            filter_condition: Optional SQL filter condition
            max_records: Maximum number of records to extract (0 = unlimited)
            
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
                last_run_time = self._get_last_successful_run_time()
                if last_run_time:
                    df = self.extract_incremental(last_run_time)
                else:
                    self.logger.log_warning(
                        component="EXTRACTOR",
                        message="No previous run found, falling back to full extraction"
                    )
                    df = self.extract_from_database(filter_condition)
            else:
                df = self.extract_from_database(filter_condition)
            
            # Apply max records limit
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
    
    def extract_from_database(
        self,
        filter_condition: Optional[str] = None
    ) -> DataFrame:
        """
        Extract data from source database table.
        
        Args:
            filter_condition: SQL WHERE clause filter
            
        Returns:
            DataFrame with source data
        """
        jdbc_url = self.config.get("jdbc_url")
        source_table = self.config.get("source_table", "etl_source_data")
        
        if jdbc_url:
            # JDBC database extraction
            df = self._extract_from_jdbc(jdbc_url, source_table, filter_condition)
        else:
            # File-based or other source extraction
            df = self._extract_from_files(filter_condition)
        
        return df
    
    def extract_from_staging(self, run_id: str) -> DataFrame:
        """
        Extract data from staging area.
        
        Args:
            run_id: Run identifier for staged data
            
        Returns:
            DataFrame with staged data
        """
        staging_path = self.config.get("staging_path", "data/staging")
        
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Extracting from staging - Run ID: {run_id}"
        )
        
        try:
            df = self.spark.read.parquet(f"{staging_path}/run_id={run_id}")
            df = df.filter(F.col("status") == "READY")
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="EXTRACTOR",
                message="Staging extraction failed",
                details=str(e)
            )
            raise
    
    def extract_incremental(self, last_run_time: datetime) -> DataFrame:
        """
        Extract only changed records since last run.
        
        Args:
            last_run_time: Timestamp of last successful run
            
        Returns:
            DataFrame with incremental data
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Extracting incremental data since {last_run_time}"
        )
        
        jdbc_url = self.config.get("jdbc_url")
        source_table = self.config.get("source_table", "etl_source_data")
        
        if jdbc_url:
            # JDBC incremental extraction
            query = f"""
                (SELECT * FROM {source_table} 
                 WHERE changed_at > '{last_run_time}') AS incremental_data
            """
            
            df = self.spark.read.format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", query) \
                .option("driver", self.config.get("jdbc_driver", "org.postgresql.Driver")) \
                .load()
        else:
            # File-based incremental extraction
            df = self._extract_from_files(filter_condition=None)
            df = df.filter(F.col("changed_at") > F.lit(last_run_time))
        
        return df
    
    def _extract_from_jdbc(
        self,
        jdbc_url: str,
        table_name: str,
        filter_condition: Optional[str] = None
    ) -> DataFrame:
        """
        Extract data from JDBC source.
        
        Args:
            jdbc_url: JDBC connection URL
            table_name: Source table name
            filter_condition: Optional filter
            
        Returns:
            DataFrame with extracted data
        """
        if filter_condition:
            query = f"(SELECT * FROM {table_name} WHERE {filter_condition}) AS filtered_data"
        else:
            query = f"(SELECT * FROM {table_name} LIMIT 1000) AS source_data"
        
        df = self.spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", query) \
            .option("driver", self.config.get("jdbc_driver", "org.postgresql.Driver")) \
            .option("user", self.config.get("db_user", "")) \
            .option("password", self.config.get("db_password", "")) \
            .load()
        
        return df
    
    def _extract_from_files(
        self,
        filter_condition: Optional[str] = None
    ) -> DataFrame:
        """
        Extract data from file sources (CSV, Parquet, etc.).
        
        Args:
            filter_condition: Optional filter condition
            
        Returns:
            DataFrame with extracted data
        """
        source_path = self.config.get("source_path", "data/source")
        source_format = self.config.get("source_format", "parquet")
        
        if source_format.lower() == "csv":
            df = self.spark.read.csv(
                source_path,
                header=True,
                schema=self.SOURCE_SCHEMA,
                inferSchema=False
            )
        elif source_format.lower() == "parquet":
            df = self.spark.read.parquet(source_path)
        elif source_format.lower() == "json":
            df = self.spark.read.json(source_path, schema=self.SOURCE_SCHEMA)
        else:
            raise ValueError(f"Unsupported source format: {source_format}")
        
        # Apply filter if provided
        if filter_condition:
            df = df.filter(filter_condition)
        
        return df
    
    def _get_last_successful_run_time(self) -> Optional[datetime]:
        """
        Get timestamp of last successful ETL run.
        
        Returns:
            Timestamp of last successful run or None
        """
        try:
            log_path = self.config.get("log_path", "data/logs")
            
            df = self.spark.read.parquet(f"{log_path}/run_log") \
                .filter(F.col("status") == "SUCCESS") \
                .orderBy(F.col("end_time").desc()) \
                .limit(1)
            
            if df.count() > 0:
                return df.select("end_time").first()[0]
            
            return None
            
        except Exception as e:
            self.logger.log_warning(
                component="EXTRACTOR",
                message="Could not retrieve last run time",
                details=str(e)
            )
            return None
    
    @staticmethod
    def _generate_run_id() -> str:
        """Generate unique run identifier."""
        return datetime.now().strftime("RUN%Y%m%d%H%M%S")


===FILE: src/transform.py===
"""
PySpark-based ETL Transformer Module
Handles data transformation, business rules, enrichment, and validation
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    TimestampType, IntegerType
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Tuple, Dict, Any
from datetime import datetime

from src.logger import ETLLogger


class ETLTransformer:
    """
    PySpark-based data transformation with business rules,
    data enrichment, and validation logic.
    """
    
    # Transformed data schema
    TRANSFORMED_SCHEMA = StructType([
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
    
    def __init__(
        self,
        spark: SparkSession,
        run_id: str,
        config: Dict[str, Any] = None
    ):
        """
        Initialize ETL Transformer.
        
        Args:
            spark: Active SparkSession
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config or {}
        self.logger = ETLLogger.get_instance()
        
        self.logger.log_info(
            component="TRANSFORMER",
            message=f"Initialized transformer - Run ID: {run_id}"
        )
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Main transformation method applying all transformation logic.
        
        Args:
            source_df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Starting transformation"
        )
        
        try:
            # Get current timestamp
            current_timestamp = F.current_timestamp()
            current_user = self.config.get("user", "spark_user")
            
            # Basic transformations
            df = source_df.select(
                F.col("id"),
                F.upper(F.trim(F.col("name"))).alias("name"),
                F.col("value"),
                self._calculate_transformed_value(
                    F.col("value"),
                    F.col("category")
                ).alias("transformed_value"),
                F.lit("TRANSFORMED").alias("status"),
                F.col("category"),
                self._calculate_priority(
                    F.col("value"),
                    F.col("category")
                ).alias("priority"),
                F.lit(self.run_id).alias("etl_run_id"),
                current_timestamp.alias("processed_at"),
                F.lit(current_user).alias("processed_by")
            )
            
            # Apply category-specific rules
            df = self._apply_category_rules(df)
            
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
    
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to transformed data.
        
        Args:
            df: DataFrame to apply rules to
            
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
            F.when(F.col("value").isNull(), "INVALID")
            .when(F.col("transformed_value") >= 750, "HIGH_VALUE")
            .when(F.col("transformed_value") >= 300, "MEDIUM_VALUE")
            .otherwise("LOW_VALUE")
        )
        
        # Rule 2: Priority override for high value items
        df = df.withColumn(
            "priority",
            F.when(F.col("transformed_value") >= 1000, 1)
            .otherwise(F.col("priority"))
        )
        
        # Rule 3: Name normalization (remove extra spaces)
        df = df.withColumn(
            "name",
            F.trim(F.regexp_replace(F.col("name"), "\\s+", " "))
        )
        
        # Rule 4: Category validation
        df = df.withColumn(
            "category",
            F.when(
                (F.col("category").isNull()) | (F.col("category") == ""),
                "UNCATEGORIZED"
            ).otherwise(F.col("category"))
        )
        
        return df
    
    def enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional calculated fields.
        
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
        enrichment_config = self._load_enrichment_config()
        
        # Apply premium category multiplier
        premium_multiplier = enrichment_config.get("premium_multiplier", 1.2)
        
        df = df.withColumn(
            "transformed_value",
            F.when(
                F.col("category") == "PREMIUM",
                F.col("transformed_value") * premium_multiplier
            ).otherwise(F.col("transformed_value"))
        )
        
        return df
    
    def validate_data(
        self,
        df: DataFrame
    ) -> Tuple[bool, List[str]]:
        """
        Validate transformed data against rules.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        validation_errors = []
        
        # Rule 1: ID is required
        null_id_count = df.filter(F.col("id").isNull()).count()
        if null_id_count > 0:
            validation_errors.append(f"{null_id_count} records with missing ID")
        
        # Rule 2: Name is required
        null_name_count = df.filter(
            F.col("name").isNull() | (F.col("name") == "")
        ).count()
        if null_name_count > 0:
            validation_errors.append(f"{null_name_count} records with missing name")
        
        # Rule 3: Value must be positive
        negative_value_count = df.filter(
            (F.col("value") < 0) | (F.col("transformed_value") < 0)
        ).count()
        if negative_value_count > 0:
            validation_errors.append(f"{negative_value_count} records with negative values")
        
        # Rule 4: Priority must be 1-5
        invalid_priority_count = df.filter(
            (F.col("priority") < 1) | (F.col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            validation_errors.append(f"{invalid_priority_count} records with invalid priority")
        
        # Rule 5: Check for duplicates
        duplicate_count = df.count() - df.select("id").distinct().count()
        if duplicate_count > 0:
            validation_errors.append(f"{duplicate_count} duplicate IDs found")
        
        is_valid = len(validation_errors) == 0
        
        if not is_valid:
            self.logger.log_warning(
                component="TRANSFORMER",
                message=f"Validation failed: {len(validation_errors)} errors found"
            )
        
        return is_valid, validation_errors
    
    def _calculate_transformed_value(
        self,
        value_col,
        category_col
    ):
        """
        Calculate derived/transformed value based on business logic.
        
        Args:
            value_col: Column reference for value
            category_col: Column reference for category
            
        Returns:
            Column expression for transformed value
        """
        return F.when(category_col == "PREMIUM", value_col * 1.5) \
            .when(category_col == "VIP", value_col * 2.0) \
            .when(category_col == "STANDARD", value_col * 1.2) \
            .when(category_col == "BASIC", value_col * 1.0) \
            .otherwise(value_col * 1.1)
    
    def _calculate_priority(self, value_col, category_col):
        """
        Calculate priority based on value and category.
        
        Args:
            value_col: Column reference for value
            category_col: Column reference for category
            
        Returns:
            Column expression for priority (1-5, 1 being highest)
        """
        return F.when(category_col == "VIP", 1) \
            .when((category_col == "PREMIUM") & (value_col >= 500), 1) \
            .when(value_col >= 800, 2) \
            .when(value_col >= 500, 3) \
            .when(value_col >= 200, 4) \
            .otherwise(5)
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply category-specific transformation rules.
        
        Args:
            df: DataFrame to apply rules to
            
        Returns:
            DataFrame with category rules applied
        """
        # Add category-specific flags or adjustments
        df = df.withColumn(
            "is_premium",
            F.col("category").isin(["PREMIUM", "VIP"])
        )
        
        return df
    
    def _load_enrichment_config(self) -> Dict[str, Any]:
        """
        Load enrichment configuration.
        
        Returns:
            Dictionary of enrichment configuration
        """
        try:
            config_path = self.config.get("config_path", "data/config")
            
            config_df = self.spark.read.parquet(f"{config_path}/etl_config") \
                .filter(F.col("is_active") == True) \
                .filter(F.col("config_type") == "BUSINESS_RULE")
            
            config_dict = {}
            for row in config_df.collect():
                config_dict[row["config_key"].lower()] = self._parse_config_value(
                    row["config_value"]
                )
            
            return config_dict
            
        except Exception as e:
            self.logger.log_warning(
                component="TRANSFORMER",
                message="Could not load enrichment config, using defaults",
                details=str(e)
            )
            return {
                "premium_multiplier": 1.5,
                "vip_multiplier": 2.0
            }
    
    @staticmethod
    def _parse_config_value(value: str) -> Any:
        """Parse configuration value to appropriate type."""
        try:
            # Try to parse as float
            return float(value)
        except ValueError:
            # Return as string
            return value


===FILE: src/load.py===
"""
PySpark-based ETL Loader Module
Handles data loading with batch processing, error handling, and reconciliation
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict, Any, List
from dataclasses import dataclass
from datetime import datetime

from src.logger import ETLLogger


@dataclass
class LoadResult:
    """Result of load operation."""
    success_count: int
    error_count: int
    total_count: int
    errors: List[str]


class ETLLoader:
    """
    PySpark-based data loader with batch processing,
    error handling, and data reconciliation.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        target_type: str = "DATABASE",
        batch_size: int = 1000,
        run_id: str = None,
        config: Dict[str, Any] = None
    ):
        """
        Initialize ETL Loader.
        
        Args:
            spark: Active SparkSession
            target_type: Type of target (DATABASE, FILE, etc.)
            batch_size: Number of records per batch
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.target_type = target_type.upper()
        self.batch_size = batch_size
        self.run_id = run_id
        self.config = config or {}
        self.logger = ETLLogger.get_instance()
        
        self.logger.log_info(
            component="LOADER",
            message=f"Initialized loader - Target: {target_type}, Batch size: {batch_size}"
        )
    
    def load_data(
        self,
        data_df: DataFrame,
        mode: str = "INSERT"
    ) -> LoadResult:
        """
        Main loading method with batch processing.
        
        Args:
            data_df: DataFrame to load
            mode: Load mode (INSERT, UPDATE, UPSERT)
            
        Returns:
            LoadResult with statistics
        """
        self.logger.log_info(
            component="LOADER",
            message=f"Starting load - Mode: {mode}, Batch size: {self.batch_size}"
        )
        
        mode = mode.upper()
        total_count = data_df.count()
        errors = []
        
        try:
            if self.target_type == "DATABASE":
                success = self.load_to_database(data_df, mode)
            else:
                success = self._load_to_files(data_df, mode)
            
            if success:
                success_count = total_count
                error_count = 0
                
                # Perform reconciliation
                if self.config.get("enable_reconciliation", True):
                    reconciled = self.reconcile_data(data_df)
                    if not reconciled:
                        self.logger.log_warning(
                            component="LOADER",
                            message="Data reconciliation failed"
                        )
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
    
    def load_to_database(
        self,
        data_df: DataFrame,
        mode: str
    ) -> bool:
        """
        Load data to database target.
        
        Args:
            data_df: DataFrame to load
            mode: Load mode
            
        Returns:
            True if successful
        """
        try:
            jdbc_url = self.config.get("jdbc_url")
            target_table = self.config.get("target_table", "etl_target_data")
            
            if not jdbc_url:
                raise ValueError("JDBC URL not configured")
            
            if mode == "INSERT":
                write_mode = "append"
            elif mode == "UPDATE":
                # For updates, we need to handle differently
                write_mode = "overwrite"
            elif mode == "UPSERT":
                # Upsert requires special handling
                return self._upsert_to_database(data_df, jdbc_url, target_table)
            else:
                write_mode = "append"
            
            # Write to database
            data_df.write.format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", target_table) \
                .option("driver", self.config.get("jdbc_driver", "org.postgresql.Driver")) \
                .option("user", self.config.get("db_user", "")) \
                .option("password", self.config.get("db_password", "")) \
                .option("batchsize", self.batch_size) \
                .mode(write_mode) \
                .save()
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Database load error",
                details=str(e)
            )
            return False
    
    def reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data against source.
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passes
        """
        try:
            jdbc_url = self.config.get("jdbc_url")
            target_table = self.config.get("target_table", "etl_target_data")
            
            if not jdbc_url:
                self.logger.log_warning(
                    component="LOADER",
                    message="Cannot reconcile: JDBC URL not configured"
                )
                return True
            
            # Read back from target
            target_df = self.spark.read.format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", f"(SELECT * FROM {target_table} WHERE etl_run_id = '{self.run_id}') AS target") \
                .option("driver", self.config.get("jdbc_driver", "org.postgresql.Driver")) \
                .option("user", self.config.get("db_user", "")) \
                .option("password", self.config.get("db_password", "")) \
                .load()
            
            # Compare counts
            loaded_count = loaded_df.count()
            target_count = target_df.count()
            
            if loaded_count != target_count:
                self.logger.log_error(
                    component="LOADER",
                    message=f"Reconciliation failed: Loaded {loaded_count} but found {target_count} in target"
                )
                return False
            
            # Compare checksums or sample data
            loaded_sum = loaded_df.agg(F.sum("value")).collect()[0][0]
            target_sum = target_df.agg(F.sum("value")).collect()[0][0]
            
            if abs(float(loaded_sum or 0) - float(target_sum or 0)) > 0.01:
                self.logger.log_error(
                    component="LOADER",
                    message="Reconciliation failed: Value sums don't match"
                )
                return False
            
            self.logger.log_info(
                component="LOADER",
                message="Data reconciliation successful"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Reconciliation error",
                details=str(e)
            )
            return False
    
    def _load_to_files(self, data_df: DataFrame, mode: str) -> bool:
        """
        Load data to file target (Parquet, CSV, etc.).
        
        Args:
            data_df: DataFrame to load
            mode: Write mode
            
        Returns:
            True if successful
        """
        try:
            target_path = self.config.get("target_path", "data/target")
            target_format = self.config.get("target_format", "parquet")
            
            write_mode = "overwrite" if mode == "UPDATE" else "append"
            
            if target_format.lower() == "parquet":
                data_df.write.mode(write_mode).parquet(target_path)
            elif target_format.lower() == "csv":
                data_df.write.mode(write_mode).option("header", True).csv(target_path)
            elif target_format.lower() == "json":
                data_df.write.mode(write_mode).json(target_path)
            else:
                raise ValueError(f"Unsupported target format: {target_format}")
            
            return True
            
        except Exception as e:
            self.logger.log_error(