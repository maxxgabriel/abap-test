# PySpark ETL Migration - Column Operations & Transformations

===FILE: src/transform.py===
"""
PySpark Transformer Module
Migrated from ABAP zcl_etl_transformer
Implements column operations, business rules, and data enrichment
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, trim, when, lit, current_timestamp, 
    regexp_replace, coalesce, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, 
    IntegerType, TimestampType
)
from typing import Dict, List, Tuple
import logging
from datetime import datetime


class PySparkTransformer:
    """
    Main transformer class for ETL data transformations
    Converts ABAP operations to PySpark DataFrame API
    """
    
    def __init__(self, run_id: str, config: Dict):
        """
        Initialize transformer with run ID and configuration
        
        Args:
            run_id: ETL run identifier
            config: Configuration dictionary with transformation rules
        """
        self.run_id = run_id
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Define output schema
        self.transformed_schema = StructType([
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
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Main transformation method - applies all transformations
        Migrates: LOOP AT -> DataFrame.withColumn() chains
        
        Args:
            df: Source DataFrame with extracted data
            
        Returns:
            Transformed DataFrame with all rules applied
        """
        self.logger.info("Starting data transformation")
        
        # Apply column transformations using withColumn chains
        # Migrates: to_upper(condense(name)) -> upper(trim(name))
        transformed_df = df \
            .withColumn("name", upper(trim(col("name")))) \
            .withColumn("name", regexp_replace(col("name"), "\\s+", " ")) \
            .withColumn("transformed_value", 
                       self._calculate_derived_values(
                           col("value"), 
                           col("category")
                       )) \
            .withColumn("status", lit("TRANSFORMED")) \
            .withColumn("priority", 
                       self._calculate_priority(
                           col("value"),
                           col("category")
                       )) \
            .withColumn("etl_run_id", lit(self.run_id)) \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("processed_by", lit(self.config.get("user", "system")))
        
        # Apply category-specific rules
        transformed_df = self._apply_category_rules(transformed_df)
        
        # Apply business rules
        transformed_df = self._apply_business_rules(transformed_df)
        
        # Enrich data
        transformed_df = self._enrich_data(transformed_df)
        
        record_count = transformed_df.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return transformed_df
    
    def _calculate_derived_values(self, value_col, category_col):
        """
        Calculate transformed value based on business logic
        Migrates: ABAP calculate_derived_values method
        
        Args:
            value_col: Column expression for value
            category_col: Column expression for category
            
        Returns:
            Column expression with calculated value
        """
        premium_multiplier = self.config.get("premium_multiplier", 1.5)
        vip_multiplier = self.config.get("vip_multiplier", 1.8)
        standard_multiplier = self.config.get("standard_multiplier", 1.2)
        
        # Migrates: CASE statement -> when().otherwise() chain
        return when(category_col == "PREMIUM", value_col * premium_multiplier) \
            .when(category_col == "VIP", value_col * vip_multiplier) \
            .when(category_col == "STANDARD", value_col * standard_multiplier) \
            .when(category_col == "BASIC", value_col * 1.0) \
            .otherwise(value_col * 1.1)
    
    def _calculate_priority(self, value_col, category_col):
        """
        Calculate priority based on value and category
        Migrates: ABAP calculate_priority method using CASE statements
        
        Args:
            value_col: Column expression for value
            category_col: Column expression for category
            
        Returns:
            Column expression with priority (1-5)
        """
        # Priority rules: Higher value = higher priority (lower number)
        # Migrates: Multiple CASE WHEN statements to when().otherwise()
        return when(value_col >= 1000, lit(1)) \
            .when((value_col >= 750) & (category_col == "PREMIUM"), lit(1)) \
            .when((value_col >= 750) & (category_col == "VIP"), lit(1)) \
            .when(value_col >= 750, lit(2)) \
            .when(value_col >= 500, lit(2)) \
            .when(value_col >= 300, lit(3)) \
            .when(value_col >= 100, lit(4)) \
            .otherwise(lit(5))
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply category-specific transformation rules
        Migrates: ABAP apply_category_rules with LOOP AT
        
        Args:
            df: DataFrame to transform
            
        Returns:
            DataFrame with category rules applied
        """
        # Apply different rules based on category
        # This replaces the LOOP AT with field-symbol updates
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", 
                 col("transformed_value") * 1.2)
            .when(col("category") == "VIP",
                 col("transformed_value") * 1.3)
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply comprehensive business rules
        Migrates: ABAP apply_business_rules method with LOOP AT assignments
        
        Args:
            df: DataFrame to transform
            
        Returns:
            DataFrame with business rules applied
        """
        self.logger.info("Applying business rules")
        
        # Rule 1: Set status based on transformed value
        # Migrates: CASE statement -> when().otherwise()
        df = df.withColumn(
            "status",
            when(col("value").isNull(), lit("INVALID"))
            .when(col("transformed_value") >= 750, lit("HIGH_VALUE"))
            .when(col("transformed_value") >= 300, lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )
        
        # Rule 2: Priority override for high value items
        # Migrates: IF statement -> when()
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, lit(1))
            .otherwise(col("priority"))
        )
        
        # Rule 3: Name normalization (already done in main transform)
        # Additional cleanup: remove extra spaces
        df = df.withColumn(
            "name",
            regexp_replace(trim(col("name")), "\\s+", " ")
        )
        
        # Rule 4: Category validation
        # Migrates: IF IS INITIAL -> coalesce()
        df = df.withColumn(
            "category",
            coalesce(col("category"), lit("UNCATEGORIZED"))
        )
        
        # Rule 5: Round transformed values to 2 decimals
        df = df.withColumn(
            "transformed_value",
            spark_round(col("transformed_value"), 2)
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional calculated fields
        Migrates: ABAP enrich_data method
        
        Args:
            df: DataFrame to enrich
            
        Returns:
            Enriched DataFrame
        """
        # Sample enrichment: add calculated fields based on config
        enable_enrichment = self.config.get("enable_enrichment", True)
        
        if enable_enrichment:
            # Add value tier classification
            df = df.withColumn(
                "value_tier",
                when(col("transformed_value") >= 1000, lit("TIER_1"))
                .when(col("transformed_value") >= 500, lit("TIER_2"))
                .when(col("transformed_value") >= 200, lit("TIER_3"))
                .otherwise(lit("TIER_4"))
            )
            
            # Add processing flags
            df = df.withColumn(
                "requires_review",
                when(col("priority") <= 2, lit(True))
                .otherwise(lit(False))
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
        errors = []
        
        # Rule 1: Check for null IDs
        null_id_count = df.filter(col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with null ID")
        
        # Rule 2: Check for empty names
        empty_name_count = df.filter(
            (col("name").isNull()) | (trim(col("name")) == "")
        ).count()
        if empty_name_count > 0:
            errors.append(f"{empty_name_count} records with empty name")
        
        # Rule 3: Check for negative values
        negative_value_count = df.filter(
            (col("value") < 0) | (col("transformed_value") < 0)
        ).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative values")
        
        # Rule 4: Check priority range
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if not is_valid:
            self.logger.warning(f"Validation failed: {errors}")
        
        return is_valid, errors


def create_transformer(run_id: str, config: Dict) -> PySparkTransformer:
    """
    Factory function to create transformer instance
    
    Args:
        run_id: ETL run identifier
        config: Configuration dictionary
        
    Returns:
        Configured PySparkTransformer instance
    """
    return PySparkTransformer(run_id, config)


===FILE: src/extract.py===
"""
PySpark Extractor Module
Migrated from ABAP zcl_etl_extractor
Handles data extraction from various sources
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, TimestampType
)
from typing import Optional, Dict
import logging


class PySparkExtractor:
    """
    Data extraction class supporting multiple source types
    """
    
    def __init__(self, spark: SparkSession, source_type: str, run_id: str, config: Dict):
        """
        Initialize extractor
        
        Args:
            spark: SparkSession instance
            source_type: Type of data source (DATABASE, STAGING, INCREMENTAL)
            run_id: ETL run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.source_type = source_type.upper() if source_type else "DATABASE"
        self.run_id = run_id
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Define source schema
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
    
    def extract_data(self, filter_condition: Optional[str] = None, 
                    max_records: int = 0) -> DataFrame:
        """
        Main extraction method - routes to specific extractor
        
        Args:
            filter_condition: Optional SQL filter condition
            max_records: Maximum number of records to extract (0 = unlimited)
            
        Returns:
            Extracted DataFrame
        """
        self.logger.info(f"Starting extraction - Source: {self.source_type}")
        
        # Route to appropriate extraction method
        if self.source_type == "DATABASE":
            df = self._extract_from_database(filter_condition)
        elif self.source_type == "STAGING":
            df = self._extract_from_staging()
        elif self.source_type == "INCREMENTAL":
            df = self._extract_incremental()
        else:
            self.logger.warning(f"Unknown source type {self.source_type}, defaulting to DATABASE")
            df = self._extract_from_database(filter_condition)
        
        # Apply max records limit
        if max_records > 0:
            df = df.limit(max_records)
        
        record_count = df.count()
        self.logger.info(f"Extracted {record_count} records")
        
        return df
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract from database source
        
        Args:
            filter_condition: SQL WHERE clause condition
            
        Returns:
            DataFrame with extracted data
        """
        jdbc_url = self.config.get("source_jdbc_url")
        table_name = self.config.get("source_table", "etl_source_data")
        
        # Build read options
        read_options = {
            "url": jdbc_url,
            "dbtable": table_name,
            "driver": self.config.get("jdbc_driver", "org.postgresql.Driver")
        }
        
        # Add credentials if provided
        if "source_user" in self.config:
            read_options["user"] = self.config["source_user"]
            read_options["password"] = self.config["source_password"]
        
        # Read from database
        df = self.spark.read.format("jdbc").options(**read_options).load()
        
        # Apply filter if provided
        if filter_condition:
            df = df.filter(filter_condition)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """
        Extract from staging area
        
        Returns:
            DataFrame with staged data
        """
        staging_path = self.config.get("staging_path")
        file_format = self.config.get("staging_format", "parquet")
        
        df = self.spark.read.format(file_format).load(staging_path)
        
        # Filter by run_id and status
        df = df.filter(
            (df.run_id == self.run_id) & 
            (df.status == "READY")
        )
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run
        
        Returns:
            DataFrame with incremental data
        """
        # Get last successful run timestamp from run log
        run_log_path = self.config.get("run_log_table")
        
        last_run_df = self.spark.read.format("jdbc").options(
            url=self.config.get("metadata_jdbc_url"),
            dbtable=f"(SELECT MAX(end_time) as last_run FROM {run_log_path} WHERE status = 'SUCCESS') as subq"
        ).load()
        
        last_run_time = last_run_df.collect()[0]["last_run"]
        
        # Extract records changed after last run
        df = self._extract_from_database()
        
        if last_run_time:
            df = df.filter(df.changed_at > last_run_time)
        
        return df


def create_extractor(spark: SparkSession, source_type: str, 
                    run_id: str, config: Dict) -> PySparkExtractor:
    """
    Factory function to create extractor instance
    
    Args:
        spark: SparkSession instance
        source_type: Type of source
        run_id: Run identifier
        config: Configuration dictionary
        
    Returns:
        Configured PySparkExtractor instance
    """
    return PySparkExtractor(spark, source_type, run_id, config)


===FILE: src/load.py===
"""
PySpark Loader Module
Migrated from ABAP zcl_etl_loader
Handles data loading to target systems
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Dict, List
import logging


class LoadResult:
    """Data class for load operation results"""
    
    def __init__(self):
        self.success_count = 0
        self.error_count = 0
        self.total_count = 0
        self.errors: List[str] = []


class PySparkLoader:
    """
    Data loader class supporting batch operations and reconciliation
    """
    
    def __init__(self, spark: SparkSession, target_type: str, 
                 batch_size: int, run_id: str, config: Dict):
        """
        Initialize loader
        
        Args:
            spark: SparkSession instance
            target_type: Target type (DATABASE, FILE, etc.)
            batch_size: Batch size for loading
            run_id: ETL run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.target_type = target_type.upper() if target_type else "DATABASE"
        self.batch_size = batch_size
        self.run_id = run_id
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def load_data(self, df: DataFrame, mode: str = "INSERT") -> LoadResult:
        """
        Main load method with batch processing
        
        Args:
            df: DataFrame to load
            mode: Load mode (INSERT, UPDATE, UPSERT)
            
        Returns:
            LoadResult with statistics
        """
        self.logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        result = LoadResult()
        result.total_count = df.count()
        
        try:
            # Process based on mode
            if mode.upper() == "INSERT":
                success = self._insert_new(df)
            elif mode.upper() == "UPDATE":
                success = self._update_existing(df)
            elif mode.upper() == "UPSERT":
                success = self._upsert_data(df)
            else:
                success = self._insert_new(df)
            
            if success:
                result.success_count = result.total_count
                result.error_count = 0
            else:
                result.success_count = 0
                result.error_count = result.total_count
                result.errors.append("Load operation failed")
            
            # Reconciliation
            if self.config.get("enable_reconciliation", True):
                reconciled = self._reconcile_data(df)
                if not reconciled:
                    self.logger.warning("Data reconciliation failed")
                    result.errors.append("Reconciliation mismatch detected")
            
            self.logger.info(
                f"Load complete - Success: {result.success_count}, "
                f"Errors: {result.error_count}"
            )
            
        except Exception as e:
            self.logger.error(f"Load error: {str(e)}")
            result.error_count = result.total_count
            result.errors.append(str(e))
        
        return result
    
    def _insert_new(self, df: DataFrame) -> bool:
        """
        Insert new records to target
        
        Args:
            df: DataFrame to insert
            
        Returns:
            True if successful
        """
        try:
            jdbc_url = self.config.get("target_jdbc_url")
            table_name = self.config.get("target_table", "etl_target_data")
            
            write_options = {
                "url": jdbc_url,
                "dbtable": table_name,
                "driver": self.config.get("jdbc_driver", "org.postgresql.Driver"),
                "batchsize": str(self.batch_size)
            }
            
            if "target_user" in self.config:
                write_options["user"] = self.config["target_user"]
                write_options["password"] = self.config["target_password"]
            
            df.write.format("jdbc").options(**write_options).mode("append").save()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Insert failed: {str(e)}")
            return False
    
    def _update_existing(self, df: DataFrame) -> bool:
        """
        Update existing records
        
        Args:
            df: DataFrame with updates
            
        Returns:
            True if successful
        """
        # For update, we need to use a temporary table and merge
        try:
            temp_table = f"temp_update_{self.run_id}"
            
            # Write to temp table
            df.createOrReplaceTempView(temp_table)
            
            # Execute merge/update using SQL
            target_table = self.config.get("target_table")
            
            merge_sql = f"""
                MERGE INTO {target_table} AS target
                USING {temp_table} AS source
                ON target.id = source.id
                WHEN MATCHED THEN UPDATE SET *
            """
            
            self.spark.sql(merge_sql)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Update failed: {str(e)}")
            return False
    
    def _upsert_data(self, df: DataFrame) -> bool:
        """
        Upsert (insert or update) data
        
        Args:
            df: DataFrame to upsert
            
        Returns:
            True if successful
        """
        try:
            # Try update first
            if not self._update_existing(df):
                # Fall back to insert
                return self._insert_new(df)
            return True
            
        except Exception as e:
            self.logger.error(f"Upsert failed: {str(e)}")
            return False
    
    def _reconcile_data(self, df: DataFrame) -> bool:
        """
        Reconcile loaded data with source
        
        Args:
            df: Source DataFrame
            
        Returns:
            True if reconciliation passes
        """
        try:
            source_count = df.count()
            
            # Read back from target
            target_df = self.spark.read.format("jdbc").options(
                url=self.config.get("target_jdbc_url"),
                dbtable=self.config.get("target_table"),
                driver=self.config.get("jdbc_driver")
            ).load()
            
            # Filter target by run_id
            target_count = target_df.filter(
                target_df.etl_run_id == self.run_id
            ).count()
            
            matches = (source_count == target_count)
            
            if not matches:
                self.logger.warning(
                    f"Reconciliation mismatch: Source={source_count}, "
                    f"Target={target_count}"
                )
            
            return matches
            
        except Exception as e:
            self.logger.error(f"Reconciliation error: {str(e)}")
            return False


def create_loader(spark: SparkSession, target_type: str, 
                 batch_size: int, run_id: str, config: Dict) -> PySparkLoader:
    """
    Factory function to create loader instance
    
    Args:
        spark: SparkSession instance
        target_type: Target type
        batch_size: Batch size
        run_id: Run identifier
        config: Configuration dictionary
        
    Returns:
        Configured PySparkLoader instance
    """
    return PySparkLoader(spark, target_type, batch_size, run_id, config)


===FILE: src/orchestrator.py===
"""
PySpark Orchestrator Module
Main ETL workflow orchestration
"""

from pyspark.sql import SparkSession
from typing import Dict
import logging
from datetime import datetime
import uuid

from src.extract import create_extractor
from src.transform import create_transformer
from src.load import create_loader, LoadResult


class ETLResult:
    """Data class for ETL execution results"""
    
    def __init__(self):
        self.run_id = ""
        self.status = "RUNNING"
        self.start_time = None
        self.end_time = None
        self.duration = 0
        self.records_extracted = 0
        self.records_transformed = 0
        self.records_loaded = 0
        self.records_failed = 0
        self.error_count = 0
        self.warning_count = 0


class PySparkOrchestrator:
    """
    Main orchestrator for ETL workflow execution
    """
    
    def __init__(self, spark: SparkSession, config: Dict, run_type: str = "MANUAL"):
        """
        Initialize orchestrator
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_type: Type of run (MANUAL, SCHEDULED, TEST)
        """
        self.spark = spark
        self.config = config
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        self.logger = logging.getLogger(__name__)
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def execute_etl(self, source_type: str = "DATABASE", 
                   target_type: str = "DATABASE",
                   filter_condition: str = None,
                   batch_size: int = 1000,
                   max_records: int = 0) -> ETLResult:
        """
        Execute complete ETL workflow
        
        Args:
            source_type: Source data type
            target_type: Target data type
            filter_condition: Optional filter
            batch_size: Batch size for loading
            max_records: Maximum records to process
            
        Returns:
            ETLResult with execution statistics
        """
        result = ETLResult()
        result.run_id = self.run_id
        result.start_time = datetime.now()
        
        self.logger.info(f"ETL execution started - Run ID: {self.run_id}")
        
        try:
            # Step 1: Extract
            extractor = create_extractor(
                self.spark, source_type, self.run_id, self.config
            )
            source_df = extractor.extract_data(filter_condition, max_records)
            result.records_extracted = source_df.count()
            
            if result.records_extracted == 0:
                self.logger.warning("No data extracted - stopping ETL")
                result.status = "NO_DATA"
                return result
            
            # Step 2: Transform
            transformer = create_transformer(self.run_id, self.config)
            transformed_df = transformer.transform_data(source_df)
            
            # Validate
            is_valid, validation_errors = transformer.validate_data(transformed_df)
            
            if not is_valid:
                self.logger.error(f"Validation failed: {validation_errors}")
                result.status = "VALIDATION_FAILED"
                result.error_count = len(validation_errors)
                return result
            
            result.records_transformed = transformed_df.count()
            
            # Step 3: Load
            loader = create_loader(
                self.spark, target_type, batch_size, self.run_id, self.config
            )
            load_result = loader.load_data(transformed_df, mode="UPSERT")
            
            result.records_loaded = load_result.success_count
            result.records_failed = load_result.error_count
            result.error_count = load_result.error_count
            
            # Determine final status
            if load_result.error_count == 0:
                result.status = "SUCCESS"
            elif load_result.success_count > 0:
                result.status = "PARTIAL_SUCCESS"
            else:
                result.status = "FAILED"
            
            result.end_time = datetime.now()
            result.duration = (result.end_time - result.start_time).total_seconds()
            
            self.logger.info(f"ETL execution completed - Status: {result.status}")
            
        except Exception as e:
            self.logger.error(f"ETL execution error: {str(e)}")
            result.status = "ERROR"
            result.end_time = datetime.now()
            result.error_count += 1
        
        return result
    
    def _generate_run_id(self) -> str:
        """Generate unique run ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"RUN_{timestamp}_{unique_id}"


def create_orchestrator(spark: SparkSession, config: Dict, 
                       run_type: str = "MANUAL") -> PySparkOrchestrator:
    """
    Factory function to create orchestrator
    
    Args:
        spark: SparkSession instance
        config: Configuration dictionary
        run_type: Run type
        
    Returns:
        Configured orchestrator instance
    """
    return PySparkOrchestrator(spark, config, run_type)


===FILE: config.yaml===
# PySpark ETL Configuration
# Migrated from ABAP ETL configuration tables

# Application Settings
app:
  name: "PySpark ETL Pipeline"
  version: "1.0.0"
  user: "etl_system"

# Spark Configuration
spark:
  app_name: "ETL_Transformer"
  master: "local[*]"
  config:
    spark.sql.shuffle.partitions: 200
    spark.sql.adaptive.enabled: true
    spark.sql.adaptive.coalescePartitions.enabled: true
    spark.executor.memory: "4g"
    spark.driver.memory: "2g"

# Source Configuration
source:
  type: "DATABASE"
  jdbc_url: "jdbc:postgresql://localhost:5432/source_db"
  jdbc_driver: "org.postgresql.Driver"
  user: "etl_user"
  password: "${SOURCE_PASSWORD}"
  table: "et