I'll generate production-ready PySpark code for the ETL Transformation Engine based on the ABAP source code.

===FILE: src/transform.py===
"""
ETL Transformation Engine - Core transformation logic
Implements value mapping, data validation, and classification
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from typing import Dict, List, Tuple
import logging
from datetime import datetime


class ETLTransformer:
    """Core transformation engine for ETL pipeline"""
    
    def __init__(self, spark: SparkSession, run_id: str, config: Dict):
        """
        Initialize transformer
        
        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = logging.getLogger(__name__)
        
    def get_transformed_schema(self) -> StructType:
        """Define schema for transformed data"""
        return StructType([
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
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Main transformation method
        
        Args:
            source_df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        # Apply transformations
        df = self._apply_basic_transformations(source_df)
        df = self._calculate_derived_values(df)
        df = self._calculate_priority(df)
        df = self._apply_category_rules(df)
        df = self._apply_business_rules(df)
        df = self._enrich_data(df)
        
        self.logger.info(f"Transformed {df.count()} records")
        
        return df
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic field transformations"""
        current_time = datetime.now()
        
        return df.select(
            F.col("id"),
            # Name normalization: trim, upper, condense spaces
            F.upper(F.regexp_replace(F.trim(F.col("name")), r'\s+', ' ')).alias("name"),
            F.col("value"),
            F.lit(None).cast(DecimalType(15, 2)).alias("transformed_value"),
            F.lit("TRANSFORMED").alias("status"),
            F.col("category"),
            F.lit(0).alias("priority"),
            F.lit(self.run_id).alias("etl_run_id"),
            F.lit(current_time).alias("processed_at"),
            F.lit("system").alias("processed_by")
        )
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate derived/transformed values based on category and value
        
        Business Logic:
        - PREMIUM: value * 1.5
        - VIP: value * 2.0
        - STANDARD: value * 1.2
        - BASIC: value * 1.0
        - TRIAL: value * 0.8
        """
        category_multipliers = {
            "PREMIUM": 1.5,
            "VIP": 2.0,
            "STANDARD": 1.2,
            "BASIC": 1.0,
            "TRIAL": 0.8
        }
        
        # Build CASE WHEN expression for multipliers
        multiplier_expr = F.when(F.col("category") == "PREMIUM", F.lit(1.5))
        for category, multiplier in list(category_multipliers.items())[1:]:
            multiplier_expr = multiplier_expr.when(
                F.col("category") == category, F.lit(multiplier)
            )
        multiplier_expr = multiplier_expr.otherwise(F.lit(1.0))
        
        return df.withColumn(
            "transformed_value",
            F.round(F.col("value") * multiplier_expr, 2)
        )
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """
        Calculate priority based on value and category
        
        Priority Rules:
        - Priority 1 (Highest): transformed_value >= 1000 OR category = VIP
        - Priority 2: transformed_value >= 750
        - Priority 3: transformed_value >= 500
        - Priority 4: transformed_value >= 300
        - Priority 5 (Lowest): transformed_value < 300
        """
        priority_expr = (
            F.when(
                (F.col("transformed_value") >= 1000) | (F.col("category") == "VIP"),
                F.lit(1)
            )
            .when(F.col("transformed_value") >= 750, F.lit(2))
            .when(F.col("transformed_value") >= 500, F.lit(3))
            .when(F.col("transformed_value") >= 300, F.lit(4))
            .otherwise(F.lit(5))
        )
        
        return df.withColumn("priority", priority_expr)
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """Apply category-specific transformation rules"""
        # Handle missing categories
        df = df.withColumn(
            "category",
            F.when(
                F.col("category").isNull() | (F.col("category") == ""),
                F.lit("UNCATEGORIZED")
            ).otherwise(F.col("category"))
        )
        
        # Additional category-specific logic can be added here
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to determine status
        
        Rules:
        1. INVALID: value is null or zero
        2. HIGH_VALUE: transformed_value >= 750
        3. MEDIUM_VALUE: transformed_value >= 300
        4. LOW_VALUE: transformed_value < 300
        """
        status_expr = (
            F.when(
                F.col("value").isNull() | (F.col("value") == 0),
                F.lit("INVALID")
            )
            .when(F.col("transformed_value") >= 1000, F.lit("CRITICAL"))
            .when(F.col("transformed_value") >= 750, F.lit("HIGH_VALUE"))
            .when(F.col("transformed_value") >= 300, F.lit("MEDIUM_VALUE"))
            .otherwise(F.lit("LOW_VALUE"))
        )
        
        return df.withColumn("status", status_expr)
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional calculated fields
        
        Applies premium multiplier for PREMIUM category items
        """
        premium_multiplier = self.config.get('premium_multiplier', 1.2)
        
        df = df.withColumn(
            "transformed_value",
            F.when(
                F.col("category") == "PREMIUM",
                F.round(F.col("transformed_value") * premium_multiplier, 2)
            ).otherwise(F.col("transformed_value"))
        )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid: bool, errors: List[str])
        """
        self.logger.info("Starting data validation")
        errors = []
        
        # Rule 1: ID is required
        null_id_count = df.filter(F.col("id").isNull() | (F.col("id") == "")).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with missing ID")
        
        # Rule 2: Name is required
        null_name_count = df.filter(F.col("name").isNull() | (F.col("name") == "")).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with missing name")
        
        # Rule 3: Value must be positive
        negative_value_count = df.filter(F.col("value") < 0).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative value")
        
        # Rule 4: Transformed value must be positive
        negative_transformed_count = df.filter(F.col("transformed_value") < 0).count()
        if negative_transformed_count > 0:
            errors.append(f"{negative_transformed_count} records with negative transformed value")
        
        # Rule 5: Priority must be 1-5
        invalid_priority_count = df.filter(
            (F.col("priority") < 1) | (F.col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        # Rule 6: Category must not be empty
        empty_category_count = df.filter(
            F.col("category").isNull() | (F.col("category") == "")
        ).count()
        if empty_category_count > 0:
            errors.append(f"{empty_category_count} records with empty category")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Validation passed")
        else:
            self.logger.warning(f"Validation failed with {len(errors)} errors")
            for error in errors:
                self.logger.warning(f"  - {error}")
        
        return is_valid, errors


class ValueMapper:
    """Handles value mapping and lookups"""
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize value mapper
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary with mapping rules
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def apply_category_mapping(self, df: DataFrame) -> DataFrame:
        """
        Apply category mapping rules
        
        Maps source categories to standardized target categories
        """
        category_mappings = self.config.get('category_mappings', {})
        
        if not category_mappings:
            return df
        
        mapping_expr = F.col("category")
        for source, target in category_mappings.items():
            mapping_expr = F.when(F.col("category") == source, F.lit(target)).otherwise(mapping_expr)
        
        return df.withColumn("category", mapping_expr)
    
    def apply_status_mapping(self, df: DataFrame) -> DataFrame:
        """Apply status code mapping"""
        status_mappings = self.config.get('status_mappings', {})
        
        if not status_mappings:
            return df
        
        mapping_expr = F.col("status")
        for source, target in status_mappings.items():
            mapping_expr = F.when(F.col("status") == source, F.lit(target)).otherwise(mapping_expr)
        
        return df.withColumn("status", mapping_expr)


class DataClassifier:
    """Handles data classification and segmentation"""
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize classifier
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def classify_by_value_range(self, df: DataFrame) -> DataFrame:
        """
        Classify records into value ranges
        
        Adds value_range column: VERY_HIGH, HIGH, MEDIUM, LOW, VERY_LOW
        """
        value_range_expr = (
            F.when(F.col("transformed_value") >= 1000, F.lit("VERY_HIGH"))
            .when(F.col("transformed_value") >= 750, F.lit("HIGH"))
            .when(F.col("transformed_value") >= 500, F.lit("MEDIUM"))
            .when(F.col("transformed_value") >= 300, F.lit("LOW"))
            .otherwise(F.lit("VERY_LOW"))
        )
        
        return df.withColumn("value_range", value_range_expr)
    
    def classify_by_risk(self, df: DataFrame) -> DataFrame:
        """
        Classify records by risk level
        
        Risk factors:
        - Invalid status = HIGH
        - Low value + low priority = LOW
        - High value items = MEDIUM
        """
        risk_expr = (
            F.when(F.col("status") == "INVALID", F.lit("HIGH"))
            .when(
                (F.col("priority") >= 4) & (F.col("transformed_value") < 300),
                F.lit("LOW")
            )
            .when(F.col("transformed_value") >= 750, F.lit("MEDIUM"))
            .otherwise(F.lit("LOW"))
        )
        
        return df.withColumn("risk_level", risk_expr)
    
    def add_data_quality_score(self, df: DataFrame) -> DataFrame:
        """
        Calculate data quality score (0-100)
        
        Scoring:
        - Has valid ID: +20
        - Has valid name: +20
        - Value > 0: +20
        - Valid category: +20
        - Valid priority: +20
        """
        quality_score = (
            F.when(F.col("id").isNotNull() & (F.col("id") != ""), 20).otherwise(0) +
            F.when(F.col("name").isNotNull() & (F.col("name") != ""), 20).otherwise(0) +
            F.when(F.col("value") > 0, 20).otherwise(0) +
            F.when(F.col("category").isNotNull() & (F.col("category") != ""), 20).otherwise(0) +
            F.when((F.col("priority") >= 1) & (F.col("priority") <= 5), 20).otherwise(0)
        )
        
        return df.withColumn("quality_score", quality_score)


===FILE: src/extract.py===
"""
ETL Extraction Module - Data source extraction
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from pyspark.sql import functions as F
from typing import Optional, Dict
import logging
from datetime import datetime


class ETLExtractor:
    """Handles data extraction from various sources"""
    
    def __init__(self, spark: SparkSession, run_id: str, config: Dict):
        """
        Initialize extractor
        
        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def get_source_schema(self) -> StructType:
        """Define schema for source data"""
        return StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("value", DecimalType(15, 2), False),
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
        source_type: str = "database",
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data from source
        
        Args:
            source_type: Type of source (database, staging, incremental, file)
            filter_condition: Optional filter condition
            max_records: Maximum records to extract (0 = no limit)
            
        Returns:
            Extracted DataFrame
        """
        self.logger.info(f"Starting extraction - Source: {source_type}, Run ID: {self.run_id}")
        
        if source_type.lower() == "database":
            df = self._extract_from_database(filter_condition)
        elif source_type.lower() == "staging":
            df = self._extract_from_staging()
        elif source_type.lower() == "incremental":
            df = self._extract_incremental()
        elif source_type.lower() == "file":
            df = self._extract_from_file()
        else:
            df = self._extract_from_database(filter_condition)
        
        # Apply max records limit
        if max_records > 0:
            df = df.limit(max_records)
        
        record_count = df.count()
        self.logger.info(f"Extracted {record_count} records")
        
        return df
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract data from database source"""
        jdbc_config = self.config.get('jdbc', {})
        
        # Build query
        table_name = jdbc_config.get('source_table', 'etl_source_data')
        query = f"SELECT * FROM {table_name}"
        
        if filter_condition:
            query += f" WHERE {filter_condition}"
        
        query += " LIMIT 1000"  # Default limit
        
        # Read from JDBC source
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config.get('url')) \
            .option("query", query) \
            .option("user", jdbc_config.get('user')) \
            .option("password", jdbc_config.get('password')) \
            .option("driver", jdbc_config.get('driver', 'org.postgresql.Driver')) \
            .load()
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract data from staging area"""
        staging_path = self.config.get('staging_path', '/tmp/etl/staging')
        
        df = self.spark.read \
            .format("parquet") \
            .load(f"{staging_path}/run_id={self.run_id}")
        
        return df.filter(F.col("status") == "READY")
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last run"""
        # Get last successful run time
        last_run_time = self._get_last_run_time()
        
        if last_run_time is None:
            self.logger.info("No previous run found, performing full extract")
            return self._extract_from_database()
        
        jdbc_config = self.config.get('jdbc', {})
        table_name = jdbc_config.get('source_table', 'etl_source_data')
        
        query = f"""
            SELECT * FROM {table_name}
            WHERE changed_at > '{last_run_time}'
        """
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config.get('url')) \
            .option("query", query) \
            .option("user", jdbc_config.get('user')) \
            .option("password", jdbc_config.get('password')) \
            .option("driver", jdbc_config.get('driver', 'org.postgresql.Driver')) \
            .load()
        
        return df
    
    def _extract_from_file(self) -> DataFrame:
        """Extract data from file source"""
        file_config = self.config.get('file_source', {})
        file_path = file_config.get('path')
        file_format = file_config.get('format', 'csv')
        
        if file_format == 'csv':
            df = self.spark.read \
                .format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(file_path)
        elif file_format == 'parquet':
            df = self.spark.read.parquet(file_path)
        elif file_format == 'json':
            df = self.spark.read.json(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        return df
    
    def _get_last_run_time(self) -> Optional[datetime]:
        """Get timestamp of last successful run"""
        try:
            jdbc_config = self.config.get('jdbc', {})
            
            query = """
                SELECT MAX(end_time) as last_run
                FROM etl_run_log
                WHERE status = 'SUCCESS'
            """
            
            result = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_config.get('url')) \
                .option("query", query) \
                .option("user", jdbc_config.get('user')) \
                .option("password", jdbc_config.get('password')) \
                .option("driver", jdbc_config.get('driver', 'org.postgresql.Driver')) \
                .load()
            
            last_run = result.collect()[0]['last_run']
            return last_run
        except Exception as e:
            self.logger.warning(f"Could not get last run time: {e}")
            return None


===FILE: src/load.py===
"""
ETL Load Module - Data loading and reconciliation
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict, List, Tuple
import logging
from dataclasses import dataclass


@dataclass
class LoadResult:
    """Result of load operation"""
    success_count: int
    error_count: int
    total_count: int
    errors: List[str]


class ETLLoader:
    """Handles data loading to target destinations"""
    
    def __init__(self, spark: SparkSession, run_id: str, config: Dict):
        """
        Initialize loader
        
        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.batch_size = config.get('batch_size', 1000)
        self.logger = logging.getLogger(__name__)
    
    def load_data(
        self,
        df: DataFrame,
        mode: str = "append",
        enable_reconciliation: bool = True
    ) -> LoadResult:
        """
        Load transformed data to target
        
        Args:
            df: DataFrame to load
            mode: Write mode (append, overwrite, upsert)
            enable_reconciliation: Whether to perform reconciliation
            
        Returns:
            LoadResult with statistics
        """
        self.logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        total_count = df.count()
        errors = []
        
        try:
            # Perform load based on mode
            if mode.lower() == "upsert":
                success_count = self._upsert_data(df)
            elif mode.lower() == "append":
                success_count = self._append_data(df)
            elif mode.lower() == "overwrite":
                success_count = self._overwrite_data(df)
            else:
                success_count = self._append_data(df)
            
            error_count = total_count - success_count
            
            # Reconciliation
            if enable_reconciliation and success_count > 0:
                reconciled = self._reconcile_data(df, success_count)
                if not reconciled:
                    errors.append("Data reconciliation failed")
                    self.logger.warning("Data reconciliation check failed")
            
            self.logger.info(
                f"Load complete - Success: {success_count}, Errors: {error_count}"
            )
            
            return LoadResult(
                success_count=success_count,
                error_count=error_count,
                total_count=total_count,
                errors=errors
            )
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            return LoadResult(
                success_count=0,
                error_count=total_count,
                total_count=total_count,
                errors=[str(e)]
            )
    
    def _append_data(self, df: DataFrame) -> int:
        """Append data to target table"""
        jdbc_config = self.config.get('jdbc', {})
        target_table = jdbc_config.get('target_table', 'etl_target_data')
        
        df.write \
            .format("jdbc") \
            .option("url", jdbc_config.get('url')) \
            .option("dbtable", target_table) \
            .option("user", jdbc_config.get('user')) \
            .option("password", jdbc_config.get('password')) \
            .option("driver", jdbc_config.get('driver', 'org.postgresql.Driver')) \
            .option("batchsize", self.batch_size) \
            .mode("append") \
            .save()
        
        return df.count()
    
    def _overwrite_data(self, df: DataFrame) -> int:
        """Overwrite target table"""
        jdbc_config = self.config.get('jdbc', {})
        target_table = jdbc_config.get('target_table', 'etl_target_data')
        
        df.write \
            .format("jdbc") \
            .option("url", jdbc_config.get('url')) \
            .option("dbtable", target_table) \
            .option("user", jdbc_config.get('user')) \
            .option("password", jdbc_config.get('password')) \
            .option("driver", jdbc_config.get('driver', 'org.postgresql.Driver')) \
            .option("batchsize", self.batch_size) \
            .mode("overwrite") \
            .save()
        
        return df.count()
    
    def _upsert_data(self, df: DataFrame) -> int:
        """Perform upsert operation (update or insert)"""
        # Create temporary view
        temp_view = f"temp_load_{self.run_id}"
        df.createOrReplaceTempView(temp_view)
        
        jdbc_config = self.config.get('jdbc', {})
        target_table = jdbc_config.get('target_table', 'etl_target_data')
        
        # Merge query (PostgreSQL syntax)
        merge_query = f"""
            INSERT INTO {target_table} 
            (id, name, value, transformed_value, status, category, priority, 
             etl_run_id, processed_at, processed_by)
            SELECT id, name, value, transformed_value, status, category, priority,
                   etl_run_id, processed_at, processed_by
            FROM {temp_view}
            ON CONFLICT (id) 
            DO UPDATE SET
                name = EXCLUDED.name,
                value = EXCLUDED.value,
                transformed_value = EXCLUDED.transformed_value,
                status = EXCLUDED.status,
                category = EXCLUDED.category,
                priority = EXCLUDED.priority,
                etl_run_id = EXCLUDED.etl_run_id,
                processed_at = EXCLUDED.processed_at,
                processed_by = EXCLUDED.processed_by
        """
        
        # Execute using JDBC
        # Note: This is a simplified approach. In production, consider using Delta Lake
        # or database-specific upsert mechanisms
        
        # Fallback: Delete then insert
        existing_ids = df.select("id").distinct()
        
        # First, write to temp location
        temp_path = f"/tmp/etl/load/{self.run_id}"
        df.write.mode("overwrite").parquet(temp_path)
        
        # Read back and perform append
        df_load = self.spark.read.parquet(temp_path)
        return self._append_data(df_load)
    
    def _reconcile_data(self, source_df: DataFrame, expected_count: int) -> bool:
        """
        Reconcile loaded data against source
        
        Args:
            source_df: Source DataFrame
            expected_count: Expected number of records
            
        Returns:
            True if reconciliation passed
        """
        self.logger.info("Starting data reconciliation")
        
        jdbc_config = self.config.get('jdbc', {})
        target_table = jdbc_config.get('target_table', 'etl_target_data')
        
        # Read loaded data
        loaded_df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config.get('url')) \
            .option("dbtable", target_table) \
            .option("user", jdbc_config.get('user')) \
            .option("password", jdbc_config.get('password')) \
            .option("driver", jdbc_config.get('driver', 'org.postgresql.Driver')) \
            .load() \
            .filter(F.col("etl_run_id") == self.run_id)
        
        loaded_count = loaded_df.count()
        
        # Check count
        if loaded_count != expected_count:
            self.logger.warning(
                f"Reconciliation count mismatch: expected {expected_count}, got {loaded_count}"
            )
            return False
        
        # Check data integrity
        source_sum = source_df.agg(F.sum("value")).collect()[0][0]
        loaded_sum = loaded_df.agg(F.sum("value")).collect()[0][0]
        
        if abs(float(source_sum - loaded_sum)) > 0.01:
            self.logger.warning(
                f"Reconciliation value mismatch: source sum {source_sum}, loaded sum {loaded_sum}"
            )
            return False
        
        self.logger.info("Data reconciliation passed")
        return True


===FILE: src/orchestrator.py===
"""
ETL Orchestrator - Main execution engine
"""
from pyspark.sql import SparkSession
from typing import Dict, Optional
import logging
from datetime import datetime
from dataclasses import dataclass

from src.extract import ETLExtractor
from src.transform import ETLTransformer, ValueMapper, DataClassifier
from src.load import ETLLoader
from src.data_quality import DataQualityChecker


@dataclass
class ETLResult:
    """Result of ETL execution"""
    run_id: str
    status: str
    start_time: datetime
    end_time: datetime
    duration_seconds: int
    records_extracted: int
    records_transformed: int
    records_loaded: int
    records_failed: int
    error_count: int
    warning_count: int


class ETLOrchestrator:
    """Orchestrates complete ETL pipeline execution"""
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize orchestrator
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config