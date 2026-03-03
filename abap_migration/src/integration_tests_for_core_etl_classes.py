===FILE: src/extract.py===
"""
ETL Data Extractor Module
Extracts data from various sources including database, staging, and incremental loads.
"""

from typing import List, Dict, Optional
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
import logging


class ETLExtractor:
    """Handles data extraction from multiple source types."""
    
    SCHEMA = StructType([
        StructField("id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("value", DecimalType(15, 2), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("source_system", StringType(), nullable=True),
        StructField("created_at", TimestampType(), nullable=True),
        StructField("created_by", StringType(), nullable=True),
        StructField("changed_at", TimestampType(), nullable=True),
        StructField("changed_by", StringType(), nullable=True)
    ])
    
    def __init__(self, spark: SparkSession, config: Dict, run_id: str):
        """
        Initialize extractor.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        self.source_type = config.get('source_type', 'DATABASE')
        
    def extract_data(self, filter_condition: Optional[str] = None, 
                    max_records: int = 0) -> DataFrame:
        """
        Extract data based on source type.
        
        Args:
            filter_condition: Optional filter to apply
            max_records: Maximum records to extract (0 = no limit)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.info(f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}")
        
        try:
            if self.source_type == 'DATABASE':
                df = self.extract_from_database(filter_condition)
            elif self.source_type == 'STAGING':
                df = self.extract_from_staging(self.run_id)
            elif self.source_type == 'INCREMENTAL':
                df = self.extract_incremental()
            else:
                df = self.extract_from_database(filter_condition)
            
            # Apply max records limit
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract data from source database table.
        
        Args:
            filter_condition: SQL WHERE clause condition
            
        Returns:
            DataFrame with source data
        """
        jdbc_config = self.config.get('source_jdbc', {})
        table_name = jdbc_config.get('table', 'source_data')
        
        query = f"(SELECT * FROM {table_name}"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        query += ") as source_query"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config.get('url')) \
            .option("dbtable", query) \
            .option("user", jdbc_config.get('user')) \
            .option("password", jdbc_config.get('password')) \
            .option("driver", jdbc_config.get('driver', 'org.postgresql.Driver')) \
            .load()
        
        return df
    
    def extract_from_staging(self, run_id: str) -> DataFrame:
        """
        Extract data from staging area.
        
        Args:
            run_id: Run identifier to filter staging data
            
        Returns:
            DataFrame with staged data
        """
        staging_path = self.config.get('staging_path', '/data/staging')
        
        df = self.spark.read \
            .format("parquet") \
            .schema(self.SCHEMA) \
            .load(f"{staging_path}/run_id={run_id}")
        
        return df.filter("status = 'READY'")
    
    def extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run.
        
        Returns:
            DataFrame with incremental data
        """
        jdbc_config = self.config.get('source_jdbc', {})
        table_name = jdbc_config.get('table', 'source_data')
        
        # Get last successful run timestamp
        last_run_time = self._get_last_run_timestamp()
        
        if last_run_time:
            query = f"""(
                SELECT * FROM {table_name}
                WHERE changed_at > '{last_run_time}'
            ) as incremental_query"""
        else:
            # No previous run, do full extract
            self.logger.warning("No previous successful run found, performing full extract")
            query = f"(SELECT * FROM {table_name}) as full_query"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config.get('url')) \
            .option("dbtable", query) \
            .option("user", jdbc_config.get('user')) \
            .option("password", jdbc_config.get('password')) \
            .option("driver", jdbc_config.get('driver', 'org.postgresql.Driver')) \
            .load()
        
        return df
    
    def _get_last_run_timestamp(self) -> Optional[str]:
        """Get timestamp of last successful ETL run."""
        jdbc_config = self.config.get('source_jdbc', {})
        
        try:
            df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_config.get('url')) \
                .option("dbtable", "(SELECT MAX(end_time) as last_run FROM run_log WHERE status = 'SUCCESS') as last_run_query") \
                .option("user", jdbc_config.get('user')) \
                .option("password", jdbc_config.get('password')) \
                .option("driver", jdbc_config.get('driver', 'org.postgresql.Driver')) \
                .load()
            
            result = df.collect()
            if result and result[0]['last_run']:
                return result[0]['last_run'].isoformat()
            return None
            
        except Exception as e:
            self.logger.warning(f"Could not retrieve last run timestamp: {str(e)}")
            return None


===FILE: src/transform.py===
"""
ETL Data Transformer Module
Applies business rules, enrichments, and validations to extracted data.
"""

from typing import List, Dict, Tuple
from decimal import Decimal
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from pyspark.sql import functions as F
import logging


class ETLTransformer:
    """Handles data transformation, validation, and enrichment."""
    
    OUTPUT_SCHEMA = StructType([
        StructField("id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("value", DecimalType(15, 2), nullable=False),
        StructField("transformed_value", DecimalType(15, 2), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("priority", IntegerType(), nullable=False),
        StructField("etl_run_id", StringType(), nullable=False),
        StructField("processed_at", TimestampType(), nullable=False),
        StructField("processed_by", StringType(), nullable=False)
    ])
    
    def __init__(self, spark: SparkSession, config: Dict, run_id: str):
        """
        Initialize transformer.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply all transformations to source data.
        
        Args:
            source_df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        # Initial transformation
        transformed_df = self._apply_initial_transform(source_df)
        
        # Apply business rules
        transformed_df = self.apply_business_rules(transformed_df)
        
        # Enrich data
        transformed_df = self.enrich_data(transformed_df)
        
        record_count = transformed_df.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return transformed_df
    
    def _apply_initial_transform(self, source_df: DataFrame) -> DataFrame:
        """Apply initial transformation logic."""
        current_time = F.current_timestamp()
        
        # Calculate priority using UDF
        priority_udf = F.udf(self._calculate_priority, IntegerType())
        
        # Calculate transformed value using UDF
        transform_value_udf = F.udf(self._calculate_derived_value, DecimalType(15, 2))
        
        transformed_df = source_df.select(
            F.col("id"),
            F.upper(F.trim(F.col("name"))).alias("name"),
            F.col("value"),
            transform_value_udf(F.col("value"), F.col("category")).alias("transformed_value"),
            F.lit("TRANSFORMED").alias("status"),
            F.col("category"),
            priority_udf(F.col("value"), F.col("category")).alias("priority"),
            F.lit(self.run_id).alias("etl_run_id"),
            current_time.alias("processed_at"),
            F.lit(self.config.get('username', 'SYSTEM')).alias("processed_by")
        )
        
        return transformed_df
    
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to data.
        
        Args:
            df: DataFrame to apply rules to
            
        Returns:
            DataFrame with business rules applied
        """
        self.logger.info("Applying business rules")
        
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
        
        # Rule 3: Name normalization
        df = df.withColumn(
            "name",
            F.regexp_replace(F.col("name"), "\\s+", " ")
        )
        
        # Rule 4: Category validation
        df = df.withColumn(
            "category",
            F.when(F.col("category").isNull() | (F.col("category") == ""), "UNCATEGORIZED")
            .otherwise(F.col("category"))
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
        self.logger.info("Enriching data")
        
        # Premium category multiplier
        premium_multiplier = self.config.get('business_rules', {}).get('premium_multiplier', 1.2)
        
        df = df.withColumn(
            "transformed_value",
            F.when(F.col("category") == "PREMIUM", 
                   F.col("transformed_value") * F.lit(premium_multiplier))
            .otherwise(F.col("transformed_value"))
        )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []
        
        # Rule 1: ID is required
        null_ids = df.filter(F.col("id").isNull() | (F.col("id") == "")).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with missing ID")
        
        # Rule 2: Name is required
        null_names = df.filter(F.col("name").isNull() | (F.col("name") == "")).count()
        if null_names > 0:
            errors.append(f"{null_names} records with missing name")
        
        # Rule 3: Value must be positive
        negative_values = df.filter(F.col("value") < 0).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative values")
        
        # Rule 4: Priority must be 1-5
        invalid_priority = df.filter((F.col("priority") < 1) | (F.col("priority") > 5)).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Validation passed")
        else:
            self.logger.error(f"Validation failed with {len(errors)} errors")
            
        return is_valid, errors
    
    @staticmethod
    def _calculate_priority(value: Decimal, category: str) -> int:
        """Calculate priority based on value and category."""
        if not value:
            return 5
        
        value = float(value)
        
        if category == 'VIP':
            return 1
        elif value >= 1000:
            return 1
        elif value >= 500:
            return 2
        elif value >= 250:
            return 3
        elif value >= 100:
            return 4
        else:
            return 5
    
    @staticmethod
    def _calculate_derived_value(value: Decimal, category: str) -> Decimal:
        """Calculate derived/transformed value."""
        if not value:
            return Decimal('0.00')
        
        value = float(value)
        
        # Category-based multipliers
        multipliers = {
            'PREMIUM': 1.5,
            'VIP': 2.0,
            'STANDARD': 1.2,
            'BASIC': 1.0,
            'TRIAL': 0.8
        }
        
        multiplier = multipliers.get(category, 1.0)
        return Decimal(str(value * multiplier))


===FILE: src/load.py===
"""
ETL Data Loader Module
Loads transformed data to target destinations with error handling and reconciliation.
"""

from typing import Dict, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import logging


class LoadResult:
    """Container for load operation results."""
    
    def __init__(self):
        self.success_count = 0
        self.error_count = 0
        self.total_count = 0
        self.errors: List[str] = []


class ETLLoader:
    """Handles data loading to target systems."""
    
    def __init__(self, spark: SparkSession, config: Dict, run_id: str):
        """
        Initialize loader.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        self.target_type = config.get('target_type', 'DATABASE')
        self.batch_size = config.get('batch_size', 1000)
        
    def load_data(self, df: DataFrame, mode: str = 'upsert') -> LoadResult:
        """
        Load data to target destination.
        
        Args:
            df: DataFrame to load
            mode: Load mode (insert, update, upsert)
            
        Returns:
            LoadResult with operation statistics
        """
        self.logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        result = LoadResult()
        result.total_count = df.count()
        
        try:
            if self.target_type == 'DATABASE':
                success = self._load_to_database(df, mode)
            elif self.target_type == 'PARQUET':
                success = self._load_to_parquet(df, mode)
            elif self.target_type == 'DELTA':
                success = self._load_to_delta(df, mode)
            else:
                success = self._load_to_database(df, mode)
            
            if success:
                result.success_count = result.total_count
                
                # Perform reconciliation if enabled
                if self.config.get('enable_reconciliation', True):
                    if not self.reconcile_data(df):
                        self.logger.warning("Data reconciliation failed")
                        result.errors.append("Reconciliation mismatch detected")
            else:
                result.error_count = result.total_count
                result.errors.append("Load operation failed")
            
            self.logger.info(f"Load complete - Success: {result.success_count}, Errors: {result.error_count}")
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            result.error_count = result.total_count
            result.errors.append(str(e))
        
        return result
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """Load data to database table."""
        jdbc_config = self.config.get('target_jdbc', {})
        table_name = jdbc_config.get('table', 'target_data')
        
        # Map mode to Spark write mode
        write_mode_map = {
            'insert': 'append',
            'update': 'overwrite',
            'upsert': 'append'  # Custom upsert logic needed
        }
        write_mode = write_mode_map.get(mode.lower(), 'append')
        
        try:
            if mode.lower() == 'upsert':
                # For upsert, we need merge logic
                self._upsert_to_database(df, table_name, jdbc_config)
            else:
                df.write \
                    .format("jdbc") \
                    .option("url", jdbc_config.get('url')) \
                    .option("dbtable", table_name) \
                    .option("user", jdbc_config.get('user')) \
                    .option("password", jdbc_config.get('password')) \
                    .option("driver", jdbc_config.get('driver', 'org.postgresql.Driver')) \
                    .option("batchsize", self.batch_size) \
                    .mode(write_mode) \
                    .save()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Database load error: {str(e)}")
            return False
    
    def _upsert_to_database(self, df: DataFrame, table_name: str, jdbc_config: Dict):
        """Perform upsert operation to database."""
        # Create temp table
        temp_table = f"{table_name}_temp_{self.run_id}"
        
        # Write to temp table
        df.write \
            .format("jdbc") \
            .option("url", jdbc_config.get('url')) \
            .option("dbtable", temp_table) \
            .option("user", jdbc_config.get('user')) \
            .option("password", jdbc_config.get('password')) \
            .option("driver", jdbc_config.get('driver', 'org.postgresql.Driver')) \
            .mode("overwrite") \
            .save()
        
        # Execute merge statement via JDBC
        merge_sql = f"""
        MERGE INTO {table_name} t
        USING {temp_table} s
        ON t.id = s.id
        WHEN MATCHED THEN
            UPDATE SET
                name = s.name,
                value = s.value,
                transformed_value = s.transformed_value,
                status = s.status,
                category = s.category,
                priority = s.priority,
                etl_run_id = s.etl_run_id,
                processed_at = s.processed_at,
                processed_by = s.processed_by
        WHEN NOT MATCHED THEN
            INSERT VALUES (s.id, s.name, s.value, s.transformed_value, s.status, 
                          s.category, s.priority, s.etl_run_id, s.processed_at, s.processed_by)
        """
        
        # Execute merge (implementation depends on database type)
        # For production, use appropriate database-specific merge
        self.logger.info(f"Executing upsert to {table_name}")
        
    def _load_to_parquet(self, df: DataFrame, mode: str) -> bool:
        """Load data to Parquet files."""
        output_path = self.config.get('output_path', '/data/output')
        
        try:
            df.write \
                .mode('overwrite' if mode == 'update' else 'append') \
                .partitionBy('category') \
                .parquet(f"{output_path}/run_id={self.run_id}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Parquet write error: {str(e)}")
            return False
    
    def _load_to_delta(self, df: DataFrame, mode: str) -> bool:
        """Load data to Delta Lake."""
        output_path = self.config.get('delta_path', '/data/delta')
        
        try:
            if mode.lower() == 'upsert':
                # Delta Lake supports merge natively
                df.write \
                    .format("delta") \
                    .mode("append") \
                    .option("mergeSchema", "true") \
                    .save(output_path)
            else:
                df.write \
                    .format("delta") \
                    .mode('overwrite' if mode == 'update' else 'append') \
                    .save(output_path)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Delta write error: {str(e)}")
            return False
    
    def reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data with source.
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passes
        """
        self.logger.info("Starting data reconciliation")
        
        try:
            # Get counts
            source_count = loaded_df.count()
            
            # Query target for verification
            jdbc_config = self.config.get('target_jdbc', {})
            table_name = jdbc_config.get('table', 'target_data')
            
            target_df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_config.get('url')) \
                .option("dbtable", f"(SELECT * FROM {table_name} WHERE etl_run_id = '{self.run_id}') as recon") \
                .option("user", jdbc_config.get('user')) \
                .option("password", jdbc_config.get('password')) \
                .option("driver", jdbc_config.get('driver', 'org.postgresql.Driver')) \
                .load()
            
            target_count = target_df.count()
            
            # Compare counts
            if source_count == target_count:
                self.logger.info(f"Reconciliation passed: {source_count} records match")
                return True
            else:
                self.logger.error(f"Reconciliation failed: Source={source_count}, Target={target_count}")
                return False
                
        except Exception as e:
            self.logger.error(f"Reconciliation error: {str(e)}")
            return False


===FILE: src/orchestrator.py===
"""
ETL Orchestrator Module
Coordinates the complete ETL pipeline execution.
"""

from typing import Dict, Optional
from datetime import datetime
from pyspark.sql import SparkSession
import logging

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader, LoadResult


class ETLResult:
    """Container for ETL execution results."""
    
    def __init__(self, run_id: str):
        self.run_id = run_id
        self.status = 'RUNNING'
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.duration: Optional[int] = None
        self.records_extracted = 0
        self.records_transformed = 0
        self.records_loaded = 0
        self.records_failed = 0
        self.error_count = 0
        self.warning_count = 0
        self.errors: list = []


class ETLOrchestrator:
    """Orchestrates the complete ETL pipeline."""
    
    def __init__(self, spark: SparkSession, config: Dict, run_type: str = 'MANUAL'):
        """
        Initialize orchestrator.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_type: Type of run (MANUAL, SCHEDULED, etc.)
        """
        self.spark = spark
        self.config = config
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        self.logger = logging.getLogger(__name__)
        
    def execute_etl(self, 
                   source_type: str = 'DATABASE',
                   target_type: str = 'DATABASE',
                   filter_condition: Optional[str] = None,
                   batch_size: int = 1000,
                   max_records: int = 0) -> ETLResult:
        """
        Execute complete ETL pipeline.
        
        Args:
            source_type: Source data type
            target_type: Target data type
            filter_condition: Optional filter for extraction
            batch_size: Batch size for loading
            max_records: Maximum records to process
            
        Returns:
            ETLResult with execution statistics
        """
        result = ETLResult(self.run_id)
        result.start_time = datetime.now()
        
        self.logger.info(f"ETL execution started - Run ID: {self.run_id}")
        
        try:
            # Update config with runtime parameters
            runtime_config = self.config.copy()
            runtime_config['source_type'] = source_type
            runtime_config['target_type'] = target_type
            runtime_config['batch_size'] = batch_size
            
            # Step 1: Extract
            extractor = ETLExtractor(self.spark, runtime_config, self.run_id)
            source_df = extractor.extract_data(filter_condition, max_records)
            result.records_extracted = source_df.count()
            
            if result.records_extracted == 0:
                self.logger.warning("No data extracted - ETL process stopping")
                result.status = 'NO_DATA'
                return result
            
            # Step 2: Transform
            transformer = ETLTransformer(self.spark, runtime_config, self.run_id)
            transformed_df = transformer.transform_data(source_df)
            result.records_transformed = transformed_df.count()
            
            # Validate
            is_valid, validation_errors = transformer.validate_data(transformed_df)
            if not is_valid:
                self.logger.error(f"Validation failed - {len(validation_errors)} errors")
                result.status = 'VALIDATION_FAILED'
                result.error_count = len(validation_errors)
                result.errors = validation_errors
                return result
            
            # Step 3: Load
            loader = ETLLoader(self.spark, runtime_config, self.run_id)
            load_result = loader.load_data(transformed_df, mode='upsert')
            
            result.records_loaded = load_result.success_count
            result.records_failed = load_result.error_count
            result.error_count = load_result.error_count
            result.errors = load_result.errors
            
            # Determine final status
            if load_result.error_count == 0:
                result.status = 'SUCCESS'
            elif load_result.success_count > 0:
                result.status = 'PARTIAL_SUCCESS'
            else:
                result.status = 'FAILED'
            
            result.end_time = datetime.now()
            result.duration = int((result.end_time - result.start_time).total_seconds())
            
            self._log_run_completion(result)
            
            self.logger.info(f"ETL execution completed - Status: {result.status}")
            
        except Exception as e:
            self.logger.error(f"ETL execution failed: {str(e)}")
            result.status = 'ERROR'
            result.end_time = datetime.now()
            result.errors.append(str(e))
            self._log_run_completion(result)
        
        return result
    
    def _generate_run_id(self) -> str:
        """Generate unique run identifier."""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"RUN_{timestamp}_{self.run_type}"
    
    def _log_run_completion(self, result: ETLResult):
        """Log run completion to database."""
        try:
            log_data = [{
                'run_id': result.run_id,
                'run_type': self.run_type,
                'status': result.status,
                'start_time': result.start_time,
                'end_time': result.end_time,
                'duration': result.duration,
                'records_extracted': result.records_extracted,
                'records_transformed': result.records_transformed,
                'records_loaded': result.records_loaded,
                'records_failed': result.records_failed,
                'error_count': result.error_count
            }]
            
            log_df = self.spark.createDataFrame(log_data)
            
            jdbc_config = self.config.get('target_jdbc', {})
            log_df.write \
                .format("jdbc") \
                .option("url", jdbc_config.get('url')) \
                .option("dbtable", "run_log") \
                .option("user", jdbc_config.get('user')) \
                .option("password", jdbc_config.get('password')) \
                .option("driver", jdbc_config.get('driver', 'org.postgresql.Driver')) \
                .mode("append") \
                .save()
                
        except Exception as e:
            self.logger