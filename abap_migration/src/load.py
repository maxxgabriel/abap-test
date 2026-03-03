"""
ETL Loading Module - Data Persistence Layer
Handles batch loading with INSERT/UPSERT modes and comprehensive result tracking.
"""
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from pyspark.sql.functions import col, current_timestamp, lit
import logging
from datetime import datetime


class LoadMode(Enum):
    """Supported loading modes"""
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    UPSERT = "UPSERT"


@dataclass
class LoadResult:
    """Result of a load operation"""
    success_count: int = 0
    error_count: int = 0
    total_count: int = 0
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class ETLLoader:
    """
    ETL Loader for batch data persistence operations.
    Supports INSERT, UPDATE, and UPSERT modes with configurable batch sizes.
    """
    
    TARGET_SCHEMA = StructType([
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
        target_type: str = "DATABASE",
        batch_size: int = 1000,
        run_id: str = None,
        target_table: str = "etl_target_data",
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the ETL Loader.
        
        Args:
            spark: SparkSession instance
            target_type: Type of target storage
            batch_size: Number of records per batch
            run_id: Unique run identifier
            target_table: Target table name
            logger: Logger instance
        """
        self.spark = spark
        self.target_type = target_type
        self.batch_size = batch_size
        self.run_id = run_id or self._generate_run_id()
        self.target_table = target_table
        self.logger = logger or logging.getLogger(__name__)
        
    def _generate_run_id(self) -> str:
        """Generate a unique run ID"""
        return f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def load_data(
        self,
        data: DataFrame,
        mode: LoadMode = LoadMode.INSERT
    ) -> LoadResult:
        """
        Load data with specified mode and batch processing.
        
        Args:
            data: DataFrame containing transformed data
            mode: Loading mode (INSERT, UPDATE, UPSERT)
            
        Returns:
            LoadResult with success/error counts and error details
        """
        self.logger.info(
            f"Starting load - Mode: {mode.value}, Batch size: {self.batch_size}"
        )
        
        result = LoadResult(total_count=data.count())
        
        try:
            # Validate schema
            if not self._validate_schema(data):
                result.errors.append("Schema validation failed")
                result.error_count = result.total_count
                return result
            
            # Process in batches
            batches = self._create_batches(data)
            
            for batch_idx, batch_df in enumerate(batches):
                batch_size = batch_df.count()
                self.logger.info(
                    f"Processing batch {batch_idx + 1}, size: {batch_size}"
                )
                
                try:
                    success = self._commit_batch(batch_df, mode)
                    if success:
                        result.success_count += batch_size
                    else:
                        result.error_count += batch_size
                        result.errors.append(
                            f"Batch {batch_idx + 1} failed to commit"
                        )
                except Exception as e:
                    result.error_count += batch_size
                    error_msg = f"Batch {batch_idx + 1} error: {str(e)}"
                    result.errors.append(error_msg)
                    self.logger.error(error_msg)
            
            # Reconciliation
            if result.success_count > 0:
                reconciled = self._reconcile_data(data)
                if not reconciled:
                    self.logger.warning("Data reconciliation check failed")
            
            self.logger.info(
                f"Load complete - Success: {result.success_count}, "
                f"Errors: {result.error_count}"
            )
            
        except Exception as e:
            self.logger.error(f"Load operation failed: {str(e)}")
            result.error_count = result.total_count
            result.errors.append(f"Load error: {str(e)}")
        
        return result
    
    def _validate_schema(self, data: DataFrame) -> bool:
        """Validate DataFrame schema against target schema"""
        try:
            required_fields = {field.name for field in self.TARGET_SCHEMA.fields}
            data_fields = set(data.columns)
            
            missing_fields = required_fields - data_fields
            if missing_fields:
                self.logger.error(f"Missing required fields: {missing_fields}")
                return False
            
            return True
        except Exception as e:
            self.logger.error(f"Schema validation error: {str(e)}")
            return False
    
    def _create_batches(self, data: DataFrame) -> List[DataFrame]:
        """
        Split DataFrame into batches.
        
        Args:
            data: Input DataFrame
            
        Returns:
            List of DataFrame batches
        """
        total_count = data.count()
        num_batches = (total_count + self.batch_size - 1) // self.batch_size
        
        batches = []
        for i in range(num_batches):
            # Use limit and offset approach for batching
            offset = i * self.batch_size
            batch = data.limit(self.batch_size).offset(offset)
            batches.append(batch)
        
        return batches
    
    def _commit_batch(self, batch: DataFrame, mode: LoadMode) -> bool:
        """
        Commit a single batch to the target.
        
        Args:
            batch: DataFrame batch to commit
            mode: Loading mode
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if mode == LoadMode.INSERT:
                return self._insert_new(batch)
            elif mode == LoadMode.UPDATE:
                return self._update_existing(batch)
            elif mode == LoadMode.UPSERT:
                return self._upsert_data(batch)
            else:
                self.logger.error(f"Unsupported mode: {mode}")
                return False
        except Exception as e:
            self.logger.error(f"Batch commit error: {str(e)}")
            return False
    
    def _insert_new(self, data: DataFrame) -> bool:
        """
        Insert new records into target.
        
        Args:
            data: DataFrame to insert
            
        Returns:
            True if successful
        """
        try:
            data.write \
                .format("jdbc") \
                .mode("append") \
                .option("dbtable", self.target_table) \
                .save()
            return True
        except Exception as e:
            self.logger.error(f"Insert error: {str(e)}")
            return False
    
    def _update_existing(self, data: DataFrame) -> bool:
        """
        Update existing records in target.
        
        Args:
            data: DataFrame with updates
            
        Returns:
            True if successful
        """
        try:
            # Create temporary view for merge operation
            temp_view = f"temp_update_{self.run_id}"
            data.createOrReplaceTempView(temp_view)
            
            # Perform update using merge
            merge_query = f"""
            MERGE INTO {self.target_table} target
            USING {temp_view} source
            ON target.id = source.id
            WHEN MATCHED THEN UPDATE SET *
            """
            
            self.spark.sql(merge_query)
            return True
        except Exception as e:
            self.logger.error(f"Update error: {str(e)}")
            return False
    
    def _upsert_data(self, data: DataFrame) -> bool:
        """
        Perform UPSERT (INSERT or UPDATE) operation.
        
        Args:
            data: DataFrame to upsert
            
        Returns:
            True if successful
        """
        try:
            # Create temporary view
            temp_view = f"temp_upsert_{self.run_id}"
            data.createOrReplaceTempView(temp_view)
            
            # Perform merge (upsert)
            merge_query = f"""
            MERGE INTO {self.target_table} target
            USING {temp_view} source
            ON target.id = source.id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
            
            self.spark.sql(merge_query)
            return True
        except Exception as e:
            self.logger.error(f"Upsert error: {str(e)}")
            # Fallback: try update then insert
            try:
                self._update_existing(data)
                return True
            except:
                try:
                    self._insert_new(data)
                    return True
                except:
                    return False
    
    def _reconcile_data(self, loaded_data: DataFrame) -> bool:
        """
        Reconcile loaded data against target.
        
        Args:
            loaded_data: DataFrame that was loaded
            
        Returns:
            True if reconciliation passes
        """
        try:
            # Read back from target
            target_df = self.spark.read \
                .format("jdbc") \
                .option("dbtable", self.target_table) \
                .load()
            
            # Filter by run_id
            target_records = target_df.filter(
                col("etl_run_id") == self.run_id
            ).count()
            
            loaded_count = loaded_data.count()
            
            if target_records == loaded_count:
                self.logger.info("Data reconciliation passed")
                return True
            else:
                self.logger.warning(
                    f"Reconciliation mismatch: "
                    f"Loaded={loaded_count}, Target={target_records}"
                )
                return False
        except Exception as e:
            self.logger.error(f"Reconciliation error: {str(e)}")
            return False
    
    def load_to_database(
        self,
        data: DataFrame,
        mode: LoadMode
    ) -> bool:
        """
        Direct database load operation.
        
        Args:
            data: DataFrame to load
            mode: Loading mode
            
        Returns:
            True if successful
        """
        try:
            result = self.load_data(data, mode)
            return result.error_count == 0
        except Exception as e:
            self.logger.error(f"Database load error: {str(e)}")
            return False


class LoaderFactory:
    """Factory for creating ETL Loader instances"""
    
    @staticmethod
    def create_loader(
        spark: SparkSession,
        config: Dict[str, Any]
    ) -> ETLLoader:
        """
        Create a loader instance from configuration.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            
        Returns:
            Configured ETLLoader instance
        """
        return ETLLoader(
            spark=spark,
            target_type=config.get("target_type", "DATABASE"),
            batch_size=config.get("batch_size", 1000),
            run_id=config.get("run_id"),
            target_table=config.get("target_table", "etl_target_data"),
            logger=logging.getLogger(config.get("logger_name", __name__))
        )