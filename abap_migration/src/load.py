"""
PySpark ETL Loading Module
Handles batch loading operations with INSERT/UPSERT modes and comprehensive error tracking.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from enum import Enum
import logging


class LoadMode(Enum):
    """Supported loading modes"""
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    UPSERT = "UPSERT"


@dataclass
class LoadResult:
    """Result of load operation"""
    success_count: int
    error_count: int
    total_count: int
    errors: List[str]
    run_id: str
    duration_seconds: float


class ETLLoader:
    """
    ETL Loader for batch operations with partitioning and error handling.
    Supports INSERT, UPDATE, and UPSERT modes.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        target_table: str,
        batch_size: int = 1000,
        run_id: str = None,
        target_type: str = "DATABASE"
    ):
        """
        Initialize ETL Loader.
        
        Args:
            spark: SparkSession instance
            target_table: Target table name
            batch_size: Number of records per batch
            run_id: Unique run identifier
            target_type: Target system type (DATABASE, etc.)
        """
        self.spark = spark
        self.target_table = target_table
        self.batch_size = batch_size
        self.run_id = run_id or self._generate_run_id()
        self.target_type = target_type
        self.logger = logging.getLogger(__name__)
        
    def _generate_run_id(self) -> str:
        """Generate unique run ID"""
        import uuid
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"RUN_{timestamp}_{str(uuid.uuid4())[:8]}"
    
    def load_data(
        self,
        data: DataFrame,
        mode: str = "INSERT",
        key_columns: List[str] = None
    ) -> LoadResult:
        """
        Load data with specified mode and batch processing.
        
        Args:
            data: DataFrame to load
            mode: Load mode (INSERT, UPDATE, UPSERT)
            key_columns: Primary key columns for UPSERT/UPDATE
            
        Returns:
            LoadResult with success/error counts and details
        """
        import time
        start_time = time.time()
        
        self.logger.info(
            f"Starting load - Mode: {mode}, Batch size: {self.batch_size}, "
            f"Total records: {data.count()}"
        )
        
        # Validate mode
        try:
            load_mode = LoadMode[mode.upper()]
        except KeyError:
            raise ValueError(f"Invalid load mode: {mode}. Must be INSERT, UPDATE, or UPSERT")
        
        # Add ETL metadata
        enriched_data = self._add_metadata(data)
        
        # Process in batches
        batches = self._partition_into_batches(enriched_data)
        
        success_count = 0
        error_count = 0
        errors = []
        
        for batch_num, batch_df in enumerate(batches, 1):
            self.logger.info(f"Processing batch {batch_num}")
            
            try:
                batch_success = self._commit_batch(
                    batch_df, 
                    load_mode, 
                    key_columns
                )
                
                if batch_success:
                    batch_count = batch_df.count()
                    success_count += batch_count
                    self.logger.info(f"Batch {batch_num} succeeded: {batch_count} records")
                else:
                    batch_count = batch_df.count()
                    error_count += batch_count
                    error_msg = f"Batch {batch_num} failed: {batch_count} records"
                    errors.append(error_msg)
                    self.logger.error(error_msg)
                    
            except Exception as e:
                batch_count = batch_df.count()
                error_count += batch_count
                error_msg = f"Batch {batch_num} exception: {str(e)}"
                errors.append(error_msg)
                self.logger.error(error_msg, exc_info=True)
        
        duration = time.time() - start_time
        total_count = success_count + error_count
        
        self.logger.info(
            f"Load complete - Success: {success_count}, "
            f"Errors: {error_count}, Duration: {duration:.2f}s"
        )
        
        # Perform reconciliation if configured
        if success_count > 0:
            self._reconcile_data(enriched_data, success_count)
        
        return LoadResult(
            success_count=success_count,
            error_count=error_count,
            total_count=total_count,
            errors=errors,
            run_id=self.run_id,
            duration_seconds=duration
        )
    
    def _add_metadata(self, data: DataFrame) -> DataFrame:
        """Add ETL metadata columns to DataFrame"""
        return data \
            .withColumn("etl_run_id", lit(self.run_id)) \
            .withColumn("etl_loaded_at", current_timestamp()) \
            .withColumn("etl_loaded_by", lit("etl_system"))
    
    def _partition_into_batches(self, data: DataFrame) -> List[DataFrame]:
        """
        Partition DataFrame into batches based on batch_size.
        
        Args:
            data: Input DataFrame
            
        Returns:
            List of DataFrame batches
        """
        total_count = data.count()
        num_batches = (total_count + self.batch_size - 1) // self.batch_size
        
        self.logger.info(
            f"Partitioning {total_count} records into {num_batches} batches "
            f"of size {self.batch_size}"
        )
        
        # Add row number for partitioning
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number, floor
        
        window_spec = Window.orderBy(lit(1))
        data_with_row = data.withColumn("_row_num", row_number().over(window_spec))
        data_with_batch = data_with_row.withColumn(
            "_batch_id",
            floor((col("_row_num") - 1) / self.batch_size)
        )
        
        batches = []
        for batch_id in range(num_batches):
            batch_df = data_with_batch \
                .filter(col("_batch_id") == batch_id) \
                .drop("_row_num", "_batch_id")
            batches.append(batch_df)
        
        return batches
    
    def _commit_batch(
        self,
        batch: DataFrame,
        mode: LoadMode,
        key_columns: List[str] = None
    ) -> bool:
        """
        Commit a single batch to target.
        
        Args:
            batch: Batch DataFrame
            mode: Load mode
            key_columns: Key columns for merge operations
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if mode == LoadMode.INSERT:
                return self._insert_batch(batch)
            elif mode == LoadMode.UPDATE:
                return self._update_batch(batch, key_columns)
            elif mode == LoadMode.UPSERT:
                return self._upsert_batch(batch, key_columns)
            else:
                raise ValueError(f"Unsupported mode: {mode}")
                
        except Exception as e:
            self.logger.error(f"Batch commit failed: {str(e)}", exc_info=True)
            return False
    
    def _insert_batch(self, batch: DataFrame) -> bool:
        """Insert batch using append mode"""
        try:
            batch.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(self.target_table)
            return True
        except Exception as e:
            self.logger.error(f"Insert batch failed: {str(e)}")
            return False
    
    def _update_batch(self, batch: DataFrame, key_columns: List[str]) -> bool:
        """Update existing records"""
        if not key_columns:
            raise ValueError("key_columns required for UPDATE mode")
        
        try:
            from delta.tables import DeltaTable
            
            # Read target as Delta table
            delta_table = DeltaTable.forName(self.spark, self.target_table)
            
            # Build merge condition
            merge_condition = " AND ".join([
                f"target.{col} = source.{col}" for col in key_columns
            ])
            
            # Update only existing records
            delta_table.alias("target") \
                .merge(
                    batch.alias("source"),
                    merge_condition
                ) \
                .whenMatchedUpdateAll() \
                .execute()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Update batch failed: {str(e)}")
            return False
    
    def _upsert_batch(self, batch: DataFrame, key_columns: List[str]) -> bool:
        """Upsert (merge) records - update if exists, insert if not"""
        if not key_columns:
            raise ValueError("key_columns required for UPSERT mode")
        
        try:
            from delta.tables import DeltaTable
            
            # Create table if not exists
            if not self.spark.catalog.tableExists(self.target_table):
                batch.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .saveAsTable(self.target_table)
                return True
            
            # Perform merge operation
            delta_table = DeltaTable.forName(self.spark, self.target_table)
            
            merge_condition = " AND ".join([
                f"target.{col} = source.{col}" for col in key_columns
            ])
            
            delta_table.alias("target") \
                .merge(
                    batch.alias("source"),
                    merge_condition
                ) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Upsert batch failed: {str(e)}")
            return False
    
    def _reconcile_data(self, loaded_data: DataFrame, expected_count: int) -> bool:
        """
        Reconcile loaded data with target table.
        
        Args:
            loaded_data: Data that was loaded
            expected_count: Expected number of records
            
        Returns:
            True if reconciliation passes
        """
        try:
            # Read back from target
            target_data = self.spark.table(self.target_table)
            
            # Filter for this run
            loaded_records = target_data.filter(col("etl_run_id") == self.run_id)
            actual_count = loaded_records.count()
            
            if actual_count == expected_count:
                self.logger.info(
                    f"Reconciliation passed: {actual_count} records verified"
                )
                return True
            else:
                self.logger.warning(
                    f"Reconciliation mismatch: Expected {expected_count}, "
                    f"Found {actual_count}"
                )
                return False
                
        except Exception as e:
            self.logger.error(f"Reconciliation failed: {str(e)}")
            return False
    
    def get_load_statistics(self) -> Dict:
        """Get statistics for the current run"""
        try:
            target_data = self.spark.table(self.target_table)
            run_data = target_data.filter(col("etl_run_id") == self.run_id)
            
            return {
                "run_id": self.run_id,
                "record_count": run_data.count(),
                "target_table": self.target_table,
                "load_timestamp": run_data.agg({"etl_loaded_at": "max"}).collect()[0][0]
            }
        except Exception as e:
            self.logger.error(f"Failed to get statistics: {str(e)}")
            return {}