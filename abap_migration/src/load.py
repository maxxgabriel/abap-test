"""
ETL Loading Module - Batch operations for data persistence
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from typing import Dict, List, Tuple
from datetime import datetime
import logging


class LoadResult:
    """Container for load operation results"""
    def __init__(self):
        self.success_count: int = 0
        self.error_count: int = 0
        self.total_count: int = 0
        self.errors: List[str] = []
        
    def to_dict(self) -> Dict:
        return {
            'success_count': self.success_count,
            'error_count': self.error_count,
            'total_count': self.total_count,
            'errors': self.errors
        }


class ETLLoader:
    """Handles data loading operations with batch processing"""
    
    def __init__(self, spark: SparkSession, config: Dict, run_id: str):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.batch_size = config.get('batch_size', 1000)
        self.target_table = config.get('target_table', 'etl_target_data')
        self.logger = logging.getLogger(__name__)
        
    def load_data(self, df: DataFrame, mode: str = 'INSERT') -> LoadResult:
        """
        Load data with batch processing and mode handling
        
        Args:
            df: Transformed DataFrame to load
            mode: Load mode - 'INSERT', 'UPDATE', 'UPSERT'
            
        Returns:
            LoadResult with success/error counts
        """
        result = LoadResult()
        result.total_count = df.count()
        
        self.logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        try:
            # Process in batches using partitioning
            batched_df = self._partition_by_batch_size(df)
            
            if mode.upper() == 'INSERT':
                success = self._insert_new(batched_df)
            elif mode.upper() == 'UPDATE':
                success = self._update_existing(batched_df)
            elif mode.upper() == 'UPSERT':
                success = self._upsert_data(batched_df)
            else:
                success = self._insert_new(batched_df)
                
            if success:
                result.success_count = result.total_count
                result.error_count = 0
            else:
                result.error_count = result.total_count
                result.errors.append("Batch load failed")
                
            # Reconcile if enabled
            if self.config.get('enable_reconciliation', True):
                self._reconcile_data(df)
                
            self.logger.info(
                f"Load complete - Success: {result.success_count}, "
                f"Errors: {result.error_count}"
            )
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            result.error_count = result.total_count
            result.errors.append(str(e))
            
        return result
    
    def _partition_by_batch_size(self, df: DataFrame) -> DataFrame:
        """
        Partition DataFrame by batch size for batch processing
        
        Args:
            df: Input DataFrame
            
        Returns:
            Repartitioned DataFrame
        """
        total_rows = df.count()
        num_partitions = max(1, (total_rows + self.batch_size - 1) // self.batch_size)
        
        self.logger.info(f"Partitioning {total_rows} rows into {num_partitions} batches")
        
        return df.repartition(num_partitions)
    
    def _insert_new(self, df: DataFrame) -> bool:
        """
        Insert new records into target table
        
        Args:
            df: DataFrame to insert
            
        Returns:
            Success flag
        """
        try:
            self.logger.info("Executing INSERT mode")
            
            df.write \
                .format(self.config.get('target_format', 'parquet')) \
                .mode('append') \
                .option("path", self.config.get('target_path', '/tmp/etl_target')) \
                .saveAsTable(self.target_table)
                
            self.logger.info(f"Successfully inserted {df.count()} records")
            return True
            
        except Exception as e:
            self.logger.error(f"Insert failed: {str(e)}")
            return False
    
    def _update_existing(self, df: DataFrame) -> bool:
        """
        Update existing records in target table
        
        Args:
            df: DataFrame with updates
            
        Returns:
            Success flag
        """
        try:
            self.logger.info("Executing UPDATE mode")
            
            # Read existing data
            existing_df = self.spark.read.table(self.target_table)
            
            # Join on ID and update
            updated_df = existing_df.alias("existing") \
                .join(df.alias("new"), on="id", how="left") \
                .select(
                    "existing.id",
                    "new.name",
                    "new.value",
                    "new.transformed_value",
                    "new.status",
                    "new.category",
                    "new.priority",
                    "new.etl_run_id",
                    "new.processed_at",
                    "new.processed_by"
                )
            
            # Overwrite partition
            updated_df.write \
                .format(self.config.get('target_format', 'parquet')) \
                .mode('overwrite') \
                .saveAsTable(self.target_table)
                
            self.logger.info(f"Successfully updated records")
            return True
            
        except Exception as e:
            self.logger.error(f"Update failed: {str(e)}")
            return False
    
    def _upsert_data(self, df: DataFrame) -> bool:
        """
        Upsert (insert or update) records in target table
        
        Args:
            df: DataFrame to upsert
            
        Returns:
            Success flag
        """
        try:
            self.logger.info("Executing UPSERT mode")
            
            # Try to read existing table
            try:
                existing_df = self.spark.read.table(self.target_table)
                
                # Identify new vs existing records
                new_records = df.alias("new") \
                    .join(existing_df.alias("existing"), on="id", how="left_anti")
                    
                existing_records = df.alias("new") \
                    .join(existing_df.alias("existing"), on="id", how="inner") \
                    .select("new.*")
                
                # Insert new records
                if new_records.count() > 0:
                    new_records.write \
                        .format(self.config.get('target_format', 'parquet')) \
                        .mode('append') \
                        .saveAsTable(self.target_table)
                    self.logger.info(f"Inserted {new_records.count()} new records")
                
                # Update existing records
                if existing_records.count() > 0:
                    self._update_existing(existing_records)
                    self.logger.info(f"Updated {existing_records.count()} existing records")
                    
            except Exception:
                # Table doesn't exist, do insert
                self._insert_new(df)
                
            return True
            
        except Exception as e:
            self.logger.error(f"Upsert failed: {str(e)}")
            return False
    
    def _reconcile_data(self, df: DataFrame) -> bool:
        """
        Reconcile loaded data with source
        
        Args:
            df: Source DataFrame to reconcile
            
        Returns:
            Reconciliation success flag
        """
        try:
            self.logger.info("Starting data reconciliation")
            
            # Read target table
            target_df = self.spark.read.table(self.target_table)
            
            # Count records
            source_count = df.count()
            target_count = target_df.filter(f"etl_run_id = '{self.run_id}'").count()
            
            if source_count == target_count:
                self.logger.info(f"Reconciliation passed: {source_count} records match")
                return True
            else:
                self.logger.warning(
                    f"Reconciliation failed: Source={source_count}, Target={target_count}"
                )
                return False
                
        except Exception as e:
            self.logger.error(f"Reconciliation error: {str(e)}")
            return False


def get_target_schema() -> StructType:
    """Define target table schema"""
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