"""
Delta Lake Loader Module - PySpark Implementation
Converts ABAP batch loader to Delta Lake merge operations with DataFrame API
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from delta.tables import DeltaTable
from typing import Dict, List, Tuple
import logging
from datetime import datetime


class DeltaLakeLoader:
    """
    PySpark implementation of ETL loader using Delta Lake merge operations.
    Replaces ABAP batch loops with DataFrame operations and COMMIT WORK with write modes.
    """
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize Delta Lake Loader
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.batch_size = config.get('batch_size', 1000)
        self.target_path = config.get('target_path')
        self.run_id = config.get('run_id', datetime.now().strftime('%Y%m%d%H%M%S'))
        
    def get_target_schema(self) -> StructType:
        """
        Define target table schema
        
        Returns:
            StructType: Target table schema
        """
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
            StructField("processed_by", StringType(), True),
            StructField("load_timestamp", TimestampType(), True)
        ])
    
    def load_data(self, df: DataFrame, mode: str = 'upsert') -> Dict:
        """
        Load data to Delta Lake target using specified mode
        Replaces ABAP batch processing with DataFrame operations
        
        Args:
            df: Input DataFrame with transformed data
            mode: Load mode - 'insert', 'update', 'upsert', 'overwrite'
            
        Returns:
            Dict: Load result statistics
        """
        self.logger.info(f"Starting load - Mode: {mode}, Records: {df.count()}")
        
        try:
            # Add load metadata
            df_with_metadata = self._add_load_metadata(df)
            
            # Repartition for optimal write performance (replaces batch loops)
            num_partitions = self._calculate_partitions(df_with_metadata.count())
            df_partitioned = df_with_metadata.repartition(num_partitions, "category")
            
            # Execute load based on mode
            if mode.lower() == 'insert':
                result = self._insert_data(df_partitioned)
            elif mode.lower() == 'update':
                result = self._update_data(df_partitioned)
            elif mode.lower() == 'upsert':
                result = self._upsert_data(df_partitioned)
            elif mode.lower() == 'overwrite':
                result = self._overwrite_data(df_partitioned)
            else:
                raise ValueError(f"Invalid mode: {mode}")
            
            # Perform data reconciliation if enabled
            if self.config.get('enable_reconciliation', True):
                reconciliation_result = self._reconcile_data(df_with_metadata)
                result['reconciliation'] = reconciliation_result
            
            self.logger.info(f"Load complete - Success: {result['success_count']}, Errors: {result['error_count']}")
            return result
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            raise
    
    def _insert_data(self, df: DataFrame) -> Dict:
        """
        Insert new records using append mode
        Replaces ABAP INSERT statements with Delta append
        
        Args:
            df: DataFrame to insert
            
        Returns:
            Dict: Insert result statistics
        """
        self.logger.info("Executing INSERT operation")
        
        try:
            # Write using append mode (equivalent to INSERT)
            df.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(self.target_path)
            
            # Delta automatically handles commit (no explicit COMMIT WORK needed)
            record_count = df.count()
            
            return {
                'success_count': record_count,
                'error_count': 0,
                'total_count': record_count,
                'mode': 'INSERT',
                'errors': []
            }
            
        except Exception as e:
            self.logger.error(f"Insert failed: {str(e)}")
            return {
                'success_count': 0,
                'error_count': df.count(),
                'total_count': df.count(),
                'mode': 'INSERT',
                'errors': [str(e)]
            }
    
    def _update_data(self, df: DataFrame) -> Dict:
        """
        Update existing records using Delta merge operation
        Replaces ABAP UPDATE statements with Delta merge
        
        Args:
            df: DataFrame with updates
            
        Returns:
            Dict: Update result statistics
        """
        self.logger.info("Executing UPDATE operation")
        
        try:
            # Check if Delta table exists
            if not DeltaTable.isDeltaTable(self.spark, self.target_path):
                raise ValueError("Target Delta table does not exist for UPDATE operation")
            
            delta_table = DeltaTable.forPath(self.spark, self.target_path)
            
            # Build update expression
            update_cols = {col: f"source.{col}" for col in df.columns if col != "id"}
            
            # Execute merge with update only
            merge_result = delta_table.alias("target") \
                .merge(
                    df.alias("source"),
                    "target.id = source.id"
                ) \
                .whenMatchedUpdate(set=update_cols) \
                .execute()
            
            # Get operation metrics
            metrics = self._get_merge_metrics()
            
            return {
                'success_count': metrics.get('num_updated_rows', 0),
                'error_count': 0,
                'total_count': df.count(),
                'mode': 'UPDATE',
                'errors': []
            }
            
        except Exception as e:
            self.logger.error(f"Update failed: {str(e)}")
            return {
                'success_count': 0,
                'error_count': df.count(),
                'total_count': df.count(),
                'mode': 'UPDATE',
                'errors': [str(e)]
            }
    
    def _upsert_data(self, df: DataFrame) -> Dict:
        """
        Upsert (merge) data using Delta merge operation
        Replaces ABAP MODIFY/UPSERT with Delta merge
        This is the recommended approach for most use cases
        
        Args:
            df: DataFrame to upsert
            
        Returns:
            Dict: Upsert result statistics
        """
        self.logger.info("Executing UPSERT operation")
        
        try:
            # Create Delta table if it doesn't exist
            if not DeltaTable.isDeltaTable(self.spark, self.target_path):
                self.logger.info("Target table doesn't exist, creating...")
                df.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .save(self.target_path)
                
                return {
                    'success_count': df.count(),
                    'error_count': 0,
                    'total_count': df.count(),
                    'mode': 'UPSERT_INITIAL',
                    'errors': []
                }
            
            delta_table = DeltaTable.forPath(self.spark, self.target_path)
            
            # Build update and insert expressions
            update_cols = {col: f"source.{col}" for col in df.columns}
            insert_cols = {col: f"source.{col}" for col in df.columns}
            
            # Execute merge (upsert) - replaces ABAP batch MODIFY logic
            delta_table.alias("target") \
                .merge(
                    df.alias("source"),
                    "target.id = source.id"
                ) \
                .whenMatchedUpdate(set=update_cols) \
                .whenNotMatchedInsert(values=insert_cols) \
                .execute()
            
            # Delta handles commit automatically - no COMMIT WORK needed
            
            # Get operation metrics
            metrics = self._get_merge_metrics()
            
            success_count = metrics.get('num_updated_rows', 0) + metrics.get('num_inserted_rows', 0)
            
            return {
                'success_count': success_count,
                'error_count': 0,
                'total_count': df.count(),
                'mode': 'UPSERT',
                'updated_rows': metrics.get('num_updated_rows', 0),
                'inserted_rows': metrics.get('num_inserted_rows', 0),
                'errors': []
            }
            
        except Exception as e:
            self.logger.error(f"Upsert failed: {str(e)}")
            return {
                'success_count': 0,
                'error_count': df.count(),
                'total_count': df.count(),
                'mode': 'UPSERT',
                'errors': [str(e)]
            }
    
    def _overwrite_data(self, df: DataFrame) -> Dict:
        """
        Overwrite entire table with new data
        
        Args:
            df: DataFrame to write
            
        Returns:
            Dict: Overwrite result statistics
        """
        self.logger.info("Executing OVERWRITE operation")
        
        try:
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save(self.target_path)
            
            record_count = df.count()
            
            return {
                'success_count': record_count,
                'error_count': 0,
                'total_count': record_count,
                'mode': 'OVERWRITE',
                'errors': []
            }
            
        except Exception as e:
            self.logger.error(f"Overwrite failed: {str(e)}")
            return {
                'success_count': 0,
                'error_count': df.count(),
                'total_count': df.count(),
                'mode': 'OVERWRITE',
                'errors': [str(e)]
            }
    
    def _add_load_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add load metadata columns to DataFrame
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: DataFrame with metadata columns
        """
        from pyspark.sql.functions import current_timestamp, lit
        
        return df \
            .withColumn("load_timestamp", current_timestamp()) \
            .withColumn("etl_run_id", lit(self.run_id))
    
    def _calculate_partitions(self, record_count: int) -> int:
        """
        Calculate optimal number of partitions based on record count
        Replaces ABAP batch size logic
        
        Args:
            record_count: Number of records
            
        Returns:
            int: Number of partitions
        """
        if record_count < 1000:
            return 1
        elif record_count < 10000:
            return 4
        elif record_count < 100000:
            return 8
        elif record_count < 1000000:
            return 16
        else:
            return 32
    
    def _reconcile_data(self, source_df: DataFrame) -> Dict:
        """
        Reconcile loaded data against source
        Replaces ABAP reconciliation logic
        
        Args:
            source_df: Source DataFrame
            
        Returns:
            Dict: Reconciliation results
        """
        self.logger.info("Performing data reconciliation")
        
        try:
            # Read target table
            target_df = self.spark.read.format("delta").load(self.target_path)
            
            # Count records
            source_count = source_df.count()
            target_count = target_df.filter(f"etl_run_id = '{self.run_id}'").count()
            
            # Check for matches
            matches = source_count == target_count
            
            return {
                'matches': matches,
                'source_count': source_count,
                'target_count': target_count,
                'difference': abs(source_count - target_count)
            }
            
        except Exception as e:
            self.logger.warning(f"Reconciliation failed: {str(e)}")
            return {
                'matches': False,
                'error': str(e)
            }
    
    def _get_merge_metrics(self) -> Dict:
        """
        Get metrics from last Delta merge operation
        
        Returns:
            Dict: Merge operation metrics
        """
        try:
            history = DeltaTable.forPath(self.spark, self.target_path).history(1)
            metrics_row = history.select("operationMetrics").first()
            
            if metrics_row and metrics_row[0]:
                return metrics_row[0].asDict()
            return {}
            
        except Exception as e:
            self.logger.warning(f"Could not retrieve merge metrics: {str(e)}")
            return {}
    
    def compact_table(self):
        """
        Optimize Delta table (compact small files)
        """
        self.logger.info("Compacting Delta table")
        try:
            delta_table = DeltaTable.forPath(self.spark, self.target_path)
            delta_table.optimize().executeCompaction()
            self.logger.info("Table compaction completed")
        except Exception as e:
            self.logger.error(f"Compaction failed: {str(e)}")
    
    def vacuum_table(self, retention_hours: int = 168):
        """
        Clean up old Delta table files
        
        Args:
            retention_hours: Retention period in hours (default 7 days)
        """
        self.logger.info(f"Vacuuming Delta table (retention: {retention_hours}h)")
        try:
            delta_table = DeltaTable.forPath(self.spark, self.target_path)
            delta_table.vacuum(retention_hours)
            self.logger.info("Table vacuum completed")
        except Exception as e:
            self.logger.error(f"Vacuum failed: {str(e)}")


def create_loader(spark: SparkSession, config: Dict) -> DeltaLakeLoader:
    """
    Factory function to create loader instance
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        
    Returns:
        DeltaLakeLoader: Configured loader instance
    """
    return DeltaLakeLoader(spark, config)