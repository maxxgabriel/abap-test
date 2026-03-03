"""
Delta Lake Loader Module
Handles data loading with Delta Lake merge operations
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit
from delta.tables import DeltaTable
from typing import Dict, List, Optional
import logging
from datetime import datetime

from src.logger import ETLLogger
from src.config import Config


class LoadResult:
    """Container for load operation results"""
    def __init__(self):
        self.success_count: int = 0
        self.error_count: int = 0
        self.total_count: int = 0
        self.errors: List[str] = []
        self.merge_stats: Optional[Dict] = None


class DeltaLakeLoader:
    """
    Handles loading transformed data to Delta Lake tables
    Replaces ABAP batch loops with DataFrame operations
    """
    
    def __init__(
        self,
        spark: SparkSession,
        target_type: str = "DATABASE",
        batch_size: int = 1000,
        run_id: str = None,
        config: Config = None
    ):
        """
        Initialize Delta Lake Loader
        
        Args:
            spark: SparkSession instance
            target_type: Target storage type (DATABASE, S3, etc.)
            batch_size: Records per partition (replaces ABAP batch loops)
            run_id: ETL run identifier
            config: Configuration object
        """
        self.spark = spark
        self.target_type = target_type
        self.batch_size = batch_size
        self.run_id = run_id or datetime.now().strftime("%Y%m%d%H%M%S")
        self.config = config or Config()
        self.logger = ETLLogger.get_instance()
        
        # Configure Delta Lake
        self._configure_delta()
    
    def _configure_delta(self):
        """Configure Delta Lake settings"""
        # Enable Delta Lake optimizations
        self.spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        self.spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
        self.spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")
        
        self.logger.log_info(
            component="LOADER",
            message=f"Delta Lake configured - Batch size: {self.batch_size}"
        )
    
    def load_data(
        self,
        df: DataFrame,
        mode: str = "UPSERT"
    ) -> LoadResult:
        """
        Load data using Delta Lake operations
        Replaces ABAP batch loops with DataFrame repartition
        
        Args:
            df: Transformed DataFrame to load
            mode: Load mode (INSERT, UPDATE, UPSERT)
        
        Returns:
            LoadResult with statistics
        """
        result = LoadResult()
        
        self.logger.log_info(
            component="LOADER",
            message=f"Starting load - Mode: {mode}, Target: {self.target_type}"
        )
        
        try:
            # Repartition data for optimal processing (replaces ABAP batch loops)
            num_partitions = max(1, df.count() // self.batch_size)
            df_partitioned = df.repartition(num_partitions)
            
            result.total_count = df_partitioned.count()
            
            # Route to appropriate load method
            if mode == "INSERT":
                success = self._insert_new(df_partitioned)
            elif mode == "UPDATE":
                success = self._update_existing(df_partitioned)
            elif mode == "UPSERT":
                success = self._upsert_data(df_partitioned, result)
            else:
                raise ValueError(f"Unsupported load mode: {mode}")
            
            if success:
                result.success_count = result.total_count
                self.logger.log_info(
                    component="LOADER",
                    message=f"Load complete - Success: {result.success_count}"
                )
            else:
                result.error_count = result.total_count
                result.errors.append("Load operation failed")
            
            # Reconcile data if enabled
            if self.config.get("ENABLE_RECONCILIATION", True):
                self._reconcile_data(df_partitioned)
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Load operation failed",
                details=str(e)
            )
            result.error_count = result.total_count
            result.errors.append(str(e))
        
        return result
    
    def _insert_new(self, df: DataFrame) -> bool:
        """
        Insert new records using Delta Lake append mode
        Replaces ABAP INSERT statement + COMMIT WORK
        
        Args:
            df: DataFrame to insert
        
        Returns:
            Success flag
        """
        try:
            target_path = self.config.get_target_path()
            
            # Write with Delta format (auto-commits, no explicit COMMIT needed)
            df.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(target_path)
            
            self.logger.log_info(
                component="LOADER",
                message=f"Inserted {df.count()} records to {target_path}"
            )
            
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
        Update existing records using Delta Lake merge
        Replaces ABAP UPDATE statement + COMMIT WORK
        
        Args:
            df: DataFrame with updates
        
        Returns:
            Success flag
        """
        try:
            target_path = self.config.get_target_path()
            
            # Check if Delta table exists
            if not DeltaTable.isDeltaTable(self.spark, target_path):
                self.logger.log_warning(
                    component="LOADER",
                    message="Target table doesn't exist, converting to insert"
                )
                return self._insert_new(df)
            
            delta_table = DeltaTable.forPath(self.spark, target_path)
            
            # Perform merge (UPDATE only, no INSERT)
            merge_result = delta_table.alias("target") \
                .merge(
                    df.alias("source"),
                    "target.id = source.id"
                ) \
                .whenMatchedUpdateAll() \
                .execute()
            
            self.logger.log_info(
                component="LOADER",
                message=f"Updated records in {target_path}",
                details=str(merge_result)
            )
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Update operation failed",
                details=str(e)
            )
            return False
    
    def _upsert_data(self, df: DataFrame, result: LoadResult) -> bool:
        """
        Perform UPSERT using Delta Lake merge operation
        Replaces ABAP UPDATE + INSERT logic + COMMIT WORK
        
        Args:
            df: DataFrame to upsert
            result: LoadResult to update with merge stats
        
        Returns:
            Success flag
        """
        try:
            target_path = self.config.get_target_path()
            
            # Create Delta table if it doesn't exist
            if not DeltaTable.isDeltaTable(self.spark, target_path):
                self.logger.log_info(
                    component="LOADER",
                    message="Creating new Delta table"
                )
                self._insert_new(df)
                return True
            
            delta_table = DeltaTable.forPath(self.spark, target_path)
            
            # Add processing metadata
            df_with_meta = df \
                .withColumn("etl_run_id", lit(self.run_id)) \
                .withColumn("processed_at", current_timestamp())
            
            # Perform MERGE operation (replaces ABAP UPSERT + COMMIT)
            merge_builder = delta_table.alias("target") \
                .merge(
                    df_with_meta.alias("source"),
                    "target.id = source.id"
                )
            
            # WHEN MATCHED: Update existing records
            merge_builder = merge_builder.whenMatchedUpdate(
                set={
                    "name": col("source.name"),
                    "value": col("source.value"),
                    "transformed_value": col("source.transformed_value"),
                    "status": col("source.status"),
                    "category": col("source.category"),
                    "priority": col("source.priority"),
                    "etl_run_id": col("source.etl_run_id"),
                    "processed_at": col("source.processed_at"),
                    "processed_by": col("source.processed_by")
                }
            )
            
            # WHEN NOT MATCHED: Insert new records
            merge_builder = merge_builder.whenNotMatchedInsertAll()
            
            # Execute merge (auto-commits, replaces COMMIT WORK)
            merge_builder.execute()
            
            # Get merge statistics
            history = delta_table.history(1).select("operationMetrics").collect()
            if history:
                result.merge_stats = history[0]["operationMetrics"]
                self.logger.log_info(
                    component="LOADER",
                    message="UPSERT completed",
                    details=f"Stats: {result.merge_stats}"
                )
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="UPSERT operation failed",
                details=str(e)
            )
            return False
    
    def _reconcile_data(self, df: DataFrame) -> bool:
        """
        Reconcile loaded data with source
        
        Args:
            df: Source DataFrame
        
        Returns:
            Match status
        """
        try:
            target_path = self.config.get_target_path()
            
            if not DeltaTable.isDeltaTable(self.spark, target_path):
                return False
            
            # Read target data
            target_df = self.spark.read.format("delta").load(target_path)
            
            # Get IDs from source
            source_ids = df.select("id").distinct()
            target_ids = target_df.select("id").distinct()
            
            # Compare counts
            source_count = source_ids.count()
            target_count = target_ids.count()
            
            matches = source_count == target_count
            
            if matches:
                self.logger.log_info(
                    component="LOADER",
                    message=f"Reconciliation successful - {source_count} records"
                )
            else:
                self.logger.log_warning(
                    component="LOADER",
                    message=f"Reconciliation mismatch - Source: {source_count}, Target: {target_count}"
                )
            
            return matches
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Reconciliation failed",
                details=str(e)
            )
            return False
    
    def optimize_table(self):
        """
        Optimize Delta table (compact small files, Z-ordering)
        """
        try:
            target_path = self.config.get_target_path()
            
            if not DeltaTable.isDeltaTable(self.spark, target_path):
                return
            
            delta_table = DeltaTable.forPath(self.spark, target_path)
            
            # Compact small files
            delta_table.optimize().executeCompaction()
            
            # Z-order by frequently filtered columns
            if self.config.get("ENABLE_ZORDER", True):
                delta_table.optimize().executeZOrderBy("category", "status")
            
            self.logger.log_info(
                component="LOADER",
                message="Table optimization completed"
            )
            
        except Exception as e:
            self.logger.log_warning(
                component="LOADER",
                message="Table optimization failed",
                details=str(e)
            )
    
    def vacuum_old_versions(self, retention_hours: int = 168):
        """
        Clean up old versions of Delta table
        
        Args:
            retention_hours: Hours to retain (default 7 days)
        """
        try:
            target_path = self.config.get_target_path()
            
            if not DeltaTable.isDeltaTable(self.spark, target_path):
                return
            
            delta_table = DeltaTable.forPath(self.spark, target_path)
            delta_table.vacuum(retention_hours)
            
            self.logger.log_info(
                component="LOADER",
                message=f"Vacuum completed - Retained {retention_hours}h"
            )
            
        except Exception as e:
            self.logger.log_warning(
                component="LOADER",
                message="Vacuum operation failed",
                details=str(e)
            )


def create_loader(
    spark: SparkSession,
    config: Config = None,
    **kwargs
) -> DeltaLakeLoader:
    """
    Factory function to create DeltaLakeLoader instance
    
    Args:
        spark: SparkSession
        config: Configuration object
        **kwargs: Additional loader parameters
    
    Returns:
        Configured DeltaLakeLoader instance
    """
    return DeltaLakeLoader(spark=spark, config=config, **kwargs)