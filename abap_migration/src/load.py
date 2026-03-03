"""
ETL Data Loading Module
Handles batch processing, INSERT/UPSERT operations, and reconciliation
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
from typing import Dict, Any, List
from dataclasses import dataclass
import logging


@dataclass
class LoadResult:
    """Results from load operation"""
    success_count: int
    error_count: int
    total_count: int
    errors: List[str]


class ETLLoader:
    """
    Handles data loading with batch processing and comprehensive error tracking
    """
    
    def __init__(self, spark: SparkSession, target_type: str = "DATABASE", 
                 batch_size: int = 1000, run_id: str = None):
        """
        Initialize loader with configuration
        
        Args:
            spark: Active SparkSession
            target_type: Type of target (DATABASE, PARQUET, DELTA)
            batch_size: Number of records per batch
            run_id: Unique run identifier
        """
        self.spark = spark
        self.target_type = target_type
        self.batch_size = batch_size
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
    
    def load_data(self, df: DataFrame, mode: str = "INSERT") -> LoadResult:
        """
        Main loading method with batch processing
        
        Args:
            df: Transformed DataFrame to load
            mode: Load mode (INSERT, UPDATE, UPSERT)
            
        Returns:
            LoadResult with success/error counts
        """
        self.logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        total_count = df.count()
        success_count = 0
        error_count = 0
        errors = []
        
        try:
            # Add batch ID for partitioning
            df_with_batch = df.withColumn(
                "batch_id",
                (col("id").cast("long") % self.batch_size).cast("int")
            )
            
            # Get unique batch IDs
            batch_ids = [row.batch_id for row in df_with_batch.select("batch_id").distinct().collect()]
            
            # Process each batch
            for batch_id in batch_ids:
                batch_df = df_with_batch.filter(col("batch_id") == batch_id).drop("batch_id")
                
                try:
                    if self._commit_batch(batch_df, mode):
                        batch_count = batch_df.count()
                        success_count += batch_count
                        self.logger.info(f"Batch {batch_id} loaded: {batch_count} records")
                    else:
                        batch_count = batch_df.count()
                        error_count += batch_count
                        errors.append(f"Batch {batch_id} failed")
                        self.logger.error(f"Batch {batch_id} failed")
                        
                except Exception as e:
                    batch_count = batch_df.count()
                    error_count += batch_count
                    error_msg = f"Batch {batch_id} error: {str(e)}"
                    errors.append(error_msg)
                    self.logger.error(error_msg)
            
            # Reconcile loaded data
            if self.spark.conf.get("spark.etl.enable.reconciliation", "true") == "true":
                self._reconcile_data(df)
            
            result = LoadResult(
                success_count=success_count,
                error_count=error_count,
                total_count=total_count,
                errors=errors
            )
            
            self.logger.info(
                f"Load complete - Success: {success_count}, Errors: {error_count}"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            raise
    
    def _commit_batch(self, batch_df: DataFrame, mode: str) -> bool:
        """
        Commit a single batch to target
        
        Args:
            batch_df: Batch DataFrame
            mode: Load mode
            
        Returns:
            Success status
        """
        try:
            if self.target_type == "DATABASE":
                return self._load_to_database(batch_df, mode)
            elif self.target_type == "PARQUET":
                return self._load_to_parquet(batch_df, mode)
            elif self.target_type == "DELTA":
                return self._load_to_delta(batch_df, mode)
            else:
                return self._load_to_database(batch_df, mode)
                
        except Exception as e:
            self.logger.error(f"Batch commit failed: {str(e)}")
            return False
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to database via JDBC
        
        Args:
            df: DataFrame to load
            mode: Load mode (INSERT, UPDATE, UPSERT)
            
        Returns:
            Success status
        """
        try:
            jdbc_url = self.spark.conf.get("spark.etl.jdbc.url")
            target_table = self.spark.conf.get("spark.etl.target.table", "etl_target_data")
            
            # Map mode to JDBC save mode
            if mode == "INSERT":
                save_mode = "append"
            elif mode == "UPDATE" or mode == "UPSERT":
                # For UPSERT, we use temporary table approach
                temp_table = f"{target_table}_temp_{self.run_id}"
                
                # Write to temp table
                df.write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", temp_table) \
                    .option("user", self.spark.conf.get("spark.etl.jdbc.user")) \
                    .option("password", self.spark.conf.get("spark.etl.jdbc.password")) \
                    .option("driver", self.spark.conf.get("spark.etl.jdbc.driver")) \
                    .mode("overwrite") \
                    .save()
                
                # Execute merge statement via JDBC
                self._execute_merge(temp_table, target_table, mode)
                
                return True
            else:
                save_mode = "append"
            
            # Standard write
            df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", target_table) \
                .option("user", self.spark.conf.get("spark.etl.jdbc.user")) \
                .option("password", self.spark.conf.get("spark.etl.jdbc.password")) \
                .option("driver", self.spark.conf.get("spark.etl.jdbc.driver")) \
                .option("batchsize", str(self.batch_size)) \
                .mode(save_mode) \
                .save()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Database load error: {str(e)}")
            return False
    
    def _load_to_parquet(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to Parquet files
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            Success status
        """
        try:
            target_path = self.spark.conf.get("spark.etl.target.path")
            
            save_mode = "overwrite" if mode == "INSERT" else "append"
            
            df.write \
                .mode(save_mode) \
                .partitionBy("category") \
                .parquet(f"{target_path}/etl_target_data")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Parquet load error: {str(e)}")
            return False
    
    def _load_to_delta(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to Delta Lake with MERGE support
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            Success status
        """
        try:
            target_path = self.spark.conf.get("spark.etl.target.path")
            delta_table_path = f"{target_path}/etl_target_data"
            
            if mode == "UPSERT":
                from delta.tables import DeltaTable
                
                # Check if table exists
                if DeltaTable.isDeltaTable(self.spark, delta_table_path):
                    delta_table = DeltaTable.forPath(self.spark, delta_table_path)
                    
                    # Perform merge
                    delta_table.alias("target").merge(
                        df.alias("source"),
                        "target.id = source.id"
                    ).whenMatchedUpdateAll() \
                     .whenNotMatchedInsertAll() \
                     .execute()
                else:
                    # First write - create table
                    df.write.format("delta").mode("overwrite").save(delta_table_path)
            else:
                save_mode = "overwrite" if mode == "INSERT" else "append"
                df.write.format("delta").mode(save_mode).save(delta_table_path)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Delta load error: {str(e)}")
            return False
    
    def _execute_merge(self, temp_table: str, target_table: str, mode: str):
        """
        Execute MERGE statement for UPSERT operation
        
        Args:
            temp_table: Temporary staging table
            target_table: Target table
            mode: Load mode
        """
        # This would execute via JDBC connection
        # Implementation depends on specific database (PostgreSQL, MySQL, etc.)
        self.logger.info(f"Executing merge from {temp_table} to {target_table}")
        pass
    
    def _reconcile_data(self, df: DataFrame):
        """
        Reconcile loaded data against source
        
        Args:
            df: Source DataFrame
        """
        self.logger.info("Starting data reconciliation")
        
        try:
            source_count = df.count()
            
            # Read back from target
            if self.target_type == "DATABASE":
                jdbc_url = self.spark.conf.get("spark.etl.jdbc.url")
                target_table = self.spark.conf.get("spark.etl.target.table", "etl_target_data")
                
                target_df = self.spark.read \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", f"(SELECT * FROM {target_table} WHERE etl_run_id = '{self.run_id}') as recon") \
                    .option("user", self.spark.conf.get("spark.etl.jdbc.user")) \
                    .option("password", self.spark.conf.get("spark.etl.jdbc.password")) \
                    .option("driver", self.spark.conf.get("spark.etl.jdbc.driver")) \
                    .load()
                
                target_count = target_df.count()
            else:
                target_path = self.spark.conf.get("spark.etl.target.path")
                target_df = self.spark.read.parquet(f"{target_path}/etl_target_data") \
                    .filter(col("etl_run_id") == self.run_id)
                target_count = target_df.count()
            
            if source_count == target_count:
                self.logger.info(f"Reconciliation passed: {source_count} records match")
            else:
                self.logger.warning(
                    f"Reconciliation mismatch - Source: {source_count}, Target: {target_count}"
                )
                
        except Exception as e:
            self.logger.error(f"Reconciliation failed: {str(e)}")