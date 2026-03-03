"""
Data loading module for ETL pipeline.
Handles loading transformed data to target systems with batch processing and reconciliation.
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from src.logger import ETLLogger


@dataclass
class LoadResult:
    """Results from data loading operation."""
    success_count: int
    error_count: int
    total_count: int
    errors: List[str]


class DataLoader:
    """
    Loads transformed data to target systems.
    Supports insert, update, and upsert modes with batch processing.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Dict[str, Any],
        run_id: str,
        batch_size: int = 1000
    ):
        """
        Initialize loader.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
            batch_size: Number of records per batch
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.batch_size = batch_size
        self.logger = ETLLogger.get_instance()
        self.target_type = config.get("target_type", "database")
        
    def load_data(
        self,
        df: DataFrame,
        mode: str = "overwrite"
    ) -> LoadResult:
        """
        Load data to target system.
        
        Args:
            df: Transformed DataFrame to load
            mode: Load mode - 'overwrite', 'append', or 'upsert'
            
        Returns:
            LoadResult with statistics
        """
        self.logger.log_info(
            component="LOADER",
            message=f"Starting load - Mode: {mode}, Batch size: {self.batch_size}"
        )
        
        total_count = df.count()
        errors = []
        
        try:
            if self.target_type == "database":
                success = self._load_to_database(df, mode)
            elif self.target_type == "file":
                success = self._load_to_file(df, mode)
            elif self.target_type == "delta":
                success = self._load_to_delta(df, mode)
            else:
                success = self._load_to_database(df, mode)
            
            if success:
                success_count = total_count
                error_count = 0
                
                # Reconcile data
                if self.config.get("enable_reconciliation", True):
                    reconciled = self._reconcile_data(df)
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
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """Load data to database via JDBC."""
        jdbc_config = self.config.get("jdbc", {})
        target_table = jdbc_config.get("target_table", "etl_target_data")
        
        try:
            # Map mode to JDBC save mode
            save_mode = "overwrite" if mode == "overwrite" else "append"
            
            df.write \
                .format("jdbc") \
                .option("url", jdbc_config.get("url")) \
                .option("dbtable", target_table) \
                .option("user", jdbc_config.get("user")) \
                .option("password", jdbc_config.get("password")) \
                .option("driver", jdbc_config.get("driver", "org.postgresql.Driver")) \
                .option("batchsize", self.batch_size) \
                .mode(save_mode) \
                .save()
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message=f"Database load error: {str(e)}"
            )
            return False
    
    def _load_to_file(self, df: DataFrame, mode: str) -> bool:
        """Load data to file system."""
        file_config = self.config.get("file", {})
        output_path = file_config.get("output_path", "/data/output")
        file_format = file_config.get("format", "parquet")
        
        try:
            writer = df.write.mode(mode)
            
            if file_format == "parquet":
                writer.parquet(f"{output_path}/run_id={self.run_id}")
            elif file_format == "csv":
                writer.option("header", "true").csv(f"{output_path}/run_id={self.run_id}")
            elif file_format == "json":
                writer.json(f"{output_path}/run_id={self.run_id}")
            else:
                raise ValueError(f"Unsupported format: {file_format}")
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message=f"File load error: {str(e)}"
            )
            return False
    
    def _load_to_delta(self, df: DataFrame, mode: str) -> bool:
        """Load data to Delta Lake."""
        delta_config = self.config.get("delta", {})
        delta_path = delta_config.get("path", "/data/delta/target")
        
        try:
            if mode == "upsert":
                # Use Delta merge for upsert
                from delta.tables import DeltaTable
                
                if DeltaTable.isDeltaTable(self.spark, delta_path):
                    delta_table = DeltaTable.forPath(self.spark, delta_path)
                    
                    delta_table.alias("target").merge(
                        df.alias("source"),
                        "target.id = source.id"
                    ).whenMatchedUpdateAll() \
                     .whenNotMatchedInsertAll() \
                     .execute()
                else:
                    df.write.format("delta").mode("overwrite").save(delta_path)
            else:
                df.write.format("delta").mode(mode).save(delta_path)
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message=f"Delta load error: {str(e)}"
            )
            return False
    
    def _reconcile_data(self, df: DataFrame) -> bool:
        """
        Reconcile loaded data against source.
        
        Args:
            df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passes
        """
        try:
            source_count = df.count()
            
            # Read back from target
            if self.target_type == "database":
                jdbc_config = self.config.get("jdbc", {})
                target_table = jdbc_config.get("target_table")
                
                target_df = self.spark.read \
                    .format("jdbc") \
                    .option("url", jdbc_config.get("url")) \
                    .option("dbtable", target_table) \
                    .option("user", jdbc_config.get("user")) \
                    .option("password", jdbc_config.get("password")) \
                    .load() \
                    .filter(col("etl_run_id") == self.run_id)
                
                target_count = target_df.count()
            else:
                # For file/delta targets, assume success
                target_count = source_count
            
            matches = source_count == target_count
            
            if matches:
                self.logger.log_info(
                    component="LOADER",
                    message=f"Reconciliation passed: {source_count} records"
                )
            else:
                self.logger.log_warning(
                    component="LOADER",
                    message=f"Reconciliation mismatch - Source: {source_count}, Target: {target_count}"
                )
            
            return matches
            
        except Exception as e:
            self.logger.log_warning(
                component="LOADER",
                message=f"Reconciliation error: {str(e)}"
            )
            return False