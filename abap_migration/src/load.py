"""Data loading module for ETL pipeline."""
from typing import Dict, Any, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
import logging

from src.utils.logger import ETLLogger
from src.utils.config import Config


class LoadResult:
    """Container for load operation results."""
    
    def __init__(self):
        self.success_count: int = 0
        self.error_count: int = 0
        self.total_count: int = 0
        self.errors: List[str] = []


class ETLLoader:
    """Handles data loading to target systems."""
    
    def __init__(self, spark: SparkSession, config: Config, run_id: str):
        """Initialize loader.
        
        Args:
            spark: SparkSession instance
            config: Configuration object
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
        self.target_type = config.get("load.target_type", "database")
        self.batch_size = int(config.get("load.batch_size", "1000"))
        
    def load_data(self, df: DataFrame, mode: str = "upsert") -> LoadResult:
        """Load data to target.
        
        Args:
            df: DataFrame to load
            mode: Load mode (insert, update, upsert)
            
        Returns:
            LoadResult object with operation statistics
        """
        result = LoadResult()
        result.total_count = df.count()
        
        self.logger.log_info(
            component="LOADER",
            message=f"Starting load - Mode: {mode}, Batch size: {self.batch_size}, Records: {result.total_count}"
        )
        
        try:
            if self.target_type.upper() == "DATABASE":
                success = self._load_to_database(df, mode)
            elif self.target_type.upper() == "PARQUET":
                success = self._load_to_parquet(df)
            elif self.target_type.upper() == "DELTA":
                success = self._load_to_delta(df, mode)
            else:
                success = self._load_to_database(df, mode)
            
            if success:
                result.success_count = result.total_count
                
                # Perform reconciliation if enabled
                if self.config.get("load.enable_reconciliation", "true").lower() == "true":
                    if not self._reconcile_data(df):
                        self.logger.log_warning(
                            component="LOADER",
                            message="Data reconciliation failed"
                        )
            else:
                result.error_count = result.total_count
                result.errors.append("Load operation failed")
            
            self.logger.log_info(
                component="LOADER",
                message=f"Load complete - Success: {result.success_count}, Errors: {result.error_count}"
            )
            
            return result
            
        except Exception as e:
            result.error_count = result.total_count
            result.errors.append(str(e))
            self.logger.log_error(
                component="LOADER",
                message="Load failed",
                details=str(e)
            )
            return result
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """Load data to database using JDBC.
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            Success status
        """
        jdbc_config = self.config.get_section("load.jdbc")
        target_table = self.config.get("load.target_table", "etl_target_data")
        
        try:
            # Map mode to appropriate save mode
            save_mode_map = {
                "insert": "append",
                "update": "overwrite",
                "upsert": "append"  # Will handle upsert logic separately
            }
            save_mode = save_mode_map.get(mode.lower(), "append")
            
            # Write to database
            df.write \
                .format("jdbc") \
                .option("url", jdbc_config["url"]) \
                .option("dbtable", target_table) \
                .option("user", jdbc_config["user"]) \
                .option("password", jdbc_config["password"]) \
                .option("driver", jdbc_config["driver"]) \
                .option("batchsize", self.batch_size) \
                .mode(save_mode) \
                .save()
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Database load error",
                details=str(e)
            )
            return False
    
    def _load_to_parquet(self, df: DataFrame) -> bool:
        """Load data to Parquet files.
        
        Args:
            df: DataFrame to load
            
        Returns:
            Success status
        """
        output_path = self.config.get("load.output_path")
        partition_cols = self.config.get("load.partition_columns", "").split(",")
        partition_cols = [col.strip() for col in partition_cols if col.strip()]
        
        try:
            writer = df.write.mode("overwrite").format("parquet")
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            writer.save(f"{output_path}/run_id={self.run_id}")
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Parquet load error",
                details=str(e)
            )
            return False
    
    def _load_to_delta(self, df: DataFrame, mode: str) -> bool:
        """Load data to Delta Lake.
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            Success status
        """
        delta_path = self.config.get("load.delta_path")
        
        try:
            if mode.lower() == "upsert":
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
                    # First load - create table
                    df.write.format("delta").mode("overwrite").save(delta_path)
            else:
                df.write.format("delta").mode(mode).save(delta_path)
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Delta load error",
                details=str(e)
            )
            return False
    
    def _reconcile_data(self, loaded_df: DataFrame) -> bool:
        """Reconcile loaded data with source.
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            Reconciliation success status
        """
        try:
            source_count = loaded_df.count()
            
            # Read back from target and compare
            if self.target_type.upper() == "DATABASE":
                jdbc_config = self.config.get_section("load.jdbc")
                target_table = self.config.get("load.target_table", "etl_target_data")
                
                target_df = self.spark.read \
                    .format("jdbc") \
                    .option("url", jdbc_config["url"]) \
                    .option("dbtable", f"(SELECT * FROM {target_table} WHERE etl_run_id = '{self.run_id}') as target") \
                    .option("user", jdbc_config["user"]) \
                    .option("password", jdbc_config["password"]) \
                    .option("driver", jdbc_config["driver"]) \
                    .load()
                
                target_count = target_df.count()
                
                if source_count == target_count:
                    self.logger.log_info(
                        component="LOADER",
                        message=f"Reconciliation successful: {source_count} records matched"
                    )
                    return True
                else:
                    self.logger.log_warning(
                        component="LOADER",
                        message=f"Reconciliation mismatch - Source: {source_count}, Target: {target_count}"
                    )
                    return False
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Reconciliation error",
                details=str(e)
            )
            return False