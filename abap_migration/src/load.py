"""
Data loading module for ETL pipeline.
Loads transformed data to target destination with reconciliation.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict, List
import logging


class Loader:
    """Handles data loading to target destination."""
    
    def __init__(self, spark: SparkSession, config: dict, run_id: str):
        """
        Initialize the loader.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.batch_size = config.get("load", {}).get("batch_size", 1000)
        self.logger = logging.getLogger(__name__)
    
    def load_data(self, df: DataFrame, mode: str = "upsert") -> Dict[str, int]:
        """
        Load data to target destination.
        
        Args:
            df: DataFrame to load
            mode: Load mode (insert, update, upsert, overwrite)
            
        Returns:
            Dictionary with load results (success_count, error_count, total_count)
        """
        self.logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        total_count = df.count()
        success_count = 0
        error_count = 0
        errors = []
        
        try:
            # Load to target
            success = self._load_to_target(df, mode)
            
            if success:
                success_count = total_count
                
                # Reconcile data if enabled
                if self.config.get("load", {}).get("enable_reconciliation", True):
                    reconcile_result = self.reconcile_data(df)
                    if not reconcile_result:
                        self.logger.warning("Data reconciliation failed")
            else:
                error_count = total_count
                errors.append("Load to target failed")
            
            self.logger.info(f"Load complete - Success: {success_count}, Errors: {error_count}")
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            error_count = total_count
            errors.append(str(e))
        
        return {
            "success_count": success_count,
            "error_count": error_count,
            "total_count": total_count,
            "errors": errors
        }
    
    def _load_to_target(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to the target destination.
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            True if successful, False otherwise
        """
        target_config = self.config.get("target", {})
        
        try:
            if target_config.get("type") == "jdbc":
                self._load_to_jdbc(df, mode, target_config)
            else:
                self._load_to_file(df, mode, target_config)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Target load error: {str(e)}")
            return False
    
    def _load_to_jdbc(self, df: DataFrame, mode: str, jdbc_config: dict):
        """Load data to JDBC target."""
        write_mode = self._convert_mode(mode)
        
        (df.write
         .format("jdbc")
         .option("url", jdbc_config["url"])
         .option("dbtable", jdbc_config["table"])
         .option("user", jdbc_config.get("user", ""))
         .option("password", jdbc_config.get("password", ""))
         .option("driver", jdbc_config.get("driver", ""))
         .option("batchsize", self.batch_size)
         .mode(write_mode)
         .save())
    
    def _load_to_file(self, df: DataFrame, mode: str, file_config: dict):
        """Load data to file target."""
        write_mode = self._convert_mode(mode)
        output_path = file_config.get("path", "data/output")
        output_format = file_config.get("format", "parquet")
        
        writer = df.write.format(output_format).mode(write_mode)
        
        # Add partitioning if configured
        partition_cols = file_config.get("partition_by", [])
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        writer.save(output_path)
    
    def _convert_mode(self, mode: str) -> str:
        """Convert load mode to Spark write mode."""
        mode_mapping = {
            "insert": "append",
            "update": "overwrite",
            "upsert": "append",
            "overwrite": "overwrite"
        }
        return mode_mapping.get(mode.lower(), "append")
    
    def reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data with target.
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passes, False otherwise
        """
        self.logger.info("Starting data reconciliation")
        
        try:
            target_config = self.config.get("target", {})
            
            # Read back from target
            if target_config.get("type") == "jdbc":
                target_df = (self.spark.read
                             .format("jdbc")
                             .option("url", target_config["url"])
                             .option("dbtable", target_config["table"])
                             .option("user", target_config.get("user", ""))
                             .option("password", target_config.get("password", ""))
                             .load())
            else:
                output_path = target_config.get("path", "data/output")
                target_df = self.spark.read.parquet(output_path)
            
            # Filter for current run
            target_df = target_df.filter(F.col("etl_run_id") == self.run_id)
            
            # Compare counts
            loaded_count = loaded_df.count()
            target_count = target_df.count()
            
            if loaded_count != target_count:
                self.logger.warning(
                    f"Record count mismatch - Loaded: {loaded_count}, Target: {target_count}"
                )
                return False
            
            # Compare checksums
            loaded_checksum = loaded_df.agg(F.sum("value")).collect()[0][0]
            target_checksum = target_df.agg(F.sum("value")).collect()[0][0]
            
            if loaded_checksum != target_checksum:
                self.logger.warning(
                    f"Checksum mismatch - Loaded: {loaded_checksum}, Target: {target_checksum}"
                )
                return False
            
            self.logger.info("Data reconciliation passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Reconciliation error: {str(e)}")
            return False