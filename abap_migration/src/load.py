"""
Data loading module for ETL system.
Loads transformed data to target destinations with reconciliation.
"""

from pyspark.sql import DataFrame, SparkSession
from typing import Dict, List
from datetime import datetime
from src.utils.logger import ETLLogger
from src.utils.config import Config


class DataLoader:
    """Handles data loading to target systems."""
    
    def __init__(self, spark: SparkSession, config: Config, run_id: str):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
        self.batch_size = config.get("load.batch_size", 1000)
    
    def load_data(
        self,
        df: DataFrame,
        target_type: str = "database",
        mode: str = "overwrite"
    ) -> Dict[str, int]:
        """
        Load data to target destination.
        
        Args:
            df: DataFrame to load
            target_type: Target destination type
            mode: Write mode (overwrite, append, upsert)
            
        Returns:
            Dictionary with load statistics
        """
        self.logger.log_info(
            component="LOADER",
            message=f"Starting load - Mode: {mode}, Batch size: {self.batch_size}"
        )
        
        result = {
            "success_count": 0,
            "error_count": 0,
            "total_count": 0,
            "errors": []
        }
        
        try:
            total_count = df.count()
            result["total_count"] = total_count
            
            if target_type.lower() == "database":
                success = self._load_to_database(df, mode)
            elif target_type.lower() == "staging":
                success = self._load_to_staging(df)
            else:
                success = self._load_to_database(df, mode)
            
            if success:
                result["success_count"] = total_count
                
                # Perform reconciliation
                if self.config.get("load.enable_reconciliation", True):
                    reconciled = self._reconcile_data(df)
                    if not reconciled:
                        self.logger.log_warning(
                            component="LOADER",
                            message="Data reconciliation failed"
                        )
            else:
                result["error_count"] = total_count
                result["errors"].append("Load operation failed")
            
            self.logger.log_info(
                component="LOADER",
                message=f"Load complete - Success: {result['success_count']}, "
                        f"Errors: {result['error_count']}"
            )
            
            # Log load metrics
            self._log_load_metrics(result)
            
            return result
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Load operation failed",
                details=str(e)
            )
            result["error_count"] = result["total_count"]
            result["errors"].append(str(e))
            return result
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """Load data to database target."""
        try:
            target_path = self.config.get("target.database.path")
            target_format = self.config.get("target.database.format", "parquet")
            
            # Convert mode to Spark write mode
            if mode.lower() == "upsert":
                # For upsert, we need to handle merge logic
                write_mode = "append"
                df = self._handle_upsert(df, target_path)
            else:
                write_mode = mode.lower()
            
            # Write with partitioning if configured
            partition_by = self.config.get("target.database.partition_by", None)
            
            writer = df.write.format(target_format).mode(write_mode)
            
            if partition_by:
                writer = writer.partitionBy(partition_by)
            
            writer.save(target_path)
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Database load failed",
                details=str(e)
            )
            return False
    
    def _handle_upsert(self, df: DataFrame, target_path: str) -> DataFrame:
        """Handle upsert logic (merge)."""
        from pyspark.sql.functions import col
        
        try:
            # Read existing data
            existing_df = self.spark.read.parquet(target_path)
            
            # Get keys for deduplication
            key_columns = ["id"]
            
            # Remove existing records with same keys
            existing_df = existing_df.join(
                df.select(key_columns),
                on=key_columns,
                how="left_anti"
            )
            
            # Union with new data
            result_df = existing_df.union(df)
            
            return result_df
            
        except Exception:
            # If target doesn't exist, return original df
            return df
    
    def _load_to_staging(self, df: DataFrame) -> bool:
        """Load data to staging area."""
        try:
            staging_path = self.config.get("target.staging.path")
            
            df.write.format("parquet").mode("append").save(staging_path)
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Staging load failed",
                details=str(e)
            )
            return False
    
    def _reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data with target.
        
        Returns:
            True if reconciliation passes, False otherwise
        """
        try:
            target_path = self.config.get("target.database.path")
            
            # Read target data
            target_df = self.spark.read.parquet(target_path)
            
            # Compare counts
            loaded_count = loaded_df.count()
            target_count = target_df.count()
            
            self.logger.log_info(
                component="LOADER",
                message=f"Reconciliation - Loaded: {loaded_count}, Target: {target_count}"
            )
            
            # Allow for slight discrepancies in append mode
            tolerance = self.config.get("load.reconciliation_tolerance", 0)
            
            return abs(target_count - loaded_count) <= tolerance
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Reconciliation failed",
                details=str(e)
            )
            return False
    
    def _log_load_metrics(self, result: Dict[str, int]):
        """Log load metrics for monitoring."""
        from src.utils.metrics import MetricsCollector
        
        metrics = MetricsCollector.get_instance()
        metrics.record_load_metrics(
            run_id=self.run_id,
            success_count=result["success_count"],
            error_count=result["error_count"],
            total_count=result["total_count"],
            timestamp=datetime.now()
        )