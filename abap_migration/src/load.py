"""
Data loading module for ETL pipeline.
Handles loading transformed data to target systems with batching and reconciliation.
"""

from typing import Dict, List, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.logger import ETLLogger


class DataLoader:
    """Loads transformed data to target systems."""
    
    def __init__(self, spark: SparkSession, target_type: str = "DATABASE", 
                 batch_size: int = 1000, run_id: str = None):
        """
        Initialize the data loader.
        
        Args:
            spark: SparkSession instance
            target_type: Type of target system
            batch_size: Number of records per batch
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.target_type = target_type.upper()
        self.batch_size = batch_size
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
    
    def load_data(self, df: DataFrame, mode: str = "INSERT") -> Dict[str, Any]:
        """
        Load data to target system.
        
        Args:
            df: DataFrame to load
            mode: Load mode ('INSERT', 'UPDATE', 'UPSERT')
            
        Returns:
            Dictionary with load results
        """
        self.logger.log_info(
            component="LOADER",
            message=f"Starting load - Mode: {mode}, Batch size: {self.batch_size}"
        )
        
        try:
            total_count = df.count()
            success_count = 0
            error_count = 0
            errors = []
            
            # Load data
            if self._load_to_database(df, mode):
                success_count = total_count
                
                # Reconcile data
                if self._reconcile_data(df):
                    self.logger.log_info(
                        component="LOADER",
                        message="Data reconciliation successful"
                    )
                else:
                    self.logger.log_warning(
                        component="LOADER",
                        message="Data reconciliation failed"
                    )
            else:
                error_count = total_count
                errors.append("Database load failed")
            
            result = {
                "success_count": success_count,
                "error_count": error_count,
                "total_count": total_count,
                "errors": errors
            }
            
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
            raise
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to database target.
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Map mode to Spark save mode
            save_mode = {
                "INSERT": "append",
                "UPDATE": "overwrite",
                "UPSERT": "append"  # Will handle upsert logic
            }.get(mode, "append")
            
            # Write to target table
            df.write \
                .format("jdbc") \
                .option("url", self.spark.conf.get("spark.etl.target.url")) \
                .option("dbtable", self.spark.conf.get("spark.etl.target.table", "etl_target_data")) \
                .option("user", self.spark.conf.get("spark.etl.target.user")) \
                .option("password", self.spark.conf.get("spark.etl.target.password")) \
                .option("batchsize", str(self.batch_size)) \
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
    
    def _reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data with source.
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            True if reconciliation matches, False otherwise
        """
        try:
            # Read back from target
            target_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.spark.conf.get("spark.etl.target.url")) \
                .option("dbtable", self.spark.conf.get("spark.etl.target.table", "etl_target_data")) \
                .option("user", self.spark.conf.get("spark.etl.target.user")) \
                .option("password", self.spark.conf.get("spark.etl.target.password")) \
                .load() \
                .filter(f"etl_run_id = '{self.run_id}'")
            
            loaded_count = loaded_df.count()
            target_count = target_df.count()
            
            if loaded_count != target_count:
                self.logger.log_warning(
                    component="LOADER",
                    message=f"Reconciliation mismatch: Loaded={loaded_count}, Target={target_count}"
                )
                return False
            
            return True
            
        except Exception as e:
            self.logger.log_warning(
                component="LOADER",
                message=f"Reconciliation error: {str(e)}"
            )
            return False