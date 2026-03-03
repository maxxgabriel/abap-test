"""
ETL Data Loading Module
Loads transformed data to target destinations with batch processing and reconciliation.
"""

from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any, List
import logging


class ETLLoader:
    """Handles data loading to target destinations."""
    
    def __init__(self, spark: SparkSession, target_type: str = "DATABASE",
                 batch_size: int = 1000, run_id: str = None):
        """
        Initialize the loader.
        
        Args:
            spark: Active SparkSession
            target_type: Target destination type
            batch_size: Batch size for loading
            run_id: Unique run identifier
        """
        self.spark = spark
        self.target_type = target_type.upper()
        self.batch_size = batch_size
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def load_data(self, df: DataFrame, mode: str = "INSERT") -> Dict[str, Any]:
        """
        Load data to target destination.
        
        Args:
            df: DataFrame to load
            mode: Load mode (INSERT, UPDATE, UPSERT)
            
        Returns:
            Dictionary with load results
        """
        self.logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        success_count = 0
        error_count = 0
        errors = []
        
        try:
            total_count = df.count()
            
            # Determine write mode
            write_mode = self._get_write_mode(mode)
            
            # Load data
            if self._load_to_database(df, write_mode):
                success_count = total_count
                self.logger.info(f"Successfully loaded {success_count} records")
            else:
                error_count = total_count
                errors.append("Database load failed")
                self.logger.error("Load failed")
            
            # Reconcile data
            if success_count > 0:
                reconciled = self.reconcile_data(df)
                if not reconciled:
                    self.logger.warning("Data reconciliation failed")
            
        except Exception as e:
            error_count = df.count()
            errors.append(str(e))
            self.logger.error(f"Load error: {str(e)}")
        
        result = {
            "success_count": success_count,
            "error_count": error_count,
            "total_count": df.count() if df else 0,
            "errors": errors
        }
        
        self.logger.info(
            f"Load complete - Success: {success_count}, Errors: {error_count}"
        )
        
        return result
    
    def _get_write_mode(self, mode: str) -> str:
        """Map load mode to Spark write mode."""
        mode_mapping = {
            "INSERT": "append",
            "UPDATE": "overwrite",
            "UPSERT": "append"  # Handled separately
        }
        return mode_mapping.get(mode.upper(), "append")
    
    def _load_to_database(self, df: DataFrame, write_mode: str) -> bool:
        """
        Load data to database.
        
        Args:
            df: DataFrame to load
            write_mode: Spark write mode
            
        Returns:
            True if successful, False otherwise
        """
        try:
            df.write \
                .format("jdbc") \
                .option("url", self.spark.conf.get("spark.etl.target.jdbc.url")) \
                .option("dbtable", "etl_target_data") \
                .option("user", self.spark.conf.get("spark.etl.target.jdbc.user")) \
                .option("password", self.spark.conf.get("spark.etl.target.jdbc.password")) \
                .option("driver", self.spark.conf.get("spark.etl.target.jdbc.driver")) \
                .option("batchsize", str(self.batch_size)) \
                .mode(write_mode) \
                .save()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Database load error: {str(e)}")
            return False
    
    def reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data against source.
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passes
        """
        self.logger.info("Starting data reconciliation")
        
        try:
            # Read back from target
            target_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.spark.conf.get("spark.etl.target.jdbc.url")) \
                .option("dbtable", "(SELECT * FROM etl_target_data WHERE etl_run_id = '{}') as target".format(self.run_id)) \
                .option("user", self.spark.conf.get("spark.etl.target.jdbc.user")) \
                .option("password", self.spark.conf.get("spark.etl.target.jdbc.password")) \
                .option("driver", self.spark.conf.get("spark.etl.target.jdbc.driver")) \
                .load()
            
            loaded_count = loaded_df.count()
            target_count = target_df.count()
            
            if loaded_count == target_count:
                self.logger.info(f"Reconciliation passed: {loaded_count} records match")
                return True
            else:
                self.logger.warning(
                    f"Reconciliation mismatch: Loaded {loaded_count}, Found {target_count}"
                )
                return False
                
        except Exception as e:
            self.logger.error(f"Reconciliation error: {str(e)}")
            return False