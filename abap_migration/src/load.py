"""
ETL Loader Module
Handles data loading to target systems
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from typing import Dict
import logging


class ETLLoader:
    """Loader class for writing data to target systems"""
    
    def __init__(self, spark: SparkSession, target_type: str, run_id: str, 
                 batch_size: int, config: dict):
        """
        Initialize loader
        
        Args:
            spark: SparkSession instance
            target_type: Type of target (DATABASE, FILE, etc.)
            run_id: Unique run identifier
            batch_size: Number of records per batch
            config: Configuration dictionary
        """
        self.spark = spark
        self.target_type = target_type.upper()
        self.run_id = run_id
        self.batch_size = batch_size
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def load_data(self, df: DataFrame, mode: str = "INSERT") -> Dict[str, int]:
        """
        Load data to target
        
        Args:
            df: DataFrame to load
            mode: Load mode (INSERT, UPDATE, UPSERT)
            
        Returns:
            Dictionary with load results
        """
        self.logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        total_count = df.count()
        success_count = 0
        error_count = 0
        errors = []
        
        try:
            if self.target_type == "DATABASE":
                success = self._load_to_database(df, mode)
                if success:
                    success_count = total_count
                else:
                    error_count = total_count
                    errors.append("Database load failed")
            elif self.target_type == "FILE":
                success = self._load_to_file(df, mode)
                if success:
                    success_count = total_count
                else:
                    error_count = total_count
                    errors.append("File load failed")
            else:
                success = self._load_to_database(df, mode)
                if success:
                    success_count = total_count
                else:
                    error_count = total_count
            
            # Reconcile data if enabled
            if self.config.get("enable_reconciliation", True):
                reconciled = self._reconcile_data(df)
                if not reconciled:
                    self.logger.warning("Data reconciliation failed")
            
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
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to database
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            True if successful
        """
        target_table = self.config.get("target_table", "etl_target_data")
        jdbc_url = self.config.get("jdbc_url")
        
        try:
            if jdbc_url:
                # JDBC connection
                save_mode = self._get_spark_mode(mode)
                
                df.write.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", target_table) \
                    .option("user", self.config.get("db_user")) \
                    .option("password", self.config.get("db_password")) \
                    .option("driver", self.config.get("jdbc_driver")) \
                    .option("batchsize", self.batch_size) \
                    .mode(save_mode) \
                    .save()
            else:
                # Write to file (for testing)
                target_path = self.config.get("target_path", "data/target")
                save_mode = self._get_spark_mode(mode)
                
                df.write \
                    .mode(save_mode) \
                    .option("header", "true") \
                    .parquet(target_path)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Database load error: {str(e)}")
            return False
    
    def _load_to_file(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to file
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            True if successful
        """
        target_path = self.config.get("target_path", "data/target")
        save_mode = self._get_spark_mode(mode)
        
        try:
            df.write \
                .mode(save_mode) \
                .option("header", "true") \
                .partitionBy("category") \
                .parquet(target_path)
            
            return True
            
        except Exception as e:
            self.logger.error(f"File load error: {str(e)}")
            return False
    
    def _get_spark_mode(self, mode: str) -> str:
        """Convert load mode to Spark save mode"""
        mode_map = {
            "INSERT": "append",
            "UPDATE": "overwrite",
            "UPSERT": "append"  # Would need merge logic for true upsert
        }
        return mode_map.get(mode.upper(), "append")
    
    def _reconcile_data(self, df: DataFrame) -> bool:
        """
        Reconcile loaded data with source
        
        Args:
            df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passes
        """
        try:
            source_count = df.count()
            
            # Read back from target
            target_path = self.config.get("target_path", "data/target")
            target_df = self.spark.read.parquet(target_path)
            target_count = target_df.filter(col("etl_run_id") == self.run_id).count()
            
            if source_count == target_count:
                self.logger.info(f"Reconciliation passed: {source_count} records")
                return True
            else:
                self.logger.warning(
                    f"Reconciliation mismatch: Source={source_count}, Target={target_count}"
                )
                return False
                
        except Exception as e:
            self.logger.error(f"Reconciliation error: {str(e)}")
            return False