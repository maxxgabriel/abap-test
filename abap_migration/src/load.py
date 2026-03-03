"""
Data loading module for ETL pipeline.
Handles writing data to target systems and reconciliation.
"""
from pyspark.sql import DataFrame
from typing import Dict, Any
import logging

from src.logger import ETLLogger


class ETLLoader:
    """
    Loader class responsible for writing data to target systems.
    Supports batch loading and data reconciliation.
    """
    
    def __init__(self, target_type: str, batch_size: int, run_id: str, config: dict):
        """
        Initialize the loader.
        
        Args:
            target_type: Type of target (DATABASE, FILE, etc.)
            batch_size: Number of records per batch
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.target_type = target_type.upper()
        self.batch_size = batch_size
        self.run_id = run_id
        self.config = config
        self.logger = ETLLogger.get_instance()
    
    def load_data(self, df: DataFrame, mode: str = "append") -> Dict[str, Any]:
        """
        Main loading method that writes data to target.
        
        Args:
            df: DataFrame to load
            mode: Write mode (append, overwrite, upsert)
            
        Returns:
            Dictionary containing load results
        """
        self.logger.log_info(
            component="LOADER",
            message=f"Starting load - Mode: {mode}, Batch size: {self.batch_size}"
        )
        
        total_count = df.count()
        success_count = 0
        error_count = 0
        errors = []
        
        try:
            if self.target_type == "DATABASE":
                success = self._load_to_database(df, mode)
            elif self.target_type == "FILE":
                success = self._load_to_file(df, mode)
            else:
                self.logger.log_warning(
                    component="LOADER",
                    message=f"Unknown target type {self.target_type}, defaulting to DATABASE"
                )
                success = self._load_to_database(df, mode)
            
            if success:
                success_count = total_count
                
                # Perform reconciliation if enabled
                if self.config.get("enable_reconciliation", True):
                    reconciled = self.reconcile_data(df)
                    if not reconciled:
                        self.logger.log_warning(
                            component="LOADER",
                            message="Data reconciliation failed"
                        )
            else:
                error_count = total_count
                errors.append("Load operation failed")
            
            self.logger.log_info(
                component="LOADER",
                message=f"Load complete - Success: {success_count}, Errors: {error_count}"
            )
            
            return {
                "success_count": success_count,
                "error_count": error_count,
                "total_count": total_count,
                "errors": errors
            }
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Load operation failed",
                details=str(e)
            )
            return {
                "success_count": 0,
                "error_count": total_count,
                "total_count": total_count,
                "errors": [str(e)]
            }
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to database target.
        
        Args:
            df: DataFrame to load
            mode: Write mode
            
        Returns:
            True if successful, False otherwise
        """
        try:
            target_table = self.config.get("target_table", "etl_target_data")
            
            # Map mode to Spark mode
            spark_mode = self._map_mode(mode)
            
            df.write \
                .format("jdbc") \
                .option("url", self.config.get("jdbc_url")) \
                .option("dbtable", target_table) \
                .option("user", self.config.get("db_user")) \
                .option("password", self.config.get("db_password")) \
                .option("driver", self.config.get("jdbc_driver", "org.postgresql.Driver")) \
                .option("batchsize", self.batch_size) \
                .mode(spark_mode) \
                .save()
            
            self.logger.log_info(
                component="LOADER",
                message=f"Successfully loaded data to {target_table}"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Database load failed",
                details=str(e)
            )
            return False
    
    def _load_to_file(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to file target.
        
        Args:
            df: DataFrame to load
            mode: Write mode
            
        Returns:
            True if successful, False otherwise
        """
        try:
            target_path = self.config.get("target_path", "/data/output")
            target_format = self.config.get("target_format", "parquet")
            
            # Map mode to Spark mode
            spark_mode = self._map_mode(mode)
            
            df.write \
                .format(target_format) \
                .mode(spark_mode) \
                .partitionBy("category") \
                .save(f"{target_path}/run_id={self.run_id}")
            
            self.logger.log_info(
                component="LOADER",
                message=f"Successfully loaded data to {target_path}"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="File load failed",
                details=str(e)
            )
            return False
    
    def _map_mode(self, mode: str) -> str:
        """
        Map custom mode to Spark write mode.
        
        Args:
            mode: Custom mode string
            
        Returns:
            Spark write mode
        """
        mode_map = {
            "insert": "append",
            "append": "append",
            "update": "overwrite",
            "overwrite": "overwrite",
            "upsert": "append"  # Upsert handled at database level
        }
        return mode_map.get(mode.lower(), "append")
    
    def reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data with source.
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passes, False otherwise
        """
        try:
            self.logger.log_info(
                component="LOADER",
                message="Starting data reconciliation"
            )
            
            source_count = loaded_df.count()
            
            # Read back from target and compare counts
            target_table = self.config.get("target_table", "etl_target_data")
            
            target_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.config.get("jdbc_url")) \
                .option("dbtable", f"(SELECT * FROM {target_table} WHERE etl_run_id = '{self.run_id}') as target") \
                .option("user", self.config.get("db_user")) \
                .option("password", self.config.get("db_password")) \
                .load()
            
            target_count = target_df.count()
            
            matches = source_count == target_count
            
            if matches:
                self.logger.log_info(
                    component="LOADER",
                    message=f"Reconciliation passed - {source_count} records matched"
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