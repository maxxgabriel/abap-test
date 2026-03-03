"""
ETL Data Loading Module
Handles loading transformed data into target systems with batching and reconciliation.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count
from typing import Dict, List
from dataclasses import dataclass
from src.logger import ETLLogger


@dataclass
class LoadResult:
    """Result of a load operation."""
    success_count: int
    error_count: int
    total_count: int
    errors: List[str]


class ETLLoader:
    """
    Loads transformed data into target systems.
    """
    
    def __init__(self, spark: SparkSession, target_type: str = "DATABASE", 
                 batch_size: int = 1000, run_id: str = None):
        """
        Initialize the loader.
        
        Args:
            spark: SparkSession instance
            target_type: Type of target system
            batch_size: Number of records per batch
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.target_type = target_type
        self.batch_size = batch_size
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
    
    def load_data(self, df: DataFrame, mode: str = "INSERT") -> LoadResult:
        """
        Load data into target system.
        
        Args:
            df: Transformed DataFrame
            mode: Load mode (INSERT, UPDATE, UPSERT)
            
        Returns:
            LoadResult: Results of the load operation
        """
        self.logger.log_info(
            component="LOADER",
            message=f"Starting load - Mode: {mode}, Batch size: {self.batch_size}"
        )
        
        errors = []
        success_count = 0
        error_count = 0
        total_count = df.count()
        
        try:
            if self.target_type == "DATABASE":
                success = self._load_to_database(df, mode)
                if success:
                    success_count = total_count
                else:
                    error_count = total_count
                    errors.append("Database load failed")
            else:
                errors.append(f"Unsupported target type: {self.target_type}")
                error_count = total_count
            
            # Reconcile data
            if success_count > 0:
                reconciled = self._reconcile_data(df)
                if not reconciled:
                    self.logger.log_warning(
                        component="LOADER",
                        message="Data reconciliation failed"
                    )
            
            self.logger.log_info(
                component="LOADER",
                message=f"Load complete - Success: {success_count}, Errors: {error_count}"
            )
            
            return LoadResult(
                success_count=success_count,
                error_count=error_count,
                total_count=total_count,
                errors=errors
            )
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Load operation failed",
                details=str(e)
            )
            return LoadResult(
                success_count=0,
                error_count=total_count,
                total_count=total_count,
                errors=[str(e)]
            )
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to database.
        
        Args:
            df: DataFrame to load
            mode: Load mode (INSERT, UPDATE, UPSERT)
            
        Returns:
            bool: Success status
        """
        try:
            jdbc_url = self.spark.conf.get("spark.etl.target.jdbc.url")
            jdbc_properties = {
                "user": self.spark.conf.get("spark.etl.target.jdbc.user"),
                "password": self.spark.conf.get("spark.etl.target.jdbc.password"),
                "driver": self.spark.conf.get("spark.etl.target.jdbc.driver")
            }
            
            # Map mode to Spark write mode
            write_mode_map = {
                "INSERT": "append",
                "UPDATE": "overwrite",
                "UPSERT": "append"  # Will handle upsert with merge logic
            }
            
            write_mode = write_mode_map.get(mode, "append")
            
            # Write to database
            df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "etl_target_data") \
                .option("user", jdbc_properties["user"]) \
                .option("password", jdbc_properties["password"]) \
                .option("driver", jdbc_properties["driver"]) \
                .option("batchsize", self.batch_size) \
                .mode(write_mode) \
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
        Reconcile loaded data with target.
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            bool: True if reconciliation matches
        """
        try:
            jdbc_url = self.spark.conf.get("spark.etl.target.jdbc.url")
            jdbc_properties = {
                "user": self.spark.conf.get("spark.etl.target.jdbc.user"),
                "password": self.spark.conf.get("spark.etl.target.jdbc.password"),
                "driver": self.spark.conf.get("spark.etl.target.jdbc.driver")
            }
            
            # Read loaded data from target
            target_df = self.spark.read.jdbc(
                url=jdbc_url,
                table=f"(SELECT * FROM etl_target_data WHERE etl_run_id = '{self.run_id}') as target",
                properties=jdbc_properties
            )
            
            source_count = loaded_df.count()
            target_count = target_df.count()
            
            if source_count != target_count:
                self.logger.log_warning(
                    component="LOADER",
                    message=f"Reconciliation mismatch: Source={source_count}, Target={target_count}"
                )
                return False
            
            self.logger.log_info(
                component="LOADER",
                message=f"Reconciliation successful: {target_count} records verified"
            )
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Reconciliation failed",
                details=str(e)
            )
            return False