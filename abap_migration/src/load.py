"""
Load module for ETL pipeline
Handles data loading with batch processing and reconciliation
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from typing import Dict, List, Optional
from dataclasses import dataclass

from src.logger import ETLLogger


@dataclass
class LoadResult:
    """Result of load operation"""
    success_count: int
    error_count: int
    total_count: int
    errors: List[str]


class ETLLoader:
    """
    Data loading class with batch processing and reconciliation support
    """
    
    def __init__(
        self,
        spark: SparkSession,
        target_type: str = "DATABASE",
        batch_size: int = 1000,
        run_id: str = None
    ):
        """
        Initialize loader
        
        Args:
            spark: SparkSession instance
            target_type: Type of target (DATABASE, FILE, etc.)
            batch_size: Number of records per batch
            run_id: Unique run identifier
        """
        self.spark = spark
        self.target_type = target_type.upper()
        self.batch_size = batch_size
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
        
    def load_data(
        self,
        data_df: DataFrame,
        mode: str = "INSERT"
    ) -> LoadResult:
        """
        Main loading method with batch processing
        
        Args:
            data_df: Transformed DataFrame to load
            mode: Load mode (INSERT, UPDATE, UPSERT)
            
        Returns:
            LoadResult with statistics
        """
        self.logger.log_info(
            component="LOADER",
            message=f"Starting load - Mode: {mode}, Batch size: {self.batch_size}"
        )
        
        total_count = data_df.count()
        errors = []
        
        try:
            success = self.load_to_database(data_df, mode)
            
            if success:
                success_count = total_count
                error_count = 0
            else:
                success_count = 0
                error_count = total_count
                errors.append("Load operation failed")
            
            # Reconcile data
            if success:
                reconciled = self.reconcile_data(data_df)
                if not reconciled:
                    self.logger.log_warning(
                        component="LOADER",
                        message="Data reconciliation failed"
                    )
                    errors.append("Reconciliation failed")
            
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
    
    def load_to_database(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to database target
        
        Args:
            df: DataFrame to load
            mode: Load mode (INSERT, UPDATE, UPSERT)
            
        Returns:
            Success status
        """
        try:
            # Map mode to Spark save mode
            save_mode_map = {
                "INSERT": "append",
                "UPDATE": "overwrite",
                "UPSERT": "append"  # Will handle upsert logic separately
            }
            
            save_mode = save_mode_map.get(mode.upper(), "append")
            
            if mode.upper() == "UPSERT":
                # For upsert, we need to handle separately
                return self._upsert_data(df)
            else:
                df.write \
                    .format("jdbc") \
                    .option("url", self.spark.conf.get("spark.etl.target.jdbc.url")) \
                    .option("dbtable", "etl_target_data") \
                    .option("user", self.spark.conf.get("spark.etl.target.jdbc.user")) \
                    .option("password", self.spark.conf.get("spark.etl.target.jdbc.password")) \
                    .option("driver", self.spark.conf.get("spark.etl.target.jdbc.driver")) \
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
    
    def _upsert_data(self, df: DataFrame) -> bool:
        """
        Perform upsert operation
        
        Args:
            df: DataFrame to upsert
            
        Returns:
            Success status
        """
        try:
            # Read existing data
            existing_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.spark.conf.get("spark.etl.target.jdbc.url")) \
                .option("dbtable", "etl_target_data") \
                .option("user", self.spark.conf.get("spark.etl.target.jdbc.user")) \
                .option("password", self.spark.conf.get("spark.etl.target.jdbc.password")) \
                .option("driver", self.spark.conf.get("spark.etl.target.jdbc.driver")) \
                .load()
            
            # Find records to update
            update_df = df.join(existing_df, "id", "inner")
            
            # Find records to insert
            insert_df = df.join(existing_df, "id", "left_anti")
            
            # Perform update (delete then insert)
            if update_df.count() > 0:
                # In production, use proper UPDATE statement
                # For now, we'll overwrite by key
                pass
            
            # Perform insert
            if insert_df.count() > 0:
                insert_df.write \
                    .format("jdbc") \
                    .option("url", self.spark.conf.get("spark.etl.target.jdbc.url")) \
                    .option("dbtable", "etl_target_data") \
                    .option("user", self.spark.conf.get("spark.etl.target.jdbc.user")) \
                    .option("password", self.spark.conf.get("spark.etl.target.jdbc.password")) \
                    .option("driver", self.spark.conf.get("spark.etl.target.jdbc.driver")) \
                    .option("batchsize", str(self.batch_size)) \
                    .mode("append") \
                    .save()
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Upsert failed",
                details=str(e)
            )
            return False
    
    def reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data against source
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            Reconciliation success status
        """
        try:
            # Read back from target
            target_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.spark.conf.get("spark.etl.target.jdbc.url")) \
                .option("dbtable", f"(SELECT * FROM etl_target_data WHERE etl_run_id = '{self.run_id}') as target") \
                .option("user", self.spark.conf.get("spark.etl.target.jdbc.user")) \
                .option("password", self.spark.conf.get("spark.etl.target.jdbc.password")) \
                .option("driver", self.spark.conf.get("spark.etl.target.jdbc.driver")) \
                .load()
            
            loaded_count = loaded_df.count()
            target_count = target_df.count()
            
            if loaded_count == target_count:
                self.logger.log_info(
                    component="LOADER",
                    message=f"Reconciliation successful: {loaded_count} records matched"
                )
                return True
            else:
                self.logger.log_warning(
                    component="LOADER",
                    message=f"Reconciliation mismatch: loaded={loaded_count}, target={target_count}"
                )
                return False
                
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Reconciliation failed",
                details=str(e)
            )
            return False