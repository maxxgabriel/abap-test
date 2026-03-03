"""
PySpark ETL Load Module
Handles data loading to target systems with batch processing and reconciliation
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from typing import Dict, Any
from dataclasses import dataclass
from src.logger import ETLLogger
from src.config import ETLConfig


@dataclass
class LoadResult:
    """Result of data loading operation"""
    success_count: int
    error_count: int
    total_count: int
    errors: list


class ETLLoader:
    """Handles data loading phase of ETL pipeline"""
    
    def __init__(self, spark: SparkSession, config: ETLConfig, run_id: str):
        """
        Initialize loader
        
        Args:
            spark: Active SparkSession
            config: ETL configuration object
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.batch_size = config.get("batch_size", 1000)
        self.logger = ETLLogger.get_instance()
    
    def load_data(
        self,
        df: DataFrame,
        mode: str = "upsert"
    ) -> LoadResult:
        """
        Load data to target system
        
        Args:
            df: DataFrame to load
            mode: Load mode (insert, update, upsert, overwrite)
            
        Returns:
            LoadResult with success/error counts
        """
        self.logger.log_info(
            component="LOADER",
            message=f"Starting load - Mode: {mode}, Batch size: {self.batch_size}"
        )
        
        try:
            total_count = df.count()
            target_config = self.config.get_target_config()
            
            # Load based on mode
            if mode.lower() == "insert":
                success = self._insert_data(df, target_config)
            elif mode.lower() == "update":
                success = self._update_data(df, target_config)
            elif mode.lower() == "upsert":
                success = self._upsert_data(df, target_config)
            elif mode.lower() == "overwrite":
                success = self._overwrite_data(df, target_config)
            else:
                self.logger.log_warning(
                    component="LOADER",
                    message=f"Unknown mode {mode}, defaulting to upsert"
                )
                success = self._upsert_data(df, target_config)
            
            if success:
                success_count = total_count
                error_count = 0
                errors = []
            else:
                success_count = 0
                error_count = total_count
                errors = ["Batch load failed"]
            
            # Reconcile data if enabled
            if self.config.get("enable_reconciliation", True):
                reconciled = self._reconcile_data(df)
                if not reconciled:
                    self.logger.log_warning(
                        component="LOADER",
                        message="Data reconciliation failed"
                    )
            
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
                error_count=df.count() if df else 0,
                total_count=df.count() if df else 0,
                errors=[str(e)]
            )
    
    def _insert_data(self, df: DataFrame, target_config: Dict[str, Any]) -> bool:
        """Insert new records"""
        try:
            df.write \
                .format(target_config.get("format", "jdbc")) \
                .mode("append") \
                .options(**target_config.get("options", {})) \
                .save()
            return True
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Insert failed",
                details=str(e)
            )
            return False
    
    def _update_data(self, df: DataFrame, target_config: Dict[str, Any]) -> bool:
        """Update existing records"""
        try:
            # For update, we need to use merge/upsert logic
            # This is a simplified version - real implementation depends on target system
            self.logger.log_info(
                component="LOADER",
                message="Update mode - using upsert logic"
            )
            return self._upsert_data(df, target_config)
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Update failed",
                details=str(e)
            )
            return False
    
    def _upsert_data(self, df: DataFrame, target_config: Dict[str, Any]) -> bool:
        """Insert new records and update existing ones"""
        try:
            # Upsert implementation depends on target system
            # For JDBC with Postgres/MySQL, we can use special modes
            # For Delta Lake, we'd use merge
            
            target_format = target_config.get("format", "jdbc")
            
            if target_format == "delta":
                # Delta Lake merge
                from delta.tables import DeltaTable
                target_path = target_config["path"]
                
                if DeltaTable.isDeltaTable(self.spark, target_path):
                    delta_table = DeltaTable.forPath(self.spark, target_path)
                    delta_table.alias("target").merge(
                        df.alias("source"),
                        "target.id = source.id"
                    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                else:
                    df.write.format("delta").mode("overwrite").save(target_path)
            else:
                # For JDBC, use overwrite or append based on config
                df.write \
                    .format(target_format) \
                    .mode("append") \
                    .options(**target_config.get("options", {})) \
                    .save()
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Upsert failed",
                details=str(e)
            )
            return False
    
    def _overwrite_data(self, df: DataFrame, target_config: Dict[str, Any]) -> bool:
        """Overwrite all data in target"""
        try:
            df.write \
                .format(target_config.get("format", "jdbc")) \
                .mode("overwrite") \
                .options(**target_config.get("options", {})) \
                .save()
            return True
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Overwrite failed",
                details=str(e)
            )
            return False
    
    def _reconcile_data(self, df: DataFrame) -> bool:
        """
        Reconcile loaded data with source
        
        Args:
            df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passes, False otherwise
        """
        try:
            target_config = self.config.get_target_config()
            
            # Read back from target
            loaded_df = self.spark.read \
                .format(target_config.get("format", "jdbc")) \
                .options(**target_config.get("options", {})) \
                .load() \
                .filter(col("etl_run_id") == self.run_id)
            
            source_count = df.count()
            target_count = loaded_df.count()
            
            if source_count == target_count:
                self.logger.log_info(
                    component="LOADER",
                    message=f"Reconciliation passed: {source_count} records match"
                )
                return True
            else:
                self.logger.log_warning(
                    component="LOADER",
                    message=f"Reconciliation mismatch: Source={source_count}, Target={target_count}"
                )
                return False
                
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Reconciliation failed",
                details=str(e)
            )
            return False