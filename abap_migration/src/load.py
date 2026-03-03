"""
PySpark Data Loader with Delta Lake
Converts ABAP loader logic to PySpark DataFrame operations
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta import DeltaTable
from typing import Dict, List
from dataclasses import dataclass
from src.logger import ETLLogger


@dataclass
class LoadResult:
    """Results of load operation"""
    success_count: int
    error_count: int
    total_count: int
    errors: List[str]


class DataLoader:
    """Loads data to target using PySpark and Delta Lake"""
    
    def __init__(self, spark: SparkSession, target_type: str = "DATABASE", 
                 batch_size: int = 1000, run_id: str = None):
        """
        Initialize loader
        
        Args:
            spark: Active SparkSession
            target_type: Type of target (DATABASE, DELTA, etc.)
            batch_size: Batch size for processing
            run_id: Unique run identifier
        """
        self.spark = spark
        self.target_type = target_type.upper()
        self.batch_size = batch_size
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
    
    def load_data(self, df: DataFrame, mode: str = "upsert") -> LoadResult:
        """
        Main load method with batch processing
        
        Args:
            df: DataFrame to load
            mode: Load mode (insert, update, upsert, overwrite)
            
        Returns:
            LoadResult with statistics
        """
        self.logger.log_info(
            component="LOADER",
            message=f"Starting load - Mode: {mode}, Batch size: {self.batch_size}"
        )
        
        total_count = df.count()
        errors = []
        success_count = 0
        error_count = 0
        
        try:
            # Execute load based on mode
            if mode.lower() == "insert":
                success = self._insert_new(df)
            elif mode.lower() == "update":
                success = self._update_existing(df)
            elif mode.lower() == "upsert":
                success = self._upsert_data(df)
            elif mode.lower() == "overwrite":
                success = self._overwrite_data(df)
            else:
                success = self._insert_new(df)
            
            if success:
                success_count = total_count
                
                # Perform reconciliation if enabled
                if self._is_reconciliation_enabled():
                    reconciled = self.reconcile_data(df)
                    if not reconciled:
                        self.logger.log_warning(
                            component="LOADER",
                            message="Data reconciliation failed"
                        )
            else:
                error_count = total_count
                errors.append("Load operation failed")
                
        except Exception as e:
            error_count = total_count
            errors.append(str(e))
            self.logger.log_error(
                component="LOADER",
                message="Load failed",
                details=str(e)
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
    
    def _insert_new(self, df: DataFrame) -> bool:
        """
        Insert new records
        Replaces: INSERT zetl_target_data FROM TABLE lt_target_tab
        
        Args:
            df: DataFrame to insert
            
        Returns:
            True if successful
        """
        try:
            target_table = self.spark.conf.get("spark.etl.target_table", "etl_target_data")
            
            df.write \
                .format("delta") \
                .mode("append") \
                .save(target_table)
            
            self.logger.log_info(
                component="LOADER",
                message=f"Inserted {df.count()} records"
            )
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Insert failed",
                details=str(e)
            )
            return False
    
    def _update_existing(self, df: DataFrame) -> bool:
        """
        Update existing records using Delta Lake merge
        
        Args:
            df: DataFrame with updates
            
        Returns:
            True if successful
        """
        try:
            target_table = self.spark.conf.get("spark.etl.target_table", "etl_target_data")
            delta_table = DeltaTable.forPath(self.spark, target_table)
            
            # Perform update using merge
            delta_table.alias("target") \
                .merge(
                    df.alias("source"),
                    "target.id = source.id"
                ) \
                .whenMatchedUpdateAll() \
                .execute()
            
            self.logger.log_info(
                component="LOADER",
                message="Update completed successfully"
            )
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Update failed",
                details=str(e)
            )
            return False
    
    def _upsert_data(self, df: DataFrame) -> bool:
        """
        Upsert (merge) data - update if exists, insert if not
        Uses Delta Lake merge operation
        
        Args:
            df: DataFrame to upsert
            
        Returns:
            True if successful
        """
        try:
            target_table = self.spark.conf.get("spark.etl.target_table", "etl_target_data")
            
            # Check if table exists
            try:
                delta_table = DeltaTable.forPath(self.spark, target_table)
                table_exists = True
            except:
                table_exists = False
            
            if table_exists:
                # Perform merge (upsert)
                delta_table.alias("target") \
                    .merge(
                        df.alias("source"),
                        "target.id = source.id"
                    ) \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()
                
                self.logger.log_info(
                    component="LOADER",
                    message="Upsert completed successfully"
                )
            else:
                # Create table with initial data
                df.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .save(target_table)
                
                self.logger.log_info(
                    component="LOADER",
                    message=f"Created new table and inserted {df.count()} records"
                )
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Upsert failed",
                details=str(e)
            )
            return False
    
    def _overwrite_data(self, df: DataFrame) -> bool:
        """
        Overwrite entire table
        
        Args:
            df: DataFrame to write
            
        Returns:
            True if successful
        """
        try:
            target_table = self.spark.conf.get("spark.etl.target_table", "etl_target_data")
            
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(target_table)
            
            self.logger.log_info(
                component="LOADER",
                message=f"Overwritten table with {df.count()} records"
            )
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Overwrite failed",
                details=str(e)
            )
            return False
    
    def reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data against source
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passes
        """
        try:
            target_table = self.spark.conf.get("spark.etl.target_table", "etl_target_data")
            
            # Read back from target
            target_df = self.spark.read \
                .format("delta") \
                .load(target_table) \
                .filter(F.col("etl_run_id") == self.run_id)
            
            source_count = loaded_df.count()
            target_count = target_df.count()
            
            if source_count != target_count:
                self.logger.log_warning(
                    component="LOADER",
                    message=f"Reconciliation mismatch: Source={source_count}, Target={target_count}"
                )
                return False
            
            # Check key integrity
            source_ids = loaded_df.select("id").distinct()
            target_ids = target_df.select("id").distinct()
            
            missing_ids = source_ids.subtract(target_ids).count()
            if missing_ids > 0:
                self.logger.log_warning(
                    component="LOADER",
                    message=f"Reconciliation failed: {missing_ids} IDs missing in target"
                )
                return False
            
            self.logger.log_info(
                component="LOADER",
                message="Data reconciliation successful"
            )
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Reconciliation failed",
                details=str(e)
            )
            return False
    
    def _is_reconciliation_enabled(self) -> bool:
        """Check if reconciliation is enabled in config"""
        try:
            config_table = self.spark.conf.get("spark.etl.config_table", "etl_config")
            
            config_df = self.spark.read \
                .format("delta") \
                .load(config_table) \
                .filter(F.col("config_key") == "ENABLE_RECONCILIATION")
            
            if config_df.count() > 0:
                value = config_df.first()["config_value"]
                return value.upper() in ["X", "TRUE", "1", "YES"]
            
            return True  # Default to enabled
            
        except:
            return True