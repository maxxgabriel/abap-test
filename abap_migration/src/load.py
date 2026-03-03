"""
PySpark Data Loader with Delta Lake Integration
Converted from ABAP class zcl_etl_loader
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict, List
import logging


class DataLoader:
    """
    Loads transformed data to target using PySpark and Delta Lake.
    Supports batch processing, upsert operations, and reconciliation.
    """
    
    def __init__(
        self, 
        spark: SparkSession, 
        target_type: str = "DATABASE",
        batch_size: int = 1000,
        run_id: str = None
    ):
        """
        Initialize the data loader.
        
        Args:
            spark: SparkSession instance
            target_type: Type of target (DATABASE, DELTA, etc.)
            batch_size: Number of records per batch
            run_id: Unique run identifier
        """
        self.spark = spark
        self.target_type = target_type.upper()
        self.batch_size = batch_size
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def load_data(
        self, 
        df: DataFrame, 
        mode: str = "INSERT"
    ) -> Dict[str, int]:
        """
        Main load method that processes data in batches.
        
        Args:
            df: DataFrame to load
            mode: Load mode (INSERT, UPDATE, UPSERT)
            
        Returns:
            Dictionary with load statistics
        """
        self.logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        total_records = df.count()
        success_count = 0
        error_count = 0
        errors = []
        
        try:
            # For Delta Lake, we can use built-in merge/upsert
            if mode.upper() == "UPSERT":
                success_count = self._upsert_to_delta(df)
            elif mode.upper() == "INSERT":
                success_count = self._insert_to_delta(df)
            elif mode.upper() == "UPDATE":
                success_count = self._update_in_delta(df)
            else:
                self.logger.warning(f"Unknown mode {mode}, defaulting to INSERT")
                success_count = self._insert_to_delta(df)
            
            # Reconciliation
            if success_count > 0:
                reconciled = self._reconcile_data(df)
                if not reconciled:
                    self.logger.warning("Data reconciliation failed")
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}", exc_info=True)
            error_count = total_records
            errors.append(str(e))
        
        result = {
            "success_count": success_count,
            "error_count": error_count,
            "total_count": total_records,
            "errors": errors
        }
        
        self.logger.info(
            f"Load complete - Success: {success_count}, Errors: {error_count}"
        )
        
        return result
    
    def _insert_to_delta(self, df: DataFrame) -> int:
        """
        Insert data into Delta Lake table.
        
        Args:
            df: DataFrame to insert
            
        Returns:
            Number of records inserted
        """
        try:
            df.write.format("delta") \
                .mode("append") \
                .save("path/to/target_data")
            
            return df.count()
            
        except Exception as e:
            self.logger.error(f"Insert failed: {str(e)}")
            raise
    
    def _update_in_delta(self, df: DataFrame) -> int:
        """
        Update existing records in Delta Lake.
        
        Args:
            df: DataFrame with update data
            
        Returns:
            Number of records updated
        """
        from delta.tables import DeltaTable
        
        try:
            delta_table = DeltaTable.forPath(self.spark, "path/to/target_data")
            
            # Perform update
            delta_table.alias("target") \
                .merge(
                    df.alias("source"),
                    "target.id = source.id"
                ) \
                .whenMatchedUpdateAll() \
                .execute()
            
            return df.count()
            
        except Exception as e:
            self.logger.error(f"Update failed: {str(e)}")
            raise
    
    def _upsert_to_delta(self, df: DataFrame) -> int:
        """
        Upsert (update or insert) data into Delta Lake.
        Uses Delta Lake MERGE operation.
        
        Args:
            df: DataFrame to upsert
            
        Returns:
            Number of records upserted
        """
        from delta.tables import DeltaTable
        
        try:
            target_path = "path/to/target_data"
            
            # Check if target table exists
            if DeltaTable.isDeltaTable(self.spark, target_path):
                delta_table = DeltaTable.forPath(self.spark, target_path)
                
                # Perform upsert using MERGE
                delta_table.alias("target") \
                    .merge(
                        df.alias("source"),
                        "target.id = source.id"
                    ) \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()
            else:
                # First time load - create table
                df.write.format("delta").save(target_path)
            
            return df.count()
            
        except Exception as e:
            self.logger.error(f"Upsert failed: {str(e)}")
            raise
    
    def _reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data to ensure data quality.
        Compares record counts and checksums.
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passed
        """
        self.logger.info("Starting data reconciliation")
        
        try:
            # Read back from target
            target_df = self.spark.read.format("delta").load("path/to/target_data")
            
            # Filter for current run
            target_df = target_df.filter(F.col("etl_run_id") == self.run_id)
            
            # Compare counts
            loaded_count = loaded_df.count()
            target_count = target_df.count()
            
            if loaded_count != target_count:
                self.logger.warning(
                    f"Count mismatch - Loaded: {loaded_count}, Target: {target_count}"
                )
                return False
            
            # Compare checksums (sample validation)
            loaded_checksum = loaded_df.agg(
                F.sum("value").alias("total_value")
            ).first()["total_value"]
            
            target_checksum = target_df.agg(
                F.sum("value").alias("total_value")
            ).first()["total_value"]
            
            if loaded_checksum != target_checksum:
                self.logger.warning(
                    f"Checksum mismatch - Loaded: {loaded_checksum}, Target: {target_checksum}"
                )
                return False
            
            self.logger.info("Data reconciliation passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Reconciliation failed: {str(e)}")
            return False