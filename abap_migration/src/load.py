"""
Data loading module for PySpark ETL pipeline.
Handles batch loading with reconciliation support.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from typing import Dict, List
import logging


class ETLLoader:
    """Handles data loading to target systems."""
    
    def __init__(
        self, 
        spark: SparkSession, 
        target_type: str, 
        batch_size: int,
        run_id: str
    ):
        """
        Initialize the loader.
        
        Args:
            spark: SparkSession instance
            target_type: Type of target (DATABASE, PARQUET, etc.)
            batch_size: Number of records per batch
            run_id: Unique run identifier
        """
        self.spark = spark
        self.target_type = target_type
        self.batch_size = batch_size
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def load_data(self, df: DataFrame, mode: str = "upsert") -> Dict[str, int]:
        """
        Load data to target.
        
        Args:
            df: DataFrame to load
            mode: Load mode (insert, update, upsert)
            
        Returns:
            Dictionary with load statistics
        """
        self.logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        try:
            total_count = df.count()
            success_count = 0
            error_count = 0
            errors = []
            
            # Process in batches
            num_batches = (total_count + self.batch_size - 1) // self.batch_size
            
            for batch_num in range(num_batches):
                start_idx = batch_num * self.batch_size
                batch_df = df.limit(self.batch_size).offset(start_idx)
                
                if self._commit_batch(batch_df, mode):
                    batch_count = batch_df.count()
                    success_count += batch_count
                    self.logger.info(f"Batch {batch_num + 1}/{num_batches} loaded successfully ({batch_count} records)")
                else:
                    batch_count = batch_df.count()
                    error_count += batch_count
                    errors.append(f"Batch {batch_num + 1} failed")
                    self.logger.error(f"Batch {batch_num + 1}/{num_batches} failed")
            
            # Reconcile data
            if success_count > 0:
                reconciled = self._reconcile_data(df)
                if not reconciled:
                    self.logger.warning("Data reconciliation failed")
            
            self.logger.info(f"Load complete - Success: {success_count}, Errors: {error_count}")
            
            return {
                "success_count": success_count,
                "error_count": error_count,
                "total_count": total_count,
                "errors": errors
            }
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            raise
    
    def _commit_batch(self, batch_df: DataFrame, mode: str) -> bool:
        """
        Commit a batch of data.
        
        Args:
            batch_df: Batch DataFrame
            mode: Load mode
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if mode == "insert":
                return self._insert_new(batch_df)
            elif mode == "update":
                return self._update_existing(batch_df)
            elif mode == "upsert":
                # Try update first, then insert
                return self._upsert_data(batch_df)
            else:
                return self._insert_new(batch_df)
        except Exception as e:
            self.logger.error(f"Batch commit failed: {str(e)}")
            return False
    
    def _insert_new(self, df: DataFrame) -> bool:
        """Insert new records."""
        try:
            df.write \
                .format("jdbc") \
                .option("url", "${db.url}") \
                .option("dbtable", "${db.target_table}") \
                .option("user", "${db.user}") \
                .option("password", "${db.password}") \
                .mode("append") \
                .save()
            return True
        except Exception as e:
            self.logger.error(f"Insert failed: {str(e)}")
            return False
    
    def _update_existing(self, df: DataFrame) -> bool:
        """Update existing records."""
        try:
            # Create temp view for merge operation
            df.createOrReplaceTempView("temp_updates")
            
            # Execute merge/update logic
            self.spark.sql(f"""
                MERGE INTO {self.spark.conf.get('db.target_table')} AS target
                USING temp_updates AS source
                ON target.id = source.id
                WHEN MATCHED THEN UPDATE SET *
            """)
            return True
        except Exception as e:
            self.logger.error(f"Update failed: {str(e)}")
            return False
    
    def _upsert_data(self, df: DataFrame) -> bool:
        """Upsert (insert or update) records."""
        try:
            # Create temp view for merge operation
            df.createOrReplaceTempView("temp_upserts")
            
            # Execute merge operation
            self.spark.sql(f"""
                MERGE INTO {self.spark.conf.get('db.target_table')} AS target
                USING temp_upserts AS source
                ON target.id = source.id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
            return True
        except Exception as e:
            self.logger.error(f"Upsert failed: {str(e)}")
            return False
    
    def _reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data with source.
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passes
        """
        self.logger.info("Reconciling loaded data")
        
        try:
            # Read back from target
            target_df = self.spark.read \
                .format("jdbc") \
                .option("url", "${db.url}") \
                .option("dbtable", "${db.target_table}") \
                .option("user", "${db.user}") \
                .option("password", "${db.password}") \
                .load() \
                .filter(f"etl_run_id = '{self.run_id}'")
            
            # Compare counts
            loaded_count = loaded_df.count()
            target_count = target_df.count()
            
            if loaded_count != target_count:
                self.logger.error(
                    f"Reconciliation failed: Loaded {loaded_count}, Found {target_count}"
                )
                return False
            
            # Compare sample records
            loaded_sample = loaded_df.select("id", "transformed_value").collect()
            target_sample = target_df.select("id", "transformed_value").collect()
            
            loaded_dict = {row["id"]: row["transformed_value"] for row in loaded_sample}
            target_dict = {row["id"]: row["transformed_value"] for row in target_sample}
            
            mismatches = 0
            for key in loaded_dict:
                if key in target_dict:
                    if loaded_dict[key] != target_dict[key]:
                        mismatches += 1
                else:
                    mismatches += 1
            
            if mismatches > 0:
                self.logger.error(f"Reconciliation failed: {mismatches} mismatches found")
                return False
            
            self.logger.info("Reconciliation passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Reconciliation error: {str(e)}")
            return False