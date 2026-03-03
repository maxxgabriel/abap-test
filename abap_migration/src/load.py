"""
ETL Data Loading Module
Loads transformed data to target destinations with reconciliation support.
"""

from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any, Tuple
import logging


class ETLLoader:
    """Handles data loading to target systems."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the loader.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        self.batch_size = config["loading"]["batch_size"]
        
    def load_data(
        self,
        df: DataFrame,
        target_type: str = "database",
        mode: str = "insert"
    ) -> Dict[str, int]:
        """
        Load data to target system.
        
        Args:
            df: DataFrame to load
            target_type: Target destination type
            mode: Load mode (insert, update, upsert)
            
        Returns:
            Dictionary with load statistics
        """
        self.logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        try:
            total_records = df.count()
            
            if target_type.lower() == "database":
                success = self._load_to_database(df, mode)
            else:
                success = self._load_to_database(df, mode)
            
            if success:
                # Perform reconciliation if enabled
                if self.config["loading"]["enable_reconciliation"]:
                    reconciled = self._reconcile_data(df)
                    if not reconciled:
                        self.logger.warning("Data reconciliation failed")
                
                result = {
                    "success_count": total_records,
                    "error_count": 0,
                    "total_count": total_records
                }
            else:
                result = {
                    "success_count": 0,
                    "error_count": total_records,
                    "total_count": total_records
                }
            
            self.logger.info(
                f"Load complete - Success: {result['success_count']}, "
                f"Errors: {result['error_count']}"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            raise
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """Load data to database target."""
        target_path = self.config["target"]["database"]["path"]
        target_format = self.config["target"]["database"]["format"]
        
        self.logger.info(f"Loading to database: {target_path}")
        
        try:
            # Map mode to Spark write mode
            spark_mode = {
                "insert": "append",
                "update": "overwrite",
                "upsert": "append"  # Will handle upsert logic separately
            }.get(mode.lower(), "append")
            
            if mode.lower() == "upsert":
                # For upsert, use merge logic if supported
                self._upsert_data(df, target_path, target_format)
            else:
                # Write data
                df.write \
                    .format(target_format) \
                    .mode(spark_mode) \
                    .save(target_path)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Database load error: {str(e)}")
            return False
    
    def _upsert_data(self, df: DataFrame, target_path: str, target_format: str):
        """Perform upsert operation (update existing, insert new)."""
        self.logger.info("Performing upsert operation")
        
        try:
            # Try to read existing data
            try:
                existing_df = self.spark.read.format(target_format).load(target_path)
                
                # Identify records to update vs insert
                from pyspark.sql.functions import col
                
                # Get IDs from new data
                new_ids = df.select("id").distinct()
                
                # Filter existing data to exclude records being updated
                existing_filtered = existing_df.join(
                    new_ids,
                    existing_df.id == new_ids.id,
                    "left_anti"
                )
                
                # Union filtered existing with new data
                final_df = existing_filtered.union(df)
                
                # Write combined data
                final_df.write \
                    .format(target_format) \
                    .mode("overwrite") \
                    .save(target_path)
                
            except Exception:
                # If target doesn't exist, just write new data
                df.write \
                    .format(target_format) \
                    .mode("overwrite") \
                    .save(target_path)
            
        except Exception as e:
            self.logger.error(f"Upsert failed: {str(e)}")
            raise
    
    def _reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data with target.
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passes
        """
        self.logger.info("Performing data reconciliation")
        
        try:
            target_path = self.config["target"]["database"]["path"]
            target_format = self.config["target"]["database"]["format"]
            
            # Read back from target
            target_df = self.spark.read.format(target_format).load(target_path)
            
            # Filter target to records from this run
            target_df = target_df.filter(f"etl_run_id = '{self.run_id}'")
            
            # Compare counts
            loaded_count = loaded_df.count()
            target_count = target_df.count()
            
            if loaded_count != target_count:
                self.logger.warning(
                    f"Reconciliation count mismatch: "
                    f"Loaded={loaded_count}, Target={target_count}"
                )
                return False
            
            # Compare sums of key metrics
            from pyspark.sql.functions import sum as spark_sum
            
            loaded_sum = loaded_df.select(spark_sum("transformed_value")).collect()[0][0]
            target_sum = target_df.select(spark_sum("transformed_value")).collect()[0][0]
            
            if loaded_sum != target_sum:
                self.logger.warning(
                    f"Reconciliation value mismatch: "
                    f"Loaded={loaded_sum}, Target={target_sum}"
                )
                return False
            
            self.logger.info("Reconciliation passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Reconciliation error: {str(e)}")
            return False
    
    def load_with_partitioning(
        self,
        df: DataFrame,
        partition_columns: list,
        **kwargs
    ) -> Dict[str, int]:
        """
        Load data with partitioning.
        
        Args:
            df: DataFrame to load
            partition_columns: Columns to partition by
            
        Returns:
            Load statistics
        """
        self.logger.info(f"Loading with partitioning: {partition_columns}")
        
        target_path = self.config["target"]["database"]["path"]
        target_format = self.config["target"]["database"]["format"]
        
        try:
            df.write \
                .format(target_format) \
                .partitionBy(*partition_columns) \
                .mode("append") \
                .save(target_path)
            
            return {
                "success_count": df.count(),
                "error_count": 0,
                "total_count": df.count()
            }
            
        except Exception as e:
            self.logger.error(f"Partitioned load failed: {str(e)}")
            raise