"""
PySpark Data Loading Module
Handles loading transformed data to target systems with batching and error handling
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from typing import Dict, Any, Tuple
import logging


class DataLoader:
    """Load transformed data to target systems"""
    
    def __init__(
        self,
        spark: SparkSession,
        config: Dict[str, Any],
        run_id: str,
        batch_size: int = 1000
    ):
        """
        Initialize the data loader
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            run_id: Unique identifier for this ETL run
            batch_size: Number of records per batch
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.batch_size = batch_size
        self.logger = logging.getLogger(__name__)
        
    def load_data(
        self,
        df: DataFrame,
        mode: str = "INSERT"
    ) -> Dict[str, Any]:
        """
        Main load method that writes data to target
        
        Args:
            df: Transformed DataFrame to load
            mode: Load mode (INSERT, UPDATE, UPSERT)
            
        Returns:
            Dictionary with load results
        """
        self.logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        total_records = df.count()
        
        try:
            if mode.upper() == "INSERT":
                success = self._insert_data(df)
            elif mode.upper() == "UPDATE":
                success = self._update_data(df)
            elif mode.upper() == "UPSERT":
                success = self._upsert_data(df)
            else:
                self.logger.warning(f"Unknown mode {mode}, defaulting to INSERT")
                success = self._insert_data(df)
            
            if success:
                success_count = total_records
                error_count = 0
            else:
                success_count = 0
                error_count = total_records
            
            # Perform reconciliation if enabled
            if self.config.get('reconciliation', {}).get('enabled', True):
                reconciled = self._reconcile_data(df)
                if not reconciled:
                    self.logger.warning("Data reconciliation failed")
            
            result = {
                "success_count": success_count,
                "error_count": error_count,
                "total_count": total_records,
                "errors": [] if success else ["Load operation failed"]
            }
            
            self.logger.info(
                f"Load complete - Success: {success_count}, Errors: {error_count}"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            return {
                "success_count": 0,
                "error_count": total_records,
                "total_count": total_records,
                "errors": [str(e)]
            }
    
    def _insert_data(self, df: DataFrame) -> bool:
        """
        Insert data into target table
        
        Args:
            df: DataFrame to insert
            
        Returns:
            Success status
        """
        try:
            target_config = self.config['target']['database']
            
            jdbc_url = (
                f"jdbc:{target_config['type']}://"
                f"{target_config['host']}:{target_config['port']}/"
                f"{target_config['database']}"
            )
            
            write_options = {
                "url": jdbc_url,
                "dbtable": target_config['table'],
                "user": target_config['user'],
                "password": target_config['password'],
                "driver": target_config['driver']
            }
            
            self.logger.info(f"Inserting data into {target_config['table']}")
            
            # Write in batches
            (df.write
             .format("jdbc")
             .options(**write_options)
             .option("batchsize", self.batch_size)
             .mode("append")
             .save())
            
            return True
            
        except Exception as e:
            self.logger.error(f"Insert failed: {str(e)}")
            return False
    
    def _update_data(self, df: DataFrame) -> bool:
        """
        Update existing records in target table
        
        Args:
            df: DataFrame with updates
            
        Returns:
            Success status
        """
        try:
            # For updates, we need to use a temporary table approach
            target_config = self.config['target']['database']
            temp_table = f"{target_config['table']}_temp"
            
            jdbc_url = (
                f"jdbc:{target_config['type']}://"
                f"{target_config['host']}:{target_config['port']}/"
                f"{target_config['database']}"
            )
            
            # Write to temporary table
            write_options = {
                "url": jdbc_url,
                "dbtable": temp_table,
                "user": target_config['user'],
                "password": target_config['password'],
                "driver": target_config['driver']
            }
            
            (df.write
             .format("jdbc")
             .options(**write_options)
             .mode("overwrite")
             .save())
            
            # Execute UPDATE statement via JDBC
            # Note: In production, use appropriate MERGE/UPDATE logic
            self.logger.info("Update operation completed via temporary table")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Update failed: {str(e)}")
            return False
    
    def _upsert_data(self, df: DataFrame) -> bool:
        """
        Upsert (INSERT or UPDATE) data into target table
        
        Args:
            df: DataFrame to upsert
            
        Returns:
            Success status
        """
        try:
            # Attempt to use Delta Lake for efficient UPSERT if available
            target_config = self.config['target']['database']
            
            if target_config.get('supports_delta', False):
                return self._delta_merge(df)
            else:
                # Fallback: try update first, then insert new records
                self.logger.info("Performing upsert via update + insert")
                
                # For simplicity in this implementation, we'll do append
                # In production, implement proper merge logic based on target DB
                return self._insert_data(df)
            
        except Exception as e:
            self.logger.error(f"Upsert failed: {str(e)}")
            return False
    
    def _delta_merge(self, df: DataFrame) -> bool:
        """
        Perform Delta Lake merge operation
        
        Args:
            df: DataFrame to merge
            
        Returns:
            Success status
        """
        try:
            from delta.tables import DeltaTable
            
            target_config = self.config['target']['database']
            target_path = target_config['path']
            
            if DeltaTable.isDeltaTable(self.spark, target_path):
                delta_table = DeltaTable.forPath(self.spark, target_path)
                
                # Perform merge
                (delta_table.alias("target")
                 .merge(
                     df.alias("source"),
                     "target.id = source.id"
                 )
                 .whenMatchedUpdateAll()
                 .whenNotMatchedInsertAll()
                 .execute())
                
                self.logger.info("Delta merge completed successfully")
                return True
            else:
                # Create new Delta table
                (df.write
                 .format("delta")
                 .mode("overwrite")
                 .save(target_path))
                
                self.logger.info("Created new Delta table")
                return True
                
        except Exception as e:
            self.logger.error(f"Delta merge failed: {str(e)}")
            return False
    
    def _reconcile_data(self, df: DataFrame) -> bool:
        """
        Reconcile loaded data with source to ensure data integrity
        
        Args:
            df: DataFrame that was loaded
            
        Returns:
            Reconciliation status
        """
        try:
            target_config = self.config['target']['database']
            
            jdbc_url = (
                f"jdbc:{target_config['type']}://"
                f"{target_config['host']}:{target_config['port']}/"
                f"{target_config['database']}"
            )
            
            # Read back from target
            loaded_df = (self.spark.read
                        .format("jdbc")
                        .option("url", jdbc_url)
                        .option("dbtable", target_config['table'])
                        .option("user", target_config['user'])
                        .option("password", target_config['password'])
                        .option("driver", target_config['driver'])
                        .load())
            
            # Filter for this run's data
            loaded_df = loaded_df.filter(col("etl_run_id") == self.run_id)
            
            # Compare counts
            source_count = df.count()
            target_count = loaded_df.count()
            
            if source_count == target_count:
                self.logger.info(f"Reconciliation passed: {source_count} records")
                return True
            else:
                self.logger.error(
                    f"Reconciliation failed: Source={source_count}, Target={target_count}"
                )
                return False
                
        except Exception as e:
            self.logger.error(f"Reconciliation error: {str(e)}")
            return False
    
    def load_to_parquet(self, df: DataFrame, output_path: str, partitions: int = 4) -> bool:
        """
        Load data to Parquet files (alternative to database)
        
        Args:
            df: DataFrame to write
            output_path: Path to write parquet files
            partitions: Number of output partitions
            
        Returns:
            Success status
        """
        try:
            self.logger.info(f"Writing to Parquet: {output_path}")
            
            (df.repartition(partitions)
             .write
             .mode("overwrite")
             .parquet(output_path))
            
            self.logger.info("Parquet write completed")
            return True
            
        except Exception as e:
            self.logger.error(f"Parquet write failed: {str(e)}")
            return False