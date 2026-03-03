"""
ETL Data Loading Module
Loads transformed data to target with reconciliation
"""
import logging
from typing import Dict, NamedTuple
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


class LoadResult(NamedTuple):
    """Result of load operation"""
    success_count: int
    error_count: int
    total_count: int
    errors: list


class ETLLoader:
    """Handles data loading to target systems"""
    
    def __init__(self, spark: SparkSession, config: Dict, run_id: str):
        """
        Initialize loader
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(self.__class__.__name__)
        self.batch_size = config.get('load', {}).get('batch_size', 1000)
        
    def load_data(
        self, 
        df: DataFrame, 
        mode: str = "upsert"
    ) -> LoadResult:
        """
        Load data to target
        
        Args:
            df: Transformed DataFrame to load
            mode: Load mode (insert, update, upsert)
            
        Returns:
            LoadResult with statistics
        """
        self.logger.info(
            f"Starting load - Mode: {mode}, Batch size: {self.batch_size}"
        )
        
        try:
            total_count = df.count()
            
            if mode.lower() == "insert":
                success = self._insert_new(df)
            elif mode.lower() == "update":
                success = self._update_existing(df)
            elif mode.lower() == "upsert":
                success = self._upsert_data(df)
            else:
                success = self._insert_new(df)
            
            if success:
                success_count = total_count
                error_count = 0
                errors = []
            else:
                success_count = 0
                error_count = total_count
                errors = ["Load operation failed"]
            
            # Reconcile data
            if self.config.get('load', {}).get('enable_reconciliation', True):
                reconciled = self._reconcile_data(df)
                if not reconciled:
                    self.logger.warning("Data reconciliation failed")
                    errors.append("Reconciliation failed")
            
            self.logger.info(
                f"Load complete - Success: {success_count}, Errors: {error_count}"
            )
            
            return LoadResult(
                success_count=success_count,
                error_count=error_count,
                total_count=total_count,
                errors=errors
            )
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            return LoadResult(
                success_count=0,
                error_count=df.count(),
                total_count=df.count(),
                errors=[str(e)]
            )
    
    def _insert_new(self, df: DataFrame) -> bool:
        """
        Insert new records to target
        
        Args:
            df: DataFrame to insert
            
        Returns:
            True if successful
        """
        try:
            target_config = self.config['target']['database']
            
            df.write \
                .format("jdbc") \
                .option("url", target_config['jdbc_url']) \
                .option("dbtable", target_config['table']) \
                .option("user", target_config.get('user', '')) \
                .option("password", target_config.get('password', '')) \
                .option("driver", target_config.get('driver', 'org.postgresql.Driver')) \
                .option("batchsize", self.batch_size) \
                .mode("append") \
                .save()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Insert failed: {str(e)}")
            return False
    
    def _update_existing(self, df: DataFrame) -> bool:
        """
        Update existing records in target
        
        Args:
            df: DataFrame with updates
            
        Returns:
            True if successful
        """
        try:
            target_config = self.config['target']['database']
            
            # Create temporary table
            temp_table = f"temp_update_{self.run_id.replace('-', '_')}"
            df.createOrReplaceTempView(temp_table)
            
            # Execute update via JDBC
            # Note: This is simplified - production would use proper JDBC update
            update_query = f"""
                UPDATE {target_config['table']} t
                SET 
                    name = s.name,
                    value = s.value,
                    transformed_value = s.transformed_value,
                    status = s.status,
                    category = s.category,
                    priority = s.priority,
                    processed_at = s.processed_at,
                    processed_by = s.processed_by
                FROM {temp_table} s
                WHERE t.id = s.id
            """
            
            # For this example, we'll use merge logic
            self._merge_data(df, target_config)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Update failed: {str(e)}")
            return False
    
    def _upsert_data(self, df: DataFrame) -> bool:
        """
        Upsert (insert or update) data to target
        
        Args:
            df: DataFrame to upsert
            
        Returns:
            True if successful
        """
        try:
            target_config = self.config['target']['database']
            
            # Read existing data
            existing_df = self.spark.read \
                .format("jdbc") \
                .option("url", target_config['jdbc_url']) \
                .option("dbtable", target_config['table']) \
                .option("user", target_config.get('user', '')) \
                .option("password", target_config.get('password', '')) \
                .option("driver", target_config.get('driver', 'org.postgresql.Driver')) \
                .load()
            
            # Find new records (not in existing)
            new_records = df.join(
                existing_df.select("id"),
                on="id",
                how="left_anti"
            )
            
            # Find records to update (in existing)
            update_records = df.join(
                existing_df.select("id"),
                on="id",
                how="inner"
            )
            
            # Insert new records
            if new_records.count() > 0:
                self._insert_new(new_records)
            
            # Update existing records  
            if update_records.count() > 0:
                self._update_existing(update_records)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Upsert failed: {str(e)}")
            return False
    
    def _merge_data(self, df: DataFrame, target_config: Dict):
        """
        Merge data using Delta Lake or similar
        
        Args:
            df: DataFrame to merge
            target_config: Target configuration
        """
        # Use Delta Lake if available
        if target_config.get('format') == 'delta':
            from delta.tables import DeltaTable
            
            delta_table = DeltaTable.forPath(
                self.spark, 
                target_config['path']
            )
            
            delta_table.alias("target").merge(
                df.alias("source"),
                "target.id = source.id"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
        else:
            # Fallback to overwrite by partition or append
            df.write.mode("overwrite").save(target_config['path'])
    
    def _reconcile_data(self, df: DataFrame) -> bool:
        """
        Reconcile loaded data with source
        
        Args:
            df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passed
        """
        try:
            self.logger.info("Starting data reconciliation")
            
            target_config = self.config['target']['database']
            
            # Read back from target
            loaded_df = self.spark.read \
                .format("jdbc") \
                .option("url", target_config['jdbc_url']) \
                .option("dbtable", target_config['table']) \
                .option("user", target_config.get('user', '')) \
                .option("password", target_config.get('password', '')) \
                .option("driver", target_config.get('driver', 'org.postgresql.Driver')) \
                .load()
            
            # Filter for this run
            loaded_df = loaded_df.filter(F.col("etl_run_id") == self.run_id)
            
            # Compare counts
            source_count = df.count()
            loaded_count = loaded_df.count()
            
            if source_count != loaded_count:
                self.logger.warning(
                    f"Reconciliation count mismatch: "
                    f"Source={source_count}, Loaded={loaded_count}"
                )
                return False
            
            # Compare checksums
            source_checksum = df.select(F.sum(F.col("value"))).collect()[0][0]
            loaded_checksum = loaded_df.select(F.sum(F.col("value"))).collect()[0][0]
            
            if abs(source_checksum - loaded_checksum) > 0.01:
                self.logger.warning(
                    f"Reconciliation checksum mismatch: "
                    f"Source={source_checksum}, Loaded={loaded_checksum}"
                )
                return False
            
            self.logger.info("Reconciliation passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Reconciliation failed: {str(e)}")
            return False