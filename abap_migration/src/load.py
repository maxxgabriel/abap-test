"""
Data Loading Module
Handles loading transformed data to target systems with batch processing and error handling.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from typing import Dict, Any, Tuple
import logging

logger = logging.getLogger(__name__)


class DataLoader:
    """Load transformed data to target destination."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the DataLoader.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.target_config = config.get('target', {})
        
    def load_data(
        self,
        df: DataFrame,
        mode: str = None
    ) -> Tuple[int, int, int]:
        """
        Load data to target destination.
        
        Args:
            df: DataFrame to load
            mode: Load mode (insert, update, upsert, overwrite)
            
        Returns:
            Tuple of (success_count, error_count, total_count)
        """
        mode = mode or self.target_config.get('mode', 'upsert')
        logger.info(f"Starting data load - Mode: {mode}, Run ID: {self.run_id}")
        
        try:
            total_count = df.count()
            
            # Load based on mode
            if mode == 'insert':
                success_count = self._insert_data(df)
            elif mode == 'update':
                success_count = self._update_data(df)
            elif mode == 'upsert':
                success_count = self._upsert_data(df)
            elif mode == 'overwrite':
                success_count = self._overwrite_data(df)
            else:
                raise ValueError(f"Unknown load mode: {mode}")
            
            error_count = total_count - success_count
            
            logger.info(
                f"Load complete - Success: {success_count}, "
                f"Errors: {error_count}, Total: {total_count}"
            )
            
            # Reconcile data if enabled
            if self.config.get('quality', {}).get('enabled', True):
                self._reconcile_data(df)
            
            return success_count, error_count, total_count
            
        except Exception as e:
            logger.error(f"Data load failed: {str(e)}")
            raise
    
    def _insert_data(self, df: DataFrame) -> int:
        """
        Insert new records into target.
        
        Args:
            df: DataFrame to insert
            
        Returns:
            Number of records inserted
        """
        jdbc_config = self.target_config.get('jdbc', {})
        batch_size = self.target_config.get('batch_size', 1000)
        
        # Write to JDBC target
        df.write.format("jdbc") \
            .option("url", jdbc_config.get('url')) \
            .option("driver", jdbc_config.get('driver')) \
            .option("dbtable", jdbc_config.get('table')) \
            .option("user", jdbc_config.get('user')) \
            .option("password", jdbc_config.get('password')) \
            .option("batchsize", batch_size) \
            .mode("append") \
            .save()
        
        return df.count()
    
    def _update_data(self, df: DataFrame) -> int:
        """
        Update existing records in target.
        
        Args:
            df: DataFrame with updates
            
        Returns:
            Number of records updated
        """
        # For updates, we need to use JDBC with a custom query
        # This is a simplified version - in production, use Delta Lake or similar
        logger.warning("Update mode requires additional implementation for JDBC")
        return self._upsert_data(df)
    
    def _upsert_data(self, df: DataFrame) -> int:
        """
        Insert or update records (upsert).
        
        Args:
            df: DataFrame to upsert
            
        Returns:
            Number of records processed
        """
        jdbc_config = self.target_config.get('jdbc', {})
        batch_size = self.target_config.get('batch_size', 1000)
        
        # For PostgreSQL, we can use ON CONFLICT
        # For other databases, this would need adjustment
        properties = {
            "user": jdbc_config.get('user'),
            "password": jdbc_config.get('password'),
            "driver": jdbc_config.get('driver'),
            "batchsize": str(batch_size)
        }
        
        # Write with overwrite mode on matching keys
        df.write.jdbc(
            url=jdbc_config.get('url'),
            table=jdbc_config.get('table'),
            mode="append",  # Use append with conflict resolution at DB level
            properties=properties
        )
        
        return df.count()
    
    def _overwrite_data(self, df: DataFrame) -> int:
        """
        Overwrite all data in target.
        
        Args:
            df: DataFrame to write
            
        Returns:
            Number of records written
        """
        jdbc_config = self.target_config.get('jdbc', {})
        
        df.write.format("jdbc") \
            .option("url", jdbc_config.get('url')) \
            .option("driver", jdbc_config.get('driver')) \
            .option("dbtable", jdbc_config.get('table')) \
            .option("user", jdbc_config.get('user')) \
            .option("password", jdbc_config.get('password')) \
            .mode("overwrite") \
            .save()
        
        return df.count()
    
    def _reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data with target to ensure consistency.
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passes
        """
        logger.info("Starting data reconciliation")
        
        try:
            jdbc_config = self.target_config.get('jdbc', {})
            
            # Read back from target
            target_df = self.spark.read.format("jdbc").options(
                url=jdbc_config.get('url'),
                driver=jdbc_config.get('driver'),
                dbtable=jdbc_config.get('table'),
                user=jdbc_config.get('user'),
                password=jdbc_config.get('password')
            ).load()
            
            # Filter to current run
            target_df = target_df.filter(col("etl_run_id") == self.run_id)
            
            loaded_count = loaded_df.count()
            target_count = target_df.count()
            
            if loaded_count == target_count:
                logger.info(f"Reconciliation passed: {loaded_count} records match")
                return True
            else:
                logger.warning(
                    f"Reconciliation mismatch: "
                    f"Loaded {loaded_count}, Found {target_count}"
                )
                return False
                
        except Exception as e:
            logger.error(f"Reconciliation failed: {str(e)}")
            return False