"""
PySpark ETL Loader Module
Loads transformed data to target with support for batch processing and reconciliation.
"""

from pyspark.sql import DataFrame
from typing import Dict
import logging

logger = logging.getLogger(__name__)


class ETLLoader:
    """Handles data loading to target systems"""
    
    def __init__(self, config: dict, run_id: str):
        """
        Initialize loader with configuration
        
        Args:
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.config = config
        self.run_id = run_id
        self.target_type = config.get('target_type', 'DATABASE')
        self.batch_size = config.get('batch_size', 1000)
    
    def load_data(self, df: DataFrame, mode: str = 'upsert') -> Dict[str, int]:
        """
        Main load method - writes data to target
        
        Args:
            df: Transformed DataFrame to load
            mode: Load mode ('insert', 'update', 'upsert')
            
        Returns:
            Dictionary with load statistics
        """
        logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        total_count = df.count()
        success_count = 0
        error_count = 0
        
        try:
            if self.target_type == 'DATABASE':
                success_count = self.load_to_database(df, mode)
            elif self.target_type == 'PARQUET':
                success_count = self.load_to_parquet(df, mode)
            elif self.target_type == 'DELTA':
                success_count = self.load_to_delta(df, mode)
            else:
                success_count = self.load_to_database(df, mode)
            
            error_count = total_count - success_count
            
            # Reconcile data
            if self.config.get('enable_reconciliation', True):
                self.reconcile_data(df, success_count)
            
        except Exception as e:
            logger.error(f"Load failed: {str(e)}")
            error_count = total_count
        
        result = {
            'success_count': success_count,
            'error_count': error_count,
            'total_count': total_count
        }
        
        logger.info(f"Load complete - Success: {success_count}, Errors: {error_count}")
        
        return result
    
    def load_to_database(self, df: DataFrame, mode: str) -> int:
        """
        Load data to database via JDBC
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            Number of successfully loaded records
        """
        target_config = self.config['targets']['database']
        
        jdbc_options = {
            "url": target_config['jdbc_url'],
            "dbtable": target_config['table'],
            "user": target_config['user'],
            "password": target_config['password'],
            "driver": target_config['driver'],
            "batchsize": str(self.batch_size)
        }
        
        # Map mode to Spark write mode
        write_mode = {
            'insert': 'append',
            'update': 'overwrite',
            'upsert': 'append'  # Requires merge logic
        }.get(mode, 'append')
        
        df.write \
            .format("jdbc") \
            .options(**jdbc_options) \
            .mode(write_mode) \
            .save()
        
        return df.count()
    
    def load_to_parquet(self, df: DataFrame, mode: str) -> int:
        """
        Load data to Parquet files
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            Number of successfully loaded records
        """
        target_path = self.config['targets']['parquet']['path']
        
        write_mode = 'append' if mode == 'insert' else 'overwrite'
        
        df.write \
            .format("parquet") \
            .mode(write_mode) \
            .partitionBy("category") \
            .save(target_path)
        
        return df.count()
    
    def load_to_delta(self, df: DataFrame, mode: str) -> int:
        """
        Load data to Delta Lake with merge support
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            Number of successfully loaded records
        """
        target_path = self.config['targets']['delta']['path']
        
        if mode == 'upsert':
            # Delta Lake merge logic
            from delta.tables import DeltaTable
            
            if DeltaTable.isDeltaTable(df.sparkSession, target_path):
                delta_table = DeltaTable.forPath(df.sparkSession, target_path)
                
                delta_table.alias("target") \
                    .merge(
                        df.alias("source"),
                        "target.id = source.id"
                    ) \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()
            else:
                df.write.format("delta").save(target_path)
        else:
            write_mode = 'append' if mode == 'insert' else 'overwrite'
            df.write.format("delta").mode(write_mode).save(target_path)
        
        return df.count()
    
    def reconcile_data(self, df: DataFrame, expected_count: int) -> bool:
        """
        Reconcile loaded data against source
        
        Args:
            df: Source DataFrame
            expected_count: Expected number of records
            
        Returns:
            True if reconciliation passes
        """
        logger.info("Performing data reconciliation")
        
        # Count records in target
        target_config = self.config['targets']['database']
        
        actual_df = df.sparkSession.read \
            .format("jdbc") \
            .options(
                url=target_config['jdbc_url'],
                dbtable=f"(SELECT COUNT(*) as count FROM {target_config['table']} WHERE etl_run_id = '{self.run_id}') as t",
                user=target_config['user'],
                password=target_config['password']
            ) \
            .load()
        
        actual_count = actual_df.first()['count']
        
        matches = actual_count == expected_count
        
        if matches:
            logger.info(f"Reconciliation passed: {actual_count} records")
        else:
            logger.warning(f"Reconciliation failed: Expected {expected_count}, Found {actual_count}")
        
        return matches