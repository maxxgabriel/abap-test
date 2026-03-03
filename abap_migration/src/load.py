"""
PySpark ETL Data Loader Module
Loads transformed data to target destinations with batching and error handling.
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any, Optional
from dataclasses import dataclass
from src.logger import ETLLogger


@dataclass
class LoadResult:
    """Result of load operation."""
    success_count: int
    error_count: int
    total_count: int
    errors: list


class DataLoader:
    """Handles data loading to target destinations."""
    
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
        self.logger = ETLLogger.get_instance()
        self.target_config = config['loading']
        self.batch_size = self.target_config.get('batch_size', 1000)
    
    def load_data(
        self,
        df: DataFrame,
        mode: str = 'append',
        target_type: Optional[str] = None
    ) -> LoadResult:
        """
        Load data to target destination.
        
        Args:
            df: DataFrame to load
            mode: Load mode ('append', 'overwrite', 'upsert')
            target_type: Optional target type override
            
        Returns:
            LoadResult containing load statistics
        """
        target_type = target_type or self.target_config['target_type']
        total_count = df.count()
        
        self.logger.log_info(
            component='LOADER',
            message=f"Starting load - Mode: {mode}, Target: {target_type}, Records: {total_count}"
        )
        
        try:
            if target_type == 'DATABASE':
                success = self._load_to_database(df, mode)
            elif target_type == 'PARQUET':
                success = self._load_to_parquet(df, mode)
            elif target_type == 'DELTA':
                success = self._load_to_delta(df, mode)
            else:
                success = self._load_to_database(df, mode)
            
            if success:
                success_count = total_count
                error_count = 0
                errors = []
                
                # Perform reconciliation if enabled
                if self.config.get('enable_reconciliation', True):
                    self._reconcile_data(df, target_type)
                
            else:
                success_count = 0
                error_count = total_count
                errors = ['Load operation failed']
            
            result = LoadResult(
                success_count=success_count,
                error_count=error_count,
                total_count=total_count,
                errors=errors
            )
            
            self.logger.log_info(
                component='LOADER',
                message=f"Load complete - Success: {success_count}, Errors: {error_count}"
            )
            
            return result
            
        except Exception as e:
            self.logger.log_error(
                component='LOADER',
                message='Load operation failed',
                details=str(e)
            )
            return LoadResult(
                success_count=0,
                error_count=total_count,
                total_count=total_count,
                errors=[str(e)]
            )
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """Load data to database target."""
        try:
            target_db = self.target_config['database']
            
            # Map mode to JDBC mode
            jdbc_mode = self._map_mode(mode)
            
            (df.write
             .format('jdbc')
             .option('url', target_db['url'])
             .option('dbtable', target_db['table'])
             .option('user', target_db['user'])
             .option('password', target_db['password'])
             .option('driver', target_db['driver'])
             .option('batchsize', self.batch_size)
             .mode(jdbc_mode)
             .save())
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component='LOADER',
                message='Database load failed',
                details=str(e)
            )
            return False
    
    def _load_to_parquet(self, df: DataFrame, mode: str) -> bool:
        """Load data to Parquet files."""
        try:
            target_path = self.target_config['parquet']['path']
            
            # Partition by date if enabled
            if self.target_config['parquet'].get('partition_by'):
                partition_cols = self.target_config['parquet']['partition_by']
                (df.write
                 .partitionBy(*partition_cols)
                 .mode(mode)
                 .parquet(target_path))
            else:
                df.write.mode(mode).parquet(target_path)
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component='LOADER',
                message='Parquet load failed',
                details=str(e)
            )
            return False
    
    def _load_to_delta(self, df: DataFrame, mode: str) -> bool:
        """Load data to Delta Lake."""
        try:
            target_path = self.target_config['delta']['path']
            
            if mode == 'upsert':
                # Perform merge operation for upsert
                from delta.tables import DeltaTable
                
                # Check if table exists
                if DeltaTable.isDeltaTable(self.spark, target_path):
                    delta_table = DeltaTable.forPath(self.spark, target_path)
                    
                    # Merge based on ID
                    (delta_table.alias('target')
                     .merge(
                         df.alias('source'),
                         'target.id = source.id'
                     )
                     .whenMatchedUpdateAll()
                     .whenNotMatchedInsertAll()
                     .execute())
                else:
                    # First time - just write
                    df.write.format('delta').mode('overwrite').save(target_path)
            else:
                df.write.format('delta').mode(mode).save(target_path)
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component='LOADER',
                message='Delta load failed',
                details=str(e)
            )
            return False
    
    def _map_mode(self, mode: str) -> str:
        """Map ETL mode to Spark write mode."""
        mode_mapping = {
            'insert': 'append',
            'upsert': 'append',
            'update': 'append',
            'overwrite': 'overwrite'
        }
        return mode_mapping.get(mode.lower(), 'append')
    
    def _reconcile_data(self, source_df: DataFrame, target_type: str) -> bool:
        """
        Reconcile loaded data with source.
        
        Args:
            source_df: Source DataFrame
            target_type: Type of target
            
        Returns:
            True if reconciliation matches, False otherwise
        """
        try:
            self.logger.log_info(
                component='LOADER',
                message='Starting data reconciliation'
            )
            
            # Read back from target
            if target_type == 'DATABASE':
                target_db = self.target_config['database']
                target_df = (self.spark.read
                            .format('jdbc')
                            .option('url', target_db['url'])
                            .option('dbtable', target_db['table'])
                            .option('user', target_db['user'])
                            .option('password', target_db['password'])
                            .option('driver', target_db['driver'])
                            .load())
                
                # Filter by run_id
                target_df = target_df.filter(target_df.etl_run_id == self.run_id)
            else:
                self.logger.log_warning(
                    component='LOADER',
                    message=f'Reconciliation not implemented for {target_type}'
                )
                return True
            
            source_count = source_df.count()
            target_count = target_df.count()
            
            if source_count == target_count:
                self.logger.log_info(
                    component='LOADER',
                    message=f'Reconciliation passed: {source_count} records'
                )
                return True
            else:
                self.logger.log_warning(
                    component='LOADER',
                    message=f'Reconciliation mismatch: Source={source_count}, Target={target_count}'
                )
                return False
                
        except Exception as e:
            self.logger.log_warning(
                component='LOADER',
                message='Reconciliation failed',
                details=str(e)
            )
            return False