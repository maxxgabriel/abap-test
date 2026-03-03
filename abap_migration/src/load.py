"""
PySpark ETL Load Module
Handles data loading to target systems with batching and reconciliation.
"""

from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any, List
from dataclasses import dataclass
import logging

from src.logger import ETLLogger


@dataclass
class LoadResult:
    """Result of load operation."""
    success_count: int
    error_count: int
    total_count: int
    errors: List[str]


class ETLLoader:
    """Loads data to target systems."""
    
    def __init__(self, spark: SparkSession, run_id: str, config: Dict[str, Any]):
        """
        Initialize ETL Loader.
        
        Args:
            spark: SparkSession instance
            run_id: Unique identifier for this ETL run
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = ETLLogger.get_instance()
        self.target_type = config.get('target', {}).get('type', 'database')
        self.batch_size = config.get('target', {}).get('batch_size', 1000)
        
    def load_data(self, df: DataFrame, mode: str = "append") -> LoadResult:
        """
        Load data to target system.
        
        Args:
            df: DataFrame to load
            mode: Load mode ('append', 'overwrite', 'upsert')
            
        Returns:
            LoadResult with statistics
        """
        self.logger.log_info(
            component='LOADER',
            message=f'Starting load - Mode: {mode}, Batch size: {self.batch_size}',
            run_id=self.run_id
        )
        
        try:
            total_count = df.count()
            errors = []
            
            if self.target_type == 'database':
                success = self._load_to_database(df, mode)
            elif self.target_type == 'file':
                success = self._load_to_file(df, mode)
            elif self.target_type == 'warehouse':
                success = self._load_to_warehouse(df, mode)
            else:
                success = self._load_to_database(df, mode)
            
            if success:
                success_count = total_count
                error_count = 0
            else:
                success_count = 0
                error_count = total_count
                errors.append("Load operation failed")
            
            # Reconcile data
            if self.config.get('target', {}).get('reconciliation', {}).get('enabled', False):
                reconciled = self._reconcile_data(df)
                if not reconciled:
                    self.logger.log_warning(
                        component='LOADER',
                        message='Data reconciliation failed',
                        run_id=self.run_id
                    )
                    errors.append("Reconciliation mismatch")
            
            result = LoadResult(
                success_count=success_count,
                error_count=error_count,
                total_count=total_count,
                errors=errors
            )
            
            self.logger.log_info(
                component='LOADER',
                message=f'Load complete - Success: {success_count}, Errors: {error_count}',
                run_id=self.run_id,
                details={
                    'success_count': success_count,
                    'error_count': error_count,
                    'total_count': total_count
                }
            )
            
            return result
            
        except Exception as e:
            self.logger.log_error(
                component='LOADER',
                message='Load failed',
                run_id=self.run_id,
                details={'error': str(e)}
            )
            raise
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """Load data to database target."""
        jdbc_config = self.config['target']['database']
        
        try:
            write_mode = self._map_load_mode(mode)
            
            df.write \
                .format("jdbc") \
                .option("url", jdbc_config['url']) \
                .option("dbtable", jdbc_config['table']) \
                .option("user", jdbc_config.get('user', '')) \
                .option("password", jdbc_config.get('password', '')) \
                .option("driver", jdbc_config.get('driver', 'org.postgresql.Driver')) \
                .option("batchsize", self.batch_size) \
                .mode(write_mode) \
                .save()
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component='LOADER',
                message='Database load error',
                run_id=self.run_id,
                details={'error': str(e)}
            )
            return False
    
    def _load_to_file(self, df: DataFrame, mode: str) -> bool:
        """Load data to file target."""
        file_config = self.config['target']['file']
        path = file_config['path']
        file_format = file_config.get('format', 'parquet')
        
        try:
            write_mode = self._map_load_mode(mode)
            
            writer = df.write.format(file_format).mode(write_mode)
            
            if file_config.get('partitions'):
                writer = writer.partitionBy(*file_config['partitions'])
            
            writer.save(path)
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component='LOADER',
                message='File load error',
                run_id=self.run_id,
                details={'error': str(e)}
            )
            return False
    
    def _load_to_warehouse(self, df: DataFrame, mode: str) -> bool:
        """Load data to data warehouse target."""
        warehouse_config = self.config['target']['warehouse']
        
        try:
            write_mode = self._map_load_mode(mode)
            
            # Example for cloud warehouse (adjust based on actual warehouse)
            df.write \
                .format(warehouse_config.get('format', 'delta')) \
                .option("path", warehouse_config['path']) \
                .mode(write_mode) \
                .saveAsTable(warehouse_config['table'])
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component='LOADER',
                message='Warehouse load error',
                run_id=self.run_id,
                details={'error': str(e)}
            )
            return False
    
    def _map_load_mode(self, mode: str) -> str:
        """Map custom load mode to Spark write mode."""
        mode_mapping = {
            'insert': 'append',
            'append': 'append',
            'overwrite': 'overwrite',
            'upsert': 'append'  # Upsert requires merge logic
        }
        return mode_mapping.get(mode.lower(), 'append')
    
    def _reconcile_data(self, df: DataFrame) -> bool:
        """
        Reconcile loaded data with source.
        
        Args:
            df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passed
        """
        try:
            # Count records in source DataFrame
            source_count = df.count()
            
            # Count records in target
            jdbc_config = self.config['target']['database']
            
            query = f"""
                SELECT COUNT(*) as count
                FROM {jdbc_config['table']}
                WHERE etl_run_id = '{self.run_id}'
            """
            
            target_df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_config['url']) \
                .option("dbtable", f"({query}) as reconcile") \
                .option("user", jdbc_config.get('user', '')) \
                .option("password", jdbc_config.get('password', '')) \
                .load()
            
            target_count = target_df.collect()[0]['count']
            
            if source_count == target_count:
                self.logger.log_info(
                    component='LOADER',
                    message=f'Reconciliation passed: {source_count} records',
                    run_id=self.run_id
                )
                return True
            else:
                self.logger.log_warning(
                    component='LOADER',
                    message=f'Reconciliation failed: Source={source_count}, Target={target_count}',
                    run_id=self.run_id
                )
                return False
                
        except Exception as e:
            self.logger.log_error(
                component='LOADER',
                message='Reconciliation error',
                run_id=self.run_id,
                details={'error': str(e)}
            )
            return False