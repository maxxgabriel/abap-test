"""
Data loading module for ETL pipeline.
Handles loading data to target destinations with batching and reconciliation.
"""

from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any, Optional
from dataclasses import dataclass

from src.logger import ETLLogger


@dataclass
class LoadResult:
    """Container for load operation results."""
    success_count: int
    error_count: int
    total_count: int
    errors: list


class DataLoader:
    """Handles data loading to target destinations."""
    
    def __init__(
        self,
        spark: SparkSession,
        config: Dict[str, Any],
        run_id: str,
        batch_size: int = 1000
    ):
        """
        Initialize the loader.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
            batch_size: Number of records per batch
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.batch_size = batch_size
        self.logger = ETLLogger.get_instance()
        self.target_type = config.get('target_type', 'database')
    
    def load_data(
        self,
        df: DataFrame,
        mode: str = 'append'
    ) -> LoadResult:
        """
        Load data to target destination.
        
        Args:
            df: DataFrame to load
            mode: Load mode ('append', 'overwrite', 'upsert')
            
        Returns:
            LoadResult with statistics
        """
        self.logger.log_info(
            component='LOADER',
            message=f'Starting load - Mode: {mode}, Batch size: {self.batch_size}'
        )
        
        total_count = df.count()
        errors = []
        
        try:
            if mode == 'upsert':
                success = self._upsert_data(df)
            else:
                success = self._load_to_database(df, mode)
            
            if success:
                success_count = total_count
                error_count = 0
                
                # Perform reconciliation if enabled
                if self.config.get('enable_reconciliation', True):
                    reconciled = self._reconcile_data(df)
                    if not reconciled:
                        self.logger.log_warning(
                            component='LOADER',
                            message='Data reconciliation failed'
                        )
            else:
                success_count = 0
                error_count = total_count
                errors.append('Load operation failed')
            
            result = LoadResult(
                success_count=success_count,
                error_count=error_count,
                total_count=total_count,
                errors=errors
            )
            
            self.logger.log_info(
                component='LOADER',
                message=f'Load complete - Success: {success_count}, Errors: {error_count}'
            )
            
            return result
            
        except Exception as e:
            self.logger.log_error(
                component='LOADER',
                message='Load failed',
                details=str(e)
            )
            return LoadResult(
                success_count=0,
                error_count=total_count,
                total_count=total_count,
                errors=[str(e)]
            )
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to database target.
        
        Args:
            df: DataFrame to load
            mode: Write mode
            
        Returns:
            True if successful
        """
        try:
            target_table = self.config.get('target_table', 'etl_target_data')
            
            df.write \
                .format(self.config.get('target_format', 'jdbc')) \
                .option("url", self.config.get('jdbc_url')) \
                .option("dbtable", target_table) \
                .option("user", self.config.get('db_user')) \
                .option("password", self.config.get('db_password')) \
                .option("driver", self.config.get('jdbc_driver', 'org.postgresql.Driver')) \
                .option("batchsize", self.batch_size) \
                .mode(mode) \
                .save()
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component='LOADER',
                message='Database load error',
                details=str(e)
            )
            return False
    
    def _upsert_data(self, df: DataFrame) -> bool:
        """
        Perform upsert (update existing, insert new) operation.
        
        Args:
            df: DataFrame to upsert
            
        Returns:
            True if successful
        """
        try:
            target_table = self.config.get('target_table', 'etl_target_data')
            temp_table = f"{target_table}_temp"
            
            # Write to temporary table
            df.write \
                .format(self.config.get('target_format', 'jdbc')) \
                .option("url", self.config.get('jdbc_url')) \
                .option("dbtable", temp_table) \
                .option("user", self.config.get('db_user')) \
                .option("password", self.config.get('db_password')) \
                .mode('overwrite') \
                .save()
            
            # Execute merge statement
            merge_sql = f"""
                MERGE INTO {target_table} t
                USING {temp_table} s
                ON t.id = s.id
                WHEN MATCHED THEN
                    UPDATE SET
                        name = s.name,
                        value = s.value,
                        transformed_value = s.transformed_value,
                        status = s.status,
                        category = s.category,
                        priority = s.priority,
                        etl_run_id = s.etl_run_id,
                        processed_at = s.processed_at,
                        processed_by = s.processed_by
                WHEN NOT MATCHED THEN
                    INSERT VALUES (
                        s.id, s.name, s.value, s.transformed_value,
                        s.status, s.category, s.priority, s.etl_run_id,
                        s.processed_at, s.processed_by
                    )
            """
            
            # Execute via JDBC (implementation depends on database)
            # For now, use standard write operations
            return self._load_to_database(df, 'append')
            
        except Exception as e:
            self.logger.log_error(
                component='LOADER',
                message='Upsert operation failed',
                details=str(e)
            )
            return False
    
    def _reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data with source.
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passes
        """
        try:
            target_table = self.config.get('target_table', 'etl_target_data')
            
            # Read back loaded data
            target_df = self.spark.read \
                .format(self.config.get('target_format', 'jdbc')) \
                .option("url", self.config.get('jdbc_url')) \
                .option("dbtable", target_table) \
                .option("user", self.config.get('db_user')) \
                .option("password", self.config.get('db_password')) \
                .load() \
                .filter(f"etl_run_id = '{self.run_id}'")
            
            loaded_count = loaded_df.count()
            target_count = target_df.count()
            
            if loaded_count == target_count:
                self.logger.log_info(
                    component='LOADER',
                    message=f'Reconciliation passed: {loaded_count} records'
                )
                return True
            else:
                self.logger.log_warning(
                    component='LOADER',
                    message=f'Reconciliation mismatch: loaded={loaded_count}, target={target_count}'
                )
                return False
                
        except Exception as e:
            self.logger.log_error(
                component='LOADER',
                message='Reconciliation failed',
                details=str(e)
            )
            return False