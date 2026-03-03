"""
PySpark ETL Loader Module
Loads transformed data to target destinations with batch processing
"""
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any, List
from dataclasses import dataclass
import logging


@dataclass
class LoadResult:
    """Result of load operation"""
    success_count: int
    error_count: int
    total_count: int
    errors: List[str]


class ETLLoader:
    """Handles data loading to various targets"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize loader
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        self.target_type = config.get('target_type', 'DATABASE')
        self.batch_size = config.get('batch_size', 1000)
        
    def load_data(self, df: DataFrame, mode: str = 'INSERT') -> LoadResult:
        """
        Main load method
        
        Args:
            df: DataFrame to load
            mode: Load mode (INSERT, UPDATE, UPSERT)
            
        Returns:
            LoadResult with statistics
        """
        self.logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        total_count = df.count()
        errors = []
        
        try:
            success = self._load_to_database(df, mode)
            
            if success:
                success_count = total_count
                error_count = 0
                self.logger.info(f"Load complete - {success_count} records loaded successfully")
            else:
                success_count = 0
                error_count = total_count
                errors.append("Batch load failed")
                self.logger.error("Load failed")
            
            # Perform reconciliation if enabled
            if self.config.get('enable_reconciliation', False):
                self._reconcile_data(df)
            
            return LoadResult(
                success_count=success_count,
                error_count=error_count,
                total_count=total_count,
                errors=errors
            )
            
        except Exception as e:
            self.logger.error(f"Load error: {str(e)}")
            return LoadResult(
                success_count=0,
                error_count=total_count,
                total_count=total_count,
                errors=[str(e)]
            )
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to database using JDBC
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            Success flag
        """
        try:
            jdbc_config = self.config['jdbc']
            target_table = self.config['target_table']
            
            # Map mode to Spark save mode
            save_mode_map = {
                'INSERT': 'append',
                'UPDATE': 'overwrite',
                'UPSERT': 'append'  # Will handle upsert with merge in post-processing
            }
            save_mode = save_mode_map.get(mode, 'append')
            
            # Write to database
            df.write \
                .format("jdbc") \
                .option("url", jdbc_config['url']) \
                .option("dbtable", target_table) \
                .option("user", jdbc_config['user']) \
                .option("password", jdbc_config['password']) \
                .option("driver", jdbc_config['driver']) \
                .option("batchsize", self.batch_size) \
                .mode(save_mode) \
                .save()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Database load error: {str(e)}")
            return False
    
    def _reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data against source
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            Match status
        """
        self.logger.info("Starting data reconciliation")
        
        try:
            jdbc_config = self.config['jdbc']
            target_table = self.config['target_table']
            
            # Read back from target
            target_df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_config['url']) \
                .option("dbtable", f"(SELECT * FROM {target_table} WHERE etl_run_id = '{self.run_id}') AS target") \
                .option("user", jdbc_config['user']) \
                .option("password", jdbc_config['password']) \
                .option("driver", jdbc_config['driver']) \
                .load()
            
            source_count = loaded_df.count()
            target_count = target_df.count()
            
            if source_count == target_count:
                self.logger.info(f"Reconciliation passed: {source_count} records match")
                return True
            else:
                self.logger.warning(
                    f"Reconciliation mismatch: Source={source_count}, Target={target_count}"
                )
                return False
                
        except Exception as e:
            self.logger.error(f"Reconciliation error: {str(e)}")
            return False