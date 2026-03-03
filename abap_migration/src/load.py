"""
ETL Load Module - Data Loading and Reconciliation
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from typing import Dict, Any
import logging


class ETLLoader:
    """Handles data loading to target systems"""
    
    def __init__(self, spark: SparkSession, config: dict, run_id: str):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.batch_size = config['load']['batch_size']
        self.logger = logging.getLogger(__name__)
    
    def load_data(
        self,
        df: DataFrame,
        mode: str = "overwrite"
    ) -> Dict[str, Any]:
        """Main loading method"""
        self.logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        result = {
            'success_count': 0,
            'error_count': 0,
            'total_count': df.count(),
            'errors': []
        }
        
        try:
            # Load to target
            success = self._load_to_target(df, mode)
            
            if success:
                result['success_count'] = result['total_count']
                
                # Reconcile if enabled
                if self.config['load'].get('enable_reconciliation', True):
                    reconciled = self._reconcile_data(df)
                    if not reconciled:
                        self.logger.warning("Data reconciliation failed")
                        result['errors'].append("Reconciliation mismatch detected")
            else:
                result['error_count'] = result['total_count']
                result['errors'].append("Load operation failed")
            
            self.logger.info(
                f"Load complete - Success: {result['success_count']}, "
                f"Errors: {result['error_count']}"
            )
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            result['error_count'] = result['total_count']
            result['errors'].append(str(e))
        
        return result
    
    def _load_to_target(self, df: DataFrame, mode: str) -> bool:
        """Load data to target destination"""
        try:
            target_config = self.config['targets']['database']
            
            df.write \
                .format(target_config['format']) \
                .mode(mode) \
                .option("url", target_config['url']) \
                .option("dbtable", target_config['table']) \
                .option("user", target_config.get('user', '')) \
                .option("password", target_config.get('password', '')) \
                .save()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Target load failed: {str(e)}")
            return False
    
    def _reconcile_data(self, loaded_df: DataFrame) -> bool:
        """Reconcile loaded data against source"""
        try:
            target_config = self.config['targets']['database']
            
            # Read back from target
            target_df = self.spark.read \
                .format(target_config['format']) \
                .option("url", target_config['url']) \
                .option("dbtable", target_config['table']) \
                .option("user", target_config.get('user', '')) \
                .option("password", target_config.get('password', '')) \
                .load()
            
            # Filter for current run
            target_df = target_df.filter(col("etl_run_id") == self.run_id)
            
            source_count = loaded_df.count()
            target_count = target_df.count()
            
            if source_count == target_count:
                self.logger.info(f"Reconciliation successful: {source_count} records match")
                return True
            else:
                self.logger.error(
                    f"Reconciliation failed: Source={source_count}, Target={target_count}"
                )
                return False
                
        except Exception as e:
            self.logger.error(f"Reconciliation error: {str(e)}")
            return False
    
    def load_to_staging(self, df: DataFrame, staging_path: str) -> bool:
        """Load to staging area"""
        try:
            df.write \
                .mode("overwrite") \
                .parquet(f"{staging_path}/{self.run_id}")
            
            self.logger.info(f"Data staged successfully at {staging_path}/{self.run_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Staging failed: {str(e)}")
            return False