"""
PySpark ETL Loader Module
Migrated from ABAP zcl_etl_loader
"""
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, List
from datetime import datetime
from src.logger import ETLLogger


class LoadResult:
    """Load operation result"""
    def __init__(self):
        self.success_count = 0
        self.error_count = 0
        self.total_count = 0
        self.errors: List[str] = []


class ETLLoader:
    """Load transformed data to target destination"""
    
    def __init__(self, spark: SparkSession, config: dict, run_id: str, 
                 target_type: str = "DATABASE", batch_size: int = 1000):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.target_type = target_type
        self.batch_size = batch_size
        self.logger = ETLLogger.get_instance()
    
    def load_data(self, df: DataFrame, mode: str = "INSERT") -> LoadResult:
        """
        Main load method with batch processing
        Converts ABAP batch processing LOOP to DataFrame partitioning
        """
        result = LoadResult()
        
        self.logger.log_info(
            component="LOADER",
            message=f"Starting load - Mode: {mode}, Batch size: {self.batch_size}"
        )
        
        try:
            result.total_count = df.count()
            
            # Load based on mode
            if self.load_to_database(df, mode):
                result.success_count = result.total_count
                result.error_count = 0
            else:
                result.success_count = 0
                result.error_count = result.total_count
                result.errors.append("Database load failed")
            
            # Reconcile loaded data
            if self._reconcile_data(df):
                self.logger.log_info(
                    component="LOADER",
                    message="Data reconciliation successful"
                )
            else:
                self.logger.log_warning(
                    component="LOADER",
                    message="Data reconciliation failed"
                )
            
            self.logger.log_info(
                component="LOADER",
                message=f"Load complete - Success: {result.success_count}, Errors: {result.error_count}"
            )
            
            return result
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Load operation failed",
                details=str(e)
            )
            result.error_count = result.total_count
            result.errors.append(str(e))
            return result
    
    def load_to_database(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to database
        Converts ABAP INSERT/UPDATE/UPSERT to Spark save modes
        """
        try:
            jdbc_config = self.config['target']['jdbc']
            
            # Map ABAP modes to Spark save modes
            # ABAP 'INSERT' -> 'append'
            # ABAP 'UPDATE' -> 'overwrite'
            # ABAP 'UPSERT' -> custom logic
            if mode == "INSERT":
                save_mode = "append"
            elif mode == "UPDATE":
                save_mode = "overwrite"
            elif mode == "UPSERT":
                # For UPSERT, use overwrite with merge logic
                save_mode = "append"
                # In production, implement proper MERGE logic
            else:
                save_mode = "append"
            
            # Write to target with batch processing
            df.write \
                .format("jdbc") \
                .option("url", jdbc_config['url']) \
                .option("dbtable", jdbc_config['table']) \
                .option("user", jdbc_config['user']) \
                .option("password", jdbc_config['password']) \
                .option("driver", jdbc_config['driver']) \
                .option("batchsize", self.batch_size) \
                .mode(save_mode) \
                .save()
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Database load error",
                details=str(e)
            )
            return False
    
    def _reconcile_data(self, df: DataFrame) -> bool:
        """
        Reconcile loaded data with source
        Verify record counts match
        """
        try:
            source_count = df.count()
            
            # Read back from target to verify
            jdbc_config = self.config['target']['jdbc']
            
            target_df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_config['url']) \
                .option("dbtable", f"(SELECT COUNT(*) as cnt FROM {jdbc_config['table']} WHERE etl_run_id = '{self.run_id}') AS counts") \
                .option("user", jdbc_config['user']) \
                .option("password", jdbc_config['password']) \
                .option("driver", jdbc_config['driver']) \
                .load()
            
            target_count = target_df.first()['cnt']
            
            return source_count == target_count
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Reconciliation error",
                details=str(e)
            )
            return False