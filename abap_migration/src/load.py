"""
ETL Data Loading Module
Loads transformed data to target systems
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from typing import Dict, Any, List
import logging


class LoadResult:
    """Result of load operation"""
    def __init__(self):
        self.success_count = 0
        self.error_count = 0
        self.total_count = 0
        self.errors: List[str] = []


class ETLLoader:
    """Handles data loading to target systems"""
    
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
        self.target_type = config.get('loading', {}).get('target_type', 'DATABASE')
        self.batch_size = config.get('loading', {}).get('batch_size', 1000)
        
    def load_data(self, df: DataFrame, mode: str = 'INSERT') -> LoadResult:
        """
        Load data to target
        
        Args:
            df: DataFrame to load
            mode: Load mode (INSERT, UPDATE, UPSERT)
            
        Returns:
            LoadResult with statistics
        """
        result = LoadResult()
        result.total_count = df.count()
        
        self.logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        try:
            if self.target_type == 'DATABASE':
                success = self.load_to_database(df, mode)
            else:
                success = self.load_to_database(df, mode)
            
            if success:
                result.success_count = result.total_count
                self.logger.info(f"Load complete - Success: {result.success_count}")
            else:
                result.error_count = result.total_count
                result.errors.append("Load operation failed")
                self.logger.error("Load operation failed")
            
            # Reconcile data if enabled
            if self.config.get('loading', {}).get('enable_reconciliation', True):
                if not self.reconcile_data(df):
                    self.logger.warning("Data reconciliation failed")
                    result.errors.append("Reconciliation check failed")
            
        except Exception as e:
            result.error_count = result.total_count
            result.errors.append(str(e))
            self.logger.error(f"Load error: {str(e)}")
        
        return result
    
    def load_to_database(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to database
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            Success flag
        """
        try:
            jdbc_config = self.config['loading']['jdbc']
            
            # Convert mode to Spark write mode
            spark_mode_map = {
                'INSERT': 'append',
                'UPDATE': 'overwrite',
                'UPSERT': 'append'  # Will use merge logic
            }
            spark_mode = spark_mode_map.get(mode, 'append')
            
            # Prepare data for target format
            df_target = df.select(
                col("id"),
                col("name"),
                col("value"),
                col("transformed_value"),
                col("status"),
                col("category"),
                col("priority"),
                col("etl_run_id"),
                col("processed_at"),
                col("processed_by")
            )
            
            # Write to database
            df_target.write \
                .format("jdbc") \
                .option("url", jdbc_config['url']) \
                .option("dbtable", jdbc_config['target_table']) \
                .option("user", jdbc_config['user']) \
                .option("password", jdbc_config['password']) \
                .option("driver", jdbc_config['driver']) \
                .option("batchsize", self.batch_size) \
                .mode(spark_mode) \
                .save()
            
            self.logger.info(f"Successfully loaded {df.count()} records to database")
            return True
            
        except Exception as e:
            self.logger.error(f"Database load error: {str(e)}")
            return False
    
    def reconcile_data(self, df: DataFrame) -> bool:
        """
        Reconcile loaded data
        
        Args:
            df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passes
        """
        self.logger.info("Starting data reconciliation")
        
        try:
            jdbc_config = self.config['loading']['jdbc']
            
            # Get record IDs from source
            source_ids = df.select("id").distinct().count()
            
            # Query target to verify records exist
            query = f"""(
                SELECT COUNT(DISTINCT id) as record_count
                FROM {jdbc_config['target_table']}
                WHERE etl_run_id = '{self.run_id}'
            ) as reconcile_data"""
            
            target_df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_config['url']) \
                .option("dbtable", query) \
                .option("user", jdbc_config['user']) \
                .option("password", jdbc_config['password']) \
                .option("driver", jdbc_config['driver']) \
                .load()
            
            target_count = target_df.collect()[0]['record_count']
            
            if source_ids == target_count:
                self.logger.info(f"Reconciliation passed: {source_ids} records matched")
                return True
            else:
                self.logger.warning(
                    f"Reconciliation failed: Source={source_ids}, Target={target_count}"
                )
                return False
                
        except Exception as e:
            self.logger.error(f"Reconciliation error: {str(e)}")
            return False