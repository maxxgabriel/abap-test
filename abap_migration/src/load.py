"""
ETL Loader Module
Loads transformed data to target systems
Migrated from zcl_etl_loader.abap
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from typing import Dict, List
import logging


class LoadResult:
    """Container for load operation results"""
    def __init__(self):
        self.success_count: int = 0
        self.error_count: int = 0
        self.total_count: int = 0
        self.errors: List[str] = []


class ETLLoader:
    """Loads data to target destinations"""
    
    def __init__(self, spark: SparkSession, target_type: str = "DATABASE", 
                 batch_size: int = 1000, run_id: str = ""):
        """
        Initialize loader
        
        Args:
            spark: SparkSession instance
            target_type: Type of target (DATABASE, FILE, etc.)
            batch_size: Records per batch
            run_id: Unique run identifier
        """
        self.spark = spark
        self.target_type = target_type if target_type else "DATABASE"
        self.batch_size = batch_size
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
    
    def load_data(self, df: DataFrame, mode: str = "INSERT") -> LoadResult:
        """
        Main load method
        
        Args:
            df: DataFrame to load
            mode: Load mode (INSERT, UPDATE, UPSERT)
            
        Returns:
            LoadResult with operation statistics
        """
        result = LoadResult()
        result.total_count = df.count()
        
        self.logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        try:
            # Process in batches using repartition
            num_partitions = max(1, result.total_count // self.batch_size)
            df_partitioned = df.repartition(num_partitions)
            
            # Load to database
            success = self._load_to_database(df_partitioned, mode)
            
            if success:
                result.success_count = result.total_count
                self.logger.info(f"Load complete - Success: {result.success_count}")
            else:
                result.error_count = result.total_count
                result.errors.append("Database load failed")
                self.logger.error(f"Load failed - Errors: {result.error_count}")
            
            # Reconcile data
            if success and self._reconcile_data(df):
                self.logger.info("Data reconciliation successful")
            else:
                self.logger.warning("Data reconciliation failed")
            
        except Exception as e:
            result.error_count = result.total_count
            result.errors.append(str(e))
            self.logger.error(f"Load error: {str(e)}")
        
        return result
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """
        Load DataFrame to database
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            Success status
        """
        try:
            # Map mode to Spark mode
            spark_mode = self._map_load_mode(mode)
            
            # Write to target table
            df.write \
                .format("jdbc") \
                .option("url", self._get_jdbc_url()) \
                .option("dbtable", "zetl_target_data") \
                .option("driver", "org.postgresql.Driver") \
                .mode(spark_mode) \
                .save()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Database load error: {str(e)}")
            return False
    
    def _map_load_mode(self, mode: str) -> str:
        """Map ABAP load mode to Spark mode"""
        mode_map = {
            "INSERT": "append",
            "UPDATE": "overwrite",
            "UPSERT": "append"  # Handle upsert logic in database
        }
        return mode_map.get(mode, "append")
    
    def _reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            Reconciliation success status
        """
        try:
            # Read back from target
            target_df = self.spark.read \
                .format("jdbc") \
                .option("url", self._get_jdbc_url()) \
                .option("dbtable", "zetl_target_data") \
                .option("driver", "org.postgresql.Driver") \
                .load() \
                .filter(col("etl_run_id") == lit(self.run_id))
            
            loaded_count = loaded_df.count()
            target_count = target_df.count()
            
            matches = loaded_count == target_count
            
            if not matches:
                self.logger.warning(
                    f"Reconciliation mismatch: Loaded={loaded_count}, Target={target_count}"
                )
            
            return matches
            
        except Exception as e:
            self.logger.error(f"Reconciliation error: {str(e)}")
            return False
    
    def _get_jdbc_url(self) -> str:
        """Get JDBC connection URL"""
        return "jdbc:postgresql://localhost:5432/etl_db"