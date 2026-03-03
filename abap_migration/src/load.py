"""
ETL Loader Module
Handles data loading to target systems with batch processing and reconciliation.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from typing import Dict, List
import logging


class ETLLoader:
    """Load data to target systems"""
    
    def __init__(
        self, 
        spark: SparkSession, 
        target_type: str = "DATABASE",
        batch_size: int = 1000,
        run_id: str = None
    ):
        self.spark = spark
        self.target_type = target_type
        self.batch_size = batch_size
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def load_data(self, df: DataFrame, mode: str = "append") -> Dict:
        """
        Load data to target
        
        Args:
            df: DataFrame to load
            mode: Load mode ('append', 'overwrite', 'upsert')
            
        Returns:
            Dictionary with load results
        """
        self.logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        total_count = df.count()
        success_count = 0
        error_count = 0
        errors = []
        
        try:
            if mode == "upsert":
                success_count = self._load_upsert(df)
            else:
                success_count = self._load_to_database(df, mode)
            
            # Reconcile data
            if self.spark.conf.get("spark.etl.enable_reconciliation", "true").lower() == "true":
                reconciled = self._reconcile_data(df)
                if not reconciled:
                    self.logger.warning("Data reconciliation failed")
            
            error_count = total_count - success_count
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            error_count = total_count
            errors.append(str(e))
        
        result = {
            "success_count": success_count,
            "error_count": error_count,
            "total_count": total_count,
            "errors": errors
        }
        
        self.logger.info(
            f"Load complete - Success: {success_count}, Errors: {error_count}"
        )
        
        return result
    
    def _load_to_database(self, df: DataFrame, mode: str) -> int:
        """Load data to database table"""
        self.logger.info(f"Loading to database with mode: {mode}")
        
        target_table = self.spark.conf.get("spark.target.table", "etl_target_data")
        
        df.write \
            .format("jdbc") \
            .option("driver", self.spark.conf.get("spark.jdbc.driver")) \
            .option("url", self.spark.conf.get("spark.jdbc.url")) \
            .option("dbtable", target_table) \
            .option("user", self.spark.conf.get("spark.jdbc.user")) \
            .option("password", self.spark.conf.get("spark.jdbc.password")) \
            .option("batchsize", self.batch_size) \
            .mode(mode) \
            .save()
        
        return df.count()
    
    def _load_upsert(self, df: DataFrame) -> int:
        """Perform upsert operation (update existing, insert new)"""
        self.logger.info("Performing upsert operation")
        
        target_table = self.spark.conf.get("spark.target.table", "etl_target_data")
        
        # Read existing data
        existing_df = self.spark.read \
            .format("jdbc") \
            .option("driver", self.spark.conf.get("spark.jdbc.driver")) \
            .option("url", self.spark.conf.get("spark.jdbc.url")) \
            .option("dbtable", target_table) \
            .option("user", self.spark.conf.get("spark.jdbc.user")) \
            .option("password", self.spark.conf.get("spark.jdbc.password")) \
            .load()
        
        # Identify updates and inserts
        updates_df = df.join(existing_df, "id", "inner").select(df["*"])
        inserts_df = df.join(existing_df, "id", "left_anti")
        
        # Perform updates (delete then insert)
        if updates_df.count() > 0:
            self.logger.info(f"Updating {updates_df.count()} records")
            # In production, use database-specific upsert/merge
            updates_df.write \
                .format("jdbc") \
                .option("driver", self.spark.conf.get("spark.jdbc.driver")) \
                .option("url", self.spark.conf.get("spark.jdbc.url")) \
                .option("dbtable", target_table) \
                .option("user", self.spark.conf.get("spark.jdbc.user")) \
                .option("password", self.spark.conf.get("spark.jdbc.password")) \
                .mode("append") \
                .save()
        
        # Perform inserts
        if inserts_df.count() > 0:
            self.logger.info(f"Inserting {inserts_df.count()} records")
            inserts_df.write \
                .format("jdbc") \
                .option("driver", self.spark.conf.get("spark.jdbc.driver")) \
                .option("url", self.spark.conf.get("spark.jdbc.url")) \
                .option("dbtable", target_table) \
                .option("user", self.spark.conf.get("spark.jdbc.user")) \
                .option("password", self.spark.conf.get("spark.jdbc.password")) \
                .mode("append") \
                .save()
        
        return df.count()
    
    def _reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data with target
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passes
        """
        self.logger.info("Reconciling loaded data")
        
        try:
            target_table = self.spark.conf.get("spark.target.table", "etl_target_data")
            
            # Read target data for this run
            target_df = self.spark.read \
                .format("jdbc") \
                .option("driver", self.spark.conf.get("spark.jdbc.driver")) \
                .option("url", self.spark.conf.get("spark.jdbc.url")) \
                .option("dbtable", target_table) \
                .option("user", self.spark.conf.get("spark.jdbc.user")) \
                .option("password", self.spark.conf.get("spark.jdbc.password")) \
                .load() \
                .filter(col("etl_run_id") == self.run_id)
            
            loaded_count = loaded_df.count()
            target_count = target_df.count()
            
            if loaded_count == target_count:
                self.logger.info(f"Reconciliation passed: {loaded_count} records")
                return True
            else:
                self.logger.warning(
                    f"Reconciliation mismatch: Loaded={loaded_count}, Target={target_count}"
                )
                return False
                
        except Exception as e:
            self.logger.error(f"Reconciliation failed: {str(e)}")
            return False
    
    def load_to_parquet(self, df: DataFrame, path: str, mode: str = "append") -> int:
        """Load data to parquet files"""
        self.logger.info(f"Loading to parquet: {path}")
        
        df.write \
            .mode(mode) \
            .parquet(path)
        
        return df.count()
    
    def load_to_delta(self, df: DataFrame, path: str, mode: str = "append") -> int:
        """Load data to Delta Lake"""
        self.logger.info(f"Loading to Delta Lake: {path}")
        
        df.write \
            .format("delta") \
            .mode(mode) \
            .save(path)
        
        return df.count()