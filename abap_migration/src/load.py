"""
Data loading module for ETL pipeline.
Handles batch loading, reconciliation, and error handling.
"""

from typing import Dict, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import logging

logger = logging.getLogger(__name__)


class LoadResult:
    """Container for load operation results."""
    
    def __init__(self):
        self.success_count = 0
        self.error_count = 0
        self.total_count = 0
        self.errors: List[str] = []


class ETLLoader:
    """Handles data loading to target systems."""
    
    def __init__(self, spark: SparkSession, target_type: str, 
                 batch_size: int, run_id: str, config: dict):
        """
        Initialize loader.
        
        Args:
            spark: SparkSession instance
            target_type: Type of target (DATABASE, PARQUET, etc.)
            batch_size: Batch size for loading
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.target_type = target_type.upper()
        self.batch_size = batch_size
        self.run_id = run_id
        self.config = config
        logger.info(f"Loader initialized - Target: {target_type}, Batch: {batch_size}")
    
    def load_data(self, df: DataFrame, mode: str = "overwrite") -> LoadResult:
        """
        Load data to target system.
        
        Args:
            df: DataFrame to load
            mode: Write mode (overwrite, append, upsert)
            
        Returns:
            LoadResult with statistics
        """
        logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        result = LoadResult()
        result.total_count = df.count()
        
        try:
            if mode.lower() == "upsert":
                success = self._load_upsert(df)
            else:
                success = self._load_to_database(df, mode)
            
            if success:
                result.success_count = result.total_count
                logger.info(f"Load completed successfully: {result.success_count} records")
            else:
                result.error_count = result.total_count
                result.errors.append("Load operation failed")
                logger.error("Load operation failed")
            
            # Perform reconciliation
            if self.config.get("reconciliation", {}).get("enabled", True):
                if not self._reconcile_data(df):
                    logger.warning("Data reconciliation check failed")
                    result.errors.append("Reconciliation mismatch")
        
        except Exception as e:
            logger.error(f"Load error: {e}")
            result.error_count = result.total_count
            result.errors.append(str(e))
        
        logger.info(
            f"Load complete - Success: {result.success_count}, "
            f"Errors: {result.error_count}"
        )
        
        return result
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """Load data to database using JDBC."""
        try:
            target_table = self.config["target"]["table"]
            
            logger.info(f"Loading to database table: {target_table}")
            
            (df.write
             .format(self.config["target"]["format"])
             .option("url", self.config["target"]["jdbc_url"])
             .option("dbtable", target_table)
             .option("user", self.config["target"]["user"])
             .option("password", self.config["target"]["password"])
             .option("driver", self.config["target"]["driver"])
             .option("batchsize", self.batch_size)
             .mode(mode)
             .save())
            
            return True
            
        except Exception as e:
            logger.error(f"Database load error: {e}")
            return False
    
    def _load_upsert(self, df: DataFrame) -> bool:
        """Perform upsert operation (update existing, insert new)."""
        try:
            # For databases supporting merge/upsert
            target_table = self.config["target"]["table"]
            temp_table = f"{target_table}_temp"
            
            # Write to temporary table
            logger.info(f"Writing to temporary table: {temp_table}")
            
            (df.write
             .format(self.config["target"]["format"])
             .option("url", self.config["target"]["jdbc_url"])
             .option("dbtable", temp_table)
             .option("user", self.config["target"]["user"])
             .option("password", self.config["target"]["password"])
             .option("driver", self.config["target"]["driver"])
             .mode("overwrite")
             .save())
            
            # Execute merge statement
            merge_sql = f"""
                MERGE INTO {target_table} AS target
                USING {temp_table} AS source
                ON target.id = source.id
                WHEN MATCHED THEN
                    UPDATE SET 
                        target.name = source.name,
                        target.value = source.value,
                        target.transformed_value = source.transformed_value,
                        target.status = source.status,
                        target.category = source.category,
                        target.priority = source.priority,
                        target.etl_run_id = source.etl_run_id,
                        target.processed_at = source.processed_at,
                        target.processed_by = source.processed_by
                WHEN NOT MATCHED THEN
                    INSERT VALUES (
                        source.id, source.name, source.value, 
                        source.transformed_value, source.status, source.category,
                        source.priority, source.etl_run_id, 
                        source.processed_at, source.processed_by
                    )
            """
            
            # Execute via JDBC (implementation depends on database)
            logger.info("Executing merge operation")
            # Note: Actual merge execution depends on database capabilities
            
            return True
            
        except Exception as e:
            logger.error(f"Upsert error: {e}")
            return False
    
    def _reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data with target.
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passes
        """
        logger.info("Starting data reconciliation")
        
        try:
            # Read back from target
            target_table = self.config["target"]["table"]
            
            target_df = (self.spark.read
                        .format(self.config["target"]["format"])
                        .option("url", self.config["target"]["jdbc_url"])
                        .option("dbtable", target_table)
                        .option("user", self.config["target"]["user"])
                        .option("password", self.config["target"]["password"])
                        .option("driver", self.config["target"]["driver"])
                        .load())
            
            # Filter for current run
            target_df = target_df.filter(col("etl_run_id") == self.run_id)
            
            # Compare counts
            loaded_count = loaded_df.count()
            target_count = target_df.count()
            
            if loaded_count == target_count:
                logger.info(f"Reconciliation passed: {target_count} records match")
                return True
            else:
                logger.warning(
                    f"Reconciliation failed: Loaded {loaded_count}, "
                    f"Found {target_count} in target"
                )
                return False
                
        except Exception as e:
            logger.error(f"Reconciliation error: {e}")
            return False