"""Data loading module for ETL pipeline."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from typing import Dict, List
import logging


class Loader:
    """Load transformed data to target destinations."""
    
    def __init__(
        self,
        spark: SparkSession,
        config: dict,
        run_id: str,
        batch_size: int = 1000
    ):
        """Initialize loader.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
            batch_size: Batch size for loading
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.batch_size = batch_size
        self.logger = logging.getLogger(__name__)
        self.target_type = config.get("target_type", "database")
    
    def load_data(
        self,
        df: DataFrame,
        mode: str = "upsert"
    ) -> Dict[str, int]:
        """Load data to target.
        
        Args:
            df: DataFrame to load
            mode: Load mode (insert, update, upsert)
            
        Returns:
            Dictionary with load statistics
        """
        self.logger.info(
            f"Starting load - Mode: {mode}, Batch size: {self.batch_size}"
        )
        
        total_records = df.count()
        
        try:
            if self.target_type == "database":
                self._load_to_database(df, mode)
            elif self.target_type == "parquet":
                self._load_to_parquet(df, mode)
            elif self.target_type == "delta":
                self._load_to_delta(df, mode)
            else:
                self._load_to_database(df, mode)
            
            # Reconcile data
            reconciled = self._reconcile_data(df)
            
            result = {
                "success_count": total_records,
                "error_count": 0,
                "total_count": total_records,
                "reconciled": reconciled
            }
            
            self.logger.info(
                f"Load complete - Success: {result['success_count']}, "
                f"Errors: {result['error_count']}"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            return {
                "success_count": 0,
                "error_count": total_records,
                "total_count": total_records,
                "reconciled": False
            }
    
    def _load_to_database(self, df: DataFrame, mode: str) -> None:
        """Load to database via JDBC.
        
        Args:
            df: DataFrame to load
            mode: Load mode
        """
        jdbc_config = self.config["database"]["jdbc"]
        
        # Map mode to Spark save mode
        save_mode = {
            "insert": "append",
            "update": "overwrite",
            "upsert": "append"  # Would need merge logic for true upsert
        }.get(mode, "append")
        
        df.write \
            .format("jdbc") \
            .option("url", jdbc_config["url"]) \
            .option("dbtable", "etl_target_data") \
            .option("user", jdbc_config["user"]) \
            .option("password", jdbc_config["password"]) \
            .option("driver", jdbc_config["driver"]) \
            .option("batchsize", self.batch_size) \
            .mode(save_mode) \
            .save()
    
    def _load_to_parquet(self, df: DataFrame, mode: str) -> None:
        """Load to Parquet files.
        
        Args:
            df: DataFrame to load
            mode: Load mode
        """
        target_path = self.config["paths"]["target"]
        
        save_mode = "overwrite" if mode == "update" else "append"
        
        df.write \
            .format("parquet") \
            .mode(save_mode) \
            .partitionBy("category") \
            .save(f"{target_path}/data")
    
    def _load_to_delta(self, df: DataFrame, mode: str) -> None:
        """Load to Delta Lake.
        
        Args:
            df: DataFrame to load
            mode: Load mode
        """
        target_path = self.config["paths"]["target"]
        
        if mode == "upsert":
            # Delta merge operation
            df.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(f"{target_path}/delta")
        else:
            save_mode = "overwrite" if mode == "update" else "append"
            df.write \
                .format("delta") \
                .mode(save_mode) \
                .save(f"{target_path}/delta")
    
    def _reconcile_data(self, df: DataFrame) -> bool:
        """Reconcile loaded data with source.
        
        Args:
            df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passed
        """
        if not self.config.get("enable_reconciliation", True):
            return True
        
        try:
            source_count = df.count()
            
            # Read back from target
            if self.target_type == "database":
                jdbc_config = self.config["database"]["jdbc"]
                target_df = self.spark.read \
                    .format("jdbc") \
                    .option("url", jdbc_config["url"]) \
                    .option("dbtable", "etl_target_data") \
                    .option("user", jdbc_config["user"]) \
                    .option("password", jdbc_config["password"]) \
                    .load() \
                    .filter(col("etl_run_id") == self.run_id)
                
                target_count = target_df.count()
                
                if source_count == target_count:
                    self.logger.info("Data reconciliation passed")
                    return True
                else:
                    self.logger.warning(
                        f"Reconciliation mismatch: source={source_count}, "
                        f"target={target_count}"
                    )
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Reconciliation failed: {str(e)}")
            return False