"""
ETL Loader Module
Handles data loading to target systems
"""
from typing import Dict
from pyspark.sql import DataFrame, SparkSession

from src.utils.logger import ETLLogger


class ETLLoader:
    """Loads data to target systems"""
    
    def __init__(self, spark: SparkSession, config: dict, run_id: str):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger(run_id=run_id)
    
    def load_data(
        self,
        df: DataFrame,
        target_type: str = "DATABASE",
        mode: str = "upsert",
        batch_size: int = 1000
    ) -> Dict[str, int]:
        """
        Load data to target
        
        Args:
            df: DataFrame to load
            target_type: Target type (DATABASE, PARQUET, DELTA)
            mode: Write mode (insert, update, upsert, overwrite)
            batch_size: Batch size for loading
            
        Returns:
            Dictionary with success_count and error_count
        """
        self.logger.log_info(
            component="LOADER",
            message=f"Starting load - Mode: {mode}, Target: {target_type}"
        )
        
        total_count = df.count()
        
        try:
            if target_type == "DATABASE":
                self._load_to_database(df, mode)
            elif target_type == "PARQUET":
                self._load_to_parquet(df, mode)
            elif target_type == "DELTA":
                self._load_to_delta(df, mode)
            else:
                self._load_to_database(df, mode)
            
            self.logger.log_info(
                component="LOADER",
                message=f"Load complete - {total_count} records loaded"
            )
            
            return {
                "success_count": total_count,
                "error_count": 0,
                "total_count": total_count
            }
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Load failed",
                details=str(e)
            )
            return {
                "success_count": 0,
                "error_count": total_count,
                "total_count": total_count
            }
    
    def _load_to_database(self, df: DataFrame, mode: str):
        """Load to database"""
        write_mode = "append" if mode in ["insert", "upsert"] else "overwrite"
        
        df.write \
            .format("jdbc") \
            .option("url", self.config["database"]["url"]) \
            .option("dbtable", self.config["database"]["target_table"]) \
            .option("user", self.config["database"]["user"]) \
            .option("password", self.config["database"]["password"]) \
            .mode(write_mode) \
            .save()
    
    def _load_to_parquet(self, df: DataFrame, mode: str):
        """Load to Parquet"""
        write_mode = "append" if mode in ["insert", "upsert"] else "overwrite"
        
        df.write \
            .format("parquet") \
            .mode(write_mode) \
            .save(self.config["output"]["parquet_path"])
    
    def _load_to_delta(self, df: DataFrame, mode: str):
        """Load to Delta Lake"""
        write_mode = "append" if mode in ["insert", "upsert"] else "overwrite"
        
        df.write \
            .format("delta") \
            .mode(write_mode) \
            .save(self.config["output"]["delta_path"])