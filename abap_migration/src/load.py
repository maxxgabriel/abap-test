"""
Data loading module for PySpark ETL pipeline.
Handles data loading to target systems with batch processing and reconciliation.
"""

from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any, List
import logging


class DataLoader:
    """Loads transformed data to target systems."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the loader.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
    
    def load_data(self, df: DataFrame, mode: str = "append") -> Dict[str, Any]:
        """
        Load data to target system.
        
        Args:
            df: DataFrame to load
            mode: Load mode (append, overwrite, upsert)
            
        Returns:
            Dictionary with load results
        """
        self.logger.info(f"Starting load - Mode: {mode}, Run ID: {self.run_id}")
        
        total_count = df.count()
        success_count = 0
        error_count = 0
        errors = []
        
        try:
            target_type = self.config.get("target", {}).get("type", "database")
            
            if target_type == "database":
                success = self._load_to_database(df, mode)
            elif target_type == "file":
                success = self._load_to_file(df, mode)
            elif target_type == "delta":
                success = self._load_to_delta(df, mode)
            else:
                raise ValueError(f"Unsupported target type: {target_type}")
            
            if success:
                success_count = total_count
                self.logger.info(f"Successfully loaded {success_count} records")
                
                # Perform reconciliation if enabled
                if self.config.get("processing", {}).get("enable_reconciliation", True):
                    self._reconcile_data(df)
            else:
                error_count = total_count
                errors.append("Load operation failed")
                
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
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """Load data to database target."""
        jdbc_config = self.config.get("target", {}).get("jdbc", {})
        table_name = self.config.get("target", {}).get("table", "target_data")
        batch_size = self.config.get("processing", {}).get("batch_size", 1000)
        
        try:
            # Map mode to JDBC mode
            jdbc_mode = self._map_load_mode(mode)
            
            df.write \
                .format("jdbc") \
                .option("url", jdbc_config.get("url")) \
                .option("dbtable", table_name) \
                .option("user", jdbc_config.get("user")) \
                .option("password", jdbc_config.get("password")) \
                .option("driver", jdbc_config.get("driver")) \
                .option("batchsize", batch_size) \
                .mode(jdbc_mode) \
                .save()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Database load error: {str(e)}")
            return False
    
    def _load_to_file(self, df: DataFrame, mode: str) -> bool:
        """Load data to file system."""
        output_path = self.config.get("target", {}).get("path")
        file_format = self.config.get("target", {}).get("format", "parquet")
        
        try:
            df.write \
                .format(file_format) \
                .mode(mode) \
                .save(output_path)
            
            return True
            
        except Exception as e:
            self.logger.error(f"File load error: {str(e)}")
            return False
    
    def _load_to_delta(self, df: DataFrame, mode: str) -> bool:
        """Load data to Delta Lake."""
        delta_path = self.config.get("target", {}).get("delta_path")
        
        try:
            if mode == "upsert":
                # Perform merge for upsert
                self._delta_merge(df, delta_path)
            else:
                df.write \
                    .format("delta") \
                    .mode(mode) \
                    .save(delta_path)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Delta load error: {str(e)}")
            return False
    
    def _delta_merge(self, df: DataFrame, delta_path: str):
        """Perform Delta merge for upsert operation."""
        from delta.tables import DeltaTable
        
        if DeltaTable.isDeltaTable(self.spark, delta_path):
            delta_table = DeltaTable.forPath(self.spark, delta_path)
            
            delta_table.alias("target") \
                .merge(
                    df.alias("source"),
                    "target.id = source.id"
                ) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
        else:
            # First load - create table
            df.write.format("delta").save(delta_path)
    
    def _map_load_mode(self, mode: str) -> str:
        """Map custom load mode to Spark/JDBC mode."""
        mode_mapping = {
            "insert": "append",
            "update": "overwrite",
            "upsert": "append",
            "append": "append",
            "overwrite": "overwrite"
        }
        return mode_mapping.get(mode.lower(), "append")
    
    def _reconcile_data(self, df: DataFrame):
        """Perform data reconciliation after load."""
        self.logger.info("Starting data reconciliation")
        
        try:
            source_count = df.count()
            
            # Read back from target
            target_type = self.config.get("target", {}).get("type", "database")
            
            if target_type == "database":
                jdbc_config = self.config.get("target", {}).get("jdbc", {})
                table_name = self.config.get("target", {}).get("table", "target_data")
                
                target_df = self.spark.read \
                    .format("jdbc") \
                    .option("url", jdbc_config.get("url")) \
                    .option("dbtable", f"(SELECT COUNT(*) as cnt FROM {table_name} WHERE etl_run_id = '{self.run_id}') as reconcile") \
                    .option("user", jdbc_config.get("user")) \
                    .option("password", jdbc_config.get("password")) \
                    .option("driver", jdbc_config.get("driver")) \
                    .load()
                
                target_count = target_df.first()["cnt"]
                
                if source_count == target_count:
                    self.logger.info(f"Reconciliation passed: {source_count} records match")
                else:
                    self.logger.warning(
                        f"Reconciliation mismatch: Source={source_count}, Target={target_count}"
                    )
            else:
                self.logger.info("Reconciliation skipped for non-database targets")
                
        except Exception as e:
            self.logger.warning(f"Reconciliation failed: {str(e)}")