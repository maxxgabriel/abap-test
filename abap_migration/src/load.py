"""
Data loading module for PySpark ETL framework.
Handles data persistence to target systems with reconciliation support.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from typing import Dict, Any, Optional
import logging


class DataLoader:
    """Loads transformed data to target systems with error handling."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the data loader.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def load_data(
        self,
        df: DataFrame,
        mode: str = "append",
        enable_reconciliation: bool = True
    ) -> Dict[str, Any]:
        """
        Load data to target system.
        
        Args:
            df: DataFrame to load
            mode: Load mode (append, overwrite, upsert)
            enable_reconciliation: Whether to perform reconciliation
            
        Returns:
            Dictionary with load results
        """
        self.logger.info(f"Starting data load - Mode: {mode}, Run ID: {self.run_id}")
        
        total_count = df.count()
        success_count = 0
        error_count = 0
        errors = []
        
        try:
            target_type = self.config["target"]["type"]
            
            if target_type == "jdbc":
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
                if enable_reconciliation:
                    reconciled = self._reconcile_data(df)
                    if not reconciled:
                        self.logger.warning("Data reconciliation failed")
            else:
                error_count = total_count
                errors.append("Load operation failed")
                
        except Exception as e:
            error_count = total_count
            errors.append(str(e))
            self.logger.error(f"Load failed: {str(e)}")
        
        return {
            "success_count": success_count,
            "error_count": error_count,
            "total_count": total_count,
            "errors": errors
        }
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """Load data to JDBC database."""
        try:
            jdbc_config = self.config["target"]["jdbc"]
            
            write_mode = self._convert_mode(mode)
            
            df.write \
                .format("jdbc") \
                .option("url", jdbc_config["url"]) \
                .option("dbtable", jdbc_config["table"]) \
                .option("user", jdbc_config.get("user", "")) \
                .option("password", jdbc_config.get("password", "")) \
                .option("driver", jdbc_config.get("driver", "")) \
                .option("batchsize", self.config.get("load", {}).get("batch_size", 1000)) \
                .mode(write_mode) \
                .save()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Database load failed: {str(e)}")
            return False
    
    def _load_to_file(self, df: DataFrame, mode: str) -> bool:
        """Load data to file system."""
        try:
            file_config = self.config["target"]["file"]
            output_path = file_config["path"]
            file_format = file_config.get("format", "parquet")
            
            write_mode = self._convert_mode(mode)
            
            writer = df.write.format(file_format).mode(write_mode)
            
            # Add partitioning if configured
            partition_cols = file_config.get("partition_by", [])
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            writer.save(output_path)
            
            return True
            
        except Exception as e:
            self.logger.error(f"File load failed: {str(e)}")
            return False
    
    def _load_to_delta(self, df: DataFrame, mode: str) -> bool:
        """Load data to Delta Lake."""
        try:
            delta_config = self.config["target"]["delta"]
            delta_path = delta_config["path"]
            
            if mode == "upsert":
                # Perform merge operation for upsert
                from delta.tables import DeltaTable
                
                merge_keys = delta_config.get("merge_keys", ["id"])
                
                if DeltaTable.isDeltaTable(self.spark, delta_path):
                    delta_table = DeltaTable.forPath(self.spark, delta_path)
                    
                    # Build merge condition
                    merge_condition = " AND ".join([
                        f"target.{key} = source.{key}" for key in merge_keys
                    ])
                    
                    # Perform merge
                    delta_table.alias("target").merge(
                        df.alias("source"),
                        merge_condition
                    ).whenMatchedUpdateAll() \
                     .whenNotMatchedInsertAll() \
                     .execute()
                else:
                    # First load - create Delta table
                    df.write.format("delta").mode("overwrite").save(delta_path)
            else:
                write_mode = self._convert_mode(mode)
                df.write.format("delta").mode(write_mode).save(delta_path)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Delta load failed: {str(e)}")
            return False
    
    def _convert_mode(self, mode: str) -> str:
        """Convert load mode to Spark write mode."""
        mode_mapping = {
            "insert": "append",
            "update": "overwrite",
            "upsert": "append",
            "append": "append",
            "overwrite": "overwrite"
        }
        return mode_mapping.get(mode.lower(), "append")
    
    def _reconcile_data(self, df: DataFrame) -> bool:
        """
        Reconcile loaded data to ensure data integrity.
        
        Args:
            df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passed
        """
        try:
            expected_count = df.count()
            
            target_type = self.config["target"]["type"]
            
            if target_type == "jdbc":
                jdbc_config = self.config["target"]["jdbc"]
                
                # Count records in target table
                actual_df = self.spark.read \
                    .format("jdbc") \
                    .option("url", jdbc_config["url"]) \
                    .option("dbtable", f"(SELECT COUNT(*) as cnt FROM {jdbc_config['table']} WHERE etl_run_id = '{self.run_id}') as reconcile") \
                    .option("user", jdbc_config.get("user", "")) \
                    .option("password", jdbc_config.get("password", "")) \
                    .option("driver", jdbc_config.get("driver", "")) \
                    .load()
                
                actual_count = actual_df.first()["cnt"]
                
                if expected_count == actual_count:
                    self.logger.info(f"Reconciliation passed: {expected_count} records")
                    return True
                else:
                    self.logger.error(f"Reconciliation failed: Expected {expected_count}, Found {actual_count}")
                    return False
            else:
                # For file-based targets, reconciliation is not performed
                self.logger.info("Reconciliation skipped for file-based target")
                return True
                
        except Exception as e:
            self.logger.error(f"Reconciliation error: {str(e)}")
            return False