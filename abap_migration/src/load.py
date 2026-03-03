"""
ETL Load Module
Implements data loading with batch processing, reconciliation, and error handling.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from typing import Dict, Any, List
from dataclasses import dataclass

from src.logger import ETLLogger


@dataclass
class LoadResult:
    """Result of load operation."""
    success_count: int
    error_count: int
    total_count: int
    errors: List[str]


class ETLLoader:
    """Handles data loading to target systems with batch processing."""
    
    def __init__(
        self,
        spark: SparkSession,
        target_type: str = "DATABASE",
        batch_size: int = 1000,
        run_id: str = None,
        config: Dict[str, Any] = None
    ):
        """
        Initialize the ETL Loader.
        
        Args:
            spark: SparkSession instance
            target_type: Type of target (DATABASE, FILE, etc.)
            batch_size: Number of records per batch
            run_id: Unique identifier for this ETL run
            config: Configuration dictionary
        """
        self.spark = spark
        self.target_type = target_type.upper()
        self.batch_size = batch_size
        self.run_id = run_id
        self.config = config or {}
        self.logger = ETLLogger.get_instance()
    
    def load_data(
        self,
        df: DataFrame,
        mode: str = "INSERT"
    ) -> LoadResult:
        """
        Load data to target system.
        
        Args:
            df: DataFrame to load
            mode: Load mode (INSERT, UPDATE, UPSERT)
            
        Returns:
            LoadResult with operation statistics
        """
        self.logger.log_info(
            component="LOADER",
            message=f"Starting load - Mode: {mode}, Batch size: {self.batch_size}"
        )
        
        try:
            total_count = df.count()
            errors = []
            
            # Load based on target type
            if self.target_type == "DATABASE":
                success = self._load_to_database(df, mode)
            else:
                success = self._load_to_file(df, mode)
            
            if success:
                success_count = total_count
                error_count = 0
                
                # Perform reconciliation if enabled
                if self.config.get("enable_reconciliation", True):
                    if not self._reconcile_data(df):
                        self.logger.log_warning(
                            component="LOADER",
                            message="Data reconciliation failed"
                        )
            else:
                success_count = 0
                error_count = total_count
                errors.append("Load operation failed")
            
            result = LoadResult(
                success_count=success_count,
                error_count=error_count,
                total_count=total_count,
                errors=errors
            )
            
            self.logger.log_info(
                component="LOADER",
                message=f"Load complete - Success: {success_count}, Errors: {error_count}"
            )
            
            return result
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Load failed",
                details=str(e)
            )
            return LoadResult(
                success_count=0,
                error_count=total_count,
                total_count=total_count,
                errors=[str(e)]
            )
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to database target.
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            True if successful
        """
        try:
            jdbc_config = self.config.get("target_jdbc", {})
            table_name = jdbc_config.get("table", "etl_target_data")
            
            # Map mode to Spark save mode
            save_mode_map = {
                "INSERT": "append",
                "UPDATE": "overwrite",
                "UPSERT": "append"  # Will be handled with merge logic
            }
            save_mode = save_mode_map.get(mode, "append")
            
            # Write to database
            df.write \
                .format("jdbc") \
                .option("url", jdbc_config.get("url")) \
                .option("dbtable", table_name) \
                .option("user", jdbc_config.get("user")) \
                .option("password", jdbc_config.get("password")) \
                .option("driver", jdbc_config.get("driver", "org.postgresql.Driver")) \
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
    
    def _load_to_file(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to file target.
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            True if successful
        """
        try:
            output_config = self.config.get("output", {})
            output_path = output_config.get("path", "/tmp/etl_output")
            output_format = output_config.get("format", "parquet")
            
            # Write to file
            df.write \
                .format(output_format) \
                .mode("overwrite" if mode == "UPDATE" else "append") \
                .partitionBy("category") \
                .save(f"{output_path}/run_id={self.run_id}")
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="File load error",
                details=str(e)
            )
            return False
    
    def _reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data with source.
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passes
        """
        try:
            # Read back from target
            jdbc_config = self.config.get("target_jdbc", {})
            table_name = jdbc_config.get("table", "etl_target_data")
            
            target_df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_config.get("url")) \
                .option("dbtable", f"(SELECT * FROM {table_name} WHERE etl_run_id = '{self.run_id}') as reconcile_data") \
                .option("user", jdbc_config.get("user")) \
                .option("password", jdbc_config.get("password")) \
                .option("driver", jdbc_config.get("driver")) \
                .load()
            
            source_count = loaded_df.count()
            target_count = target_df.count()
            
            matches = source_count == target_count
            
            if not matches:
                self.logger.log_warning(
                    component="LOADER",
                    message=f"Reconciliation mismatch - Source: {source_count}, Target: {target_count}"
                )
            
            return matches
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Reconciliation error",
                details=str(e)
            )
            return False