"""
ETL Data Loading Module
Loads transformed data into target systems with batch processing and reconciliation.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from typing import Dict, Any
from dataclasses import dataclass
from src.logger import ETLLogger


@dataclass
class LoadResult:
    """Result of load operation."""
    success_count: int
    error_count: int
    total_count: int
    errors: list


class ETLLoader:
    """Handles loading of transformed data to target systems."""
    
    def __init__(self, target_type: str = "DATABASE", batch_size: int = 1000,
                 run_id: str = None, config: dict = None):
        """
        Initialize the loader.
        
        Args:
            target_type: Type of target (DATABASE, FILE, etc.)
            batch_size: Number of records per batch
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.target_type = target_type
        self.batch_size = batch_size
        self.run_id = run_id
        self.config = config or {}
        self.logger = ETLLogger.get_instance()
        
    def load_data(self, df: DataFrame, mode: str = "upsert") -> LoadResult:
        """
        Load data to target system.
        
        Args:
            df: DataFrame to load
            mode: Load mode (insert, update, upsert)
            
        Returns:
            LoadResult object with statistics
        """
        self.logger.log_info(
            component="LOADER",
            message=f"Starting load - Mode: {mode}, Batch size: {self.batch_size}"
        )
        
        total_count = df.count()
        errors = []
        
        try:
            if self.target_type == "DATABASE":
                success = self._load_to_database(df, mode)
            else:
                success = self._load_to_file(df, mode)
            
            if success:
                success_count = total_count
                error_count = 0
            else:
                success_count = 0
                error_count = total_count
                errors.append("Load operation failed")
            
            # Reconcile data
            if self.config.get("processing", {}).get("enable_reconciliation", True):
                reconciled = self._reconcile_data(df)
                if not reconciled:
                    self.logger.log_warning(
                        component="LOADER",
                        message="Data reconciliation failed"
                    )
            
            self.logger.log_info(
                component="LOADER",
                message=f"Load complete - Success: {success_count}, Errors: {error_count}"
            )
            
            return LoadResult(
                success_count=success_count,
                error_count=error_count,
                total_count=total_count,
                errors=errors
            )
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Load operation failed",
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
            Success status
        """
        try:
            target_config = self.config.get("target", {})
            table_name = target_config.get("table", "target_data")
            
            # Get connection properties
            connection_props = {
                "driver": "org.postgresql.Driver",
                "user": target_config.get("connection", {}).get("user", "etl_user"),
                "password": target_config.get("connection", {}).get("password", ""),
            }
            
            jdbc_url = (
                f"jdbc:postgresql://{target_config.get('connection', {}).get('host', 'localhost')}:"
                f"{target_config.get('connection', {}).get('port', 5432)}/"
                f"{target_config.get('connection', {}).get('database', 'etl_target')}"
            )
            
            # Map mode to Spark write mode
            write_mode = {
                "insert": "append",
                "update": "overwrite",
                "upsert": "append"  # Would need merge logic for true upsert
            }.get(mode.lower(), "append")
            
            # Write data in batches
            df.write.jdbc(
                url=jdbc_url,
                table=table_name,
                mode=write_mode,
                properties=connection_props,
                batchsize=self.batch_size
            )
            
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
        Load data to file system.
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            Success status
        """
        try:
            target_config = self.config.get("target", {})
            output_path = target_config.get("path", f"/output/run_{self.run_id}")
            
            # Write as parquet with partitioning
            df.write.mode(mode).partitionBy("category").parquet(output_path)
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="File load error",
                details=str(e)
            )
            return False
    
    def _reconcile_data(self, df: DataFrame) -> bool:
        """
        Reconcile loaded data with source.
        
        Args:
            df: DataFrame that was loaded
            
        Returns:
            Reconciliation success status
        """
        try:
            # Simple reconciliation: verify record count matches
            source_count = df.count()
            
            # Query target to verify
            target_config = self.config.get("target", {})
            table_name = target_config.get("table", "target_data")
            
            connection_props = {
                "driver": "org.postgresql.Driver",
                "user": target_config.get("connection", {}).get("user", "etl_user"),
                "password": target_config.get("connection", {}).get("password", ""),
            }
            
            jdbc_url = (
                f"jdbc:postgresql://{target_config.get('connection', {}).get('host', 'localhost')}:"
                f"{target_config.get('connection', {}).get('port', 5432)}/"
                f"{target_config.get('connection', {}).get('database', 'etl_target')}"
            )
            
            target_df = df.sparkSession.read.jdbc(
                url=jdbc_url,
                table=table_name,
                properties=connection_props
            )
            
            target_df = target_df.filter(col("etl_run_id") == self.run_id)
            target_count = target_df.count()
            
            reconciled = source_count == target_count
            
            if reconciled:
                self.logger.log_info(
                    component="LOADER",
                    message=f"Reconciliation successful: {source_count} records"
                )
            else:
                self.logger.log_warning(
                    component="LOADER",
                    message=f"Reconciliation mismatch: source={source_count}, target={target_count}"
                )
            
            return reconciled
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Reconciliation error",
                details=str(e)
            )
            return False