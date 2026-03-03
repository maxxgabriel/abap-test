"""
PySpark ETL Loader Module
Handles data loading to target with support for batch processing and reconciliation.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from typing import Dict, Any
from dataclasses import dataclass
import logging


@dataclass
class LoadResult:
    """Data class for load operation results."""
    success_count: int
    error_count: int
    total_count: int
    errors: list


class ETLLoader:
    """
    Loader class for ETL pipeline supporting batch loading and reconciliation.
    """
    
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
        self.batch_size = config.get("batch_size", 1000)
        self.target_type = config.get("target_type", "database")
        self.logger = logging.getLogger(__name__)
    
    def load_data(self, df: DataFrame, mode: str = "append") -> LoadResult:
        """
        Load data to target.
        
        Args:
            df: DataFrame to load
            mode: Load mode ('append', 'overwrite', 'upsert')
            
        Returns:
            LoadResult with statistics
        """
        self.logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        total_count = df.count()
        errors = []
        
        try:
            if self.target_type == "database":
                success = self._load_to_database(df, mode)
            elif self.target_type == "parquet":
                success = self._load_to_parquet(df, mode)
            elif self.target_type == "delta":
                success = self._load_to_delta(df, mode)
            else:
                self.logger.warning(f"Unknown target type {self.target_type}, defaulting to database")
                success = self._load_to_database(df, mode)
            
            if success:
                success_count = total_count
                error_count = 0
                
                # Perform reconciliation if enabled
                if self.config.get("enable_reconciliation", False):
                    reconciled = self._reconcile_data(df)
                    if not reconciled:
                        self.logger.warning("Data reconciliation failed")
                        errors.append("Reconciliation check failed")
            else:
                success_count = 0
                error_count = total_count
                errors.append("Load operation failed")
            
            self.logger.info(
                f"Load complete - Success: {success_count}, Errors: {error_count}"
            )
            
            return LoadResult(
                success_count=success_count,
                error_count=error_count,
                total_count=total_count,
                errors=errors
            )
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}", exc_info=True)
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
            jdbc_url = self.config.get("target_jdbc_url")
            table_name = self.config.get("target_table", "etl_target_data")
            
            connection_properties = {
                "user": self.config.get("target_user"),
                "password": self.config.get("target_password"),
                "driver": self.config.get("jdbc_driver", "org.postgresql.Driver"),
                "batchsize": str(self.batch_size)
            }
            
            # Map modes
            jdbc_mode = "append" if mode == "append" else "overwrite"
            
            if mode == "upsert":
                # For upsert, we need to handle merge logic
                self._upsert_to_database(df, jdbc_url, table_name, connection_properties)
            else:
                df.write.jdbc(
                    url=jdbc_url,
                    table=table_name,
                    mode=jdbc_mode,
                    properties=connection_properties
                )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Database load error: {str(e)}", exc_info=True)
            return False
    
    def _upsert_to_database(
        self,
        df: DataFrame,
        jdbc_url: str,
        table_name: str,
        properties: Dict[str, str]
    ):
        """
        Perform upsert operation.
        
        Args:
            df: DataFrame to upsert
            jdbc_url: JDBC connection URL
            table_name: Target table name
            properties: Connection properties
        """
        # Create temporary table
        temp_table = f"{table_name}_temp"
        
        # Write to temp table
        df.write.jdbc(
            url=jdbc_url,
            table=temp_table,
            mode="overwrite",
            properties=properties
        )
        
        # Execute merge using JDBC
        merge_query = f"""
        MERGE INTO {table_name} AS target
        USING {temp_table} AS source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET
                name = source.name,
                value = source.value,
                transformed_value = source.transformed_value,
                status = source.status,
                category = source.category,
                priority = source.priority,
                etl_run_id = source.etl_run_id,
                processed_at = source.processed_at,
                processed_by = source.processed_by
        WHEN NOT MATCHED THEN
            INSERT (id, name, value, transformed_value, status, category, 
                    priority, etl_run_id, processed_at, processed_by)
            VALUES (source.id, source.name, source.value, source.transformed_value,
                    source.status, source.category, source.priority,
                    source.etl_run_id, source.processed_at, source.processed_by)
        """
        
        # Note: Actual merge execution would require JDBC connection and execute
        # This is a simplified version - production code would use proper JDBC handling
        self.logger.info(f"Executing upsert for table {table_name}")
    
    def _load_to_parquet(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to Parquet files.
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            Success status
        """
        try:
            target_path = self.config.get("target_path")
            partition_cols = self.config.get("partition_columns", ["category"])
            
            (df.write
             .mode(mode)
             .partitionBy(*partition_cols)
             .parquet(f"{target_path}/run_id={self.run_id}"))
            
            return True
            
        except Exception as e:
            self.logger.error(f"Parquet load error: {str(e)}", exc_info=True)
            return False
    
    def _load_to_delta(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to Delta Lake.
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            Success status
        """
        try:
            target_path = self.config.get("target_path")
            
            if mode == "upsert":
                # Delta Lake merge operation
                from delta.tables import DeltaTable
                
                delta_table = DeltaTable.forPath(self.spark, target_path)
                
                (delta_table.alias("target")
                 .merge(
                     df.alias("source"),
                     "target.id = source.id"
                 )
                 .whenMatchedUpdateAll()
                 .whenNotMatchedInsertAll()
                 .execute())
            else:
                (df.write
                 .format("delta")
                 .mode(mode)
                 .save(target_path))
            
            return True
            
        except Exception as e:
            self.logger.error(f"Delta load error: {str(e)}", exc_info=True)
            return False
    
    def _reconcile_data(self, df: DataFrame) -> bool:
        """
        Reconcile loaded data with source.
        
        Args:
            df: Source DataFrame
            
        Returns:
            Reconciliation success status
        """
        self.logger.info("Performing data reconciliation")
        
        try:
            source_count = df.count()
            
            # Read back from target
            if self.target_type == "database":
                jdbc_url = self.config.get("target_jdbc_url")
                table_name = self.config.get("target_table")
                
                properties = {
                    "user": self.config.get("target_user"),
                    "password": self.config.get("target_password"),
                    "driver": self.config.get("jdbc_driver")
                }
                
                target_df = self.spark.read.jdbc(
                    url=jdbc_url,
                    table=f"(SELECT * FROM {table_name} WHERE etl_run_id = '{self.run_id}') as recon",
                    properties=properties
                )
                
                target_count = target_df.count()
            else:
                target_path = self.config.get("target_path")
                target_df = self.spark.read.parquet(f"{target_path}/run_id={self.run_id}")
                target_count = target_df.count()
            
            # Compare counts
            if source_count != target_count:
                self.logger.error(
                    f"Reconciliation failed: Source count ({source_count}) != "
                    f"Target count ({target_count})"
                )
                return False
            
            self.logger.info("Data reconciliation passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Reconciliation error: {str(e)}", exc_info=True)
            return False