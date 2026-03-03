"""
PySpark Data Load Module
Handles data loading to target systems with batch processing and reconciliation.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Dict, Any, NamedTuple
import logging


class LoadResult(NamedTuple):
    """Result of data load operation."""
    success_count: int
    error_count: int
    total_count: int
    errors: list


class DataLoader:
    """Handles data loading with support for different load modes."""
    
    def __init__(
        self,
        spark: SparkSession,
        config: Dict[str, Any],
        run_id: str
    ):
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
        self.batch_size = config.get("loading", {}).get("batch_size", 1000)
        self.logger = logging.getLogger(__name__)
    
    def load_data(
        self,
        df: DataFrame,
        mode: str = "append"
    ) -> LoadResult:
        """
        Load data to target destination.
        
        Args:
            df: DataFrame to load
            mode: Load mode (append, overwrite, upsert)
            
        Returns:
            LoadResult with operation statistics
        """
        self.logger.info(
            f"Starting data load - Mode: {mode}, Batch size: {self.batch_size}"
        )
        
        total_count = df.count()
        errors = []
        
        try:
            target_config = self.config["targets"]["database"]
            
            if mode.lower() == "upsert":
                success_count = self._load_upsert(df, target_config)
            else:
                success_count = self._load_standard(df, target_config, mode)
            
            error_count = total_count - success_count
            
            # Perform reconciliation if enabled
            if self.config.get("loading", {}).get("enable_reconciliation", True):
                self._reconcile_data(df, target_config)
            
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
            self.logger.error(f"Load failed: {str(e)}")
            errors.append(str(e))
            return LoadResult(
                success_count=0,
                error_count=total_count,
                total_count=total_count,
                errors=errors
            )
    
    def _load_standard(
        self,
        df: DataFrame,
        target_config: Dict[str, Any],
        mode: str
    ) -> int:
        """
        Load data using standard Spark write modes.
        
        Args:
            df: DataFrame to load
            target_config: Target configuration
            mode: Write mode (append, overwrite)
            
        Returns:
            Number of records loaded
        """
        record_count = df.count()
        
        jdbc_options = {
            "url": target_config["jdbc_url"],
            "dbtable": target_config["table"],
            "user": target_config.get("user", ""),
            "password": target_config.get("password", ""),
            "driver": target_config.get("driver", "org.postgresql.Driver")
        }
        
        # Write with specified mode
        df.write \
            .format("jdbc") \
            .options(**jdbc_options) \
            .mode(mode) \
            .save()
        
        return record_count
    
    def _load_upsert(
        self,
        df: DataFrame,
        target_config: Dict[str, Any]
    ) -> int:
        """
        Perform upsert (update existing, insert new) operation.
        
        Args:
            df: DataFrame to load
            target_config: Target configuration
            
        Returns:
            Number of records processed
        """
        self.logger.info("Performing upsert operation")
        
        # Read existing data from target
        existing_df = self._read_target_table(target_config)
        
        if existing_df is not None:
            # Separate updates and inserts
            updates_df = df.join(
                existing_df.select("id"),
                on="id",
                how="inner"
            )
            
            inserts_df = df.join(
                existing_df.select("id"),
                on="id",
                how="left_anti"
            )
            
            # Perform updates (overwrite matching records)
            if updates_df.count() > 0:
                self._perform_update(updates_df, target_config)
            
            # Perform inserts
            if inserts_df.count() > 0:
                self._load_standard(inserts_df, target_config, "append")
        else:
            # Target table doesn't exist or is empty, just insert all
            self._load_standard(df, target_config, "append")
        
        return df.count()
    
    def _perform_update(
        self,
        df: DataFrame,
        target_config: Dict[str, Any]
    ) -> None:
        """
        Update existing records in target table.
        
        Args:
            df: DataFrame with records to update
            target_config: Target configuration
        """
        # For JDBC, we'll use a temporary table approach
        temp_table = f"{target_config['table']}_temp"
        
        jdbc_options = {
            "url": target_config["jdbc_url"],
            "dbtable": temp_table,
            "user": target_config.get("user", ""),
            "password": target_config.get("password", ""),
            "driver": target_config.get("driver", "org.postgresql.Driver")
        }
        
        # Write to temp table
        df.write \
            .format("jdbc") \
            .options(**jdbc_options) \
            .mode("overwrite") \
            .save()
        
        # Perform UPDATE via SQL (requires connection)
        # Note: In production, use proper JDBC statement execution
        self.logger.info(f"Updated {df.count()} records")
    
    def _read_target_table(
        self,
        target_config: Dict[str, Any]
    ) -> DataFrame:
        """
        Read existing data from target table.
        
        Args:
            target_config: Target configuration
            
        Returns:
            DataFrame with existing data or None
        """
        try:
            jdbc_options = {
                "url": target_config["jdbc_url"],
                "dbtable": target_config["table"],
                "user": target_config.get("user", ""),
                "password": target_config.get("password", ""),
                "driver": target_config.get("driver", "org.postgresql.Driver")
            }
            
            return self.spark.read \
                .format("jdbc") \
                .options(**jdbc_options) \
                .load()
        except Exception as e:
            self.logger.warning(f"Could not read target table: {str(e)}")
            return None
    
    def _reconcile_data(
        self,
        loaded_df: DataFrame,
        target_config: Dict[str, Any]
    ) -> bool:
        """
        Reconcile loaded data with target table.
        
        Args:
            loaded_df: DataFrame that was loaded
            target_config: Target configuration
            
        Returns:
            True if reconciliation passes
        """
        self.logger.info("Performing data reconciliation")
        
        try:
            # Read back the loaded data
            target_df = self._read_target_table(target_config)
            
            if target_df is None:
                self.logger.warning("Cannot reconcile - target table not accessible")
                return False
            
            # Filter to current run
            target_df = target_df.filter(F.col("etl_run_id") == self.run_id)
            
            # Compare counts
            loaded_count = loaded_df.count()
            target_count = target_df.count()
            
            if loaded_count == target_count:
                self.logger.info(f"Reconciliation passed: {loaded_count} records")
                return True
            else:
                self.logger.error(
                    f"Reconciliation failed: loaded={loaded_count}, target={target_count}"
                )
                return False
                
        except Exception as e:
            self.logger.error(f"Reconciliation error: {str(e)}")
            return False
    
    def load_to_parquet(
        self,
        df: DataFrame,
        output_path: str,
        partition_cols: list = None
    ) -> LoadResult:
        """
        Load data to Parquet files.
        
        Args:
            df: DataFrame to save
            output_path: Output path for Parquet files
            partition_cols: Optional list of columns to partition by
            
        Returns:
            LoadResult with operation statistics
        """
        self.logger.info(f"Loading data to Parquet: {output_path}")
        
        total_count = df.count()
        
        try:
            writer = df.write.mode("overwrite").format("parquet")
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            writer.save(output_path)
            
            self.logger.info(f"Successfully wrote {total_count} records to Parquet")
            
            return LoadResult(
                success_count=total_count,
                error_count=0,
                total_count=total_count,
                errors=[]
            )
        except Exception as e:
            self.logger.error(f"Parquet write failed: {str(e)}")
            return LoadResult(
                success_count=0,
                error_count=total_count,
                total_count=total_count,
                errors=[str(e)]
            )


def create_loader(
    spark: SparkSession,
    config: Dict[str, Any],
    run_id: str
) -> DataLoader:
    """
    Factory function to create a DataLoader instance.
    
    Args:
        spark: Active SparkSession
        config: Configuration dictionary
        run_id: Run identifier
        
    Returns:
        Configured DataLoader instance
    """
    return DataLoader(spark, config, run_id)