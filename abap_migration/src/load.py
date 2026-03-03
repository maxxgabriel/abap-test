"""
ETL Loading Module with Batch Processing
Supports INSERT/UPSERT modes, batch partitioning, and comprehensive tracking
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit
from typing import Dict, List, Optional, Tuple
import logging
from datetime import datetime


class ETLLoader:
    """
    Data loading component with batch processing capabilities.
    Supports INSERT/UPSERT modes and tracks success/error counts.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        target_type: str = "database",
        batch_size: int = 1000,
        run_id: str = None
    ):
        """
        Initialize ETL Loader.
        
        Args:
            spark: SparkSession instance
            target_type: Target destination type (database, parquet, delta)
            batch_size: Number of records per batch
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.target_type = target_type
        self.batch_size = batch_size
        self.run_id = run_id or self._generate_run_id()
        self.logger = logging.getLogger(__name__)
        
    def _generate_run_id(self) -> str:
        """Generate unique run ID."""
        return f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def load_data(
        self,
        data: DataFrame,
        mode: str = "insert",
        target_table: str = None,
        target_path: str = None
    ) -> Dict[str, int]:
        """
        Load data with specified mode and track results.
        
        Args:
            data: Transformed DataFrame to load
            mode: Load mode - 'insert', 'update', 'upsert'
            target_table: Target table name (for database)
            target_path: Target path (for file-based storage)
            
        Returns:
            Dictionary with success_count, error_count, total_count
        """
        self.logger.info(
            f"Starting data load - Mode: {mode}, "
            f"Batch size: {self.batch_size}, Run ID: {self.run_id}"
        )
        
        result = {
            "success_count": 0,
            "error_count": 0,
            "total_count": data.count(),
            "errors": []
        }
        
        if result["total_count"] == 0:
            self.logger.warning("No data to load")
            return result
        
        try:
            # Process data in batches
            batches = self._create_batches(data)
            
            for batch_num, batch_df in enumerate(batches, 1):
                self.logger.info(f"Processing batch {batch_num}")
                
                try:
                    batch_result = self._commit_batch(
                        batch_df, 
                        mode, 
                        target_table, 
                        target_path
                    )
                    
                    result["success_count"] += batch_result["success"]
                    result["error_count"] += batch_result["errors"]
                    
                except Exception as e:
                    error_msg = f"Batch {batch_num} failed: {str(e)}"
                    self.logger.error(error_msg)
                    result["errors"].append(error_msg)
                    result["error_count"] += batch_df.count()
            
            # Reconciliation check
            if self._reconcile_data(data, target_table, target_path):
                self.logger.info("Data reconciliation successful")
            else:
                self.logger.warning("Data reconciliation failed")
                result["errors"].append("Reconciliation mismatch detected")
            
            self.logger.info(
                f"Load complete - Success: {result['success_count']}, "
                f"Errors: {result['error_count']}"
            )
            
        except Exception as e:
            self.logger.error(f"Load process failed: {str(e)}")
            result["error_count"] = result["total_count"]
            result["errors"].append(str(e))
        
        return result
    
    def _create_batches(self, data: DataFrame) -> List[DataFrame]:
        """
        Split DataFrame into batches for processing.
        
        Args:
            data: Input DataFrame
            
        Returns:
            List of DataFrame batches
        """
        total_count = data.count()
        num_batches = (total_count + self.batch_size - 1) // self.batch_size
        
        self.logger.info(
            f"Creating {num_batches} batches from {total_count} records"
        )
        
        batches = []
        
        # Add batch number column for partitioning
        data_with_batch = data.withColumn(
            "batch_num",
            (col("monotonically_increasing_id()") / self.batch_size).cast("int")
        )
        
        for i in range(num_batches):
            batch = data_with_batch.filter(col("batch_num") == i).drop("batch_num")
            batches.append(batch)
        
        return batches
    
    def _commit_batch(
        self,
        batch: DataFrame,
        mode: str,
        target_table: Optional[str],
        target_path: Optional[str]
    ) -> Dict[str, int]:
        """
        Commit a single batch to target.
        
        Args:
            batch: Batch DataFrame
            mode: Load mode
            target_table: Target table name
            target_path: Target file path
            
        Returns:
            Dictionary with success and error counts
        """
        batch_count = batch.count()
        
        try:
            if self.target_type == "database" and target_table:
                self._load_to_database(batch, mode, target_table)
            elif self.target_type == "parquet" and target_path:
                self._load_to_parquet(batch, mode, target_path)
            elif self.target_type == "delta" and target_path:
                self._load_to_delta(batch, mode, target_path)
            else:
                raise ValueError(f"Invalid target configuration")
            
            return {"success": batch_count, "errors": 0}
            
        except Exception as e:
            self.logger.error(f"Batch commit failed: {str(e)}")
            return {"success": 0, "errors": batch_count}
    
    def _load_to_database(
        self,
        data: DataFrame,
        mode: str,
        table_name: str
    ) -> bool:
        """
        Load data to database table.
        
        Args:
            data: DataFrame to load
            mode: Load mode (insert, update, upsert)
            table_name: Target table name
            
        Returns:
            Success status
        """
        try:
            # Add metadata columns
            data_with_metadata = data.withColumn(
                "loaded_at", current_timestamp()
            ).withColumn(
                "etl_run_id", lit(self.run_id)
            )
            
            if mode.lower() == "insert":
                data_with_metadata.write.jdbc(
                    url=self.spark.conf.get("spark.jdbc.url"),
                    table=table_name,
                    mode="append",
                    properties={
                        "user": self.spark.conf.get("spark.jdbc.user"),
                        "password": self.spark.conf.get("spark.jdbc.password"),
                        "driver": self.spark.conf.get("spark.jdbc.driver")
                    }
                )
            elif mode.lower() == "upsert":
                # Create temporary view for merge
                temp_view = f"{table_name}_temp"
                data_with_metadata.createOrReplaceTempView(temp_view)
                
                # Execute merge/upsert logic
                self._execute_upsert(table_name, temp_view)
            elif mode.lower() == "update":
                self._update_existing(data_with_metadata, table_name)
            else:
                raise ValueError(f"Unsupported mode: {mode}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Database load failed: {str(e)}")
            return False
    
    def _load_to_parquet(
        self,
        data: DataFrame,
        mode: str,
        target_path: str
    ) -> bool:
        """
        Load data to Parquet files.
        
        Args:
            data: DataFrame to load
            mode: Load mode
            target_path: Target directory path
            
        Returns:
            Success status
        """
        try:
            write_mode = "append" if mode.lower() == "insert" else "overwrite"
            
            data.write.parquet(
                target_path,
                mode=write_mode,
                partitionBy=["category"]
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Parquet load failed: {str(e)}")
            return False
    
    def _load_to_delta(
        self,
        data: DataFrame,
        mode: str,
        target_path: str
    ) -> bool:
        """
        Load data to Delta Lake.
        
        Args:
            data: DataFrame to load
            mode: Load mode
            target_path: Target directory path
            
        Returns:
            Success status
        """
        try:
            if mode.lower() == "upsert":
                # Use Delta merge for upsert
                from delta.tables import DeltaTable
                
                if DeltaTable.isDeltaTable(self.spark, target_path):
                    delta_table = DeltaTable.forPath(self.spark, target_path)
                    
                    delta_table.alias("target").merge(
                        data.alias("source"),
                        "target.id = source.id"
                    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                else:
                    # First load - just write
                    data.write.format("delta").save(target_path)
            else:
                write_mode = "append" if mode.lower() == "insert" else "overwrite"
                data.write.format("delta").mode(write_mode).save(target_path)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Delta load failed: {str(e)}")
            return False
    
    def _execute_upsert(self, target_table: str, source_view: str) -> None:
        """
        Execute upsert operation using SQL merge.
        
        Args:
            target_table: Target table name
            source_view: Source temporary view name
        """
        merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING {source_view} AS source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        
        self.spark.sql(merge_sql)
    
    def _update_existing(self, data: DataFrame, table_name: str) -> bool:
        """
        Update existing records in target table.
        
        Args:
            data: DataFrame with updates
            table_name: Target table name
            
        Returns:
            Success status
        """
        try:
            # Create temporary table
            temp_table = f"{table_name}_updates"
            data.createOrReplaceTempView(temp_table)
            
            # Execute update
            update_sql = f"""
            UPDATE {table_name} AS target
            SET target.name = source.name,
                target.value = source.value,
                target.transformed_value = source.transformed_value,
                target.status = source.status,
                target.category = source.category,
                target.priority = source.priority,
                target.processed_at = source.processed_at
            FROM {temp_table} AS source
            WHERE target.id = source.id
            """
            
            self.spark.sql(update_sql)
            return True
            
        except Exception as e:
            self.logger.error(f"Update failed: {str(e)}")
            return False
    
    def _reconcile_data(
        self,
        loaded_data: DataFrame,
        target_table: Optional[str],
        target_path: Optional[str]
    ) -> bool:
        """
        Reconcile loaded data with target to verify integrity.
        
        Args:
            loaded_data: DataFrame that was loaded
            target_table: Target table name
            target_path: Target file path
            
        Returns:
            True if reconciliation passes
        """
        try:
            expected_count = loaded_data.count()
            
            # Read back from target
            if self.target_type == "database" and target_table:
                actual_df = self.spark.read.jdbc(
                    url=self.spark.conf.get("spark.jdbc.url"),
                    table=target_table,
                    properties={
                        "user": self.spark.conf.get("spark.jdbc.user"),
                        "password": self.spark.conf.get("spark.jdbc.password")
                    }
                ).filter(col("etl_run_id") == self.run_id)
                
            elif target_path:
                if self.target_type == "parquet":
                    actual_df = self.spark.read.parquet(target_path)
                elif self.target_type == "delta":
                    actual_df = self.spark.read.format("delta").load(target_path)
                else:
                    return False
                    
                actual_df = actual_df.filter(col("etl_run_id") == self.run_id)
            else:
                return False
            
            actual_count = actual_df.count()
            
            if expected_count != actual_count:
                self.logger.error(
                    f"Reconciliation failed: "
                    f"Expected {expected_count}, got {actual_count}"
                )
                return False
            
            # Additional validation: check for data quality
            null_check = actual_df.filter(
                col("id").isNull() | col("name").isNull()
            ).count()
            
            if null_check > 0:
                self.logger.error(f"Found {null_check} records with null keys")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Reconciliation failed: {str(e)}")
            return False
    
    def handle_load_error(
        self,
        record_id: str,
        error: Exception,
        error_log_table: str = "etl_error_log"
    ) -> None:
        """
        Log load errors to error table.
        
        Args:
            record_id: ID of failed record
            error: Exception that occurred
            error_log_table: Error log table name
        """
        try:
            error_data = self.spark.createDataFrame([{
                "run_id": self.run_id,
                "record_id": record_id,
                "error_message": str(error),
                "error_timestamp": datetime.now(),
                "component": "LOADER"
            }])
            
            error_data.write.jdbc(
                url=self.spark.conf.get("spark.jdbc.url"),
                table=error_log_table,
                mode="append",
                properties={
                    "user": self.spark.conf.get("spark.jdbc.user"),
                    "password": self.spark.conf.get("spark.jdbc.password")
                }
            )
            
        except Exception as e:
            self.logger.error(f"Failed to log error: {str(e)}")


def create_loader(
    spark: SparkSession,
    config: Dict
) -> ETLLoader:
    """
    Factory function to create ETLLoader with configuration.
    
    Args:
        spark: SparkSession instance
        config: Configuration dictionary
        
    Returns:
        Configured ETLLoader instance
    """
    return ETLLoader(
        spark=spark,
        target_type=config.get("target_type", "database"),
        batch_size=config.get("batch_size", 1000),
        run_id=config.get("run_id")
    )