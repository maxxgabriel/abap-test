"""
Delta Lake Loader Module
Implements merge operations (INSERT/UPDATE/UPSERT) using Delta Lake merge capabilities.
Replaces ABAP batch loops with DataFrame.repartition() for optimized parallel processing.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit
from delta import DeltaTable
from typing import Dict, List, Optional
import logging
from datetime import datetime
from dataclasses import dataclass

from src.config_manager import ConfigManager
from src.logger import ETLLogger


@dataclass
class LoadResult:
    """Result metrics from load operation"""
    success_count: int
    error_count: int
    total_count: int
    errors: List[str]
    run_id: str
    duration_seconds: float


class DeltaLakeLoader:
    """
    Delta Lake Loader with merge operations.
    Supports INSERT, UPDATE, and UPSERT modes using Delta Lake merge.
    """

    def __init__(
        self,
        spark: SparkSession,
        config_manager: ConfigManager,
        run_id: str,
        target_type: str = "DATABASE",
        batch_size: int = 1000
    ):
        """
        Initialize Delta Lake Loader.

        Args:
            spark: SparkSession instance
            config_manager: Configuration manager
            run_id: Unique ETL run identifier
            target_type: Target type (DATABASE, DELTA_LAKE, etc.)
            batch_size: Records per partition (for repartitioning)
        """
        self.spark = spark
        self.config = config_manager
        self.run_id = run_id
        self.target_type = target_type
        self.batch_size = batch_size
        self.logger = ETLLogger(component="LOADER")

        # Get target path from config
        self.target_path = self.config.get("delta.target_path")
        self.enable_reconciliation = self.config.get("data_quality.enable_reconciliation", True)

    def load_data(
        self,
        data: DataFrame,
        mode: str = "UPSERT",
        primary_keys: Optional[List[str]] = None
    ) -> LoadResult:
        """
        Load data to Delta Lake with specified merge mode.

        Args:
            data: Transformed DataFrame to load
            mode: Load mode - INSERT, UPDATE, or UPSERT
            primary_keys: Primary key columns for merge operations

        Returns:
            LoadResult with metrics
        """
        start_time = datetime.now()
        self.logger.log_info(
            f"Starting load - Mode: {mode}, Batch size: {self.batch_size}, Records: {data.count()}"
        )

        if primary_keys is None:
            primary_keys = ["id"]

        try:
            # Validate data before loading
            if data.rdd.isEmpty():
                self.logger.log_warning("No data to load - skipping load operation")
                return LoadResult(
                    success_count=0,
                    error_count=0,
                    total_count=0,
                    errors=[],
                    run_id=self.run_id,
                    duration_seconds=0.0
                )

            # Repartition for optimal parallel processing
            # Replaces ABAP batch loops with Spark's distributed processing
            num_partitions = max(1, data.count() // self.batch_size)
            data_repartitioned = data.repartition(num_partitions)

            self.logger.log_info(f"Repartitioned data into {num_partitions} partitions")

            # Execute load based on mode
            if mode.upper() == "INSERT":
                result = self._insert_new(data_repartitioned, primary_keys)
            elif mode.upper() == "UPDATE":
                result = self._update_existing(data_repartitioned, primary_keys)
            elif mode.upper() == "UPSERT":
                result = self._upsert_data(data_repartitioned, primary_keys)
            else:
                raise ValueError(f"Unsupported load mode: {mode}")

            # Reconcile data if enabled
            if self.enable_reconciliation and result.error_count == 0:
                self._reconcile_data(data_repartitioned, primary_keys)

            # Calculate duration
            end_time = datetime.now()
            result.duration_seconds = (end_time - start_time).total_seconds()

            self.logger.log_info(
                f"Load complete - Success: {result.success_count}, "
                f"Errors: {result.error_count}, Duration: {result.duration_seconds:.2f}s"
            )

            return result

        except Exception as e:
            self.logger.log_error(f"Load failed: {str(e)}", details=str(e))
            raise

    def _insert_new(self, data: DataFrame, primary_keys: List[str]) -> LoadResult:
        """
        Insert new records using Delta Lake append mode.
        Replaces ABAP INSERT statement with Delta Lake append.

        Args:
            data: DataFrame to insert
            primary_keys: Primary key columns

        Returns:
            LoadResult
        """
        try:
            total_count = data.count()
            self.logger.log_info(f"Inserting {total_count} new records")

            # Add metadata columns
            data_with_metadata = data.withColumn("etl_run_id", lit(self.run_id)) \
                                    .withColumn("processed_at", current_timestamp()) \
                                    .withColumn("processed_by", lit(self.spark.sparkContext.sparkUser()))

            # Write to Delta Lake (append mode)
            # This replaces ABAP: INSERT zetl_target_data FROM TABLE lt_target_tab
            data_with_metadata.write \
                .format("delta") \
                .mode("append") \
                .save(self.target_path)

            # Delta Lake auto-commits (no explicit COMMIT WORK needed)
            self.logger.log_info(f"Successfully inserted {total_count} records")

            return LoadResult(
                success_count=total_count,
                error_count=0,
                total_count=total_count,
                errors=[],
                run_id=self.run_id,
                duration_seconds=0.0
            )

        except Exception as e:
            error_msg = f"Insert failed: {str(e)}"
            self.logger.log_error(error_msg)
            return LoadResult(
                success_count=0,
                error_count=data.count(),
                total_count=data.count(),
                errors=[error_msg],
                run_id=self.run_id,
                duration_seconds=0.0
            )

    def _update_existing(self, data: DataFrame, primary_keys: List[str]) -> LoadResult:
        """
        Update existing records using Delta Lake merge operation.
        Replaces ABAP UPDATE statement with Delta merge.

        Args:
            data: DataFrame with updates
            primary_keys: Primary key columns for matching

        Returns:
            LoadResult
        """
        try:
            total_count = data.count()
            self.logger.log_info(f"Updating {total_count} existing records")

            # Add metadata columns
            data_with_metadata = data.withColumn("etl_run_id", lit(self.run_id)) \
                                    .withColumn("processed_at", current_timestamp()) \
                                    .withColumn("processed_by", lit(self.spark.sparkContext.sparkUser()))

            # Load Delta table
            delta_table = DeltaTable.forPath(self.spark, self.target_path)

            # Build merge condition
            merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in primary_keys])

            # Perform merge (UPDATE only)
            # This replaces ABAP: UPDATE zetl_target_data FROM TABLE lt_target_tab
            delta_table.alias("target") \
                .merge(
                    data_with_metadata.alias("source"),
                    merge_condition
                ) \
                .whenMatchedUpdateAll() \
                .execute()

            # Get metrics from Delta Lake history
            metrics = self._get_merge_metrics(delta_table)

            self.logger.log_info(f"Successfully updated {metrics['num_updated_rows']} records")

            return LoadResult(
                success_count=metrics.get("num_updated_rows", total_count),
                error_count=0,
                total_count=total_count,
                errors=[],
                run_id=self.run_id,
                duration_seconds=0.0
            )

        except Exception as e:
            error_msg = f"Update failed: {str(e)}"
            self.logger.log_error(error_msg)
            return LoadResult(
                success_count=0,
                error_count=data.count(),
                total_count=data.count(),
                errors=[error_msg],
                run_id=self.run_id,
                duration_seconds=0.0
            )

    def _upsert_data(self, data: DataFrame, primary_keys: List[str]) -> LoadResult:
        """
        Upsert data using Delta Lake merge operation.
        Replaces ABAP UPDATE + INSERT logic with single Delta merge.

        Args:
            data: DataFrame to upsert
            primary_keys: Primary key columns for matching

        Returns:
            LoadResult
        """
        try:
            total_count = data.count()
            self.logger.log_info(f"Upserting {total_count} records")

            # Add metadata columns
            data_with_metadata = data.withColumn("etl_run_id", lit(self.run_id)) \
                                    .withColumn("processed_at", current_timestamp()) \
                                    .withColumn("processed_by", lit(self.spark.sparkContext.sparkUser()))

            # Create or load Delta table
            if not self._delta_table_exists():
                self.logger.log_info("Target table doesn't exist - creating new table")
                data_with_metadata.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .save(self.target_path)
                return LoadResult(
                    success_count=total_count,
                    error_count=0,
                    total_count=total_count,
                    errors=[],
                    run_id=self.run_id,
                    duration_seconds=0.0
                )

            delta_table = DeltaTable.forPath(self.spark, self.target_path)

            # Build merge condition
            merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in primary_keys])

            # Perform merge (UPSERT)
            # This replaces ABAP UPSERT logic (try UPDATE, then INSERT)
            delta_table.alias("target") \
                .merge(
                    data_with_metadata.alias("source"),
                    merge_condition
                ) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()

            # Get metrics from Delta Lake history
            metrics = self._get_merge_metrics(delta_table)
            
            success_count = metrics.get("num_updated_rows", 0) + metrics.get("num_inserted_rows", 0)

            self.logger.log_info(
                f"Successfully upserted records - "
                f"Updated: {metrics.get('num_updated_rows', 0)}, "
                f"Inserted: {metrics.get('num_inserted_rows', 0)}"
            )

            return LoadResult(
                success_count=success_count,
                error_count=0,
                total_count=total_count,
                errors=[],
                run_id=self.run_id,
                duration_seconds=0.0
            )

        except Exception as e:
            error_msg = f"Upsert failed: {str(e)}"
            self.logger.log_error(error_msg)
            return LoadResult(
                success_count=0,
                error_count=data.count(),
                total_count=data.count(),
                errors=[error_msg],
                run_id=self.run_id,
                duration_seconds=0.0
            )

    def _reconcile_data(self, loaded_data: DataFrame, primary_keys: List[str]) -> bool:
        """
        Reconcile loaded data with target to ensure consistency.

        Args:
            loaded_data: DataFrame that was loaded
            primary_keys: Primary key columns

        Returns:
            True if reconciliation passes
        """
        try:
            self.logger.log_info("Starting data reconciliation")

            # Read target data
            target_data = self.spark.read.format("delta").load(self.target_path)

            # Join on primary keys
            join_condition = [loaded_data[key] == target_data[key] for key in primary_keys]
            matched = loaded_data.join(target_data, join_condition, "inner")

            loaded_count = loaded_data.count()
            matched_count = matched.count()

            if loaded_count == matched_count:
                self.logger.log_info(f"Reconciliation passed - {matched_count} records matched")
                return True
            else:
                self.logger.log_warning(
                    f"Reconciliation mismatch - Loaded: {loaded_count}, Matched: {matched_count}"
                )
                return False

        except Exception as e:
            self.logger.log_error(f"Reconciliation failed: {str(e)}")
            return False

    def _delta_table_exists(self) -> bool:
        """Check if Delta table exists at target path"""
        try:
            DeltaTable.forPath(self.spark, self.target_path)
            return True
        except:
            return False

    def _get_merge_metrics(self, delta_table: DeltaTable) -> Dict:
        """
        Extract metrics from Delta Lake merge operation.

        Args:
            delta_table: DeltaTable instance

        Returns:
            Dictionary with merge metrics
        """
        try:
            # Get last operation from history
            history = delta_table.history(1)
            if history.count() > 0:
                last_op = history.first()
                metrics = last_op.operationMetrics if last_op.operationMetrics else {}
                return {
                    "num_updated_rows": int(metrics.get("numTargetRowsUpdated", 0)),
                    "num_inserted_rows": int(metrics.get("numTargetRowsInserted", 0)),
                    "num_deleted_rows": int(metrics.get("numTargetRowsDeleted", 0))
                }
        except Exception as e:
            self.logger.log_warning(f"Could not retrieve merge metrics: {str(e)}")

        return {"num_updated_rows": 0, "num_inserted_rows": 0, "num_deleted_rows": 0}

    def optimize_table(self, z_order_columns: Optional[List[str]] = None):
        """
        Optimize Delta table with OPTIMIZE and optional Z-ORDER.

        Args:
            z_order_columns: Columns to use for Z-ORDER optimization
        """
        try:
            self.logger.log_info("Starting table optimization")
            delta_table = DeltaTable.forPath(self.spark, self.target_path)

            if z_order_columns:
                delta_table.optimize().executeZOrderBy(z_order_columns)
                self.logger.log_info(f"Optimized with Z-ORDER on {z_order_columns}")
            else:
                delta_table.optimize().executeCompaction()
                self.logger.log_info("Optimized with compaction")

        except Exception as e:
            self.logger.log_error(f"Optimization failed: {str(e)}")

    def vacuum_table(self, retention_hours: int = 168):
        """
        Vacuum old files from Delta table.

        Args:
            retention_hours: Hours to retain old files (default 7 days)
        """
        try:
            self.logger.log_info(f"Starting vacuum with {retention_hours}h retention")
            delta_table = DeltaTable.forPath(self.spark, self.target_path)
            delta_table.vacuum(retention_hours)
            self.logger.log_info("Vacuum completed")
        except Exception as e:
            self.logger.log_error(f"Vacuum failed: {str(e)}")