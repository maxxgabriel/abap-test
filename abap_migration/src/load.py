"""
ETL Data Loading Module

Migrated from ABAP zcl_etl_loader
Handles data loading to target destinations with batch processing and reconciliation.
"""

from pyspark.sql import DataFrame, SparkSession
from typing import Dict
from dataclasses import dataclass

from src.logger import ETLLogger


@dataclass
class LoadResult:
    """Result of load operation"""
    success_count: int = 0
    error_count: int = 0
    total_count: int = 0
    errors: list = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class ETLLoader:
    """
    Data loader supporting multiple targets and load modes
    """

    def __init__(self, spark: SparkSession, target_type: str, batch_size: int, run_id: str, config: dict):
        """
        Initialize loader

        Args:
            spark: SparkSession instance
            target_type: Target type (DATABASE, PARQUET, DELTA, etc.)
            batch_size: Batch size for loading
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.target_type = target_type.upper()
        self.batch_size = batch_size
        self.run_id = run_id
        self.config = config
        self.logger = ETLLogger.get_instance()

    def load_data(self, df: DataFrame, mode: str = "append") -> LoadResult:
        """
        Load data to target

        Args:
            df: DataFrame to load
            mode: Load mode (append, overwrite, upsert)

        Returns:
            LoadResult with statistics
        """
        self.logger.log_info(
            component="LOADER",
            message=f"Starting load - Mode: {mode}, Target: {self.target_type}, Batch size: {self.batch_size}"
        )

        result = LoadResult()
        result.total_count = df.count()

        try:
            if self.target_type == "DATABASE":
                success = self._load_to_database(df, mode)
            elif self.target_type == "PARQUET":
                success = self._load_to_parquet(df, mode)
            elif self.target_type == "DELTA":
                success = self._load_to_delta(df, mode)
            else:
                self.logger.log_warning(
                    component="LOADER",
                    message=f"Unknown target type: {self.target_type}, defaulting to DATABASE"
                )
                success = self._load_to_database(df, mode)

            if success:
                result.success_count = result.total_count
                result.error_count = 0

                # Perform reconciliation if enabled
                if self.config.get("enable_reconciliation", True):
                    reconciled = self._reconcile_data(df)
                    if not reconciled:
                        self.logger.log_warning(
                            component="LOADER",
                            message="Data reconciliation failed"
                        )
                        result.errors.append("Reconciliation check failed")
            else:
                result.error_count = result.total_count
                result.errors.append("Load operation failed")

            self.logger.log_info(
                component="LOADER",
                message=f"Load complete - Success: {result.success_count}, Errors: {result.error_count}"
            )

            return result

        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Load failed",
                details=str(e)
            )
            result.error_count = result.total_count
            result.errors.append(str(e))
            return result

    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """Load data to database table"""
        try:
            target_table = self.config.get("target_table", "etl_target_data")

            write_mode = "append"
            if mode.lower() == "overwrite":
                write_mode = "overwrite"
            elif mode.lower() == "upsert":
                # For upsert, we need to handle it differently
                # First try to update existing records, then insert new ones
                write_mode = "append"

            df.write \
                .format("jdbc") \
                .option("url", self.config["jdbc_url"]) \
                .option("dbtable", target_table) \
                .option("user", self.config.get("db_user", "")) \
                .option("password", self.config.get("db_password", "")) \
                .option("driver", self.config.get("jdbc_driver", "org.postgresql.Driver")) \
                .option("batchsize", self.batch_size) \
                .mode(write_mode) \
                .save()

            return True

        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Database load error",
                details=str(e)
            )
            return False

    def _load_to_parquet(self, df: DataFrame, mode: str) -> bool:
        """Load data to Parquet files"""
        try:
            target_path = self.config.get("target_path", f"/data/output/{self.run_id}")

            df.write \
                .format("parquet") \
                .mode(mode) \
                .partitionBy("category") \
                .save(target_path)

            return True

        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Parquet load error",
                details=str(e)
            )
            return False

    def _load_to_delta(self, df: DataFrame, mode: str) -> bool:
        """Load data to Delta Lake"""
        try:
            target_path = self.config.get("target_path", f"/data/delta/{self.run_id}")

            if mode.lower() == "upsert":
                # Delta Lake supports merge/upsert natively
                df.write \
                    .format("delta") \
                    .option("mergeSchema", "true") \
                    .mode("append") \
                    .save(target_path)
            else:
                df.write \
                    .format("delta") \
                    .mode(mode) \
                    .save(target_path)

            return True

        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Delta load error",
                details=str(e)
            )
            return False

    def _reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data with source

        Args:
            loaded_df: DataFrame that was loaded

        Returns:
            True if reconciliation passes
        """
        try:
            # Count records in target
            target_table = self.config.get("target_table", "etl_target_data")

            target_count_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.config["jdbc_url"]) \
                .option("dbtable", f"(SELECT COUNT(*) as cnt FROM {target_table} WHERE etl_run_id = '{self.run_id}') as tc") \
                .option("user", self.config.get("db_user", "")) \
                .option("password", self.config.get("db_password", "")) \
                .load()

            target_count = target_count_df.first()["cnt"]
            loaded_count = loaded_df.count()

            if target_count == loaded_count:
                self.logger.log_info(
                    component="LOADER",
                    message=f"Reconciliation passed: {target_count} records match"
                )
                return True
            else:
                self.logger.log_warning(
                    component="LOADER",
                    message=f"Reconciliation failed: Loaded {loaded_count}, Found {target_count} in target"
                )
                return False

        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Reconciliation error",
                details=str(e)
            )
            return False