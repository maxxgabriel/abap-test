"""
ETL Load Module
Handles data loading to target systems with reconciliation
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from typing import Dict, Any, List
import logging
from src.logger import ETLLogger
from src.config import Config


class LoadResult:
    """Container for load operation results"""
    
    def __init__(self):
        self.success_count = 0
        self.error_count = 0
        self.total_count = 0
        self.errors: List[str] = []


class ETLLoader:
    """Load transformed data to target systems"""
    
    def __init__(self, spark: SparkSession, config: Config, run_id: str):
        """
        Initialize loader
        
        Args:
            spark: SparkSession instance
            config: Configuration object
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
        self.target_type = config.get("loading.target_type", "database")
        self.batch_size = config.get("loading.batch_size", 1000)
        
    def load_data(
        self,
        transformed_df: DataFrame,
        mode: str = "upsert"
    ) -> LoadResult:
        """
        Load transformed data to target
        
        Args:
            transformed_df: Transformed data DataFrame
            mode: Load mode ('insert', 'update', 'upsert', 'overwrite')
            
        Returns:
            LoadResult with operation statistics
        """
        self.logger.log_info(
            component="LOADER",
            message=f"Starting load - Mode: {mode}, Batch size: {self.batch_size}"
        )
        
        result = LoadResult()
        result.total_count = transformed_df.count()
        
        try:
            # Route to appropriate loader
            if self.target_type.upper() == "DATABASE":
                success = self._load_to_database(transformed_df, mode)
            elif self.target_type.upper() == "PARQUET":
                success = self._load_to_parquet(transformed_df, mode)
            elif self.target_type.upper() == "DELTA":
                success = self._load_to_delta(transformed_df, mode)
            else:
                self.logger.log_warning(
                    component="LOADER",
                    message=f"Unknown target type {self.target_type}, defaulting to DATABASE"
                )
                success = self._load_to_database(transformed_df, mode)
            
            if success:
                result.success_count = result.total_count
                result.error_count = 0
                
                # Perform reconciliation if enabled
                if self.config.get("loading.enable_reconciliation", True):
                    reconciled = self._reconcile_data(transformed_df)
                    if not reconciled:
                        self.logger.log_warning(
                            component="LOADER",
                            message="Data reconciliation check failed"
                        )
                        result.errors.append("Reconciliation mismatch detected")
            else:
                result.success_count = 0
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
                message="Load operation failed",
                details=str(e)
            )
            result.error_count = result.total_count
            result.errors.append(str(e))
            return result
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to database target
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            True if successful
        """
        target_table = self.config.get("loading.target_table", "etl_target_data")
        
        try:
            # Map mode to JDBC mode
            jdbc_mode = {
                "insert": "append",
                "update": "overwrite",
                "upsert": "append",
                "overwrite": "overwrite"
            }.get(mode.lower(), "append")
            
            # Write to database
            df.write \
                .format("jdbc") \
                .option("url", self.config.get("database.jdbc_url")) \
                .option("dbtable", target_table) \
                .option("user", self.config.get("database.user")) \
                .option("password", self.config.get("database.password")) \
                .option("driver", self.config.get("database.driver")) \
                .option("batchsize", self.batch_size) \
                .mode(jdbc_mode) \
                .save()
            
            self.logger.log_info(
                component="LOADER",
                message=f"Successfully loaded {df.count()} records to {target_table}"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message=f"Database load failed for table {target_table}",
                details=str(e)
            )
            return False
    
    def _load_to_parquet(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to Parquet files
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            True if successful
        """
        target_path = self.config.get("loading.target_path")
        
        try:
            spark_mode = mode if mode in ["append", "overwrite"] else "append"
            
            df.write \
                .mode(spark_mode) \
                .partitionBy("category") \
                .parquet(target_path)
            
            self.logger.log_info(
                component="LOADER",
                message=f"Successfully loaded {df.count()} records to {target_path}"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Parquet load failed",
                details=str(e)
            )
            return False
    
    def _load_to_delta(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to Delta Lake
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            True if successful
        """
        target_path = self.config.get("loading.delta_path")
        
        try:
            if mode.lower() == "upsert":
                # Use Delta merge for upsert
                from delta.tables import DeltaTable
                
                if DeltaTable.isDeltaTable(self.spark, target_path):
                    delta_table = DeltaTable.forPath(self.spark, target_path)
                    
                    delta_table.alias("target").merge(
                        df.alias("source"),
                        "target.id = source.id"
                    ).whenMatchedUpdateAll() \
                     .whenNotMatchedInsertAll() \
                     .execute()
                else:
                    df.write.format("delta").mode("overwrite").save(target_path)
            else:
                spark_mode = mode if mode in ["append", "overwrite"] else "append"
                df.write.format("delta").mode(spark_mode).save(target_path)
            
            self.logger.log_info(
                component="LOADER",
                message=f"Successfully loaded {df.count()} records to Delta at {target_path}"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Delta load failed",
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
            target_table = self.config.get("loading.target_table", "etl_target_data")
            
            # Count records in target for this run
            query = f"""
                SELECT COUNT(*) as target_count
                FROM {target_table}
                WHERE etl_run_id = '{self.run_id}'
            """
            
            target_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.config.get("database.jdbc_url")) \
                .option("dbtable", f"({query}) AS recon") \
                .option("user", self.config.get("database.user")) \
                .option("password", self.config.get("database.password")) \
                .option("driver", self.config.get("database.driver")) \
                .load()
            
            target_count = target_df.first()["target_count"]
            source_count = loaded_df.count()
            
            matches = target_count == source_count
            
            if matches:
                self.logger.log_info(
                    component="LOADER",
                    message=f"Reconciliation passed: {source_count} records"
                )
            else:
                self.logger.log_warning(
                    component="LOADER",
                    message=f"Reconciliation mismatch: Source={source_count}, Target={target_count}"
                )
            
            return matches
            
        except Exception as e:
            self.logger.log_warning(
                component="LOADER",
                message="Reconciliation check failed",
                details=str(e)
            )
            return False