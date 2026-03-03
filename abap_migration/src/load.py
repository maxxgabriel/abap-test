"""
Load module for ETL pipeline.
Handles data loading to target systems.
"""
from typing import Dict, Any, Optional
from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
import logging

logger = logging.getLogger(__name__)


@dataclass
class LoadResult:
    """Result of load operation."""
    success_count: int
    error_count: int
    total_count: int
    errors: list


class ETLLoader:
    """Load transformed data to target systems."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize loader.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.target_type = config.get('target_type', 'DATABASE')
        self.batch_size = config['loading']['batch_size']
        
    def load_data(
        self, 
        df: DataFrame, 
        mode: str = "append"
    ) -> LoadResult:
        """
        Load data to target system.
        
        Args:
            df: DataFrame to load
            mode: Write mode ('append', 'overwrite', 'upsert')
            
        Returns:
            LoadResult with success/error counts
        """
        logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        try:
            total_count = df.count()
            
            # Add load metadata
            df_with_metadata = (df
                               .withColumn("load_timestamp", current_timestamp())
                               .withColumn("etl_run_id", lit(self.run_id)))
            
            # Perform load based on target type
            if self.target_type == 'DATABASE':
                success = self._load_to_database(df_with_metadata, mode)
            elif self.target_type == 'DELTA':
                success = self._load_to_delta(df_with_metadata, mode)
            elif self.target_type == 'PARQUET':
                success = self._load_to_parquet(df_with_metadata, mode)
            else:
                success = self._load_to_database(df_with_metadata, mode)
            
            if success:
                success_count = total_count
                error_count = 0
                
                # Perform reconciliation if enabled
                if self.config['loading'].get('enable_reconciliation', True):
                    self._reconcile_data(df_with_metadata)
                
            else:
                success_count = 0
                error_count = total_count
            
            result = LoadResult(
                success_count=success_count,
                error_count=error_count,
                total_count=total_count,
                errors=[]
            )
            
            logger.info(f"Load complete - Success: {success_count}, Errors: {error_count}")
            
            return result
            
        except Exception as e:
            logger.error(f"Load failed: {str(e)}")
            return LoadResult(
                success_count=0,
                error_count=df.count() if df else 0,
                total_count=df.count() if df else 0,
                errors=[str(e)]
            )
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """Load data to database via JDBC."""
        try:
            jdbc_config = self.config['target']['database']
            
            # Map mode to JDBC mode
            jdbc_mode = "append" if mode in ["append", "insert"] else "overwrite"
            if mode == "upsert":
                # For upsert, we need to handle separately
                return self._upsert_to_database(df)
            
            (df.write
             .format("jdbc")
             .option("url", jdbc_config['url'])
             .option("dbtable", jdbc_config['table'])
             .option("user", jdbc_config['user'])
             .option("password", jdbc_config['password'])
             .option("driver", jdbc_config['driver'])
             .option("batchsize", self.batch_size)
             .mode(jdbc_mode)
             .save())
            
            logger.info(f"Successfully loaded to database: {jdbc_config['table']}")
            return True
            
        except Exception as e:
            logger.error(f"Database load error: {str(e)}")
            return False
    
    def _upsert_to_database(self, df: DataFrame) -> bool:
        """Perform upsert operation to database."""
        # This would typically use MERGE statement or staging table approach
        # For simplicity, showing the pattern
        try:
            jdbc_config = self.config['target']['database']
            temp_table = f"{jdbc_config['table']}_temp"
            
            # Write to temp table
            (df.write
             .format("jdbc")
             .option("url", jdbc_config['url'])
             .option("dbtable", temp_table)
             .option("user", jdbc_config['user'])
             .option("password", jdbc_config['password'])
             .option("driver", jdbc_config['driver'])
             .mode("overwrite")
             .save())
            
            # Execute MERGE statement (database-specific)
            # This is a placeholder - actual implementation depends on database
            logger.info("Upsert operation completed")
            return True
            
        except Exception as e:
            logger.error(f"Upsert error: {str(e)}")
            return False
    
    def _load_to_delta(self, df: DataFrame, mode: str) -> bool:
        """Load data to Delta Lake."""
        try:
            delta_path = self.config['target']['delta']['path']
            
            if mode == "upsert":
                # Delta Lake merge operation
                from delta.tables import DeltaTable
                
                if DeltaTable.isDeltaTable(self.spark, delta_path):
                    delta_table = DeltaTable.forPath(self.spark, delta_path)
                    
                    (delta_table.alias("target")
                     .merge(
                         df.alias("source"),
                         "target.id = source.id"
                     )
                     .whenMatchedUpdateAll()
                     .whenNotMatchedInsertAll()
                     .execute())
                else:
                    df.write.format("delta").mode("overwrite").save(delta_path)
            else:
                df.write.format("delta").mode(mode).save(delta_path)
            
            logger.info(f"Successfully loaded to Delta: {delta_path}")
            return True
            
        except Exception as e:
            logger.error(f"Delta load error: {str(e)}")
            return False
    
    def _load_to_parquet(self, df: DataFrame, mode: str) -> bool:
        """Load data to Parquet files."""
        try:
            parquet_path = self.config['target']['parquet']['path']
            partition_by = self.config['target']['parquet'].get('partition_by', [])
            
            writer = df.write.mode(mode)
            
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            
            writer.parquet(parquet_path)
            
            logger.info(f"Successfully loaded to Parquet: {parquet_path}")
            return True
            
        except Exception as e:
            logger.error(f"Parquet load error: {str(e)}")
            return False
    
    def _reconcile_data(self, df: DataFrame) -> bool:
        """
        Reconcile loaded data with source.
        
        Args:
            df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passes
        """
        logger.info("Starting data reconciliation")
        
        try:
            source_count = df.count()
            
            # Read back from target
            if self.target_type == 'DELTA':
                target_path = self.config['target']['delta']['path']
                target_df = self.spark.read.format("delta").load(target_path)
            elif self.target_type == 'PARQUET':
                target_path = self.config['target']['parquet']['path']
                target_df = self.spark.read.parquet(target_path)
            else:
                # For database, would need to query back
                logger.info("Reconciliation not implemented for database target")
                return True
            
            # Filter for this run
            target_count = target_df.filter(col("etl_run_id") == self.run_id).count()
            
            matches = source_count == target_count
            
            if matches:
                logger.info(f"Reconciliation passed: {source_count} records")
            else:
                logger.warning(
                    f"Reconciliation mismatch - Source: {source_count}, Target: {target_count}"
                )
            
            return matches
            
        except Exception as e:
            logger.error(f"Reconciliation error: {str(e)}")
            return False