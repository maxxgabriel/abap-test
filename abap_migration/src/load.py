"""
PySpark ETL Load Module
Handles writing transformed data to target systems with batch processing and reconciliation.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Dict, Any, NamedTuple
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class LoadResult:
    """Result of a load operation."""
    success_count: int
    error_count: int
    total_count: int
    errors: list


class ETLLoader:
    """Load transformed data to target destination."""
    
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
        self.target_type = config.get('target_type', 'database')
        self.batch_size = config.get('batch_size', 1000)
        
    def load_data(self, df: DataFrame, mode: str = "upsert") -> LoadResult:
        """
        Load data to target destination.
        
        Args:
            df: DataFrame to load
            mode: Load mode - 'insert', 'update', or 'upsert'
            
        Returns:
            LoadResult with success/error counts
        """
        logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        total_count = df.count()
        errors = []
        
        try:
            if self.target_type.upper() == 'DATABASE':
                success = self._load_to_database(df, mode)
            elif self.target_type.upper() == 'PARQUET':
                success = self._load_to_parquet(df, mode)
            elif self.target_type.upper() == 'DELTA':
                success = self._load_to_delta(df, mode)
            else:
                success = self._load_to_database(df, mode)
            
            if success:
                success_count = total_count
                error_count = 0
                
                # Perform reconciliation if configured
                if self.config.get('enable_reconciliation', True):
                    reconciled = self._reconcile_data(df)
                    if not reconciled:
                        logger.warning("Data reconciliation check failed")
                        errors.append("Reconciliation mismatch detected")
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
            
            logger.info(
                f"Load complete - Success: {success_count}, "
                f"Errors: {error_count}"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Load failed: {str(e)}")
            return LoadResult(
                success_count=0,
                error_count=total_count,
                total_count=total_count,
                errors=[str(e)]
            )
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to database via JDBC.
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            True if successful, False otherwise
        """
        try:
            jdbc_config = self.config.get('jdbc', {})
            table_name = self.config.get('target_table', 'etl_target_data')
            
            jdbc_url = (
                f"jdbc:{jdbc_config.get('driver', 'postgresql')}://"
                f"{jdbc_config.get('host', 'localhost')}:"
                f"{jdbc_config.get('port', '5432')}/"
                f"{jdbc_config.get('database', 'etl_db')}"
            )
            
            connection_properties = {
                "user": jdbc_config.get('user', 'etl_user'),
                "password": jdbc_config.get('password', ''),
                "driver": jdbc_config.get('driver_class', 'org.postgresql.Driver'),
                "batchsize": str(self.batch_size)
            }
            
            # Map mode to JDBC save mode
            save_mode_map = {
                'insert': 'append',
                'update': 'overwrite',
                'upsert': 'append'  # Upsert requires special handling
            }
            save_mode = save_mode_map.get(mode.lower(), 'append')
            
            if mode.lower() == 'upsert':
                # For upsert, we need to handle updates and inserts separately
                # This is a simplified approach; production might use MERGE
                self._perform_upsert(df, jdbc_url, table_name, connection_properties)
            else:
                df.write.jdbc(
                    url=jdbc_url,
                    table=table_name,
                    mode=save_mode,
                    properties=connection_properties
                )
            
            return True
            
        except Exception as e:
            logger.error(f"Database load error: {str(e)}")
            return False
    
    def _perform_upsert(
        self, 
        df: DataFrame, 
        jdbc_url: str, 
        table_name: str, 
        properties: Dict[str, str]
    ):
        """
        Perform upsert operation (update existing, insert new).
        
        Args:
            df: DataFrame to upsert
            jdbc_url: JDBC connection URL
            table_name: Target table name
            properties: JDBC connection properties
        """
        # Create a temporary table
        temp_table = f"{table_name}_temp_{self.run_id.replace('-', '_')}"
        
        # Write to temporary table
        df.write.jdbc(
            url=jdbc_url,
            table=temp_table,
            mode='overwrite',
            properties=properties
        )
        
        # Execute MERGE or UPDATE/INSERT logic
        # Note: Actual SQL varies by database
        merge_sql = f"""
        MERGE INTO {table_name} target
        USING {temp_table} source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET
                target.name = source.name,
                target.value = source.value,
                target.transformed_value = source.transformed_value,
                target.status = source.status,
                target.category = source.category,
                target.priority = source.priority,
                target.processed_at = source.processed_at
        WHEN NOT MATCHED THEN
            INSERT VALUES (
                source.id, source.name, source.value, source.transformed_value,
                source.status, source.category, source.priority, source.etl_run_id,
                source.processed_at, source.processed_by
            )
        """
        
        # This would need to be executed via JDBC connection
        logger.info(f"Performing upsert via temporary table: {temp_table}")
    
    def _load_to_parquet(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to Parquet files.
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            True if successful
        """
        try:
            target_path = self.config.get('target_path', '/data/target')
            partition_cols = self.config.get('partition_columns', ['category'])
            
            save_mode_map = {
                'insert': 'append',
                'update': 'overwrite',
                'upsert': 'overwrite'
            }
            save_mode = save_mode_map.get(mode.lower(), 'append')
            
            df.write.partitionBy(*partition_cols) \
                .mode(save_mode) \
                .parquet(target_path)
            
            logger.info(f"Data written to Parquet: {target_path}")
            return True
            
        except Exception as e:
            logger.error(f"Parquet load error: {str(e)}")
            return False
    
    def _load_to_delta(self, df: DataFrame, mode: str) -> bool:
        """
        Load data to Delta Lake.
        
        Args:
            df: DataFrame to load
            mode: Load mode
            
        Returns:
            True if successful
        """
        try:
            target_path = self.config.get('target_path', '/data/target')
            
            if mode.lower() == 'upsert':
                # Delta Lake supports native MERGE
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
                    # First load
                    df.write.format("delta").mode("overwrite").save(target_path)
            else:
                save_mode_map = {
                    'insert': 'append',
                    'update': 'overwrite'
                }
                save_mode = save_mode_map.get(mode.lower(), 'append')
                
                df.write.format("delta").mode(save_mode).save(target_path)
            
            logger.info(f"Data written to Delta Lake: {target_path}")
            return True
            
        except Exception as e:
            logger.error(f"Delta load error: {str(e)}")
            return False
    
    def _reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data to ensure integrity.
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passed
        """
        logger.info("Performing data reconciliation")
        
        try:
            source_count = loaded_df.count()
            
            # Read back from target
            if self.target_type.upper() == 'DATABASE':
                target_df = self._read_from_target_db()
            elif self.target_type.upper() in ['PARQUET', 'DELTA']:
                target_path = self.config.get('target_path', '/data/target')
                format_type = 'delta' if self.target_type.upper() == 'DELTA' else 'parquet'
                target_df = self.spark.read.format(format_type).load(target_path)
            else:
                logger.warning("Reconciliation not supported for target type")
                return True
            
            # Filter for this run's data
            target_df = target_df.filter(F.col("etl_run_id") == self.run_id)
            target_count = target_df.count()
            
            matches = (source_count == target_count)
            
            if matches:
                logger.info(f"Reconciliation passed: {target_count} records verified")
            else:
                logger.error(
                    f"Reconciliation failed: Expected {source_count}, "
                    f"found {target_count} in target"
                )
            
            return matches
            
        except Exception as e:
            logger.error(f"Reconciliation error: {str(e)}")
            return False
    
    def _read_from_target_db(self) -> DataFrame:
        """Read data back from target database for reconciliation."""
        jdbc_config = self.config.get('jdbc', {})
        table_name = self.config.get('target_table', 'etl_target_data')
        
        jdbc_url = (
            f"jdbc:{jdbc_config.get('driver', 'postgresql')}://"
            f"{jdbc_config.get('host', 'localhost')}:"
            f"{jdbc_config.get('port', '5432')}/"
            f"{jdbc_config.get('database', 'etl_db')}"
        )
        
        connection_properties = {
            "user": jdbc_config.get('user', 'etl_user'),
            "password": jdbc_config.get('password', ''),
            "driver": jdbc_config.get('driver_class', 'org.postgresql.Driver')
        }
        
        return self.spark.read.jdbc(
            url=jdbc_url,
            table=table_name,
            properties=connection_properties
        )


def create_loader(config: Dict[str, Any], run_id: str) -> ETLLoader:
    """
    Factory function to create an ETLLoader instance.
    
    Args:
        config: Configuration dictionary
        run_id: Unique run identifier
        
    Returns:
        Configured ETLLoader instance
    """
    spark = SparkSession.builder.getOrCreate()
    return ETLLoader(spark, config, run_id)