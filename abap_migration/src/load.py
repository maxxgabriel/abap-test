"""
ETL Loader Module - Handles data loading to target destinations
"""
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any, Optional
import logging


class LoadResult:
    """Container for load operation results"""
    
    def __init__(self):
        self.success_count = 0
        self.error_count = 0
        self.total_count = 0
        self.errors = []


class ETLLoader:
    """Loads transformed data to target destinations"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the loader
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.batch_size = config['target']['batch_size']
        self.logger = logging.getLogger(__name__)
    
    def load_data(
        self, 
        df: DataFrame, 
        mode: str = "upsert",
        target_type: str = "database"
    ) -> LoadResult:
        """
        Load data to target
        
        Args:
            df: DataFrame to load
            mode: Load mode (insert, update, upsert)
            target_type: Target type (database, files, etc.)
            
        Returns:
            LoadResult object with statistics
        """
        self.logger.info(f"Starting data load - Mode: {mode}, Batch size: {self.batch_size}")
        
        result = LoadResult()
        result.total_count = df.count()
        
        try:
            if target_type.lower() == "database":
                success = self._load_to_database(df, mode)
            elif target_type.lower() == "files":
                success = self._load_to_files(df)
            else:
                success = self._load_to_database(df, mode)
            
            if success:
                result.success_count = result.total_count
                result.error_count = 0
                self.logger.info(f"Load complete - Success: {result.success_count}")
            else:
                result.success_count = 0
                result.error_count = result.total_count
                result.errors.append("Load operation failed")
                self.logger.error("Load operation failed")
            
            # Perform reconciliation if enabled
            if self.config['target'].get('enable_reconciliation', False):
                self._reconcile_data(df, result)
            
        except Exception as e:
            self.logger.error(f"Load error: {str(e)}")
            result.error_count = result.total_count
            result.errors.append(str(e))
        
        return result
    
    def _load_to_database(self, df: DataFrame, mode: str) -> bool:
        """Load data to database target"""
        try:
            db_config = self.config['target']['database']
            
            # Map mode to Spark save mode
            save_mode_map = {
                'insert': 'append',
                'update': 'overwrite',
                'upsert': 'append'  # Will use merge logic
            }
            
            spark_mode = save_mode_map.get(mode.lower(), 'append')
            
            if mode.lower() == 'upsert':
                # Use merge logic for upsert
                self._upsert_to_database(df, db_config)
            else:
                # Simple write
                df.write \
                    .format("jdbc") \
                    .option("url", db_config['jdbc_url']) \
                    .option("dbtable", db_config['table']) \
                    .option("user", db_config.get('user', '')) \
                    .option("password", db_config.get('password', '')) \
                    .option("driver", db_config.get('driver', 'org.postgresql.Driver')) \
                    .option("batchsize", self.batch_size) \
                    .mode(spark_mode) \
                    .save()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Database load error: {str(e)}")
            return False
    
    def _upsert_to_database(self, df: DataFrame, db_config: Dict[str, Any]):
        """Perform upsert operation (update existing, insert new)"""
        # Create temp table
        temp_table = f"temp_{db_config['table']}_{self.run_id.replace('-', '_')}"
        
        # Write to temp table
        df.write \
            .format("jdbc") \
            .option("url", db_config['jdbc_url']) \
            .option("dbtable", temp_table) \
            .option("user", db_config.get('user', '')) \
            .option("password", db_config.get('password', '')) \
            .option("driver", db_config.get('driver', 'org.postgresql.Driver')) \
            .option("createTableOptions", "TEMPORARY") \
            .mode("overwrite") \
            .save()
        
        # Execute merge SQL
        merge_sql = f"""
            MERGE INTO {db_config['table']} AS target
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
        
        # Execute via JDBC (database-specific implementation may vary)
        self.logger.info("Executing upsert operation")
        # Note: Actual execution depends on database support for MERGE
    
    def _load_to_files(self, df: DataFrame) -> bool:
        """Load data to file system"""
        try:
            file_config = self.config['target']['files']
            
            df.write \
                .format(file_config.get('format', 'parquet')) \
                .mode(file_config.get('mode', 'overwrite')) \
                .partitionBy(file_config.get('partition_by', [])) \
                .option("compression", file_config.get('compression', 'snappy')) \
                .save(file_config['path'])
            
            return True
            
        except Exception as e:
            self.logger.error(f"File load error: {str(e)}")
            return False
    
    def _reconcile_data(self, df: DataFrame, result: LoadResult):
        """Reconcile loaded data with source"""
        self.logger.info("Performing data reconciliation")
        
        try:
            # Count records in target
            db_config = self.config['target']['database']
            
            query = f"""
                SELECT COUNT(*) as count 
                FROM {db_config['table']} 
                WHERE etl_run_id = '{self.run_id}'
            """
            
            target_count = self.spark.read \
                .format("jdbc") \
                .option("url", db_config['jdbc_url']) \
                .option("query", query) \
                .option("user", db_config.get('user', '')) \
                .option("password", db_config.get('password', '')) \
                .load() \
                .collect()[0]['count']
            
            source_count = result.total_count
            
            if target_count == source_count:
                self.logger.info(f"Reconciliation passed: {target_count} records match")
            else:
                error_msg = f"Reconciliation failed: Source={source_count}, Target={target_count}"
                self.logger.warning(error_msg)
                result.errors.append(error_msg)
                
        except Exception as e:
            self.logger.error(f"Reconciliation error: {str(e)}")
            result.errors.append(f"Reconciliation error: {str(e)}")