"""
PySpark Data Loading Module
Implements batch loading with reconciliation
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict, Any, Tuple
import logging


class SparkLoader:
    """
    Handles data loading to target systems with batching and reconciliation
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize loader
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.batch_size = config.get("loading", {}).get("batch_size", 1000)
        self.logger = logging.getLogger(__name__)
    
    def load_data(
        self,
        df: DataFrame,
        mode: str = "append",
        enable_reconciliation: bool = True
    ) -> Dict[str, Any]:
        """
        Load data to target with batching and error handling
        
        Args:
            df: DataFrame to load
            mode: Load mode (append, overwrite, upsert)
            enable_reconciliation: Whether to perform reconciliation
            
        Returns:
            Dictionary with load results
        """
        self.logger.info(f"Starting data load - Mode: {mode}, Records: {df.count()}")
        
        initial_count = df.count()
        success_count = 0
        error_count = 0
        errors = []
        
        try:
            if mode.lower() == "upsert":
                success_count = self._load_upsert(df)
            elif mode.lower() == "insert":
                success_count = self._load_insert(df)
            elif mode.lower() == "update":
                success_count = self._load_update(df)
            else:
                success_count = self._load_append(df)
            
            error_count = initial_count - success_count
            
            # Perform reconciliation if enabled
            if enable_reconciliation and success_count > 0:
                reconciled = self._reconcile_data(df)
                if not reconciled:
                    errors.append("Data reconciliation failed")
            
        except Exception as e:
            self.logger.error(f"Load error: {str(e)}")
            errors.append(str(e))
            error_count = initial_count
        
        result = {
            "success_count": success_count,
            "error_count": error_count,
            "total_count": initial_count,
            "errors": errors
        }
        
        self.logger.info(
            f"Load complete - Success: {success_count}, Errors: {error_count}"
        )
        
        return result
    
    def _load_append(self, df: DataFrame) -> int:
        """
        Append data to target table
        
        Args:
            df: DataFrame to load
            
        Returns:
            Number of records loaded
        """
        jdbc_config = self.config.get("database", {})
        target_table = jdbc_config.get("target_table", "etl_target_data")
        
        df.write \
            .format("jdbc") \
            .option("url", jdbc_config.get("url")) \
            .option("dbtable", target_table) \
            .option("user", jdbc_config.get("user")) \
            .option("password", jdbc_config.get("password")) \
            .option("driver", jdbc_config.get("driver", "org.postgresql.Driver")) \
            .option("batchsize", self.batch_size) \
            .mode("append") \
            .save()
        
        return df.count()
    
    def _load_insert(self, df: DataFrame) -> int:
        """
        Insert new records only
        
        Args:
            df: DataFrame to load
            
        Returns:
            Number of records loaded
        """
        return self._load_append(df)
    
    def _load_update(self, df: DataFrame) -> int:
        """
        Update existing records
        
        Args:
            df: DataFrame to load
            
        Returns:
            Number of records updated
        """
        jdbc_config = self.config.get("database", {})
        target_table = jdbc_config.get("target_table", "etl_target_data")
        
        # Create temp table
        temp_table = f"temp_{self.run_id}"
        df.createOrReplaceTempView(temp_table)
        
        # Build update query
        update_query = f"""
        UPDATE {target_table} t
        SET 
            name = s.name,
            value = s.value,
            transformed_value = s.transformed_value,
            status = s.status,
            category = s.category,
            priority = s.priority,
            etl_run_id = s.etl_run_id,
            processed_at = s.processed_at,
            processed_by = s.processed_by
        FROM {temp_table} s
        WHERE t.id = s.id
        """
        
        # Execute via JDBC
        connection = self._get_jdbc_connection()
        cursor = connection.cursor()
        cursor.execute(update_query)
        updated_count = cursor.rowcount
        connection.commit()
        cursor.close()
        connection.close()
        
        return updated_count
    
    def _load_upsert(self, df: DataFrame) -> int:
        """
        Upsert (insert or update) records
        
        Args:
            df: DataFrame to load
            
        Returns:
            Number of records processed
        """
        jdbc_config = self.config.get("database", {})
        target_table = jdbc_config.get("target_table", "etl_target_data")
        
        # Read existing IDs
        existing_df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config.get("url")) \
            .option("dbtable", f"(SELECT id FROM {target_table}) as existing") \
            .option("user", jdbc_config.get("user")) \
            .option("password", jdbc_config.get("password")) \
            .option("driver", jdbc_config.get("driver")) \
            .load()
        
        # Split into updates and inserts
        updates_df = df.join(existing_df, "id", "inner")
        inserts_df = df.join(existing_df, "id", "left_anti")
        
        update_count = updates_df.count()
        insert_count = inserts_df.count()
        
        # Perform updates
        if update_count > 0:
            self._load_update(updates_df)
        
        # Perform inserts
        if insert_count > 0:
            self._load_insert(inserts_df)
        
        return update_count + insert_count
    
    def _reconcile_data(self, df: DataFrame) -> bool:
        """
        Reconcile loaded data with source
        
        Args:
            df: Source DataFrame
            
        Returns:
            True if reconciliation passes
        """
        self.logger.info("Starting data reconciliation")
        
        jdbc_config = self.config.get("database", {})
        target_table = jdbc_config.get("target_table", "etl_target_data")
        
        # Read target data for this run
        target_df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config.get("url")) \
            .option("dbtable", f"(SELECT * FROM {target_table} WHERE etl_run_id = '{self.run_id}') as target") \
            .option("user", jdbc_config.get("user")) \
            .option("password", jdbc_config.get("password")) \
            .option("driver", jdbc_config.get("driver")) \
            .load()
        
        source_count = df.count()
        target_count = target_df.count()
        
        # Check counts match
        if source_count != target_count:
            self.logger.warning(
                f"Reconciliation mismatch - Source: {source_count}, Target: {target_count}"
            )
            return False
        
        # Verify key columns match
        source_ids = df.select("id").distinct().count()
        target_ids = target_df.select("id").distinct().count()
        
        if source_ids != target_ids:
            self.logger.warning(
                f"ID reconciliation mismatch - Source: {source_ids}, Target: {target_ids}"
            )
            return False
        
        self.logger.info("Reconciliation passed")
        return True
    
    def _get_jdbc_connection(self):
        """
        Get JDBC connection (placeholder for actual implementation)
        
        Returns:
            Database connection
        """
        # This would use actual JDBC connection in production
        # Using psycopg2 or similar driver
        pass