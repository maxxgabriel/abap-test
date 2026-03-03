"""ETL Data Loading Module"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from typing import Dict, List
import logging


class ETLLoader:
    """Load data to target destination"""
    
    def __init__(self, spark: SparkSession, run_id: str, target_type: str = "DATABASE", batch_size: int = 1000):
        self.spark = spark
        self.run_id = run_id
        self.target_type = target_type
        self.batch_size = batch_size
        self.logger = logging.getLogger(__name__)
    
    def load_data(self, df: DataFrame, mode: str = "INSERT") -> Dict[str, int]:
        """Main loading method"""
        self.logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        result = {
            "success_count": 0,
            "error_count": 0,
            "total_count": df.count()
        }
        
        try:
            if mode.upper() == "INSERT":
                self._insert_data(df)
            elif mode.upper() == "UPDATE":
                self._update_data(df)
            elif mode.upper() == "UPSERT":
                self._upsert_data(df)
            else:
                self._insert_data(df)
            
            result["success_count"] = result["total_count"]
            
            # Reconcile data
            if self._reconcile_data(df):
                self.logger.info("Data reconciliation successful")
            else:
                self.logger.warning("Data reconciliation failed")
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            result["error_count"] = result["total_count"]
            result["success_count"] = 0
        
        self.logger.info(f"Load complete - Success: {result['success_count']}, Errors: {result['error_count']}")
        
        return result
    
    def _insert_data(self, df: DataFrame) -> None:
        """Insert data into target table"""
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/etl_db") \
            .option("dbtable", "etl_target_data") \
            .option("user", "etl_user") \
            .option("password", "etl_password") \
            .option("driver", "org.postgresql.Driver") \
            .option("batchsize", self.batch_size) \
            .mode("append") \
            .save()
    
    def _update_data(self, df: DataFrame) -> None:
        """Update existing records"""
        # For update, we need to use a temporary table
        temp_table = f"temp_update_{self.run_id}"
        df.createOrReplaceTempView(temp_table)
        
        # Write to temp table
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/etl_db") \
            .option("dbtable", temp_table) \
            .option("user", "etl_user") \
            .option("password", "etl_password") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        
        # Execute update via JDBC
        update_query = f"""
        UPDATE etl_target_data t
        SET name = s.name,
            value = s.value,
            transformed_value = s.transformed_value,
            status = s.status,
            category = s.category,
            priority = s.priority,
            processed_at = s.processed_at,
            processed_by = s.processed_by
        FROM {temp_table} s
        WHERE t.id = s.id
        """
        
        # Execute via JDBC connection
        self._execute_sql(update_query)
    
    def _upsert_data(self, df: DataFrame) -> None:
        """Insert or update data (upsert)"""
        # Try update first
        try:
            self._update_data(df)
        except Exception as e:
            self.logger.warning(f"Update failed, attempting insert: {str(e)}")
            self._insert_data(df)
    
    def _reconcile_data(self, df: DataFrame) -> bool:
        """Reconcile loaded data with source"""
        try:
            # Count records in target
            target_df = self.spark.read \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://localhost:5432/etl_db") \
                .option("dbtable", f"(SELECT COUNT(*) as cnt FROM etl_target_data WHERE etl_run_id = '{self.run_id}') AS target") \
                .option("user", "etl_user") \
                .option("password", "etl_password") \
                .option("driver", "org.postgresql.Driver") \
                .load()
            
            target_count = target_df.collect()[0]["cnt"]
            source_count = df.count()
            
            matches = target_count == source_count
            
            if not matches:
                self.logger.warning(f"Reconciliation mismatch: Source={source_count}, Target={target_count}")
            
            return matches
            
        except Exception as e:
            self.logger.error(f"Reconciliation failed: {str(e)}")
            return False
    
    def _execute_sql(self, sql: str) -> None:
        """Execute SQL statement via JDBC"""
        # This is a placeholder - in production, use proper JDBC connection
        self.logger.info(f"Executing SQL: {sql}")