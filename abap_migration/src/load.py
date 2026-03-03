"""
PySpark ETL Loader Module
Migrated from zcl_etl_loader.abap
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Dict, List
import logging


class LoadResult:
    """Result of data loading operation."""
    
    def __init__(self):
        self.success_count: int = 0
        self.error_count: int = 0
        self.total_count: int = 0
        self.errors: List[str] = []


class ETLLoader:
    """Load transformed data to target destination."""
    
    def __init__(self, target_type: str = "DATABASE", batch_size: int = 1000, 
                 run_id: str = None, spark: SparkSession = None):
        """
        Initialize ETL Loader.
        
        Args:
            target_type: Type of target (DATABASE, etc.)
            batch_size: Number of records per batch
            run_id: Unique run identifier
            spark: SparkSession instance
        """
        self.target_type = target_type
        self.batch_size = batch_size
        self.run_id = run_id
        self.spark = spark or SparkSession.builder.getOrCreate()
        self.logger = logging.getLogger(__name__)
        
    def load_data(self, df: DataFrame, mode: str = "INSERT") -> LoadResult:
        """
        Load data to target destination.
        
        Args:
            df: DataFrame to load
            mode: Load mode (INSERT, UPDATE, UPSERT)
            
        Returns:
            LoadResult with operation statistics
        """
        self.logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        result = LoadResult()
        result.total_count = df.count()
        
        try:
            # Load based on mode
            if mode == "INSERT":
                success = self._insert_new(df)
            elif mode == "UPDATE":
                success = self._update_existing(df)
            elif mode == "UPSERT":
                success = self._upsert(df)
            else:
                success = self._insert_new(df)
            
            if success:
                result.success_count = result.total_count
                self.logger.info(f"Successfully loaded {result.success_count} records")
            else:
                result.error_count = result.total_count
                result.errors.append("Load operation failed")
                self.logger.error("Load operation failed")
            
            # Reconcile data
            if success:
                reconciled = self._reconcile_data(df)
                if not reconciled:
                    self.logger.warning("Data reconciliation failed")
                    result.errors.append("Reconciliation check failed")
            
        except Exception as e:
            self.logger.error(f"Load error: {str(e)}")
            result.error_count = result.total_count
            result.errors.append(str(e))
        
        self.logger.info(f"Load complete - Success: {result.success_count}, Errors: {result.error_count}")
        
        return result
    
    def _insert_new(self, df: DataFrame) -> bool:
        """
        Insert new records to target.
        
        Args:
            df: DataFrame to insert
            
        Returns:
            True if successful
        """
        try:
            df.write \
                .format("jdbc") \
                .option("url", self._get_jdbc_url()) \
                .option("dbtable", "zetl_target_data") \
                .option("driver", self._get_jdbc_driver()) \
                .mode("append") \
                .save()
            
            return True
        except Exception as e:
            self.logger.error(f"Insert failed: {str(e)}")
            return False
    
    def _update_existing(self, df: DataFrame) -> bool:
        """
        Update existing records in target.
        
        Args:
            df: DataFrame with updates
            
        Returns:
            True if successful
        """
        try:
            # Create temporary view for merge operation
            df.createOrReplaceTempView("updates")
            
            # Execute update via JDBC or Delta Lake merge
            # This is a simplified version - actual implementation depends on target DB
            self.spark.sql("""
                MERGE INTO zetl_target_data AS target
                USING updates AS source
                ON target.id = source.id
                WHEN MATCHED THEN UPDATE SET *
            """)
            
            return True
        except Exception as e:
            self.logger.error(f"Update failed: {str(e)}")
            return False
    
    def _upsert(self, df: DataFrame) -> bool:
        """
        Insert or update records (upsert).
        
        Args:
            df: DataFrame to upsert
            
        Returns:
            True if successful
        """
        try:
            # Create temporary view
            df.createOrReplaceTempView("upserts")
            
            # Execute merge operation
            self.spark.sql("""
                MERGE INTO zetl_target_data AS target
                USING upserts AS source
                ON target.id = source.id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
            
            return True
        except Exception as e:
            self.logger.error(f"Upsert failed: {str(e)}")
            # Fallback: try update then insert
            try:
                self._update_existing(df)
            except:
                pass
            try:
                self._insert_new(df)
                return True
            except:
                return False
    
    def _reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data with target to verify integrity.
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            True if reconciliation passes
        """
        try:
            source_count = loaded_df.count()
            
            # Read back from target
            target_df = self.spark.read \
                .format("jdbc") \
                .option("url", self._get_jdbc_url()) \
                .option("dbtable", "zetl_target_data") \
                .option("driver", self._get_jdbc_driver()) \
                .load() \
                .filter(F.col("etl_run_id") == self.run_id)
            
            target_count = target_df.count()
            
            if source_count != target_count:
                self.logger.warning(
                    f"Reconciliation mismatch: Source={source_count}, Target={target_count}"
                )
                return False
            
            self.logger.info("Data reconciliation successful")
            return True
            
        except Exception as e:
            self.logger.error(f"Reconciliation error: {str(e)}")
            return False
    
    def _get_jdbc_url(self) -> str:
        """Get JDBC connection URL from config."""
        return "jdbc:postgresql://localhost:5432/etl_db"
    
    def _get_jdbc_driver(self) -> str:
        """Get JDBC driver class name."""
        return "org.postgresql.Driver"