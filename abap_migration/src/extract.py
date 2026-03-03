"""
Extract module for Delta Lake ETL pipeline.
Replaces ABAP extractor with PySpark DataFrame operations.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit
from datetime import datetime
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


class DataExtractor:
    """Extract data from various sources using PySpark DataFrame API."""
    
    def __init__(self, spark: SparkSession, run_id: str, config: Dict[str, Any]):
        """
        Initialize extractor.
        
        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.source_type = config.get('source_type', 'DATABASE')
        
    def extract_data(
        self,
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data based on source type.
        
        Args:
            filter_condition: SQL WHERE clause filter
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        logger.info(f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}")
        
        if self.source_type == 'DATABASE':
            df = self.extract_from_database(filter_condition)
        elif self.source_type == 'STAGING':
            df = self.extract_from_staging()
        elif self.source_type == 'INCREMENTAL':
            df = self.extract_incremental()
        else:
            df = self.extract_from_database(filter_condition)
            
        # Apply record limit if specified
        if max_records > 0:
            df = df.limit(max_records)
            
        record_count = df.count()
        logger.info(f"Extracted {record_count} records")
        
        return df
    
    def extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract from source database/table.
        
        Args:
            filter_condition: SQL WHERE clause
            
        Returns:
            DataFrame with source data
        """
        jdbc_config = self.config['jdbc']
        
        query = f"(SELECT * FROM {jdbc_config['source_table']}"
        
        if filter_condition:
            query += f" WHERE {filter_condition}"
        
        query += f" LIMIT {self.config.get('default_limit', 1000)}) AS source"
        
        df = (self.spark.read
              .format("jdbc")
              .option("url", jdbc_config['url'])
              .option("dbtable", query)
              .option("user", jdbc_config['user'])
              .option("password", jdbc_config['password'])
              .option("driver", jdbc_config['driver'])
              .load())
        
        return df
    
    def extract_from_staging(self) -> DataFrame:
        """
        Extract from staging area.
        
        Returns:
            DataFrame with staged data
        """
        staging_path = self.config['staging_path']
        
        df = (self.spark.read
              .format("delta")
              .load(staging_path)
              .filter(col("run_id") == self.run_id)
              .filter(col("status") == "READY"))
        
        logger.info(f"Extracted {df.count()} records from staging")
        return df
    
    def extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run.
        
        Returns:
            DataFrame with incremental data
        """
        # Get last successful run timestamp
        last_run_time = self._get_last_run_timestamp()
        
        if last_run_time:
            logger.info(f"Extracting incremental data since {last_run_time}")
            jdbc_config = self.config['jdbc']
            
            query = f"""(
                SELECT * FROM {jdbc_config['source_table']}
                WHERE changed_at > '{last_run_time}'
            ) AS incremental"""
            
            df = (self.spark.read
                  .format("jdbc")
                  .option("url", jdbc_config['url'])
                  .option("dbtable", query)
                  .option("user", jdbc_config['user'])
                  .option("password", jdbc_config['password'])
                  .option("driver", jdbc_config['driver'])
                  .load())
        else:
            logger.warning("No last run timestamp found, performing full extraction")
            df = self.extract_from_database()
            
        return df
    
    def _get_last_run_timestamp(self) -> Optional[str]:
        """
        Get timestamp of last successful run.
        
        Returns:
            Timestamp string or None
        """
        try:
            run_log_path = self.config.get('run_log_path')
            if not run_log_path:
                return None
                
            df = (self.spark.read
                  .format("delta")
                  .load(run_log_path)
                  .filter(col("status") == "SUCCESS")
                  .orderBy(col("end_time").desc())
                  .limit(1))
            
            if df.count() > 0:
                return df.first()['end_time']
                
        except Exception as e:
            logger.warning(f"Could not retrieve last run timestamp: {e}")
            
        return None