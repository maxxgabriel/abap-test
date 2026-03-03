"""
Data extraction module for PySpark ETL framework.
Handles data extraction from various sources with filtering and incremental load support.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp
from datetime import datetime
from typing import Optional, Dict, Any
import logging


class DataExtractor:
    """Extracts data from various sources for ETL processing."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the data extractor.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def extract_data(
        self,
        source_type: str = "database",
        filter_expr: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data from specified source.
        
        Args:
            source_type: Type of source (database, staging, incremental)
            filter_expr: Optional filter expression
            max_records: Maximum records to extract (0 = no limit)
            
        Returns:
            DataFrame containing extracted data
        """
        self.logger.info(f"Starting extraction - Source: {source_type}, Run ID: {self.run_id}")
        
        try:
            if source_type == "database":
                df = self._extract_from_database(filter_expr)
            elif source_type == "staging":
                df = self._extract_from_staging()
            elif source_type == "incremental":
                df = self._extract_incremental()
            else:
                df = self._extract_from_database(filter_expr)
            
            # Apply record limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _extract_from_database(self, filter_expr: Optional[str] = None) -> DataFrame:
        """Extract data from database source."""
        jdbc_config = self.config["source"]["jdbc"]
        
        query = f"(SELECT * FROM {jdbc_config['table']}"
        if filter_expr:
            query += f" WHERE {filter_expr}"
        query += ") as source_data"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config["url"]) \
            .option("dbtable", query) \
            .option("user", jdbc_config.get("user", "")) \
            .option("password", jdbc_config.get("password", "")) \
            .option("driver", jdbc_config.get("driver", "")) \
            .load()
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract data from staging area."""
        staging_path = self.config["source"]["staging"]["path"]
        file_format = self.config["source"]["staging"].get("format", "parquet")
        
        df = self.spark.read \
            .format(file_format) \
            .load(f"{staging_path}/run_id={self.run_id}")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last successful run."""
        jdbc_config = self.config["source"]["jdbc"]
        
        # Get last successful run timestamp
        last_run_query = f"""
        (SELECT MAX(end_time) as last_run_time 
         FROM {jdbc_config.get('log_table', 'etl_run_log')}
         WHERE status = 'SUCCESS') as last_run
        """
        
        last_run_df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config["url"]) \
            .option("dbtable", last_run_query) \
            .option("user", jdbc_config.get("user", "")) \
            .option("password", jdbc_config.get("password", "")) \
            .option("driver", jdbc_config.get("driver", "")) \
            .load()
        
        last_run_time = last_run_df.first()["last_run_time"]
        
        if last_run_time:
            filter_expr = f"changed_at > '{last_run_time}'"
            self.logger.info(f"Incremental load since: {last_run_time}")
            return self._extract_from_database(filter_expr)
        else:
            self.logger.warning("No previous successful run found, performing full load")
            return self._extract_from_database()