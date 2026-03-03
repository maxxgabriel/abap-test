"""
ETL Data Extraction Module
Handles extraction from various data sources with support for incremental loads.
"""

from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from datetime import datetime

from src.config_manager import ConfigManager
from src.logger import ETLLogger


class ETLExtractor:
    """Extract data from various sources."""
    
    def __init__(self, spark: SparkSession, source_type: str = "DATABASE", run_id: str = ""):
        """
        Initialize extractor.
        
        Args:
            spark: Active Spark session
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            run_id: Unique run identifier
        """
        self.spark = spark
        self.source_type = source_type
        self.run_id = run_id
        self.config = ConfigManager()
        self.logger = ETLLogger(component="EXTRACTOR", run_id=run_id)
    
    def extract_data(
        self,
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data based on source type.
        
        Args:
            filter_condition: Optional SQL filter condition
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            Extracted DataFrame
        """
        self.logger.log_info(f"Starting extraction - Source: {self.source_type}")
        
        try:
            if self.source_type == "DATABASE":
                df = self._extract_from_database(filter_condition)
            elif self.source_type == "STAGING":
                df = self._extract_from_staging()
            elif self.source_type == "INCREMENTAL":
                df = self._extract_incremental()
            else:
                df = self._extract_from_database(filter_condition)
            
            # Apply max records limit
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.log_info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.log_error(f"Extraction failed: {str(e)}")
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract from source database table."""
        source_table = self.config.get("source_table", "etl_source_data")
        
        query = f"SELECT * FROM {source_table}"
        
        if filter_condition:
            query += f" WHERE {filter_condition}"
        
        df = self.spark.sql(query)
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract from staging table."""
        staging_table = self.config.get("staging_table", "etl_staging")
        
        df = self.spark.sql(f"""
            SELECT *
            FROM {staging_table}
            WHERE run_id = '{self.run_id}'
              AND status = 'READY'
        """)
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last successful run."""
        # Get last successful run timestamp
        run_log_table = self.config.get("run_log_table", "etl_run_log")
        
        last_run_df = self.spark.sql(f"""
            SELECT MAX(end_time) as last_run_time
            FROM {run_log_table}
            WHERE status = 'SUCCESS'
        """)
        
        last_run_time = last_run_df.first()["last_run_time"]
        
        if last_run_time:
            source_table = self.config.get("source_table", "etl_source_data")
            df = self.spark.sql(f"""
                SELECT *
                FROM {source_table}
                WHERE changed_at > '{last_run_time}'
            """)
        else:
            # No previous run, do full extraction
            self.logger.log_info("No previous run found, performing full extraction")
            df = self._extract_from_database()
        
        return df