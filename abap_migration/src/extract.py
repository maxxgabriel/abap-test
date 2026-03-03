"""
ETL Extractor
Handles data extraction from various sources.
Migrated from ABAP class zcl_etl_extractor
"""

from typing import Optional
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit

from src.logger import ETLLogger
from src.config import ETLConfig


class ETLExtractor:
    """Handles data extraction from various sources."""
    
    def __init__(self, spark: SparkSession, config: ETLConfig,
                 source_type: str = 'DATABASE', run_id: str = ''):
        """
        Initialize ETL extractor.
        
        Args:
            spark: SparkSession instance
            config: ETL configuration
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL, FILE)
            run_id: Run identifier
        """
        self.spark = spark
        self.config = config
        self.source_type = source_type
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
        
    def extract_data(self, filter_clause: str = '', 
                    max_records: int = 0) -> DataFrame:
        """
        Extract data based on source type.
        
        Args:
            filter_clause: SQL filter clause
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.log_info(
            component='EXTRACTOR',
            message=f"Starting extraction - Source: {self.source_type}"
        )
        
        try:
            if self.source_type == 'DATABASE':
                df = self._extract_from_database(filter_clause)
            elif self.source_type == 'STAGING':
                df = self._extract_from_staging()
            elif self.source_type == 'INCREMENTAL':
                df = self._extract_incremental()
            elif self.source_type == 'FILE':
                df = self._extract_from_file()
            else:
                df = self._extract_from_database(filter_clause)
            
            # Apply max records limit
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.log_info(
                component='EXTRACTOR',
                message=f"Extracted {record_count} records"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component='EXTRACTOR',
                message='Extraction failed',
                details=str(e)
            )
            raise
    
    def _extract_from_database(self, filter_clause: str = '') -> DataFrame:
        """Extract data from database table."""
        source_table = self.config.get('tables.source', 'etl_source_data')
        
        query = f"SELECT * FROM {source_table}"
        if filter_clause:
            query += f" WHERE {filter_clause}"
        
        return self.spark.sql(query)
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract data from staging area."""
        staging_table = self.config.get('tables.staging', 'etl_staging')
        
        query = f"""
            SELECT * FROM {staging_table}
            WHERE run_id = '{self.run_id}'
              AND status = 'READY'
        """
        
        return self.spark.sql(query)
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last successful run."""
        source_table = self.config.get('tables.source', 'etl_source_data')
        run_log_table = self.config.get('tables.run_log', 'etl_run_log')
        
        # Get last successful run time
        last_run_query = f"""
            SELECT MAX(end_time) as last_run_time
            FROM {run_log_table}
            WHERE status = 'SUCCESS'
        """
        
        last_run_result = self.spark.sql(last_run_query).collect()
        
        if last_run_result and last_run_result[0]['last_run_time']:
            last_run_time = last_run_result[0]['last_run_time']
            
            query = f"""
                SELECT * FROM {source_table}
                WHERE changed_at > '{last_run_time}'
            """
        else:
            # No previous successful run, extract all
            query = f"SELECT * FROM {source_table}"
        
        return self.spark.sql(query)
    
    def _extract_from_file(self) -> DataFrame:
        """Extract data from file source."""
        source_path = self.config.get('paths.source', '/data/source')
        file_format = self.config.get('formats.source', 'parquet')
        
        return self.spark.read.format(file_format).load(source_path)