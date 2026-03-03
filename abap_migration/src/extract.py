"""
Data Extraction Module
Handles extraction from various sources including database, staging, and incremental loads.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


class DataExtractor:
    """Extract data from various sources."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the DataExtractor.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.source_config = config.get('source', {})
        
    def extract_data(
        self,
        source_type: Optional[str] = None,
        filter_condition: Optional[str] = None,
        max_records: Optional[int] = None
    ) -> DataFrame:
        """
        Extract data based on source type.
        
        Args:
            source_type: Type of source (database, staging, incremental)
            filter_condition: SQL WHERE clause for filtering
            max_records: Maximum number of records to extract
            
        Returns:
            DataFrame with extracted data
        """
        source_type = source_type or self.source_config.get('type', 'database')
        logger.info(f"Starting extraction - Source: {source_type}, Run ID: {self.run_id}")
        
        try:
            if source_type == 'database':
                df = self._extract_from_database(filter_condition)
            elif source_type == 'staging':
                df = self._extract_from_staging()
            elif source_type == 'incremental':
                df = self._extract_incremental()
            else:
                raise ValueError(f"Unknown source type: {source_type}")
            
            # Apply record limit if specified
            if max_records and max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract data from database source.
        
        Args:
            filter_condition: SQL WHERE clause
            
        Returns:
            DataFrame with source data
        """
        jdbc_config = self.source_config.get('jdbc', {})
        
        # Build JDBC options
        jdbc_options = {
            "url": jdbc_config.get('url'),
            "driver": jdbc_config.get('driver'),
            "dbtable": jdbc_config.get('table'),
            "user": jdbc_config.get('user'),
            "password": jdbc_config.get('password')
        }
        
        # Read from JDBC source
        df = self.spark.read.format("jdbc").options(**jdbc_options).load()
        
        # Apply filter if provided
        if filter_condition:
            df = df.filter(filter_condition)
        elif self.source_config.get('filter'):
            df = df.filter(self.source_config.get('filter'))
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """
        Extract data from staging area.
        
        Returns:
            DataFrame with staged data
        """
        staging_table = self.source_config.get('staging_table', 'staging_data')
        
        df = self.spark.read.format("jdbc").options(
            url=self.source_config.get('jdbc', {}).get('url'),
            driver=self.source_config.get('jdbc', {}).get('driver'),
            dbtable=staging_table,
            user=self.source_config.get('jdbc', {}).get('user'),
            password=self.source_config.get('jdbc', {}).get('password')
        ).load()
        
        # Filter by run_id and status
        df = df.filter(
            (col("run_id") == self.run_id) & 
            (col("status") == "READY")
        )
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run.
        
        Returns:
            DataFrame with incremental data
        """
        # Get last successful run timestamp
        last_run_time = self._get_last_run_time()
        
        if last_run_time:
            logger.info(f"Extracting records changed after {last_run_time}")
            df = self._extract_from_database()
            df = df.filter(col("changed_at") > last_run_time)
        else:
            logger.warning("No previous successful run found, performing full extraction")
            df = self._extract_from_database()
        
        return df
    
    def _get_last_run_time(self) -> Optional[str]:
        """
        Get timestamp of last successful ETL run.
        
        Returns:
            Timestamp string or None
        """
        try:
            run_log_df = self.spark.read.format("jdbc").options(
                url=self.source_config.get('jdbc', {}).get('url'),
                driver=self.source_config.get('jdbc', {}).get('driver'),
                dbtable="etl_run_log",
                user=self.source_config.get('jdbc', {}).get('user'),
                password=self.source_config.get('jdbc', {}).get('password')
            ).load()
            
            last_run = run_log_df.filter(col("status") == "SUCCESS") \
                .orderBy(col("end_time").desc()) \
                .select("end_time") \
                .first()
            
            return last_run['end_time'] if last_run else None
            
        except Exception as e:
            logger.warning(f"Could not retrieve last run time: {str(e)}")
            return None