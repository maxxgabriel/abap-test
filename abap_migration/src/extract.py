"""
PySpark ETL Extractor Module
Extracts data from various sources with support for full, incremental, and staging modes.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class ETLExtractor:
    """Handles data extraction from various sources"""
    
    def __init__(self, spark: SparkSession, config: dict, run_id: str):
        """
        Initialize extractor with Spark session and configuration
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.source_type = config.get('source_type', 'DATABASE')
        
    def get_source_schema(self) -> StructType:
        """Define source data schema"""
        return StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("value", DecimalType(15, 2), True),
            StructField("status", StringType(), True),
            StructField("category", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", StringType(), True),
            StructField("changed_at", TimestampType(), True),
            StructField("changed_by", StringType(), True)
        ])
    
    def extract_data(self, filter_condition: Optional[str] = None, 
                     max_records: int = 0) -> DataFrame:
        """
        Main extraction method - routes to appropriate extraction logic
        
        Args:
            filter_condition: Optional SQL WHERE clause
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame containing extracted data
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
        
        # Apply max records limit
        if max_records > 0:
            df = df.limit(max_records)
        
        record_count = df.count()
        logger.info(f"Extracted {record_count} records")
        
        return df
    
    def extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract data from database source
        
        Args:
            filter_condition: SQL WHERE clause for filtering
            
        Returns:
            DataFrame with extracted data
        """
        source_config = self.config['sources']['database']
        
        jdbc_options = {
            "url": source_config['jdbc_url'],
            "dbtable": source_config['table'],
            "user": source_config['user'],
            "password": source_config['password'],
            "driver": source_config['driver']
        }
        
        df = self.spark.read \
            .format("jdbc") \
            .options(**jdbc_options) \
            .load()
        
        # Apply filter if provided
        if filter_condition:
            df = df.filter(filter_condition)
        
        return df
    
    def extract_from_staging(self) -> DataFrame:
        """
        Extract data from staging area
        
        Returns:
            DataFrame with staged data
        """
        staging_path = self.config['sources']['staging']['path']
        
        df = self.spark.read \
            .format("parquet") \
            .load(staging_path) \
            .filter(f"run_id = '{self.run_id}' AND status = 'READY'")
        
        return df
    
    def extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run
        
        Returns:
            DataFrame with incremental data
        """
        # Get last successful run timestamp
        last_run_df = self.spark.read \
            .format("jdbc") \
            .options(
                url=self.config['sources']['database']['jdbc_url'],
                dbtable="(SELECT MAX(end_time) as last_run FROM etl_run_log WHERE status = 'SUCCESS') as t",
                user=self.config['sources']['database']['user'],
                password=self.config['sources']['database']['password']
            ) \
            .load()
        
        last_run_time = last_run_df.first()['last_run']
        
        if last_run_time:
            filter_condition = f"changed_at > '{last_run_time}'"
        else:
            filter_condition = None
        
        return self.extract_from_database(filter_condition)