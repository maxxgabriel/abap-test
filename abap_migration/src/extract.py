"""
Data extraction module for ETL pipeline.
Extracts data from various sources including database, staging, and incremental loads.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional
import logging


class ETLExtractor:
    """Extracts data from configured sources."""
    
    def __init__(self, spark: SparkSession, config: dict, run_id: str):
        """
        Initialize the extractor.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        self.source_type = config.get('source_type', 'DATABASE')
        
    def get_source_schema(self) -> StructType:
        """Define source data schema."""
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
            StructField("changed_by", StringType(), True),
        ])
        
    def extract_data(
        self, 
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data based on configured source type.
        
        Args:
            filter_condition: Optional SQL filter condition
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.info(f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}")
        
        if self.source_type == 'DATABASE':
            df = self._extract_from_database(filter_condition)
        elif self.source_type == 'STAGING':
            df = self._extract_from_staging()
        elif self.source_type == 'INCREMENTAL':
            df = self._extract_incremental()
        else:
            df = self._extract_from_database(filter_condition)
            
        # Apply max records limit if specified
        if max_records > 0:
            df = df.limit(max_records)
            
        record_count = df.count()
        self.logger.info(f"Extracted {record_count} records")
        
        return df
        
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract data from database source."""
        jdbc_config = self.config['jdbc']
        
        query = f"(SELECT * FROM {jdbc_config['source_table']}"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        query += ") AS source_data"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", query) \
            .option("user", jdbc_config['user']) \
            .option("password", jdbc_config['password']) \
            .option("driver", jdbc_config['driver']) \
            .load()
            
        return df
        
    def _extract_from_staging(self) -> DataFrame:
        """Extract data from staging area."""
        staging_path = self.config['staging']['path']
        
        df = self.spark.read \
            .format(self.config['staging']['format']) \
            .option("header", "true") \
            .schema(self.get_source_schema()) \
            .load(f"{staging_path}/run_id={self.run_id}")
            
        return df
        
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last successful run."""
        jdbc_config = self.config['jdbc']
        
        # Get last successful run time
        last_run_query = f"""
        (SELECT MAX(end_time) as last_run_time 
         FROM {jdbc_config['run_log_table']} 
         WHERE status = 'SUCCESS') AS last_run
        """
        
        last_run_df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", last_run_query) \
            .option("user", jdbc_config['user']) \
            .option("password", jdbc_config['password']) \
            .option("driver", jdbc_config['driver']) \
            .load()
            
        last_run_time = last_run_df.first()['last_run_time']
        
        if last_run_time:
            filter_condition = f"changed_at > '{last_run_time}'"
            self.logger.info(f"Incremental load from: {last_run_time}")
        else:
            filter_condition = None
            self.logger.info("No previous run found, performing full load")
            
        return self._extract_from_database(filter_condition)