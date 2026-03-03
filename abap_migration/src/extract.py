"""
Data extraction module for ETL pipeline.
Handles extraction from various source types including database, staging, and incremental loads.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional, Dict, Any
import logging

from src.logger import ETLLogger


class DataExtractor:
    """Handles data extraction from various sources."""
    
    # Define source data schema
    SOURCE_SCHEMA = StructType([
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
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the extractor.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
        self.source_type = config.get('source_type', 'database')
        
    def extract_data(
        self,
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data based on source type.
        
        Args:
            filter_condition: Optional filter to apply
            max_records: Maximum number of records to extract (0 = unlimited)
            
        Returns:
            DataFrame containing extracted data
        """
        self.logger.log_info(
            component='EXTRACTOR',
            message=f'Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}'
        )
        
        try:
            # Route to appropriate extraction method
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
            self.logger.log_info(
                component='EXTRACTOR',
                message=f'Extracted {record_count} records'
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component='EXTRACTOR',
                message='Extraction failed',
                details=str(e)
            )
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract data from database source.
        
        Args:
            filter_condition: SQL filter condition
            
        Returns:
            DataFrame with extracted data
        """
        source_table = self.config.get('source_table', 'etl_source_data')
        
        query = f"SELECT * FROM {source_table}"
        
        if filter_condition:
            query += f" WHERE {filter_condition}"
        
        query += " LIMIT 1000"
        
        df = self.spark.read \
            .format(self.config.get('source_format', 'jdbc')) \
            .option("url", self.config.get('jdbc_url')) \
            .option("dbtable", f"({query}) as source") \
            .option("user", self.config.get('db_user')) \
            .option("password", self.config.get('db_password')) \
            .option("driver", self.config.get('jdbc_driver', 'org.postgresql.Driver')) \
            .load()
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """
        Extract data from staging area.
        
        Returns:
            DataFrame with staged data
        """
        staging_table = self.config.get('staging_table', 'etl_staging')
        
        query = f"""
            SELECT id, parsed_data
            FROM {staging_table}
            WHERE run_id = '{self.run_id}'
            AND status = 'READY'
        """
        
        df = self.spark.read \
            .format(self.config.get('source_format', 'jdbc')) \
            .option("url", self.config.get('jdbc_url')) \
            .option("dbtable", f"({query}) as staging") \
            .option("user", self.config.get('db_user')) \
            .option("password", self.config.get('db_password')) \
            .load()
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run.
        
        Returns:
            DataFrame with incremental data
        """
        # Get last successful run timestamp
        run_log_table = self.config.get('run_log_table', 'etl_run_log')
        last_run_query = f"""
            SELECT MAX(end_time) as last_run
            FROM {run_log_table}
            WHERE status = 'SUCCESS'
        """
        
        last_run_df = self.spark.read \
            .format(self.config.get('source_format', 'jdbc')) \
            .option("url", self.config.get('jdbc_url')) \
            .option("dbtable", f"({last_run_query}) as last_run") \
            .option("user", self.config.get('db_user')) \
            .option("password", self.config.get('db_password')) \
            .load()
        
        last_run_time = last_run_df.first()['last_run']
        
        if last_run_time:
            source_table = self.config.get('source_table', 'etl_source_data')
            incremental_query = f"""
                SELECT *
                FROM {source_table}
                WHERE changed_at > '{last_run_time}'
            """
            
            df = self.spark.read \
                .format(self.config.get('source_format', 'jdbc')) \
                .option("url", self.config.get('jdbc_url')) \
                .option("dbtable", f"({incremental_query}) as incremental") \
                .option("user", self.config.get('db_user')) \
                .option("password", self.config.get('db_password')) \
                .load()
        else:
            # No previous run, do full extraction
            df = self._extract_from_database()
        
        return df