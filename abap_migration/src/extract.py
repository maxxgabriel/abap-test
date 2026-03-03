"""
Data extraction module for ETL pipeline.
Handles extraction from various sources including database, staging, and incremental loads.
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
import logging

from src.logger import ETLLogger


class DataExtractor:
    """Extracts data from various source systems."""
    
    def __init__(self, spark: SparkSession, source_type: str = "DATABASE", run_id: str = None):
        """
        Initialize the data extractor.
        
        Args:
            spark: SparkSession instance
            source_type: Type of source ('DATABASE', 'STAGING', 'INCREMENTAL')
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.source_type = source_type.upper()
        self.run_id = run_id or self._generate_run_id()
        self.logger = ETLLogger.get_instance()
        
    @staticmethod
    def _generate_run_id() -> str:
        """Generate a unique run ID."""
        return f"RUN{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    def get_source_schema(self) -> StructType:
        """
        Define the schema for source data.
        
        Returns:
            StructType schema definition
        """
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
    
    def extract_data(self, filter_condition: Optional[str] = None, 
                     max_records: int = 0) -> DataFrame:
        """
        Extract data based on source type.
        
        Args:
            filter_condition: Optional filter to apply
            max_records: Maximum number of records to extract (0 = no limit)
            
        Returns:
            DataFrame containing extracted data
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}"
        )
        
        try:
            if self.source_type == "DATABASE":
                df = self.extract_from_database(filter_condition)
            elif self.source_type == "STAGING":
                df = self.extract_from_staging()
            elif self.source_type == "INCREMENTAL":
                df = self.extract_incremental()
            else:
                df = self.extract_from_database(filter_condition)
            
            # Apply max records limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.log_info(
                component="EXTRACTOR",
                message=f"Extracted {record_count} records"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="EXTRACTOR",
                message="Extraction failed",
                details=str(e)
            )
            raise
    
    def extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract data from database source.
        
        Args:
            filter_condition: Optional SQL WHERE clause condition
            
        Returns:
            DataFrame with extracted data
        """
        schema = self.get_source_schema()
        
        # Read from source table
        df = self.spark.read \
            .format("jdbc") \
            .option("url", self.spark.conf.get("spark.etl.source.url")) \
            .option("dbtable", self.spark.conf.get("spark.etl.source.table", "etl_source_data")) \
            .option("user", self.spark.conf.get("spark.etl.source.user")) \
            .option("password", self.spark.conf.get("spark.etl.source.password")) \
            .schema(schema) \
            .load()
        
        # Apply filter if provided
        if filter_condition:
            df = df.filter(filter_condition)
        
        return df
    
    def extract_from_staging(self) -> DataFrame:
        """
        Extract data from staging area.
        
        Returns:
            DataFrame with staged data
        """
        df = self.spark.read \
            .format("parquet") \
            .load(self.spark.conf.get("spark.etl.staging.path", "/data/staging")) \
            .filter(f"run_id = '{self.run_id}' AND status = 'READY'")
        
        return df
    
    def extract_incremental(self) -> DataFrame:
        """
        Extract only changed data since last successful run.
        
        Returns:
            DataFrame with incremental data
        """
        # Get last successful run timestamp
        last_run_time = self._get_last_successful_run_time()
        
        if last_run_time:
            df = self.extract_from_database(f"changed_at > '{last_run_time}'")
        else:
            # First run - extract all data
            self.logger.log_info(
                component="EXTRACTOR",
                message="No previous run found, performing full extraction"
            )
            df = self.extract_from_database()
        
        return df
    
    def _get_last_successful_run_time(self) -> Optional[datetime]:
        """
        Get timestamp of last successful ETL run.
        
        Returns:
            Timestamp of last successful run or None
        """
        try:
            run_log_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.spark.conf.get("spark.etl.source.url")) \
                .option("dbtable", "etl_run_log") \
                .option("user", self.spark.conf.get("spark.etl.source.user")) \
                .option("password", self.spark.conf.get("spark.etl.source.password")) \
                .load()
            
            last_run = run_log_df.filter("status = 'SUCCESS'") \
                .orderBy("end_time", ascending=False) \
                .limit(1) \
                .collect()
            
            if last_run:
                return last_run[0]["end_time"]
            return None
            
        except Exception as e:
            self.logger.log_warning(
                component="EXTRACTOR",
                message=f"Could not retrieve last run time: {str(e)}"
            )
            return None