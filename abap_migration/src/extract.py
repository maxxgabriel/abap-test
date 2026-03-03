"""
Data Extraction Module
Handles data extraction from various sources
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DecimalType, TimestampType
)
from typing import Optional
from datetime import datetime

from src.config import Config
from src.logger import ETLLogger


class Extractor:
    """Extract data from configured sources"""
    
    def __init__(
        self,
        spark: SparkSession,
        config: Config,
        run_id: str,
        source_type: str = "DATABASE"
    ):
        """
        Initialize extractor
        
        Args:
            spark: Active SparkSession
            config: Configuration object
            run_id: Unique run identifier
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.source_type = source_type
        self.logger = ETLLogger(run_id=run_id)
    
    def extract_data(
        self,
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data from configured source
        
        Args:
            filter_condition: Optional SQL filter condition
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Starting extraction - Source: {self.source_type}"
        )
        
        try:
            if self.source_type == "DATABASE":
                df = self._extract_from_database(filter_condition)
            elif self.source_type == "STAGING":
                df = self._extract_from_staging()
            elif self.source_type == "INCREMENTAL":
                df = self._extract_incremental()
            else:
                df = self._extract_from_database(filter_condition)
            
            # Apply max records limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            
            self.logger.log_info(
                component="EXTRACTOR",
                message=f"Extraction complete: {record_count} records"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="EXTRACTOR",
                message="Extraction failed",
                details=str(e)
            )
            raise
    
    def _extract_from_database(
        self,
        filter_condition: Optional[str] = None
    ) -> DataFrame:
        """
        Extract from database source
        
        Args:
            filter_condition: Optional SQL filter
            
        Returns:
            DataFrame with source data
        """
        source_path = self.config.get("source_database_path", "/mnt/etl/source_data")
        
        df = self.spark.read.format("delta").load(source_path)
        
        if filter_condition:
            df = df.filter(filter_condition)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """
        Extract from staging area
        
        Returns:
            DataFrame with staged data
        """
        staging_path = self.config.get("staging_path", "/mnt/etl/staging")
        
        df = self.spark.read.format("delta") \
            .load(staging_path) \
            .filter(f"run_id = '{self.run_id}' AND status = 'READY'")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last run
        
        Returns:
            DataFrame with incremental data
        """
        source_path = self.config.get("source_database_path", "/mnt/etl/source_data")
        run_log_path = self.config.get("run_log_path", "/mnt/etl/run_log")
        
        # Get last successful run time
        run_log_df = self.spark.read.format("delta").load(run_log_path)
        
        last_run = run_log_df \
            .filter("status = 'SUCCESS'") \
            .orderBy("end_time", ascending=False) \
            .first()
        
        if last_run:
            last_run_time = last_run['end_time']
            
            df = self.spark.read.format("delta").load(source_path) \
                .filter(f"changed_at > '{last_run_time}'")
        else:
            # No previous run, extract all data
            df = self.spark.read.format("delta").load(source_path)
        
        return df
    
    def get_source_schema(self) -> StructType:
        """
        Get schema for source data
        
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