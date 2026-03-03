"""
Data extraction module for PySpark ETL pipeline.
Provides extraction from various sources with incremental load support.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional
import logging


class ETLExtractor:
    """Handles data extraction from various sources."""
    
    def __init__(self, spark: SparkSession, source_type: str, run_id: str):
        """
        Initialize the extractor.
        
        Args:
            spark: SparkSession instance
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            run_id: Unique run identifier
        """
        self.spark = spark
        self.source_type = source_type
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    @staticmethod
    def get_source_schema() -> StructType:
        """Define source data schema."""
        return StructType([
            StructField("id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("value", DecimalType(15, 2), nullable=True),
            StructField("status", StringType(), nullable=True),
            StructField("category", StringType(), nullable=True),
            StructField("source_system", StringType(), nullable=True),
            StructField("created_at", TimestampType(), nullable=True),
            StructField("created_by", StringType(), nullable=True),
            StructField("changed_at", TimestampType(), nullable=True),
            StructField("changed_by", StringType(), nullable=True),
        ])
    
    def extract_data(
        self, 
        filter_condition: Optional[str] = None, 
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data based on source type.
        
        Args:
            filter_condition: Optional filter to apply
            max_records: Maximum number of records to extract (0 for unlimited)
            
        Returns:
            DataFrame containing extracted data
            
        Raises:
            Exception: If extraction fails
        """
        self.logger.info(f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}")
        
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
            self.logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract data from database source.
        
        Args:
            filter_condition: SQL WHERE clause filter
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.info("Extracting from database")
        
        # Read from source table (can be configured for JDBC, Parquet, etc.)
        df = self.spark.read \
            .format("jdbc") \
            .option("url", "${db.url}") \
            .option("dbtable", "${db.source_table}") \
            .option("user", "${db.user}") \
            .option("password", "${db.password}") \
            .load()
        
        # Apply filter if provided
        if filter_condition:
            df = df.filter(filter_condition)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """
        Extract data from staging area.
        
        Returns:
            DataFrame with staged data
        """
        self.logger.info(f"Extracting from staging for run_id: {self.run_id}")
        
        df = self.spark.read \
            .format("parquet") \
            .load("${staging.path}") \
            .filter(f"run_id = '{self.run_id}' AND status = 'READY'")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run.
        
        Returns:
            DataFrame with incremental data
        """
        self.logger.info("Extracting incremental data")
        
        # Get last successful run timestamp
        last_run_df = self.spark.read \
            .format("jdbc") \
            .option("url", "${db.url}") \
            .option("dbtable", "${db.run_log_table}") \
            .option("user", "${db.user}") \
            .option("password", "${db.password}") \
            .load() \
            .filter("status = 'SUCCESS'") \
            .orderBy("end_time", ascending=False) \
            .limit(1)
        
        if last_run_df.count() > 0:
            last_run_time = last_run_df.first()["end_time"]
            self.logger.info(f"Last successful run: {last_run_time}")
            
            # Extract records changed after last run
            df = self.spark.read \
                .format("jdbc") \
                .option("url", "${db.url}") \
                .option("dbtable", "${db.source_table}") \
                .option("user", "${db.user}") \
                .option("password", "${db.password}") \
                .load() \
                .filter(f"changed_at > '{last_run_time}'")
        else:
            self.logger.warning("No previous successful run found, performing full extraction")
            df = self._extract_from_database()
        
        return df