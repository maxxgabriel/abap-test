"""
ETL Data Extraction Module
Extracts data from various sources with support for full, incremental, and staged loads.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional, Dict, Any
import logging


class ETLExtractor:
    """Handles data extraction from multiple source types."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
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
        
    def get_source_schema(self) -> StructType:
        """Define the schema for source data."""
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
        source_type: str = "database",
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data based on source type.
        
        Args:
            source_type: Type of source (database, staging, incremental)
            filter_condition: Optional filter to apply
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame containing extracted data
        """
        self.logger.info(f"Starting extraction - Source: {source_type}, Run ID: {self.run_id}")
        
        try:
            if source_type.lower() == "database":
                df = self._extract_from_database(filter_condition)
            elif source_type.lower() == "staging":
                df = self._extract_from_staging()
            elif source_type.lower() == "incremental":
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
        """Extract data from database source."""
        source_path = self.config["source"]["database"]["path"]
        source_format = self.config["source"]["database"]["format"]
        
        self.logger.info(f"Extracting from database: {source_path}")
        
        df = self.spark.read \
            .format(source_format) \
            .schema(self.get_source_schema()) \
            .load(source_path)
        
        # Apply filter if provided
        if filter_condition:
            df = df.filter(filter_condition)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract data from staging area."""
        staging_path = self.config["source"]["staging"]["path"]
        staging_format = self.config["source"]["staging"]["format"]
        
        self.logger.info(f"Extracting from staging: {staging_path}")
        
        df = self.spark.read \
            .format(staging_format) \
            .schema(self.get_source_schema()) \
            .load(staging_path) \
            .filter(f"run_id = '{self.run_id}' AND status = 'READY'")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last successful run."""
        self.logger.info("Performing incremental extraction")
        
        # Get last successful run timestamp
        run_log_path = self.config["metadata"]["run_log_path"]
        
        last_run_df = self.spark.read \
            .format("parquet") \
            .load(run_log_path) \
            .filter("status = 'SUCCESS'") \
            .orderBy("end_time", ascending=False) \
            .limit(1)
        
        if last_run_df.count() == 0:
            self.logger.warning("No previous successful run found, performing full load")
            return self._extract_from_database()
        
        last_run_time = last_run_df.select("end_time").collect()[0][0]
        
        # Extract records changed after last run
        source_path = self.config["source"]["database"]["path"]
        source_format = self.config["source"]["database"]["format"]
        
        df = self.spark.read \
            .format(source_format) \
            .schema(self.get_source_schema()) \
            .load(source_path) \
            .filter(f"changed_at > '{last_run_time}'")
        
        return df
    
    def extract_with_metadata(self, **kwargs) -> DataFrame:
        """
        Extract data and add extraction metadata.
        
        Returns:
            DataFrame with added metadata columns
        """
        df = self.extract_data(**kwargs)
        
        # Add metadata columns
        from pyspark.sql.functions import lit, current_timestamp
        
        df = df.withColumn("extraction_timestamp", current_timestamp()) \
               .withColumn("run_id", lit(self.run_id)) \
               .withColumn("source_system", lit(self.config["source"]["system_name"]))
        
        return df