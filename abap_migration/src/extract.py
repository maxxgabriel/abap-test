"""
ETL Extractor Module
Handles data extraction from various sources
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional
import logging


class ETLExtractor:
    """Extractor class for reading data from various sources"""
    
    def __init__(self, spark: SparkSession, source_type: str, run_id: str, config: dict):
        """
        Initialize extractor
        
        Args:
            spark: SparkSession instance
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.source_type = source_type.upper()
        self.run_id = run_id
        self.config = config
        self.logger = logging.getLogger(__name__)
        
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
        Extract data based on source type
        
        Args:
            filter_condition: Optional SQL filter condition
            max_records: Maximum records to extract (0 for unlimited)
            
        Returns:
            DataFrame containing extracted data
        """
        self.logger.info(f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}")
        
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
            self.logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract from database source
        
        Args:
            filter_condition: SQL WHERE clause condition
            
        Returns:
            DataFrame with source data
        """
        source_table = self.config.get("source_table", "etl_source_data")
        jdbc_url = self.config.get("jdbc_url")
        
        if jdbc_url:
            # JDBC connection
            df = self.spark.read.format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", source_table) \
                .option("user", self.config.get("db_user")) \
                .option("password", self.config.get("db_password")) \
                .option("driver", self.config.get("jdbc_driver")) \
                .load()
        else:
            # Read from file (for testing)
            source_path = self.config.get("source_path", "data/source")
            df = self.spark.read \
                .schema(self.get_source_schema()) \
                .option("header", "true") \
                .csv(source_path)
        
        # Apply filter if provided
        if filter_condition:
            df = df.filter(filter_condition)
        
        return df
    
    def extract_from_staging(self) -> DataFrame:
        """
        Extract from staging area
        
        Returns:
            DataFrame with staged data
        """
        staging_path = self.config.get("staging_path", "data/staging")
        
        df = self.spark.read \
            .schema(self.get_source_schema()) \
            .option("header", "true") \
            .parquet(f"{staging_path}/run_id={self.run_id}")
        
        return df.filter("status = 'READY'")
    
    def extract_incremental(self) -> DataFrame:
        """
        Extract incremental data since last successful run
        
        Returns:
            DataFrame with incremental data
        """
        # Get last successful run timestamp
        last_run_time = self._get_last_successful_run_time()
        
        if last_run_time:
            self.logger.info(f"Extracting incremental data since {last_run_time}")
            df = self.extract_from_database()
            return df.filter(f"changed_at > '{last_run_time}'")
        else:
            self.logger.warning("No previous successful run found, performing full extraction")
            return self.extract_from_database()
    
    def _get_last_successful_run_time(self) -> Optional[str]:
        """Get timestamp of last successful ETL run"""
        run_log_path = self.config.get("run_log_path", "data/run_logs")
        
        try:
            run_log_df = self.spark.read.parquet(run_log_path)
            last_run = run_log_df.filter("status = 'SUCCESS'") \
                .orderBy("end_time", ascending=False) \
                .limit(1) \
                .collect()
            
            if last_run:
                return last_run[0]["end_time"]
        except Exception as e:
            self.logger.warning(f"Could not read run log: {str(e)}")
        
        return None