"""
Data extraction module for ETL pipeline.
Handles extraction from various data sources including database, staging, and incremental loads.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional
import logging

from src.logger import ETLLogger


class ETLExtractor:
    """
    Extractor class responsible for reading data from various sources.
    Supports full, incremental, and staged extraction modes.
    """
    
    def __init__(self, spark: SparkSession, source_type: str, run_id: str, config: dict):
        """
        Initialize the extractor.
        
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
        self.logger = ETLLogger.get_instance()
        
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
    
    def extract_data(self, filter_condition: Optional[str] = None, 
                     max_records: int = 0) -> DataFrame:
        """
        Main extraction method that routes to appropriate extraction logic.
        
        Args:
            filter_condition: Optional filter to apply
            max_records: Maximum number of records to extract (0 = unlimited)
            
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
                self.logger.log_warning(
                    component="EXTRACTOR",
                    message=f"Unknown source type {self.source_type}, defaulting to DATABASE"
                )
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
            DataFrame containing extracted data
        """
        source_table = self.config.get("source_table", "etl_source_data")
        
        query = f"SELECT * FROM {source_table}"
        
        if filter_condition:
            query += f" WHERE {filter_condition}"
        
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Executing query: {query}"
        )
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", self.config.get("jdbc_url")) \
            .option("dbtable", f"({query}) as src") \
            .option("user", self.config.get("db_user")) \
            .option("password", self.config.get("db_password")) \
            .option("driver", self.config.get("jdbc_driver", "org.postgresql.Driver")) \
            .load()
        
        return df
    
    def extract_from_staging(self) -> DataFrame:
        """
        Extract data from staging area.
        
        Returns:
            DataFrame containing staged data
        """
        staging_path = self.config.get("staging_path", "/data/staging")
        staging_format = self.config.get("staging_format", "parquet")
        
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Reading from staging: {staging_path}"
        )
        
        df = self.spark.read \
            .format(staging_format) \
            .load(f"{staging_path}/run_id={self.run_id}")
        
        return df
    
    def extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run.
        
        Returns:
            DataFrame containing incremental data
        """
        # Get last successful run timestamp
        last_run_time = self._get_last_run_time()
        
        if last_run_time:
            self.logger.log_info(
                component="EXTRACTOR",
                message=f"Performing incremental load since {last_run_time}"
            )
            filter_condition = f"changed_at > '{last_run_time}'"
        else:
            self.logger.log_info(
                component="EXTRACTOR",
                message="No previous run found, performing full load"
            )
            filter_condition = None
        
        return self.extract_from_database(filter_condition)
    
    def _get_last_run_time(self) -> Optional[str]:
        """
        Get the timestamp of the last successful ETL run.
        
        Returns:
            Timestamp string or None if no previous run exists
        """
        try:
            run_log_table = self.config.get("run_log_table", "etl_run_log")
            
            query = f"""
                SELECT MAX(end_time) as last_run_time
                FROM {run_log_table}
                WHERE status = 'SUCCESS'
            """
            
            df = self.spark.read \
                .format("jdbc") \
                .option("url", self.config.get("jdbc_url")) \
                .option("dbtable", f"({query}) as last_run") \
                .option("user", self.config.get("db_user")) \
                .option("password", self.config.get("db_password")) \
                .load()
            
            result = df.collect()
            if result and result[0]["last_run_time"]:
                return result[0]["last_run_time"].isoformat()
            
            return None
            
        except Exception as e:
            self.logger.log_warning(
                component="EXTRACTOR",
                message="Could not retrieve last run time",
                details=str(e)
            )
            return None