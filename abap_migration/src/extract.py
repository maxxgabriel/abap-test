"""
ETL Extraction Module
Handles data extraction from various sources with comprehensive error handling
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from typing import Optional, Dict, Any
import logging
from datetime import datetime
from src.logger import ETLLogger
from src.config import Config


class ETLExtractor:
    """Extract data from various sources for ETL pipeline"""
    
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
    
    def __init__(self, spark: SparkSession, config: Config, run_id: str):
        """
        Initialize extractor
        
        Args:
            spark: SparkSession instance
            config: Configuration object
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
        self.source_type = config.get("extraction.source_type", "database")
        
    def extract_data(
        self, 
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Main extraction method - routes to appropriate source
        
        Args:
            filter_condition: Optional SQL WHERE clause
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}"
        )
        
        try:
            # Route to appropriate extraction method
            if self.source_type.upper() == "DATABASE":
                df = self._extract_from_database(filter_condition)
            elif self.source_type.upper() == "STAGING":
                df = self._extract_from_staging()
            elif self.source_type.upper() == "INCREMENTAL":
                df = self._extract_incremental()
            else:
                self.logger.log_warning(
                    component="EXTRACTOR",
                    message=f"Unknown source type {self.source_type}, defaulting to DATABASE"
                )
                df = self._extract_from_database(filter_condition)
            
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
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract data from database source
        
        Args:
            filter_condition: Optional WHERE clause
            
        Returns:
            DataFrame with source data
        """
        source_table = self.config.get("extraction.source_table", "etl_source_data")
        
        try:
            # Build query
            query = f"SELECT * FROM {source_table}"
            
            if filter_condition:
                query += f" WHERE {filter_condition}"
            
            # Read from database
            df = self.spark.read \
                .format("jdbc") \
                .option("url", self.config.get("database.jdbc_url")) \
                .option("dbtable", f"({query}) AS source") \
                .option("user", self.config.get("database.user")) \
                .option("password", self.config.get("database.password")) \
                .option("driver", self.config.get("database.driver")) \
                .load()
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="EXTRACTOR",
                message=f"Database extraction failed for table {source_table}",
                details=str(e)
            )
            raise
    
    def _extract_from_staging(self) -> DataFrame:
        """
        Extract data from staging area
        
        Returns:
            DataFrame with staged data
        """
        staging_path = self.config.get("extraction.staging_path")
        
        try:
            df = self.spark.read \
                .format("parquet") \
                .schema(self.SOURCE_SCHEMA) \
                .load(staging_path) \
                .filter(f"run_id = '{self.run_id}'")
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="EXTRACTOR",
                message="Staging extraction failed",
                details=str(e)
            )
            raise
    
    def _extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run
        
        Returns:
            DataFrame with incremental data
        """
        try:
            # Get last successful run timestamp
            last_run_time = self._get_last_successful_run_time()
            
            if last_run_time:
                filter_condition = f"changed_at > timestamp'{last_run_time}'"
                self.logger.log_info(
                    component="EXTRACTOR",
                    message=f"Incremental extraction from {last_run_time}"
                )
            else:
                self.logger.log_warning(
                    component="EXTRACTOR",
                    message="No previous run found, performing full extraction"
                )
                filter_condition = None
            
            return self._extract_from_database(filter_condition)
            
        except Exception as e:
            self.logger.log_error(
                component="EXTRACTOR",
                message="Incremental extraction failed",
                details=str(e)
            )
            raise
    
    def _get_last_successful_run_time(self) -> Optional[str]:
        """
        Get timestamp of last successful ETL run
        
        Returns:
            Timestamp string or None if no previous run
        """
        run_log_table = self.config.get("metadata.run_log_table", "etl_run_log")
        
        try:
            query = f"""
                SELECT MAX(end_time) as last_run_time
                FROM {run_log_table}
                WHERE status = 'SUCCESS'
            """
            
            result = self.spark.read \
                .format("jdbc") \
                .option("url", self.config.get("database.jdbc_url")) \
                .option("dbtable", f"({query}) AS last_run") \
                .option("user", self.config.get("database.user")) \
                .option("password", self.config.get("database.password")) \
                .option("driver", self.config.get("database.driver")) \
                .load()
            
            last_time = result.first()
            return last_time["last_run_time"] if last_time else None
            
        except Exception as e:
            self.logger.log_warning(
                component="EXTRACTOR",
                message="Could not retrieve last run time",
                details=str(e)
            )
            return None