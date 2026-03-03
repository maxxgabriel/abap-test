"""
ETL Data Extraction Module
Handles data extraction from various sources including database, staging, and incremental loads.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from typing import Optional, Dict, Any
from datetime import datetime
import logging
from src.logger import ETLLogger


class ETLExtractor:
    """
    Extracts data from various sources for ETL processing.
    """
    
    def __init__(self, spark: SparkSession, source_type: str = "DATABASE", run_id: str = None):
        """
        Initialize the extractor.
        
        Args:
            spark: SparkSession instance
            source_type: Type of data source (DATABASE, STAGING, INCREMENTAL)
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.source_type = source_type
        self.run_id = run_id or self._generate_run_id()
        self.logger = ETLLogger.get_instance()
        
    @staticmethod
    def _generate_run_id() -> str:
        """Generate a unique run ID."""
        return datetime.now().strftime("RUN%Y%m%d%H%M%S")
    
    @staticmethod
    def get_source_schema() -> StructType:
        """
        Define the schema for source data.
        
        Returns:
            StructType: Schema for source data
        """
        return StructType([
            StructField("id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("value", DecimalType(15, 2), nullable=True),
            StructField("status", StringType(), nullable=True),
            StructField("category", StringType(), nullable=True),
            StructField("source_system", StringType(), nullable=True),
            StructField("created_at", TimestampType(), nullable=True),
            StructField("created_by", StringType(), nullable=True),
            StructField("changed_at", TimestampType(), nullable=True),
            StructField("changed_by", StringType(), nullable=True)
        ])
    
    def extract_data(self, filter_value: Optional[str] = None, 
                    max_records: int = 0) -> DataFrame:
        """
        Extract data based on source type.
        
        Args:
            filter_value: Optional filter criteria
            max_records: Maximum number of records to extract (0 = no limit)
            
        Returns:
            DataFrame: Extracted data
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Starting extraction - Source: {self.source_type}"
        )
        
        try:
            if self.source_type == "DATABASE":
                df = self.extract_from_database(filter_value)
            elif self.source_type == "STAGING":
                df = self.extract_from_staging(self.run_id)
            elif self.source_type == "INCREMENTAL":
                last_run_time = self._get_last_successful_run_time()
                if last_run_time:
                    df = self.extract_incremental(last_run_time)
                else:
                    self.logger.log_warning(
                        component="EXTRACTOR",
                        message="No previous run found, falling back to full extraction"
                    )
                    df = self.extract_from_database(filter_value)
            else:
                df = self.extract_from_database(filter_value)
            
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
    
    def extract_from_database(self, filter_value: Optional[str] = None) -> DataFrame:
        """
        Extract data from database.
        
        Args:
            filter_value: Optional filter for status field
            
        Returns:
            DataFrame: Extracted data
        """
        try:
            # Read from JDBC source (example with PostgreSQL)
            jdbc_url = self.spark.conf.get("spark.etl.source.jdbc.url")
            jdbc_properties = {
                "user": self.spark.conf.get("spark.etl.source.jdbc.user"),
                "password": self.spark.conf.get("spark.etl.source.jdbc.password"),
                "driver": self.spark.conf.get("spark.etl.source.jdbc.driver")
            }
            
            query = "(SELECT * FROM etl_source_data"
            if filter_value:
                query += f" WHERE status = '{filter_value}'"
            query += " LIMIT 1000) as source_data"
            
            df = self.spark.read.jdbc(
                url=jdbc_url,
                table=query,
                properties=jdbc_properties
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="EXTRACTOR",
                message="Database extraction failed",
                details=str(e)
            )
            raise
    
    def extract_from_staging(self, run_id: str) -> DataFrame:
        """
        Extract data from staging area.
        
        Args:
            run_id: Run identifier
            
        Returns:
            DataFrame: Extracted data
        """
        try:
            staging_path = self.spark.conf.get("spark.etl.staging.path")
            
            df = self.spark.read \
                .format("parquet") \
                .load(f"{staging_path}/run_id={run_id}") \
                .filter("status = 'READY'")
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="EXTRACTOR",
                message="Staging extraction failed",
                details=str(e)
            )
            raise
    
    def extract_incremental(self, last_run_time: datetime) -> DataFrame:
        """
        Extract only changed records since last run.
        
        Args:
            last_run_time: Timestamp of last successful run
            
        Returns:
            DataFrame: Extracted data
        """
        try:
            jdbc_url = self.spark.conf.get("spark.etl.source.jdbc.url")
            jdbc_properties = {
                "user": self.spark.conf.get("spark.etl.source.jdbc.user"),
                "password": self.spark.conf.get("spark.etl.source.jdbc.password"),
                "driver": self.spark.conf.get("spark.etl.source.jdbc.driver")
            }
            
            query = f"""(
                SELECT * FROM etl_source_data
                WHERE changed_at > '{last_run_time.isoformat()}'
            ) as incremental_data"""
            
            df = self.spark.read.jdbc(
                url=jdbc_url,
                table=query,
                properties=jdbc_properties
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="EXTRACTOR",
                message="Incremental extraction failed",
                details=str(e)
            )
            raise
    
    def _get_last_successful_run_time(self) -> Optional[datetime]:
        """
        Get the timestamp of the last successful ETL run.
        
        Returns:
            datetime: Last successful run time or None
        """
        try:
            jdbc_url = self.spark.conf.get("spark.etl.source.jdbc.url")
            jdbc_properties = {
                "user": self.spark.conf.get("spark.etl.source.jdbc.user"),
                "password": self.spark.conf.get("spark.etl.source.jdbc.password"),
                "driver": self.spark.conf.get("spark.etl.source.jdbc.driver")
            }
            
            query = """(
                SELECT end_time FROM etl_run_log
                WHERE status = 'SUCCESS'
                ORDER BY end_time DESC
                LIMIT 1
            ) as last_run"""
            
            df = self.spark.read.jdbc(
                url=jdbc_url,
                table=query,
                properties=jdbc_properties
            )
            
            if df.count() > 0:
                return df.first()["end_time"]
            return None
            
        except Exception as e:
            self.logger.log_warning(
                component="EXTRACTOR",
                message="Could not retrieve last run time",
                details=str(e)
            )
            return None