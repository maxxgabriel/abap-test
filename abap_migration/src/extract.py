"""
ETL Data Extraction Module
Extracts data from various sources including database, staging, and incremental loads.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional
import logging


class ETLExtractor:
    """Handles data extraction from multiple source types."""
    
    def __init__(self, spark: SparkSession, source_type: str = "DATABASE", run_id: str = None):
        """
        Initialize the extractor.
        
        Args:
            spark: Active SparkSession
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            run_id: Unique run identifier
        """
        self.spark = spark
        self.source_type = source_type.upper()
        self.run_id = run_id or self._generate_run_id()
        self.logger = logging.getLogger(__name__)
        
    def _generate_run_id(self) -> str:
        """Generate unique run ID."""
        return f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def _get_source_schema(self) -> StructType:
        """Define schema for source data."""
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
            filter_condition: Optional SQL filter condition
            max_records: Maximum records to extract (0 = no limit)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.info(f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}")
        
        try:
            if self.source_type == "DATABASE":
                df = self.extract_from_database(filter_condition)
            elif self.source_type == "STAGING":
                df = self.extract_from_staging(self.run_id)
            elif self.source_type == "INCREMENTAL":
                df = self.extract_incremental()
            else:
                df = self.extract_from_database(filter_condition)
            
            # Apply max records limit
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
        Extract from database source.
        
        Args:
            filter_condition: SQL WHERE clause condition
            
        Returns:
            DataFrame with database data
        """
        self.logger.info("Extracting from database")
        
        query = "(SELECT * FROM etl_source_data"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        query += " LIMIT 1000) as source"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", self.spark.conf.get("spark.etl.source.jdbc.url")) \
            .option("dbtable", query) \
            .option("user", self.spark.conf.get("spark.etl.source.jdbc.user")) \
            .option("password", self.spark.conf.get("spark.etl.source.jdbc.password")) \
            .option("driver", self.spark.conf.get("spark.etl.source.jdbc.driver")) \
            .load()
        
        return df
    
    def extract_from_staging(self, run_id: str) -> DataFrame:
        """
        Extract from staging area.
        
        Args:
            run_id: Run identifier to extract
            
        Returns:
            DataFrame with staging data
        """
        self.logger.info(f"Extracting from staging for run_id: {run_id}")
        
        query = f"""(
            SELECT * FROM etl_staging
            WHERE run_id = '{run_id}' AND status = 'READY'
        ) as staging"""
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", self.spark.conf.get("spark.etl.source.jdbc.url")) \
            .option("dbtable", query) \
            .option("user", self.spark.conf.get("spark.etl.source.jdbc.user")) \
            .option("password", self.spark.conf.get("spark.etl.source.jdbc.password")) \
            .option("driver", self.spark.conf.get("spark.etl.source.jdbc.driver")) \
            .load()
        
        return df
    
    def extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run.
        
        Returns:
            DataFrame with incremental data
        """
        self.logger.info("Extracting incremental data")
        
        # Get last successful run time
        last_run_query = """(
            SELECT MAX(end_time) as last_run
            FROM etl_run_log
            WHERE status = 'SUCCESS'
        ) as last_run"""
        
        last_run_df = self.spark.read \
            .format("jdbc") \
            .option("url", self.spark.conf.get("spark.etl.source.jdbc.url")) \
            .option("dbtable", last_run_query) \
            .option("user", self.spark.conf.get("spark.etl.source.jdbc.user")) \
            .option("password", self.spark.conf.get("spark.etl.source.jdbc.password")) \
            .option("driver", self.spark.conf.get("spark.etl.source.jdbc.driver")) \
            .load()
        
        last_run_time = last_run_df.collect()[0]["last_run"]
        
        if last_run_time:
            query = f"""(
                SELECT * FROM etl_source_data
                WHERE changed_at > '{last_run_time}'
            ) as incremental"""
        else:
            # First run - extract all
            query = "(SELECT * FROM etl_source_data) as incremental"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", self.spark.conf.get("spark.etl.source.jdbc.url")) \
            .option("dbtable", query) \
            .option("user", self.spark.conf.get("spark.etl.source.jdbc.user")) \
            .option("password", self.spark.conf.get("spark.etl.source.jdbc.password")) \
            .option("driver", self.spark.conf.get("spark.etl.source.jdbc.driver")) \
            .load()
        
        return df