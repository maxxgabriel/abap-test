"""
ETL Data Extraction Module

This module handles extraction of data from various source systems including
database, staging areas, and incremental loads.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, TimestampType
)
from pyspark.sql.functions import col, current_timestamp, lit
from typing import Optional, Dict, Any
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ETLExtractor:
    """
    Handles data extraction from multiple source types with filtering
    and record limiting capabilities.
    """
    
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
    
    def __init__(self, spark: SparkSession, run_id: str, source_type: str = "DATABASE"):
        """
        Initialize the extractor.
        
        Args:
            spark: Active SparkSession
            run_id: Unique run identifier
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
        """
        self.spark = spark
        self.run_id = run_id
        self.source_type = source_type.upper()
        logger.info(f"ETLExtractor initialized - Run ID: {run_id}, Source: {source_type}")
    
    def extract_data(
        self,
        config: Dict[str, Any],
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Main extraction method that routes to appropriate extraction logic.
        
        Args:
            config: Configuration dictionary with connection details
            filter_condition: Optional SQL filter condition
            max_records: Maximum number of records to extract (0 = unlimited)
            
        Returns:
            Extracted DataFrame
        """
        logger.info(f"Starting extraction - Source: {self.source_type}")
        
        try:
            if self.source_type == "DATABASE":
                df = self._extract_from_database(config, filter_condition)
            elif self.source_type == "STAGING":
                df = self._extract_from_staging(config)
            elif self.source_type == "INCREMENTAL":
                df = self._extract_incremental(config)
            else:
                df = self._extract_from_database(config, filter_condition)
            
            # Apply max records limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _extract_from_database(
        self,
        config: Dict[str, Any],
        filter_condition: Optional[str] = None
    ) -> DataFrame:
        """
        Extract data from database source.
        
        Args:
            config: Database connection configuration
            filter_condition: Optional WHERE clause
            
        Returns:
            Extracted DataFrame
        """
        logger.info("Extracting from database source")
        
        # Build JDBC connection properties
        jdbc_url = config.get("jdbc_url")
        table_name = config.get("source_table", "etl_source_data")
        
        # Build query with optional filter
        if filter_condition:
            query = f"(SELECT * FROM {table_name} WHERE {filter_condition}) AS filtered_data"
        else:
            query = f"(SELECT * FROM {table_name} LIMIT 1000) AS source_data"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", query) \
            .option("user", config.get("db_user")) \
            .option("password", config.get("db_password")) \
            .option("driver", config.get("db_driver", "org.postgresql.Driver")) \
            .load()
        
        return df
    
    def _extract_from_staging(self, config: Dict[str, Any]) -> DataFrame:
        """
        Extract data from staging area.
        
        Args:
            config: Staging configuration
            
        Returns:
            Extracted DataFrame
        """
        logger.info(f"Extracting from staging - Run ID: {self.run_id}")
        
        staging_path = config.get("staging_path")
        
        df = self.spark.read \
            .format("parquet") \
            .schema(self.SOURCE_SCHEMA) \
            .load(staging_path) \
            .filter(col("run_id") == self.run_id) \
            .filter(col("status") == "READY")
        
        return df
    
    def _extract_incremental(self, config: Dict[str, Any]) -> DataFrame:
        """
        Extract only changed records since last successful run.
        
        Args:
            config: Database configuration
            
        Returns:
            Incremental DataFrame
        """
        logger.info("Extracting incremental data")
        
        # Get last successful run timestamp
        last_run_time = self._get_last_run_time(config)
        
        if last_run_time:
            logger.info(f"Extracting changes since: {last_run_time}")
            filter_condition = f"changed_at > '{last_run_time}'"
        else:
            logger.warning("No previous run found, performing full extraction")
            filter_condition = None
        
        return self._extract_from_database(config, filter_condition)
    
    def _get_last_run_time(self, config: Dict[str, Any]) -> Optional[str]:
        """
        Retrieve timestamp of last successful ETL run.
        
        Args:
            config: Database configuration
            
        Returns:
            Last run timestamp or None
        """
        jdbc_url = config.get("jdbc_url")
        
        try:
            query = """
                (SELECT MAX(end_time) as last_run 
                 FROM etl_run_log 
                 WHERE status = 'SUCCESS') AS last_run_query
            """
            
            df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", query) \
                .option("user", config.get("db_user")) \
                .option("password", config.get("db_password")) \
                .load()
            
            result = df.collect()
            if result and result[0]["last_run"]:
                return result[0]["last_run"].isoformat()
            
        except Exception as e:
            logger.warning(f"Could not retrieve last run time: {str(e)}")
        
        return None


def create_extractor(spark: SparkSession, run_id: str, source_type: str = "DATABASE") -> ETLExtractor:
    """
    Factory function to create an ETLExtractor instance.
    
    Args:
        spark: SparkSession
        run_id: Run identifier
        source_type: Source type
        
    Returns:
        ETLExtractor instance
    """
    return ETLExtractor(spark, run_id, source_type)