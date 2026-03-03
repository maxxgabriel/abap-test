"""
Data extraction module for ETL pipeline.
Supports database, staging, and incremental extraction modes.
"""

from typing import List, Dict, Optional
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
import logging

logger = logging.getLogger(__name__)


class ETLExtractor:
    """Handles data extraction from various sources."""
    
    SCHEMA = StructType([
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
    
    def __init__(self, spark: SparkSession, source_type: str, run_id: str, config: Dict):
        """
        Initialize extractor.
        
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
        logger.info(f"Extractor initialized - Source: {self.source_type}, Run ID: {run_id}")
    
    def extract_data(self, filter_condition: Optional[str] = None, 
                    max_records: int = 0) -> DataFrame:
        """
        Extract data based on source type.
        
        Args:
            filter_condition: Optional filter SQL condition
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        logger.info(f"Starting extraction - Source: {self.source_type}")
        
        if self.source_type == "DATABASE":
            df = self._extract_from_database(filter_condition)
        elif self.source_type == "STAGING":
            df = self._extract_from_staging()
        elif self.source_type == "INCREMENTAL":
            df = self._extract_incremental()
        else:
            logger.warning(f"Unknown source type {self.source_type}, defaulting to DATABASE")
            df = self._extract_from_database(filter_condition)
        
        # Apply max records limit
        if max_records > 0:
            df = df.limit(max_records)
        
        record_count = df.count()
        logger.info(f"Extracted {record_count} records")
        
        return df
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract data from source database."""
        source_table = self.config["source"]["table"]
        
        logger.info(f"Extracting from database table: {source_table}")
        
        # Read from source table
        df = (self.spark.read
              .format(self.config["source"]["format"])
              .option("url", self.config["source"]["jdbc_url"])
              .option("dbtable", source_table)
              .option("user", self.config["source"]["user"])
              .option("password", self.config["source"]["password"])
              .option("driver", self.config["source"]["driver"])
              .load())
        
        # Apply filter if provided
        if filter_condition:
            logger.info(f"Applying filter: {filter_condition}")
            df = df.filter(filter_condition)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract data from staging area."""
        staging_path = self.config["staging"]["path"]
        
        logger.info(f"Extracting from staging: {staging_path}/{self.run_id}")
        
        df = (self.spark.read
              .format(self.config["staging"]["format"])
              .schema(self.SCHEMA)
              .load(f"{staging_path}/{self.run_id}"))
        
        # Filter for ready status
        df = df.filter("status = 'READY'")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last successful run."""
        logger.info("Performing incremental extraction")
        
        # Get last successful run timestamp
        last_run_time = self._get_last_run_time()
        
        if last_run_time:
            logger.info(f"Extracting changes since: {last_run_time}")
            filter_condition = f"changed_at > '{last_run_time}'"
            return self._extract_from_database(filter_condition)
        else:
            logger.warning("No previous run found, performing full extraction")
            return self._extract_from_database()
    
    def _get_last_run_time(self) -> Optional[str]:
        """Get timestamp of last successful run."""
        run_log_table = self.config["metadata"]["run_log_table"]
        
        try:
            df = (self.spark.read
                  .format(self.config["source"]["format"])
                  .option("url", self.config["source"]["jdbc_url"])
                  .option("dbtable", run_log_table)
                  .option("user", self.config["source"]["user"])
                  .option("password", self.config["source"]["password"])
                  .option("driver", self.config["source"]["driver"])
                  .load())
            
            last_run = (df.filter("status = 'SUCCESS'")
                       .orderBy("end_time", ascending=False)
                       .select("end_time")
                       .first())
            
            return str(last_run.end_time) if last_run else None
            
        except Exception as e:
            logger.error(f"Error retrieving last run time: {e}")
            return None