"""
Data extraction module for PySpark ETL pipeline.
Handles data extraction from various sources with configurable parameters.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from typing import Optional, Dict, Any
from datetime import datetime
import logging


class DataExtractor:
    """Extracts data from various sources for ETL processing."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the extractor.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique identifier for this ETL run
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
            StructField("changed_by", StringType(), True)
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
            filter_condition: Optional filter SQL condition
            max_records: Maximum number of records to extract (0 = no limit)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.info(f"Starting extraction - Source: {source_type}, Run ID: {self.run_id}")
        
        if source_type.lower() == "database":
            df = self._extract_from_database(filter_condition)
        elif source_type.lower() == "staging":
            df = self._extract_from_staging()
        elif source_type.lower() == "incremental":
            df = self._extract_incremental()
        else:
            self.logger.warning(f"Unknown source type: {source_type}, defaulting to database")
            df = self._extract_from_database(filter_condition)
        
        # Apply record limit if specified
        if max_records > 0:
            df = df.limit(max_records)
        
        record_count = df.count()
        self.logger.info(f"Extracted {record_count} records")
        
        return df
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract data from database source."""
        jdbc_config = self.config.get("source", {}).get("jdbc", {})
        
        query = self.config.get("source", {}).get("query", "source_data")
        
        # Build query with filter if provided
        if filter_condition:
            query = f"(SELECT * FROM {query} WHERE {filter_condition}) as filtered_data"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config.get("url")) \
            .option("dbtable", query) \
            .option("user", jdbc_config.get("user")) \
            .option("password", jdbc_config.get("password")) \
            .option("driver", jdbc_config.get("driver")) \
            .load()
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract data from staging area."""
        staging_path = self.config.get("source", {}).get("staging_path")
        file_format = self.config.get("source", {}).get("format", "parquet")
        
        df = self.spark.read \
            .format(file_format) \
            .load(staging_path) \
            .filter(f"run_id = '{self.run_id}' AND status = 'READY'")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last run."""
        # Get last successful run timestamp
        last_run_time = self._get_last_run_time()
        
        if last_run_time:
            filter_condition = f"changed_at > '{last_run_time}'"
            self.logger.info(f"Incremental load from: {last_run_time}")
        else:
            self.logger.warning("No previous run found, performing full extraction")
            filter_condition = None
        
        return self._extract_from_database(filter_condition)
    
    def _get_last_run_time(self) -> Optional[str]:
        """Retrieve the timestamp of the last successful run."""
        jdbc_config = self.config.get("source", {}).get("jdbc", {})
        
        try:
            df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_config.get("url")) \
                .option("dbtable", "(SELECT MAX(end_time) as last_run FROM etl_run_log WHERE status = 'SUCCESS') as last_run") \
                .option("user", jdbc_config.get("user")) \
                .option("password", jdbc_config.get("password")) \
                .option("driver", jdbc_config.get("driver")) \
                .load()
            
            result = df.first()
            return result["last_run"] if result and result["last_run"] else None
        except Exception as e:
            self.logger.warning(f"Could not retrieve last run time: {str(e)}")
            return None