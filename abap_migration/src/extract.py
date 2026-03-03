"""Data extraction module for ETL pipeline."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, TimestampType
)
from datetime import datetime
from typing import Optional
import logging


class Extractor:
    """Extract data from various sources."""
    
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
        StructField("changed_by", StringType(), True),
    ])
    
    def __init__(self, spark: SparkSession, config: dict, run_id: str):
        """Initialize extractor.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        self.source_type = config.get("source_type", "database")
    
    def extract_data(
        self,
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """Extract data from configured source.
        
        Args:
            filter_condition: Optional filter SQL condition
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame containing extracted data
        """
        self.logger.info(
            f"Starting extraction - Source: {self.source_type}, "
            f"Run ID: {self.run_id}"
        )
        
        if self.source_type == "database":
            df = self._extract_from_database(filter_condition)
        elif self.source_type == "staging":
            df = self._extract_from_staging()
        elif self.source_type == "incremental":
            df = self._extract_incremental()
        else:
            df = self._extract_from_database(filter_condition)
        
        # Apply max records limit
        if max_records > 0:
            df = df.limit(max_records)
        
        record_count = df.count()
        self.logger.info(f"Extracted {record_count} records")
        
        return df
    
    def _extract_from_database(
        self,
        filter_condition: Optional[str] = None
    ) -> DataFrame:
        """Extract from database source.
        
        Args:
            filter_condition: Optional WHERE clause condition
            
        Returns:
            DataFrame with extracted data
        """
        jdbc_config = self.config["database"]["jdbc"]
        
        query = "(SELECT * FROM etl_source_data"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        query += " LIMIT 1000) AS source"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config["url"]) \
            .option("dbtable", query) \
            .option("user", jdbc_config["user"]) \
            .option("password", jdbc_config["password"]) \
            .option("driver", jdbc_config["driver"]) \
            .load()
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract from staging area.
        
        Returns:
            DataFrame with staged data
        """
        staging_path = self.config["paths"]["staging"]
        
        df = self.spark.read \
            .format("parquet") \
            .schema(self.SOURCE_SCHEMA) \
            .load(f"{staging_path}/run_id={self.run_id}")
        
        return df.filter("status = 'READY'")
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last run.
        
        Returns:
            DataFrame with incremental data
        """
        jdbc_config = self.config["database"]["jdbc"]
        
        # Get last successful run timestamp
        last_run_query = """
            (SELECT MAX(end_time) as last_run
             FROM etl_run_log
             WHERE status = 'SUCCESS') AS last_run
        """
        
        last_run_df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config["url"]) \
            .option("dbtable", last_run_query) \
            .option("user", jdbc_config["user"]) \
            .option("password", jdbc_config["password"]) \
            .load()
        
        last_run_time = last_run_df.first()["last_run"]
        
        if last_run_time:
            query = f"""
                (SELECT * FROM etl_source_data
                 WHERE changed_at > '{last_run_time}') AS incremental
            """
        else:
            # No previous run, do full extract
            query = "(SELECT * FROM etl_source_data) AS incremental"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config["url"]) \
            .option("dbtable", query) \
            .option("user", jdbc_config["user"]) \
            .option("password", jdbc_config["password"]) \
            .load()
        
        return df