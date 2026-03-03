"""
Data extraction module for ETL pipeline.
Extracts data from various sources including database, staging, and incremental loads.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from typing import Optional
import logging
from datetime import datetime
import yaml


class Extractor:
    """Handles data extraction from various sources."""
    
    def __init__(self, spark: SparkSession, config: dict, run_id: str):
        """
        Initialize the extractor.
        
        Args:
            spark: SparkSession instance
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
            StructField("id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("value", DecimalType(15, 2), nullable=False),
            StructField("status", StringType(), nullable=False),
            StructField("category", StringType(), nullable=False),
            StructField("source_system", StringType(), nullable=True),
            StructField("created_at", TimestampType(), nullable=True),
            StructField("created_by", StringType(), nullable=True),
            StructField("changed_at", TimestampType(), nullable=True),
            StructField("changed_by", StringType(), nullable=True)
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
            filter_condition: Optional filter condition
            max_records: Maximum number of records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.info(f"Starting extraction - Source: {source_type}, Run ID: {self.run_id}")
        
        try:
            if source_type.lower() == "database":
                df = self.extract_from_database(filter_condition)
            elif source_type.lower() == "staging":
                df = self.extract_from_staging()
            elif source_type.lower() == "incremental":
                df = self.extract_incremental()
            else:
                self.logger.warning(f"Unknown source type: {source_type}, defaulting to database")
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
        Extract data from database source.
        
        Args:
            filter_condition: Optional SQL WHERE clause condition
            
        Returns:
            DataFrame with extracted data
        """
        source_config = self.config.get("source", {})
        jdbc_config = source_config.get("jdbc", {})
        
        if jdbc_config:
            # JDBC source
            df = (self.spark.read
                  .format("jdbc")
                  .option("url", jdbc_config["url"])
                  .option("dbtable", jdbc_config["table"])
                  .option("user", jdbc_config.get("user", ""))
                  .option("password", jdbc_config.get("password", ""))
                  .option("driver", jdbc_config.get("driver", ""))
                  .load())
        else:
            # CSV/File source as fallback
            file_path = source_config.get("file_path", "data/source_data.csv")
            df = (self.spark.read
                  .format("csv")
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .schema(self.get_source_schema())
                  .load(file_path))
        
        # Apply filter if provided
        if filter_condition:
            df = df.filter(filter_condition)
        
        return df
    
    def extract_from_staging(self) -> DataFrame:
        """
        Extract data from staging area.
        
        Returns:
            DataFrame with staged data
        """
        staging_path = self.config.get("staging", {}).get("path", "data/staging")
        
        df = (self.spark.read
              .format("parquet")
              .load(f"{staging_path}/run_id={self.run_id}"))
        
        # Filter for READY status
        df = df.filter("status = 'READY'")
        
        return df
    
    def extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run.
        
        Returns:
            DataFrame with incremental data
        """
        # Get last successful run timestamp
        last_run_time = self._get_last_run_time()
        
        if last_run_time:
            self.logger.info(f"Performing incremental load from: {last_run_time}")
            df = self.extract_from_database()
            df = df.filter(df.changed_at > last_run_time)
        else:
            self.logger.info("No previous run found, performing full load")
            df = self.extract_from_database()
        
        return df
    
    def _get_last_run_time(self) -> Optional[datetime]:
        """
        Get the timestamp of the last successful run.
        
        Returns:
            Timestamp of last successful run or None
        """
        run_log_path = self.config.get("run_log", {}).get("path", "data/run_log")
        
        try:
            run_log_df = (self.spark.read
                          .format("parquet")
                          .load(run_log_path))
            
            last_run = (run_log_df
                        .filter("status = 'SUCCESS'")
                        .orderBy("end_time", ascending=False)
                        .select("end_time")
                        .first())
            
            return last_run["end_time"] if last_run else None
            
        except Exception as e:
            self.logger.warning(f"Could not read run log: {str(e)}")
            return None