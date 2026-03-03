"""
PySpark Data Extractor with Delta Lake Integration
Replaces ABAP zcl_etl_extractor class with DataFrame operations
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional, Dict, Any
import logging


class DataExtractor:
    """
    PySpark extractor supporting multiple source types with Delta Lake integration.
    Replaces SQL WHERE filters with DataFrame.filter() and implements incremental loading.
    """
    
    def __init__(self, spark: SparkSession, source_type: str = "database", run_id: str = None):
        """
        Initialize the extractor.
        
        Args:
            spark: SparkSession instance
            source_type: Type of source (database, staging, incremental)
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.source_type = source_type.upper()
        self.run_id = run_id or self._generate_run_id()
        self.logger = logging.getLogger(__name__)
        
    def _generate_run_id(self) -> str:
        """Generate a unique run ID based on timestamp."""
        return f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def get_source_schema(self) -> StructType:
        """Define schema for source data matching ABAP ty_source_data structure."""
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
    
    def extract_data(
        self,
        filter_expr: Optional[str] = None,
        max_records: int = 0,
        **kwargs
    ) -> DataFrame:
        """
        Main extraction method that routes to appropriate source handler.
        
        Args:
            filter_expr: Filter expression for DataFrame.filter()
            max_records: Maximum number of records to extract (0 = no limit)
            **kwargs: Additional parameters for specific extractors
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.info(f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}")
        
        try:
            # Route to appropriate extractor
            if self.source_type == "DATABASE":
                df = self.extract_from_database(filter_expr)
            elif self.source_type == "STAGING":
                df = self.extract_from_staging(self.run_id)
            elif self.source_type == "INCREMENTAL":
                last_run_time = kwargs.get("last_run_time") or self._get_last_successful_run_time()
                df = self.extract_incremental(last_run_time)
            else:
                self.logger.warning(f"Unknown source type {self.source_type}, defaulting to DATABASE")
                df = self.extract_from_database(filter_expr)
            
            # Apply max records limit using DataFrame.limit()
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def extract_from_database(self, filter_expr: Optional[str] = None) -> DataFrame:
        """
        Extract from database source using DataFrame API.
        Replaces ABAP SELECT with DataFrame operations.
        
        Args:
            filter_expr: SQL-like filter expression (e.g., "status = 'ACTIVE'")
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.info("Extracting from database source")
        
        # Read from source table (Delta Lake or other format)
        df = self.spark.read.format("delta").load("/mnt/data/source/etl_source_data")
        
        # Apply filter using DataFrame.filter() instead of SQL WHERE
        if filter_expr:
            df = df.filter(filter_expr)
            self.logger.info(f"Applied filter: {filter_expr}")
        
        return df
    
    def extract_from_staging(self, run_id: str) -> DataFrame:
        """
        Extract from staging area filtered by run_id.
        Uses DataFrame.filter() for row selection.
        
        Args:
            run_id: Run identifier to filter staging data
            
        Returns:
            DataFrame with staged data
        """
        self.logger.info(f"Extracting from staging - Run ID: {run_id}")
        
        # Read from staging table
        df = self.spark.read.format("delta").load("/mnt/data/staging/etl_staging")
        
        # Filter using DataFrame API instead of SQL WHERE
        df = df.filter(
            (col("run_id") == run_id) & 
            (col("status") == "READY")
        )
        
        return df
    
    def extract_incremental(self, last_run_time: datetime) -> DataFrame:
        """
        Extract only changed records since last run using Delta Lake time travel.
        Replaces ABAP timestamp-based WHERE clause with DataFrame operations.
        
        Args:
            last_run_time: Timestamp of last successful run
            
        Returns:
            DataFrame with incremental changes
        """
        self.logger.info(f"Extracting incremental data since {last_run_time}")
        
        # Option 1: Use Delta Lake time travel for point-in-time query
        # This is more efficient than filtering by timestamp
        df_current = self.spark.read.format("delta").load("/mnt/data/source/etl_source_data")
        
        # Option 2: Filter by changed_at timestamp using DataFrame.filter()
        df_incremental = df_current.filter(col("changed_at") > lit(last_run_time))
        
        # Alternative Option 3: Use Delta Lake Change Data Feed (CDC)
        # Uncomment if Delta CDC is enabled on source table
        # df_incremental = self.spark.read.format("delta") \
        #     .option("readChangeFeed", "true") \
        #     .option("startingTimestamp", last_run_time.isoformat()) \
        #     .load("/mnt/data/source/etl_source_data")
        
        return df_incremental
    
    def _get_last_successful_run_time(self) -> datetime:
        """
        Get timestamp of last successful ETL run from run log.
        Uses DataFrame operations instead of SQL SELECT.
        
        Returns:
            Timestamp of last successful run
        """
        try:
            # Read run log
            run_log_df = self.spark.read.format("delta").load("/mnt/data/logs/etl_run_log")
            
            # Filter for successful runs and get max end_time
            last_run_df = run_log_df \
                .filter(col("status") == "SUCCESS") \
                .select("end_time") \
                .orderBy(col("end_time").desc()) \
                .limit(1)
            
            # Collect result
            result = last_run_df.collect()
            
            if result:
                return result[0]["end_time"]
            else:
                # No previous successful run, return epoch
                self.logger.warning("No previous successful run found, returning epoch time")
                return datetime(1970, 1, 1)
                
        except Exception as e:
            self.logger.error(f"Error retrieving last run time: {str(e)}")
            return datetime(1970, 1, 1)
    
    def extract_with_delta_time_travel(
        self, 
        version: Optional[int] = None, 
        timestamp: Optional[str] = None
    ) -> DataFrame:
        """
        Extract using Delta Lake time travel capabilities.
        This provides snapshot isolation for consistent reads.
        
        Args:
            version: Delta table version number
            timestamp: Timestamp string (ISO format)
            
        Returns:
            DataFrame from specified version/timestamp
        """
        reader = self.spark.read.format("delta")
        
        if version is not None:
            reader = reader.option("versionAsOf", version)
            self.logger.info(f"Reading Delta table version {version}")
        elif timestamp is not None:
            reader = reader.option("timestampAsOf", timestamp)
            self.logger.info(f"Reading Delta table as of {timestamp}")
        
        return reader.load("/mnt/data/source/etl_source_data")
    
    def extract_with_watermark(
        self,
        watermark_column: str = "changed_at",
        watermark_delay: str = "10 minutes"
    ) -> DataFrame:
        """
        Extract streaming data with watermark for late-arriving data.
        Useful for near-realtime incremental processing.
        
        Args:
            watermark_column: Column to use for watermarking
            watermark_delay: How late data can arrive
            
        Returns:
            Streaming DataFrame with watermark
        """
        self.logger.info(f"Extracting with watermark on {watermark_column}")
        
        # Read as stream from Delta source
        stream_df = self.spark.readStream \
            .format("delta") \
            .load("/mnt/data/source/etl_source_data")
        
        # Apply watermark for handling late data
        watermarked_df = stream_df.withWatermark(watermark_column, watermark_delay)
        
        return watermarked_df
    
    def validate_extraction(self, df: DataFrame) -> Dict[str, Any]:
        """
        Validate extracted data quality.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Dictionary with validation metrics
        """
        metrics = {
            "total_records": df.count(),
            "null_ids": df.filter(col("id").isNull()).count(),
            "null_names": df.filter(col("name").isNull()).count(),
            "null_values": df.filter(col("value").isNull()).count(),
        }
        
        self.logger.info(f"Extraction validation: {metrics}")
        return metrics