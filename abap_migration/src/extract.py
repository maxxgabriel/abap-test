"""
PySpark Data Extractor with Delta Lake Integration
Converts ABAP extractor logic to PySpark DataFrame operations
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from delta import DeltaTable
from datetime import datetime
from typing import Optional, Dict, Any
import logging
from src.logger import ETLLogger


class DataExtractor:
    """Extracts data from various sources using PySpark DataFrame API"""
    
    def __init__(self, spark: SparkSession, source_type: str = "DATABASE", run_id: str = None):
        """
        Initialize extractor with spark session and configuration
        
        Args:
            spark: Active SparkSession
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            run_id: Unique run identifier
        """
        self.spark = spark
        self.source_type = source_type.upper()
        self.run_id = run_id or self._generate_run_id()
        self.logger = ETLLogger.get_instance()
        
    def _generate_run_id(self) -> str:
        """Generate unique run ID"""
        return f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    @staticmethod
    def get_source_schema() -> StructType:
        """Define schema for source data"""
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
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Main extraction method - orchestrates extraction based on source type
        
        Args:
            filter_condition: Filter expression for DataFrame.filter()
            max_records: Maximum number of records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}"
        )
        
        try:
            # Route to appropriate extraction method
            if self.source_type == "DATABASE":
                df = self.extract_from_database(filter_condition)
            elif self.source_type == "STAGING":
                df = self.extract_from_staging(self.run_id)
            elif self.source_type == "INCREMENTAL":
                last_run_time = self._get_last_successful_run_time()
                if last_run_time:
                    df = self.extract_incremental(last_run_time)
                else:
                    self.logger.log_warning(
                        component="EXTRACTOR",
                        message="No previous successful run found, performing full extract"
                    )
                    df = self.extract_from_database(filter_condition)
            else:
                df = self.extract_from_database(filter_condition)
            
            # Apply record limit if specified (replaces DELETE ... FROM logic)
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
        Extract from database source using DataFrame API
        Replaces: SELECT * FROM zetl_source_data WHERE ...
        
        Args:
            filter_condition: Column expression for filtering (e.g., "status = 'ACTIVE'")
            
        Returns:
            DataFrame with filtered data
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message="Extracting from database source"
        )
        
        # Read from Delta Lake table (or other sources)
        source_table = self.spark.conf.get("spark.etl.source_table", "etl_source_data")
        
        df = self.spark.read \
            .format("delta") \
            .load(source_table)
        
        # Apply filter using DataFrame.filter() instead of SQL WHERE
        if filter_condition:
            df = df.filter(filter_condition)
            self.logger.log_info(
                component="EXTRACTOR",
                message=f"Applied filter: {filter_condition}"
            )
        
        return df
    
    def extract_from_staging(self, run_id: str) -> DataFrame:
        """
        Extract from staging area
        Replaces: SELECT * FROM zetl_staging WHERE run_id = @iv_run_id AND status = 'READY'
        
        Args:
            run_id: Run identifier to filter staging data
            
        Returns:
            DataFrame with staged data
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Extracting from staging - Run ID: {run_id}"
        )
        
        staging_table = self.spark.conf.get("spark.etl.staging_table", "etl_staging")
        
        df = self.spark.read \
            .format("delta") \
            .load(staging_table) \
            .filter((F.col("run_id") == run_id) & (F.col("status") == "READY"))
        
        # Parse raw_data if stored as JSON/string
        if "raw_data" in df.columns:
            df = df.withColumn("parsed_data", F.from_json(F.col("raw_data"), self.get_source_schema()))
            df = df.select("parsed_data.*")
        
        return df
    
    def extract_incremental(self, last_run_time: datetime) -> DataFrame:
        """
        Extract incremental data using Delta Lake time travel
        Replaces: SELECT * FROM zetl_source_data WHERE changed_at > @iv_last_run_time
        
        Args:
            last_run_time: Timestamp of last successful run
            
        Returns:
            DataFrame with changed records since last run
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Extracting incremental data since {last_run_time}"
        )
        
        source_table = self.spark.conf.get("spark.etl.source_table", "etl_source_data")
        
        # Option 1: Filter by changed_at timestamp
        df = self.spark.read \
            .format("delta") \
            .load(source_table) \
            .filter(F.col("changed_at") > F.lit(last_run_time))
        
        # Option 2: Use Delta Lake time travel (if available)
        # deltaTable = DeltaTable.forPath(self.spark, source_table)
        # df = deltaTable.toDF().filter(F.col("changed_at") > F.lit(last_run_time))
        
        return df
    
    def extract_with_watermark(self, watermark_column: str = "changed_at", delay: str = "1 hour") -> DataFrame:
        """
        Extract using Spark Structured Streaming watermarking
        For real-time/streaming scenarios
        
        Args:
            watermark_column: Column to use for watermarking
            delay: Watermark delay threshold
            
        Returns:
            Streaming DataFrame with watermark applied
        """
        source_table = self.spark.conf.get("spark.etl.source_table", "etl_source_data")
        
        df = self.spark.readStream \
            .format("delta") \
            .load(source_table) \
            .withWatermark(watermark_column, delay)
        
        return df
    
    def extract_using_delta_time_travel(self, version: Optional[int] = None, timestamp: Optional[str] = None) -> DataFrame:
        """
        Extract specific version using Delta Lake time travel
        
        Args:
            version: Specific version number
            timestamp: Timestamp string (e.g., "2024-01-01 00:00:00")
            
        Returns:
            DataFrame at specified version/timestamp
        """
        source_table = self.spark.conf.get("spark.etl.source_table", "etl_source_data")
        
        if version is not None:
            df = self.spark.read \
                .format("delta") \
                .option("versionAsOf", version) \
                .load(source_table)
            self.logger.log_info(
                component="EXTRACTOR",
                message=f"Extracted data from version {version}"
            )
        elif timestamp is not None:
            df = self.spark.read \
                .format("delta") \
                .option("timestampAsOf", timestamp) \
                .load(source_table)
            self.logger.log_info(
                component="EXTRACTOR",
                message=f"Extracted data from timestamp {timestamp}"
            )
        else:
            df = self.spark.read.format("delta").load(source_table)
        
        return df
    
    def _get_last_successful_run_time(self) -> Optional[datetime]:
        """
        Get timestamp of last successful ETL run
        Replaces: SELECT SINGLE end_time FROM zetl_run_log WHERE status = 'SUCCESS'
        
        Returns:
            Timestamp of last successful run or None
        """
        try:
            run_log_table = self.spark.conf.get("spark.etl.run_log_table", "etl_run_log")
            
            df = self.spark.read \
                .format("delta") \
                .load(run_log_table) \
                .filter(F.col("status") == "SUCCESS") \
                .orderBy(F.col("end_time").desc()) \
                .limit(1) \
                .select("end_time")
            
            if df.count() > 0:
                return df.first()["end_time"]
            else:
                return None
                
        except Exception as e:
            self.logger.log_warning(
                component="EXTRACTOR",
                message=f"Could not retrieve last run time: {str(e)}"
            )
            return None