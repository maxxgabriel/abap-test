"""
PySpark Data Extractor with Delta Lake Integration
Converted from ABAP class zcl_etl_extractor
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from pyspark.sql import functions as F
from datetime import datetime
from typing import Optional
import logging


class DataExtractor:
    """
    Extracts data from various sources using PySpark DataFrame API.
    Replaces SQL WHERE filters with DataFrame.filter() and row limits with DataFrame.limit().
    Supports Delta Lake time travel for incremental loads.
    """
    
    def __init__(self, spark: SparkSession, source_type: str = "DATABASE", run_id: str = None):
        """
        Initialize the data extractor.
        
        Args:
            spark: SparkSession instance
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            run_id: Unique run identifier
        """
        self.spark = spark
        self.source_type = source_type.upper()
        self.run_id = run_id or self._generate_run_id()
        self.logger = logging.getLogger(__name__)
        
    def _generate_run_id(self) -> str:
        """Generate a unique run ID."""
        return f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def get_source_schema(self) -> StructType:
        """
        Define the schema for source data.
        
        Returns:
            StructType schema definition
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
    
    def extract_data(
        self, 
        filter_expr: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Main extraction method that routes to appropriate source.
        Replaces ABAP CASE statement with Python dispatch pattern.
        
        Args:
            filter_expr: Filter expression for data (replaces ABAP WHERE clause)
            max_records: Maximum number of records to extract (0 = unlimited)
            
        Returns:
            PySpark DataFrame with extracted data
        """
        self.logger.info(f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}")
        
        try:
            # Dispatch to appropriate extraction method
            if self.source_type == "DATABASE":
                df = self.extract_from_database(filter_expr)
            elif self.source_type == "STAGING":
                df = self.extract_from_staging(self.run_id)
            elif self.source_type == "INCREMENTAL":
                last_run_time = self._get_last_successful_run_time()
                if last_run_time:
                    df = self.extract_incremental(last_run_time)
                else:
                    self.logger.warning("No previous run found, falling back to full extraction")
                    df = self.extract_from_database(filter_expr)
            else:
                self.logger.warning(f"Unknown source type: {self.source_type}, defaulting to DATABASE")
                df = self.extract_from_database(filter_expr)
            
            # Apply row limit if specified (replaces ABAP DELETE FROM)
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}", exc_info=True)
            raise
    
    def extract_from_database(self, filter_expr: Optional[str] = None) -> DataFrame:
        """
        Extract data from database source.
        Uses DataFrame.filter() instead of SQL WHERE clause.
        
        Args:
            filter_expr: Filter expression (e.g., "status = 'ACTIVE'")
            
        Returns:
            Filtered DataFrame
        """
        self.logger.info("Extracting from database")
        
        # Read from Delta Lake or other source
        df = self.spark.read.format("delta").load("path/to/source_data")
        
        # Apply filter using DataFrame API (replaces SQL WHERE)
        if filter_expr:
            df = df.filter(filter_expr)
        
        return df
    
    def extract_from_staging(self, run_id: str) -> DataFrame:
        """
        Extract data from staging area.
        Filters by run_id and status using DataFrame API.
        
        Args:
            run_id: Run identifier to filter staging data
            
        Returns:
            Staged DataFrame
        """
        self.logger.info(f"Extracting from staging - Run ID: {run_id}")
        
        df = self.spark.read.format("delta").load("path/to/staging")
        
        # Filter using DataFrame API (replaces SQL WHERE)
        df = df.filter(
            (F.col("run_id") == run_id) & 
            (F.col("status") == "READY")
        )
        
        return df
    
    def extract_incremental(self, last_run_time: datetime) -> DataFrame:
        """
        Extract incremental data using Delta Lake time travel.
        Replaces timestamp-based SQL WHERE with DataFrame.filter().
        
        Args:
            last_run_time: Timestamp of last successful run
            
        Returns:
            DataFrame containing only changed records
        """
        self.logger.info(f"Extracting incremental data since {last_run_time}")
        
        # Option 1: Using Delta Lake time travel
        # df = self.spark.read.format("delta") \
        #     .option("versionAsOf", version_number) \
        #     .load("path/to/source_data")
        
        # Option 2: Using timestamp filter (replaces SQL WHERE changed_at > @timestamp)
        df = self.spark.read.format("delta").load("path/to/source_data")
        df = df.filter(F.col("changed_at") > F.lit(last_run_time))
        
        return df
    
    def _get_last_successful_run_time(self) -> Optional[datetime]:
        """
        Get the timestamp of the last successful run.
        Replaces ABAP SELECT with DataFrame operations.
        
        Returns:
            Timestamp of last successful run or None
        """
        try:
            run_log_df = self.spark.read.format("delta").load("path/to/run_log")
            
            # Filter for successful runs and get the most recent
            successful_runs = run_log_df.filter(F.col("status") == "SUCCESS") \
                .orderBy(F.col("end_time").desc()) \
                .limit(1)
            
            if successful_runs.count() > 0:
                return successful_runs.first()["end_time"]
            return None
            
        except Exception as e:
            self.logger.warning(f"Could not retrieve last run time: {str(e)}")
            return None