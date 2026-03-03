"""
ETL Extraction module for reading data from various sources.
"""

from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from src.logger import ETLLogger


class ETLExtractor:
    """Handles data extraction from multiple source types."""
    
    def __init__(self, spark: SparkSession, source_type: str = "DATABASE", run_id: str = ""):
        """
        Initialize extractor.
        
        Args:
            spark: Active SparkSession
            source_type: Source type (DATABASE, STAGING, INCREMENTAL)
            run_id: Unique run identifier
        """
        self.spark = spark
        self.source_type = source_type
        self.run_id = run_id
        self.logger = ETLLogger()
    
    def extract_data(
        self,
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data from configured source.
        
        Args:
            filter_condition: Optional SQL filter condition
            max_records: Maximum records to extract (0 = unlimited)
        
        Returns:
            DataFrame containing extracted data
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Starting extraction - Source: {self.source_type}"
        )
        
        if self.source_type == "DATABASE":
            df = self._extract_from_database(filter_condition)
        elif self.source_type == "STAGING":
            df = self._extract_from_staging()
        elif self.source_type == "INCREMENTAL":
            df = self._extract_incremental()
        else:
            df = self._extract_from_database(filter_condition)
        
        # Apply max records limit
        if max_records > 0:
            df = df.limit(max_records)
        
        record_count = df.count()
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Extracted {record_count} records"
        )
        
        return df
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract from source database table."""
        df = self.spark.table("etl_source_data")
        
        if filter_condition:
            df = df.filter(filter_condition)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract from staging table."""
        df = self.spark.table("etl_staging").filter(
            f"run_id = '{self.run_id}' AND status = 'READY'"
        )
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract incremental changes since last run."""
        # Get last successful run time
        last_run_df = self.spark.table("etl_run_log").filter(
            "status = 'SUCCESS'"
        ).orderBy("end_time", ascending=False).limit(1)
        
        if last_run_df.count() > 0:
            last_run_time = last_run_df.first()["end_time"]
            df = self.spark.table("etl_source_data").filter(
                f"changed_at > '{last_run_time}'"
            )
        else:
            df = self.spark.table("etl_source_data")
        
        return df