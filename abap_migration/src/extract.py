"""
Data Extraction Module
Handles data extraction from various sources.
"""

from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp
from datetime import datetime

from src.logger import ETLLogger
from src.config import ConfigManager


class Extractor:
    """Handles extraction of data from various sources."""
    
    def __init__(self, spark: SparkSession, source_type: str, run_id: str):
        """
        Initialize extractor.
        
        Args:
            spark: SparkSession instance
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            run_id: Unique run identifier
        """
        self.spark = spark
        self.source_type = source_type
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
        self.config = ConfigManager.get_instance()
    
    def extract_data(
        self,
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> Optional[DataFrame]:
        """
        Extract data based on source type.
        
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
        
        try:
            if self.source_type == "DATABASE":
                df = self._extract_from_database(filter_condition)
            elif self.source_type == "STAGING":
                df = self._extract_from_staging()
            elif self.source_type == "INCREMENTAL":
                df = self._extract_incremental()
            else:
                df = self._extract_from_database(filter_condition)
            
            if df is None:
                return None
            
            # Apply max records limit
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
    
    def _extract_from_database(
        self,
        filter_condition: Optional[str] = None
    ) -> DataFrame:
        """Extract data from source database table."""
        source_table = self.config.get("source_table", "etl_source_data")
        
        df = self.spark.table(source_table)
        
        if filter_condition:
            df = df.filter(filter_condition)
        
        return df.select(
            "id",
            "name",
            "value",
            "status",
            "category",
            "source_system",
            "created_at",
            "created_by",
            "changed_at",
            "changed_by"
        )
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract data from staging table."""
        staging_table = self.config.get("staging_table", "etl_staging")
        
        df = self.spark.table(staging_table) \
            .filter((col("run_id") == self.run_id) & (col("status") == "READY"))
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last successful run."""
        # Get last successful run time
        run_log_table = self.config.get("run_log_table", "etl_run_log")
        
        last_run_df = self.spark.table(run_log_table) \
            .filter(col("status") == "SUCCESS") \
            .orderBy(col("end_time").desc()) \
            .limit(1)
        
        if last_run_df.count() == 0:
            # No previous successful run, do full extract
            return self._extract_from_database()
        
        last_run_time = last_run_df.collect()[0]["end_time"]
        
        source_table = self.config.get("source_table", "etl_source_data")
        
        return self.spark.table(source_table) \
            .filter(col("changed_at") > last_run_time)