"""
Data extraction module for ETL system.
Extracts data from various sources with monitoring and error handling.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional
import logging
from src.utils.logger import ETLLogger
from src.utils.config import Config


class DataExtractor:
    """Handles data extraction from multiple sources."""
    
    def __init__(self, spark: SparkSession, config: Config, run_id: str):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
        
    def get_source_schema(self) -> StructType:
        """Define source data schema."""
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
        source_type: str = "database",
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data from configured source.
        
        Args:
            source_type: Type of source (database, staging, incremental)
            filter_condition: Optional filter to apply
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame containing extracted data
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Starting extraction - Source: {source_type}, Run ID: {self.run_id}"
        )
        
        try:
            if source_type.lower() == "database":
                df = self._extract_from_database(filter_condition)
            elif source_type.lower() == "staging":
                df = self._extract_from_staging()
            elif source_type.lower() == "incremental":
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
            
            # Log extraction metrics
            self._log_extraction_metrics(record_count, source_type)
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="EXTRACTOR",
                message="Extraction failed",
                details=str(e)
            )
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract data from database source."""
        source_path = self.config.get("source.database.path")
        source_format = self.config.get("source.database.format", "parquet")
        
        df = self.spark.read.format(source_format).schema(self.get_source_schema()).load(source_path)
        
        if filter_condition:
            df = df.filter(filter_condition)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract data from staging area."""
        staging_path = self.config.get("source.staging.path")
        
        df = self.spark.read.format("parquet").load(staging_path)
        df = df.filter(f"run_id = '{self.run_id}' AND status = 'READY'")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last run."""
        from pyspark.sql.functions import col, max as spark_max
        
        source_path = self.config.get("source.database.path")
        run_log_path = self.config.get("monitoring.run_log_path")
        
        # Get last successful run timestamp
        try:
            run_log_df = self.spark.read.parquet(run_log_path)
            last_run_time = run_log_df.filter(
                col("status") == "SUCCESS"
            ).agg(spark_max("end_time").alias("max_time")).collect()[0]["max_time"]
        except Exception:
            # If no previous run, do full extract
            self.logger.log_warning(
                component="EXTRACTOR",
                message="No previous run found, performing full extraction"
            )
            return self._extract_from_database()
        
        df = self.spark.read.format("parquet").schema(self.get_source_schema()).load(source_path)
        df = df.filter(col("changed_at") > last_run_time)
        
        return df
    
    def _log_extraction_metrics(self, record_count: int, source_type: str):
        """Log extraction metrics for monitoring."""
        from src.utils.metrics import MetricsCollector
        
        metrics = MetricsCollector.get_instance()
        metrics.record_extraction_metrics(
            run_id=self.run_id,
            source_type=source_type,
            record_count=record_count,
            timestamp=datetime.now()
        )