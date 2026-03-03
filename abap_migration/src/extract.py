"""
PySpark ETL Extraction Module
Handles data extraction from various sources with comprehensive error handling
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from typing import Optional, Dict, Any
import logging
from datetime import datetime
from src.logger import ETLLogger
from src.config import ETLConfig


class ETLExtractor:
    """Handles data extraction phase of ETL pipeline"""
    
    def __init__(self, spark: SparkSession, config: ETLConfig, run_id: str):
        """
        Initialize extractor with Spark session and configuration
        
        Args:
            spark: Active SparkSession
            config: ETL configuration object
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
        
    def get_source_schema(self) -> StructType:
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
            StructField("changed_by", StringType(), True)
        ])
    
    def extract_data(
        self,
        source_type: str = "database",
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data from configured source
        
        Args:
            source_type: Type of source (database, staging, incremental)
            filter_condition: Optional SQL filter condition
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame containing extracted data
            
        Raises:
            Exception: If extraction fails
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
                self.logger.log_warning(
                    component="EXTRACTOR",
                    message=f"Unknown source type {source_type}, defaulting to database"
                )
                df = self._extract_from_database(filter_condition)
            
            # Apply max records limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.log_info(
                component="EXTRACTOR",
                message=f"Extracted {record_count} records successfully"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="EXTRACTOR",
                message="Extraction failed",
                details=str(e)
            )
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract data from source database/table"""
        source_config = self.config.get_source_config()
        
        df = self.spark.read \
            .format(source_config.get("format", "jdbc")) \
            .options(**source_config.get("options", {})) \
            .schema(self.get_source_schema()) \
            .load()
        
        # Apply filter if provided
        if filter_condition:
            df = df.filter(filter_condition)
        
        # Apply any additional source filters from config
        if "filter" in source_config:
            df = df.filter(source_config["filter"])
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract data from staging area"""
        staging_config = self.config.get_staging_config()
        
        df = self.spark.read \
            .format(staging_config.get("format", "parquet")) \
            .load(staging_config["path"]) \
            .filter(f"run_id = '{self.run_id}' AND status = 'READY'")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last successful run"""
        # Get last successful run timestamp from run log
        run_log_path = self.config.get("paths", {}).get("run_log")
        
        try:
            run_log_df = self.spark.read.parquet(run_log_path)
            last_run = run_log_df \
                .filter("status = 'SUCCESS'") \
                .orderBy("end_time", ascending=False) \
                .select("end_time") \
                .first()
            
            if last_run:
                last_run_time = last_run["end_time"]
                self.logger.log_info(
                    component="EXTRACTOR",
                    message=f"Incremental extraction from: {last_run_time}"
                )
                
                df = self._extract_from_database()
                df = df.filter(df.changed_at > last_run_time)
                return df
            else:
                self.logger.log_warning(
                    component="EXTRACTOR",
                    message="No previous successful run found, performing full extraction"
                )
                return self._extract_from_database()
                
        except Exception as e:
            self.logger.log_warning(
                component="EXTRACTOR",
                message=f"Could not read run log, performing full extraction: {str(e)}"
            )
            return self._extract_from_database()