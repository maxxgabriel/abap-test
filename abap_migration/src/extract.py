"""
PySpark Data Extraction Module

Migrated from ABAP zcl_etl_extractor class.
Provides data extraction from various sources with filtering and incremental load support.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
from typing import Optional, Dict, Any
from datetime import datetime
import logging

from src.logger import ETLLogger
from src.config import ConfigManager


class DataExtractor:
    """Extract data from various sources for ETL pipeline."""
    
    def __init__(self, source_type: str = "DATABASE", run_id: str = None):
        """
        Initialize extractor with source type and run ID.
        
        Args:
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            run_id: Unique identifier for this ETL run
        """
        self.source_type = source_type.upper()
        self.run_id = run_id or self._generate_run_id()
        self.logger = ETLLogger.get_instance()
        self.config = ConfigManager()
        self.spark = SparkSession.builder.appName("ETL_Extractor").getOrCreate()
        
    def _generate_run_id(self) -> str:
        """Generate unique run ID."""
        return f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def extract_data(
        self,
        filter_clause: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Main extraction method routing to appropriate source.
        
        Args:
            filter_clause: Optional WHERE clause filter
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Starting extraction - Source: {self.source_type}"
        )
        
        try:
            # Route to appropriate extraction method
            if self.source_type == "DATABASE":
                df = self._extract_from_database(filter_clause)
            elif self.source_type == "STAGING":
                df = self._extract_from_staging(self.run_id)
            elif self.source_type == "INCREMENTAL":
                last_run_time = self._get_last_successful_run_time()
                if last_run_time:
                    df = self._extract_incremental(last_run_time)
                else:
                    df = self._extract_from_database(filter_clause)
            else:
                df = self._extract_from_database(filter_clause)
            
            # Apply max records limit if specified
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
    
    def _extract_from_database(self, filter_clause: Optional[str] = None) -> DataFrame:
        """
        Extract from source database table.
        
        Args:
            filter_clause: Optional filter condition
            
        Returns:
            DataFrame with source data
        """
        source_table = self.config.get("source_table", "etl_source_data")
        
        # Read from database
        df = self.spark.read \
            .format("jdbc") \
            .option("url", self.config.get("jdbc_url")) \
            .option("dbtable", source_table) \
            .option("user", self.config.get("db_user")) \
            .option("password", self.config.get("db_password")) \
            .load()
        
        # Apply filter if provided
        if filter_clause:
            df = df.filter(filter_clause)
        
        return df
    
    def _extract_from_staging(self, run_id: str) -> DataFrame:
        """
        Extract from staging area.
        
        Args:
            run_id: Run identifier to filter staging data
            
        Returns:
            DataFrame with staged data
        """
        staging_table = self.config.get("staging_table", "etl_staging")
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", self.config.get("jdbc_url")) \
            .option("dbtable", staging_table) \
            .option("user", self.config.get("db_user")) \
            .option("password", self.config.get("db_password")) \
            .load()
        
        # Filter by run_id and status
        df = df.filter(
            (col("run_id") == run_id) & 
            (col("status") == "READY")
        )
        
        return df
    
    def _extract_incremental(self, last_run_time: str) -> DataFrame:
        """
        Extract only changed records since last run.
        
        Args:
            last_run_time: Timestamp of last successful run
            
        Returns:
            DataFrame with incremental data
        """
        source_table = self.config.get("source_table", "etl_source_data")
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", self.config.get("jdbc_url")) \
            .option("dbtable", source_table) \
            .option("user", self.config.get("db_user")) \
            .option("password", self.config.get("db_password")) \
            .load()
        
        # Filter by changed_at timestamp
        df = df.filter(col("changed_at") > lit(last_run_time))
        
        return df
    
    def _get_last_successful_run_time(self) -> Optional[str]:
        """
        Get timestamp of last successful ETL run.
        
        Returns:
            Timestamp string or None if no successful runs found
        """
        run_log_table = self.config.get("run_log_table", "etl_run_log")
        
        try:
            df = self.spark.read \
                .format("jdbc") \
                .option("url", self.config.get("jdbc_url")) \
                .option("dbtable", run_log_table) \
                .option("user", self.config.get("db_user")) \
                .option("password", self.config.get("db_password")) \
                .load()
            
            # Get last successful run
            last_run = df.filter(col("status") == "SUCCESS") \
                .orderBy(col("end_time").desc()) \
                .limit(1) \
                .select("end_time") \
                .collect()
            
            if last_run:
                return last_run[0]["end_time"]
            return None
            
        except Exception as e:
            self.logger.log_warning(
                component="EXTRACTOR",
                message="Could not retrieve last run time",
                details=str(e)
            )
            return None