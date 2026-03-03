"""Data extraction module for ETL pipeline."""
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
import logging

from src.utils.logger import ETLLogger
from src.utils.config import Config


class ETLExtractor:
    """Handles data extraction from various sources."""
    
    SCHEMA = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("value", DecimalType(15, 2), False),
        StructField("status", StringType(), False),
        StructField("category", StringType(), False),
        StructField("source_system", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("created_by", StringType(), True),
        StructField("changed_at", TimestampType(), True),
        StructField("changed_by", StringType(), True),
    ])
    
    def __init__(self, spark: SparkSession, config: Config, run_id: str):
        """Initialize extractor.
        
        Args:
            spark: SparkSession instance
            config: Configuration object
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
        self.source_type = config.get("extract.source_type", "database")
        
    def extract_data(self, 
                     filter_condition: Optional[str] = None,
                     max_records: int = 0) -> DataFrame:
        """Extract data based on source type.
        
        Args:
            filter_condition: SQL WHERE clause condition
            max_records: Maximum number of records to extract (0 = unlimited)
            
        Returns:
            DataFrame containing extracted data
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}"
        )
        
        try:
            if self.source_type.upper() == "DATABASE":
                df = self._extract_from_database(filter_condition)
            elif self.source_type.upper() == "STAGING":
                df = self._extract_from_staging()
            elif self.source_type.upper() == "INCREMENTAL":
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
            
        except Exception as e:
            self.logger.log_error(
                component="EXTRACTOR",
                message="Extraction failed",
                details=str(e)
            )
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract data from database source.
        
        Args:
            filter_condition: SQL WHERE clause condition
            
        Returns:
            DataFrame with extracted data
        """
        jdbc_config = self.config.get_section("extract.jdbc")
        table_name = self.config.get("extract.source_table", "etl_source_data")
        
        query = f"(SELECT * FROM {table_name}"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        query += ") as source_data"
        
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
        """Extract data from staging area.
        
        Returns:
            DataFrame with staged data
        """
        staging_path = self.config.get("extract.staging_path")
        
        df = self.spark.read \
            .format("parquet") \
            .schema(self.SCHEMA) \
            .load(f"{staging_path}/run_id={self.run_id}")
        
        return df.filter("status = 'READY'")
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last run.
        
        Returns:
            DataFrame with incremental data
        """
        last_run_time = self._get_last_successful_run_time()
        
        if last_run_time:
            filter_condition = f"changed_at > '{last_run_time}'"
            self.logger.log_info(
                component="EXTRACTOR",
                message=f"Incremental extraction since: {last_run_time}"
            )
        else:
            filter_condition = None
            self.logger.log_warning(
                component="EXTRACTOR",
                message="No previous run found - performing full extraction"
            )
        
        return self._extract_from_database(filter_condition)
    
    def _get_last_successful_run_time(self) -> Optional[datetime]:
        """Get timestamp of last successful run.
        
        Returns:
            Timestamp of last successful run or None
        """
        jdbc_config = self.config.get_section("extract.jdbc")
        log_table = self.config.get("monitoring.run_log_table", "etl_run_log")
        
        query = f"""(
            SELECT MAX(end_time) as last_run_time 
            FROM {log_table}
            WHERE status = 'SUCCESS'
        ) as last_run"""
        
        try:
            df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_config["url"]) \
                .option("dbtable", query) \
                .option("user", jdbc_config["user"]) \
                .option("password", jdbc_config["password"]) \
                .option("driver", jdbc_config["driver"]) \
                .load()
            
            row = df.first()
            return row.last_run_time if row and row.last_run_time else None
            
        except Exception as e:
            self.logger.log_warning(
                component="EXTRACTOR",
                message="Could not retrieve last run time",
                details=str(e)
            )
            return None