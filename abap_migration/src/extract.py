"""
ETL Data Extraction Module
Extracts data from various sources (database, staging, incremental).
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import Optional
from datetime import datetime
from src.logger import ETLLogger
import yaml


class ETLExtractor:
    """Handles data extraction from various sources."""
    
    # Define schema for source data
    SOURCE_SCHEMA = StructType([
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
    
    def __init__(self, spark: SparkSession, source_type: str = "DATABASE", 
                 run_id: str = None, config: dict = None):
        """
        Initialize the extractor.
        
        Args:
            spark: SparkSession instance
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.source_type = source_type
        self.run_id = run_id or self._generate_run_id()
        self.config = config or {}
        self.logger = ETLLogger.get_instance()
        
    def _generate_run_id(self) -> str:
        """Generate a unique run ID."""
        return f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def extract_data(self, filter_condition: Optional[str] = None, 
                     max_records: int = 0) -> DataFrame:
        """
        Extract data based on source type.
        
        Args:
            filter_condition: Optional SQL filter condition
            max_records: Maximum number of records to extract (0 = no limit)
            
        Returns:
            DataFrame containing extracted data
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Starting extraction - Source: {self.source_type}"
        )
        
        try:
            # Route to appropriate extraction method
            if self.source_type == "DATABASE":
                df = self._extract_from_database(filter_condition)
            elif self.source_type == "STAGING":
                df = self._extract_from_staging()
            elif self.source_type == "INCREMENTAL":
                df = self._extract_incremental()
            else:
                df = self._extract_from_database(filter_condition)
            
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
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract data from database source.
        
        Args:
            filter_condition: Optional filter condition
            
        Returns:
            DataFrame with extracted data
        """
        # Get configuration
        source_config = self.config.get("source", {})
        table_name = source_config.get("table", "source_data")
        
        # Read from database (example using JDBC)
        connection_props = {
            "driver": "org.postgresql.Driver",
            "user": source_config.get("connection", {}).get("user", "etl_user"),
            "password": source_config.get("connection", {}).get("password", ""),
        }
        
        jdbc_url = (
            f"jdbc:postgresql://{source_config.get('connection', {}).get('host', 'localhost')}:"
            f"{source_config.get('connection', {}).get('port', 5432)}/"
            f"{source_config.get('connection', {}).get('database', 'etl_source')}"
        )
        
        # Read data
        df = self.spark.read.jdbc(
            url=jdbc_url,
            table=table_name,
            properties=connection_props
        )
        
        # Apply filter if provided
        if filter_condition:
            df = df.filter(filter_condition)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """
        Extract data from staging area.
        
        Returns:
            DataFrame with staged data
        """
        # Read from staging table/files
        staging_path = self.config.get("staging", {}).get("path", "/staging")
        
        df = self.spark.read.parquet(f"{staging_path}/run_{self.run_id}")
        df = df.filter(df.status == "READY")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last run.
        
        Returns:
            DataFrame with incremental data
        """
        # Get last successful run timestamp
        last_run_time = self._get_last_run_time()
        
        # Extract records changed after last run
        source_config = self.config.get("source", {})
        table_name = source_config.get("table", "source_data")
        
        connection_props = {
            "driver": "org.postgresql.Driver",
            "user": source_config.get("connection", {}).get("user", "etl_user"),
            "password": source_config.get("connection", {}).get("password", ""),
        }
        
        jdbc_url = (
            f"jdbc:postgresql://{source_config.get('connection', {}).get('host', 'localhost')}:"
            f"{source_config.get('connection', {}).get('port', 5432)}/"
            f"{source_config.get('connection', {}).get('database', 'etl_source')}"
        )
        
        df = self.spark.read.jdbc(
            url=jdbc_url,
            table=table_name,
            properties=connection_props
        )
        
        if last_run_time:
            df = df.filter(df.changed_at > last_run_time)
        
        return df
    
    def _get_last_run_time(self) -> Optional[datetime]:
        """
        Get timestamp of last successful run.
        
        Returns:
            Timestamp of last run or None
        """
        try:
            # Query run log for last successful run
            run_log_df = self.spark.read.jdbc(
                url=self._get_jdbc_url(),
                table="run_log",
                properties=self._get_connection_props()
            )
            
            last_run = run_log_df.filter(
                run_log_df.status == "SUCCESS"
            ).orderBy(
                run_log_df.end_time.desc()
            ).first()
            
            return last_run.end_time if last_run else None
            
        except Exception as e:
            self.logger.log_warning(
                component="EXTRACTOR",
                message="Could not retrieve last run time",
                details=str(e)
            )
            return None
    
    def _get_jdbc_url(self) -> str:
        """Get JDBC URL from config."""
        source_config = self.config.get("source", {})
        return (
            f"jdbc:postgresql://{source_config.get('connection', {}).get('host', 'localhost')}:"
            f"{source_config.get('connection', {}).get('port', 5432)}/"
            f"{source_config.get('connection', {}).get('database', 'etl_source')}"
        )
    
    def _get_connection_props(self) -> dict:
        """Get connection properties from config."""
        source_config = self.config.get("source", {})
        return {
            "driver": "org.postgresql.Driver",
            "user": source_config.get("connection", {}).get("user", "etl_user"),
            "password": source_config.get("connection", {}).get("password", ""),
        }