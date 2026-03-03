"""
PySpark ETL Extractor Module
Handles data extraction from various sources with support for full, incremental, and staged loads.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from typing import Optional, Dict, Any
from datetime import datetime
import logging


class ETLExtractor:
    """
    Extractor class for ETL pipeline supporting multiple source types.
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the extractor.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        self.source_schema = self._get_source_schema()
    
    def _get_source_schema(self) -> StructType:
        """Define the schema for source data."""
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
        Extract data based on source type.
        
        Args:
            source_type: Type of source ('database', 'staging', 'incremental')
            filter_condition: Optional filter condition
            max_records: Maximum records to extract (0 = no limit)
            
        Returns:
            DataFrame containing extracted data
        """
        self.logger.info(f"Starting extraction - Source: {source_type}, Run ID: {self.run_id}")
        
        try:
            if source_type.lower() == "database":
                df = self._extract_from_database(filter_condition)
            elif source_type.lower() == "staging":
                df = self._extract_from_staging()
            elif source_type.lower() == "incremental":
                df = self._extract_incremental()
            else:
                self.logger.warning(f"Unknown source type {source_type}, defaulting to database")
                df = self._extract_from_database(filter_condition)
            
            # Apply max records limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}", exc_info=True)
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract from database source.
        
        Args:
            filter_condition: SQL WHERE clause condition
            
        Returns:
            DataFrame with extracted data
        """
        jdbc_url = self.config.get("source_jdbc_url")
        table_name = self.config.get("source_table", "etl_source_data")
        
        connection_properties = {
            "user": self.config.get("source_user"),
            "password": self.config.get("source_password"),
            "driver": self.config.get("jdbc_driver", "org.postgresql.Driver")
        }
        
        # Base query
        query = f"(SELECT * FROM {table_name}"
        
        if filter_condition:
            query += f" WHERE {filter_condition}"
        
        query += f" LIMIT {self.config.get('extraction_limit', 1000)}) as source_data"
        
        df = self.spark.read.jdbc(
            url=jdbc_url,
            table=query,
            properties=connection_properties
        )
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """
        Extract from staging area.
        
        Returns:
            DataFrame with staged data
        """
        staging_path = self.config.get("staging_path")
        file_format = self.config.get("staging_format", "parquet")
        
        df = (self.spark.read
              .format(file_format)
              .schema(self.source_schema)
              .load(f"{staging_path}/run_id={self.run_id}"))
        
        return df.filter("status = 'READY'")
    
    def _extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run.
        
        Returns:
            DataFrame with incremental data
        """
        # Get last successful run timestamp
        last_run_time = self._get_last_run_timestamp()
        
        if last_run_time is None:
            self.logger.warning("No previous run found, performing full extraction")
            return self._extract_from_database()
        
        self.logger.info(f"Extracting incremental data since {last_run_time}")
        
        jdbc_url = self.config.get("source_jdbc_url")
        table_name = self.config.get("source_table", "etl_source_data")
        
        connection_properties = {
            "user": self.config.get("source_user"),
            "password": self.config.get("source_password"),
            "driver": self.config.get("jdbc_driver")
        }
        
        query = f"""(
            SELECT * FROM {table_name}
            WHERE changed_at > '{last_run_time}'
        ) as incremental_data"""
        
        df = self.spark.read.jdbc(
            url=jdbc_url,
            table=query,
            properties=connection_properties
        )
        
        return df
    
    def _get_last_run_timestamp(self) -> Optional[str]:
        """
        Retrieve timestamp of last successful run.
        
        Returns:
            Timestamp string or None if no previous run
        """
        jdbc_url = self.config.get("metadata_jdbc_url")
        
        connection_properties = {
            "user": self.config.get("metadata_user"),
            "password": self.config.get("metadata_password"),
            "driver": self.config.get("jdbc_driver")
        }
        
        query = """(
            SELECT end_time
            FROM etl_run_log
            WHERE status = 'SUCCESS'
            ORDER BY end_time DESC
            LIMIT 1
        ) as last_run"""
        
        try:
            df = self.spark.read.jdbc(
                url=jdbc_url,
                table=query,
                properties=connection_properties
            )
            
            if df.count() > 0:
                return df.first()["end_time"]
            return None
            
        except Exception as e:
            self.logger.warning(f"Could not retrieve last run timestamp: {str(e)}")
            return None