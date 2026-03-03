"""
ETL Extraction Module
Handles data extraction from various sources with filtering capabilities
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from pyspark.sql.functions import col, current_timestamp
import logging
from typing import Optional, Dict


class ETLExtractor:
    """Data extraction component with multiple source support."""
    
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
    
    def __init__(
        self,
        spark: SparkSession,
        source_type: str = "database",
        run_id: str = None
    ):
        """
        Initialize ETL Extractor.
        
        Args:
            spark: SparkSession instance
            source_type: Type of data source (database, staging, incremental)
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.source_type = source_type
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
    
    def extract_data(
        self,
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data from configured source.
        
        Args:
            filter_condition: Optional SQL WHERE clause filter
            max_records: Maximum number of records to extract (0 = unlimited)
            
        Returns:
            Extracted DataFrame
        """
        self.logger.info(
            f"Starting extraction - Source: {self.source_type}, "
            f"Run ID: {self.run_id}"
        )
        
        try:
            if self.source_type.lower() == "database":
                df = self.extract_from_database(filter_condition)
            elif self.source_type.lower() == "staging":
                df = self.extract_from_staging(self.run_id)
            elif self.source_type.lower() == "incremental":
                last_run_time = self._get_last_run_time()
                df = self.extract_incremental(last_run_time)
            else:
                df = self.extract_from_database(filter_condition)
            
            # Apply max records limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def extract_from_database(
        self,
        filter_condition: Optional[str] = None
    ) -> DataFrame:
        """
        Extract data from database table.
        
        Args:
            filter_condition: Optional SQL WHERE clause
            
        Returns:
            DataFrame with extracted data
        """
        table_name = "etl_source_data"
        
        query = f"SELECT * FROM {table_name}"
        
        if filter_condition:
            query += f" WHERE {filter_condition}"
        
        query += " LIMIT 1000"  # Safety limit
        
        df = self.spark.read.jdbc(
            url=self.spark.conf.get("spark.jdbc.url"),
            table=f"({query}) as source_data",
            properties={
                "user": self.spark.conf.get("spark.jdbc.user"),
                "password": self.spark.conf.get("spark.jdbc.password"),
                "driver": self.spark.conf.get("spark.jdbc.driver", "org.postgresql.Driver")
            }
        )
        
        return df
    
    def extract_from_staging(self, run_id: str) -> DataFrame:
        """
        Extract data from staging table.
        
        Args:
            run_id: Run ID to filter staging data
            
        Returns:
            DataFrame with staged data
        """
        staging_table = "etl_staging"
        
        df = self.spark.read.jdbc(
            url=self.spark.conf.get("spark.jdbc.url"),
            table=f"(SELECT * FROM {staging_table} WHERE run_id = '{run_id}' AND status = 'READY') as staged",
            properties={
                "user": self.spark.conf.get("spark.jdbc.user"),
                "password": self.spark.conf.get("spark.jdbc.password")
            }
        )
        
        return df
    
    def extract_incremental(self, last_run_time: str) -> DataFrame:
        """
        Extract only changed records since last run.
        
        Args:
            last_run_time: Timestamp of last successful run
            
        Returns:
            DataFrame with incremental data
        """
        table_name = "etl_source_data"
        
        query = f"""
        SELECT * FROM {table_name}
        WHERE changed_at > '{last_run_time}'
        """
        
        df = self.spark.read.jdbc(
            url=self.spark.conf.get("spark.jdbc.url"),
            table=f"({query}) as incremental_data",
            properties={
                "user": self.spark.conf.get("spark.jdbc.user"),
                "password": self.spark.conf.get("spark.jdbc.password")
            }
        )
        
        return df
    
    def _get_last_run_time(self) -> str:
        """
        Get timestamp of last successful ETL run.
        
        Returns:
            Timestamp string
        """
        query = """
        SELECT MAX(end_time) as last_run
        FROM etl_run_log
        WHERE status = 'SUCCESS'
        """
        
        try:
            df = self.spark.read.jdbc(
                url=self.spark.conf.get("spark.jdbc.url"),
                table=f"({query}) as last_run",
                properties={
                    "user": self.spark.conf.get("spark.jdbc.user"),
                    "password": self.spark.conf.get("spark.jdbc.password")
                }
            )
            
            result = df.first()
            return result["last_run"] if result and result["last_run"] else "1900-01-01"
            
        except Exception as e:
            self.logger.warning(f"Could not get last run time: {str(e)}")
            return "1900-01-01"