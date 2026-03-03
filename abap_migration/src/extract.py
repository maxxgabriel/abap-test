"""
ETL Data Extraction Module
Extracts data from various sources with support for incremental loading
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional, Dict, Any
import logging


class ETLExtractor:
    """
    Handles data extraction from multiple source types with filtering and incremental capabilities
    """
    
    def __init__(self, spark: SparkSession, source_type: str = "DATABASE", run_id: str = None):
        """
        Initialize extractor with Spark session and configuration
        
        Args:
            spark: Active SparkSession
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            run_id: Unique run identifier
        """
        self.spark = spark
        self.source_type = source_type
        self.run_id = run_id or self._generate_run_id()
        self.logger = logging.getLogger(__name__)
        
    def _generate_run_id(self) -> str:
        """Generate unique run ID"""
        return f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def get_source_schema(self) -> StructType:
        """
        Define schema for source data
        
        Returns:
            StructType schema definition
        """
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
    
    def extract_data(self, filter_condition: Optional[str] = None, 
                    max_records: int = 0) -> DataFrame:
        """
        Main extraction method routing to appropriate source
        
        Args:
            filter_condition: SQL filter condition
            max_records: Maximum records to extract (0 = no limit)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.info(f"Starting extraction - Source: {self.source_type}, Run: {self.run_id}")
        
        try:
            if self.source_type == "DATABASE":
                df = self.extract_from_database(filter_condition)
            elif self.source_type == "STAGING":
                df = self.extract_from_staging(self.run_id)
            elif self.source_type == "INCREMENTAL":
                last_run_time = self._get_last_successful_run_time()
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
    
    def extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract data from database source
        
        Args:
            filter_condition: Optional WHERE clause condition
            
        Returns:
            DataFrame with source data
        """
        self.logger.info("Extracting from database")
        
        jdbc_config = self.spark.conf.get("spark.etl.jdbc.url")
        table_name = self.spark.conf.get("spark.etl.source.table", "etl_source_data")
        
        query = f"(SELECT * FROM {table_name}"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        query += " LIMIT 1000) as source_query"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config) \
            .option("dbtable", query) \
            .option("user", self.spark.conf.get("spark.etl.jdbc.user")) \
            .option("password", self.spark.conf.get("spark.etl.jdbc.password")) \
            .option("driver", self.spark.conf.get("spark.etl.jdbc.driver")) \
            .load()
        
        return df
    
    def extract_from_staging(self, run_id: str) -> DataFrame:
        """
        Extract data from staging area
        
        Args:
            run_id: Run identifier to extract staged data for
            
        Returns:
            DataFrame with staged data
        """
        self.logger.info(f"Extracting from staging for run: {run_id}")
        
        staging_path = self.spark.conf.get("spark.etl.staging.path")
        
        df = self.spark.read \
            .parquet(f"{staging_path}/run_id={run_id}") \
            .filter("status = 'READY'")
        
        return df
    
    def extract_incremental(self, last_run_time: Optional[datetime]) -> DataFrame:
        """
        Extract only changed records since last successful run
        
        Args:
            last_run_time: Timestamp of last successful run
            
        Returns:
            DataFrame with incremental data
        """
        self.logger.info(f"Extracting incremental data since: {last_run_time}")
        
        jdbc_config = self.spark.conf.get("spark.etl.jdbc.url")
        table_name = self.spark.conf.get("spark.etl.source.table", "etl_source_data")
        
        query = f"(SELECT * FROM {table_name}"
        if last_run_time:
            query += f" WHERE changed_at > '{last_run_time}'"
        query += ") as incremental_query"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config) \
            .option("dbtable", query) \
            .option("user", self.spark.conf.get("spark.etl.jdbc.user")) \
            .option("password", self.spark.conf.get("spark.etl.jdbc.password")) \
            .option("driver", self.spark.conf.get("spark.etl.jdbc.driver")) \
            .load()
        
        return df
    
    def _get_last_successful_run_time(self) -> Optional[datetime]:
        """
        Retrieve timestamp of last successful ETL run
        
        Returns:
            Datetime of last run or None
        """
        try:
            jdbc_config = self.spark.conf.get("spark.etl.jdbc.url")
            
            query = """(
                SELECT MAX(end_time) as last_run_time 
                FROM etl_run_log 
                WHERE status = 'SUCCESS'
            ) as last_run_query"""
            
            df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_config) \
                .option("dbtable", query) \
                .option("user", self.spark.conf.get("spark.etl.jdbc.user")) \
                .option("password", self.spark.conf.get("spark.etl.jdbc.password")) \
                .option("driver", self.spark.conf.get("spark.etl.jdbc.driver")) \
                .load()
            
            row = df.first()
            return row["last_run_time"] if row else None
            
        except Exception as e:
            self.logger.warning(f"Could not retrieve last run time: {str(e)}")
            return None