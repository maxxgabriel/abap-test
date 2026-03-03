"""
ETL Extractor Module
Handles data extraction from various sources with incremental load support.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
import logging
from typing import Optional


class ETLExtractor:
    """Extract data from source systems"""
    
    def __init__(self, spark: SparkSession, source_type: str = "DATABASE", run_id: str = None):
        self.spark = spark
        self.source_type = source_type
        self.run_id = run_id or self._generate_run_id()
        self.logger = logging.getLogger(__name__)
        
    def _generate_run_id(self) -> str:
        """Generate unique run ID"""
        return f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def get_source_schema(self) -> StructType:
        """Define source data schema"""
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
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data based on source type
        
        Args:
            filter_condition: SQL filter condition
            max_records: Maximum records to extract (0 = no limit)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.info(f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}")
        
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
        self.logger.info(f"Extracted {record_count} records")
        
        return df
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract from source database table"""
        self.logger.info("Extracting from database")
        
        # Read from source table
        df = self.spark.read \
            .format("jdbc") \
            .option("driver", self.spark.conf.get("spark.jdbc.driver")) \
            .option("url", self.spark.conf.get("spark.jdbc.url")) \
            .option("dbtable", self.spark.conf.get("spark.source.table", "etl_source_data")) \
            .option("user", self.spark.conf.get("spark.jdbc.user")) \
            .option("password", self.spark.conf.get("spark.jdbc.password")) \
            .load()
        
        # Apply filter if provided
        if filter_condition:
            df = df.filter(filter_condition)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract from staging area"""
        self.logger.info(f"Extracting from staging for run_id: {self.run_id}")
        
        staging_path = self.spark.conf.get("spark.staging.path", "/staging/etl_data")
        
        df = self.spark.read \
            .parquet(f"{staging_path}/run_id={self.run_id}")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last successful run"""
        self.logger.info("Extracting incremental data")
        
        # Get last successful run timestamp
        last_run_time = self._get_last_run_timestamp()
        
        if last_run_time:
            self.logger.info(f"Extracting records changed after {last_run_time}")
            filter_condition = f"changed_at > '{last_run_time}'"
        else:
            self.logger.warning("No previous run found, extracting all records")
            filter_condition = None
        
        return self._extract_from_database(filter_condition)
    
    def _get_last_run_timestamp(self) -> Optional[str]:
        """Get timestamp of last successful ETL run"""
        try:
            run_log_df = self.spark.read \
                .format("jdbc") \
                .option("driver", self.spark.conf.get("spark.jdbc.driver")) \
                .option("url", self.spark.conf.get("spark.jdbc.url")) \
                .option("dbtable", "etl_run_log") \
                .option("user", self.spark.conf.get("spark.jdbc.user")) \
                .option("password", self.spark.conf.get("spark.jdbc.password")) \
                .load()
            
            last_run = run_log_df \
                .filter("status = 'SUCCESS'") \
                .orderBy("end_time", ascending=False) \
                .first()
            
            return last_run["end_time"] if last_run else None
        except Exception as e:
            self.logger.warning(f"Could not retrieve last run timestamp: {e}")
            return None