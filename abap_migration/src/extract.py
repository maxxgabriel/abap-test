"""
ETL Extractor Module - Handles data extraction from various sources
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from datetime import datetime
import yaml
import logging
from typing import Optional, Dict, Any


class ETLExtractor:
    """Extracts data from various sources for ETL processing"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the extractor
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def get_source_schema(self) -> StructType:
        """Define the source data schema"""
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
        Extract data based on source type
        
        Args:
            source_type: Type of source (database, staging, incremental)
            filter_condition: Optional filter to apply
            max_records: Maximum records to extract (0 = unlimited)
            
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
                df = self._extract_from_database(filter_condition)
            
            # Apply max records limit
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract data from database source"""
        db_config = self.config['source']['database']
        
        query = f"SELECT * FROM {db_config['table']}"
        
        if filter_condition:
            query += f" WHERE {filter_condition}"
        
        query += f" LIMIT {db_config.get('default_limit', 1000)}"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", db_config['jdbc_url']) \
            .option("query", query) \
            .option("user", db_config.get('user', '')) \
            .option("password", db_config.get('password', '')) \
            .option("driver", db_config.get('driver', 'org.postgresql.Driver')) \
            .load()
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract data from staging area"""
        staging_config = self.config['source']['staging']
        
        df = self.spark.read \
            .format(staging_config.get('format', 'parquet')) \
            .load(staging_config['path']) \
            .filter(f"run_id = '{self.run_id}' AND status = 'READY'")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last run"""
        # Get last successful run timestamp
        last_run_time = self._get_last_run_time()
        
        db_config = self.config['source']['database']
        
        query = f"""
            SELECT * FROM {db_config['table']}
            WHERE changed_at > '{last_run_time}'
        """
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", db_config['jdbc_url']) \
            .option("query", query) \
            .option("user", db_config.get('user', '')) \
            .option("password", db_config.get('password', '')) \
            .option("driver", db_config.get('driver', 'org.postgresql.Driver')) \
            .load()
        
        return df
    
    def _get_last_run_time(self) -> str:
        """Get timestamp of last successful run"""
        log_config = self.config['logging']['database']
        
        query = f"""
            SELECT MAX(end_time) as last_run
            FROM {log_config['run_log_table']}
            WHERE status = 'SUCCESS'
        """
        
        result = self.spark.read \
            .format("jdbc") \
            .option("url", log_config['jdbc_url']) \
            .option("query", query) \
            .option("user", log_config.get('user', '')) \
            .option("password", log_config.get('password', '')) \
            .load()
        
        last_run = result.collect()[0]['last_run']
        
        if last_run:
            return last_run.strftime('%Y-%m-%d %H:%M:%S')
        else:
            # If no previous run, return epoch
            return '1970-01-01 00:00:00'