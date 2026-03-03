"""
PySpark ETL Extractor Module
Extracts data from various sources (Database, Staging, Incremental)
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional, Dict, Any
import logging


class ETLExtractor:
    """Handles data extraction from various sources"""
    
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
        StructField("changed_by", StringType(), True),
    ])
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize extractor
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        self.source_type = config.get('source_type', 'DATABASE')
        
    def extract_data(self, 
                     filter_condition: Optional[str] = None,
                     max_records: int = 0) -> DataFrame:
        """
        Main extraction method - routes to appropriate extractor
        
        Args:
            filter_condition: Optional SQL filter condition
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.info(f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}")
        
        try:
            # Route to appropriate extraction method
            if self.source_type == 'DATABASE':
                df = self._extract_from_database(filter_condition)
            elif self.source_type == 'STAGING':
                df = self._extract_from_staging()
            elif self.source_type == 'INCREMENTAL':
                df = self._extract_incremental()
            else:
                self.logger.warning(f"Unknown source type {self.source_type}, defaulting to DATABASE")
                df = self._extract_from_database(filter_condition)
            
            # Apply max records limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract from database table
        
        Args:
            filter_condition: Optional WHERE clause condition
            
        Returns:
            DataFrame with extracted data
        """
        jdbc_config = self.config['jdbc']
        table_name = self.config['source_table']
        
        # Build query
        if filter_condition:
            query = f"(SELECT * FROM {table_name} WHERE {filter_condition}) AS filtered_data"
        else:
            query = f"(SELECT * FROM {table_name} LIMIT 1000) AS source_data"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", query) \
            .option("user", jdbc_config['user']) \
            .option("password", jdbc_config['password']) \
            .option("driver", jdbc_config['driver']) \
            .schema(self.SOURCE_SCHEMA) \
            .load()
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """
        Extract from staging area
        
        Returns:
            DataFrame with staged data
        """
        staging_path = self.config['staging_path']
        
        df = self.spark.read \
            .format(self.config.get('staging_format', 'parquet')) \
            .schema(self.SOURCE_SCHEMA) \
            .load(f"{staging_path}/run_id={self.run_id}")
        
        return df.filter("status = 'READY'")
    
    def _extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run
        
        Returns:
            DataFrame with incremental data
        """
        # Get last successful run timestamp
        last_run_time = self._get_last_run_timestamp()
        
        jdbc_config = self.config['jdbc']
        table_name = self.config['source_table']
        
        if last_run_time:
            query = f"""(
                SELECT * FROM {table_name} 
                WHERE changed_at > TIMESTAMP '{last_run_time}'
            ) AS incremental_data"""
        else:
            self.logger.warning("No previous run found, performing full extraction")
            query = f"(SELECT * FROM {table_name}) AS source_data"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", query) \
            .option("user", jdbc_config['user']) \
            .option("password", jdbc_config['password']) \
            .option("driver", jdbc_config['driver']) \
            .schema(self.SOURCE_SCHEMA) \
            .load()
        
        return df
    
    def _get_last_run_timestamp(self) -> Optional[str]:
        """
        Get timestamp of last successful run
        
        Returns:
            Timestamp string or None
        """
        jdbc_config = self.config['jdbc']
        
        query = """(
            SELECT end_time 
            FROM etl_run_log 
            WHERE status = 'SUCCESS' 
            ORDER BY end_time DESC 
            LIMIT 1
        ) AS last_run"""
        
        try:
            df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_config['url']) \
                .option("dbtable", query) \
                .option("user", jdbc_config['user']) \
                .option("password", jdbc_config['password']) \
                .option("driver", jdbc_config['driver']) \
                .load()
            
            if df.count() > 0:
                return df.first()['end_time'].strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            self.logger.warning(f"Could not retrieve last run time: {str(e)}")
        
        return None