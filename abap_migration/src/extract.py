"""
ETL Data Extraction Module
Extracts data from various sources (database, staging, incremental)
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional, Dict, Any
import logging


class ETLExtractor:
    """Handles data extraction from various sources"""
    
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
        self.source_type = config.get('extraction', {}).get('source_type', 'DATABASE')
        
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
            StructField("changed_by", StringType(), True)
        ])
    
    def extract_data(self, filter_value: Optional[str] = None, 
                     max_records: int = 0) -> DataFrame:
        """
        Extract data based on source type
        
        Args:
            filter_value: Optional filter criteria
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.info(f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}")
        
        try:
            if self.source_type == 'DATABASE':
                df = self.extract_from_database(filter_value)
            elif self.source_type == 'STAGING':
                df = self.extract_from_staging()
            elif self.source_type == 'INCREMENTAL':
                df = self.extract_incremental()
            else:
                df = self.extract_from_database(filter_value)
            
            # Apply max records limit
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def extract_from_database(self, filter_value: Optional[str] = None) -> DataFrame:
        """
        Extract from database source
        
        Args:
            filter_value: Optional status filter
            
        Returns:
            DataFrame with extracted data
        """
        jdbc_config = self.config['extraction']['jdbc']
        
        query = "(SELECT * FROM zetl_source_data"
        if filter_value:
            query += f" WHERE status = '{filter_value}'"
        query += " LIMIT 1000) as source_data"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", query) \
            .option("user", jdbc_config['user']) \
            .option("password", jdbc_config['password']) \
            .option("driver", jdbc_config['driver']) \
            .load()
        
        return df
    
    def extract_from_staging(self) -> DataFrame:
        """
        Extract from staging area
        
        Returns:
            DataFrame with staged data
        """
        jdbc_config = self.config['extraction']['jdbc']
        
        query = f"""(
            SELECT 
                id,
                name,
                value,
                'STAGED' as status,
                category,
                source_system,
                created_at,
                created_by,
                changed_at,
                changed_by
            FROM zetl_staging
            WHERE run_id = '{self.run_id}' AND status = 'READY'
        ) as staging_data"""
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", query) \
            .option("user", jdbc_config['user']) \
            .option("password", jdbc_config['password']) \
            .option("driver", jdbc_config['driver']) \
            .load()
        
        return df
    
    def extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run
        
        Returns:
            DataFrame with incremental data
        """
        jdbc_config = self.config['extraction']['jdbc']
        
        # Get last successful run time
        last_run_query = """(
            SELECT MAX(end_time) as last_run_time
            FROM zetl_run_log
            WHERE status = 'SUCCESS'
        ) as last_run"""
        
        last_run_df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", last_run_query) \
            .option("user", jdbc_config['user']) \
            .option("password", jdbc_config['password']) \
            .option("driver", jdbc_config['driver']) \
            .load()
        
        last_run_time = last_run_df.collect()[0]['last_run_time']
        
        if last_run_time:
            query = f"""(
                SELECT * FROM zetl_source_data
                WHERE changed_at > '{last_run_time}'
            ) as incremental_data"""
        else:
            self.logger.warning("No previous successful run found, extracting all data")
            query = "(SELECT * FROM zetl_source_data LIMIT 1000) as incremental_data"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", query) \
            .option("user", jdbc_config['user']) \
            .option("password", jdbc_config['password']) \
            .option("driver", jdbc_config['driver']) \
            .load()
        
        return df