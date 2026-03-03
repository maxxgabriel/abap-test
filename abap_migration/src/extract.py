"""
PySpark ETL Extraction Module
Handles data extraction from various sources with monitoring and logging.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional, Dict, Any
import logging

from src.logger import ETLLogger


class ETLExtractor:
    """Extracts data from various sources for ETL processing."""
    
    SOURCE_SCHEMA = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("value", DecimalType(15, 2), False),
        StructField("status", StringType(), False),
        StructField("category", StringType(), False),
        StructField("source_system", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("created_by", StringType(), False),
        StructField("changed_at", TimestampType(), False),
        StructField("changed_by", StringType(), False),
    ])
    
    def __init__(self, spark: SparkSession, run_id: str, config: Dict[str, Any]):
        """
        Initialize ETL Extractor.
        
        Args:
            spark: SparkSession instance
            run_id: Unique identifier for this ETL run
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = ETLLogger.get_instance()
        self.source_type = config.get('source', {}).get('type', 'database')
        
    def extract_data(self, 
                     filter_condition: Optional[str] = None,
                     max_records: int = 0) -> DataFrame:
        """
        Extract data based on configured source type.
        
        Args:
            filter_condition: SQL WHERE clause filter
            max_records: Maximum number of records to extract (0 = unlimited)
            
        Returns:
            Extracted DataFrame
        """
        self.logger.log_info(
            component='EXTRACTOR',
            message=f'Starting extraction - Source: {self.source_type}',
            run_id=self.run_id
        )
        
        try:
            if self.source_type == 'database':
                df = self._extract_from_database(filter_condition)
            elif self.source_type == 'staging':
                df = self._extract_from_staging()
            elif self.source_type == 'incremental':
                df = self._extract_incremental()
            elif self.source_type == 'file':
                df = self._extract_from_file()
            else:
                df = self._extract_from_database(filter_condition)
            
            # Apply max records limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            
            self.logger.log_info(
                component='EXTRACTOR',
                message=f'Extracted {record_count} records',
                run_id=self.run_id,
                details={'record_count': record_count, 'source_type': self.source_type}
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component='EXTRACTOR',
                message='Extraction failed',
                run_id=self.run_id,
                details={'error': str(e)}
            )
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract data from database source."""
        jdbc_config = self.config['source']['database']
        
        query = f"SELECT * FROM {jdbc_config['table']}"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", f"({query}) as source") \
            .option("user", jdbc_config.get('user', '')) \
            .option("password", jdbc_config.get('password', '')) \
            .option("driver", jdbc_config.get('driver', 'org.postgresql.Driver')) \
            .load()
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract data from staging area."""
        staging_path = self.config['source']['staging']['path']
        file_format = self.config['source']['staging'].get('format', 'parquet')
        
        df = self.spark.read \
            .format(file_format) \
            .load(staging_path) \
            .filter(f"run_id = '{self.run_id}' AND status = 'READY'")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last run."""
        jdbc_config = self.config['source']['database']
        
        # Get last successful run timestamp
        last_run_time = self._get_last_run_time()
        
        query = f"""
            SELECT * FROM {jdbc_config['table']}
            WHERE changed_at > '{last_run_time}'
        """
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", f"({query}) as source") \
            .option("user", jdbc_config.get('user', '')) \
            .option("password", jdbc_config.get('password', '')) \
            .load()
        
        return df
    
    def _extract_from_file(self) -> DataFrame:
        """Extract data from file source."""
        file_config = self.config['source']['file']
        path = file_config['path']
        file_format = file_config.get('format', 'csv')
        
        reader = self.spark.read.format(file_format)
        
        if file_format == 'csv':
            reader = reader \
                .option("header", file_config.get('header', True)) \
                .option("inferSchema", file_config.get('infer_schema', True))
        
        df = reader.load(path)
        
        return df
    
    def _get_last_run_time(self) -> str:
        """Get timestamp of last successful run."""
        jdbc_config = self.config['source']['database']
        
        query = """
            SELECT MAX(end_time) as last_run
            FROM etl_run_log
            WHERE status = 'SUCCESS'
        """
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", f"({query}) as last_run") \
            .option("user", jdbc_config.get('user', '')) \
            .option("password", jdbc_config.get('password', '')) \
            .load()
        
        last_run = df.collect()[0]['last_run']
        return last_run if last_run else '1970-01-01 00:00:00'