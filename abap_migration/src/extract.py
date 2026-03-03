"""
PySpark ETL Data Extractor Module
Extracts data from various sources with support for incremental and full loads.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from typing import Optional, Dict, Any
from datetime import datetime
import yaml
from src.logger import ETLLogger


class DataExtractor:
    """Handles data extraction from multiple source types."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the DataExtractor.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
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
        source_type: Optional[str] = None,
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data based on source type.
        
        Args:
            source_type: Type of source ('DATABASE', 'STAGING', 'INCREMENTAL')
            filter_condition: Optional filter condition
            max_records: Maximum number of records to extract (0 for unlimited)
            
        Returns:
            DataFrame containing extracted data
        """
        source_type = source_type or self.config['extraction']['source_type']
        
        self.logger.log_info(
            component='EXTRACTOR',
            message=f"Starting extraction - Source: {source_type}, Run ID: {self.run_id}"
        )
        
        try:
            if source_type == 'DATABASE':
                df = self._extract_from_database(filter_condition)
            elif source_type == 'STAGING':
                df = self._extract_from_staging()
            elif source_type == 'INCREMENTAL':
                df = self._extract_incremental()
            else:
                df = self._extract_from_database(filter_condition)
            
            # Apply max records limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.log_info(
                component='EXTRACTOR',
                message=f"Extracted {record_count} records"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component='EXTRACTOR',
                message='Extraction failed',
                details=str(e)
            )
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract data from database source."""
        source_config = self.config['extraction']['database']
        
        df = (self.spark.read
              .format(source_config['format'])
              .option("url", source_config['url'])
              .option("dbtable", source_config['table'])
              .option("user", source_config['user'])
              .option("password", source_config['password'])
              .option("driver", source_config['driver'])
              .load())
        
        # Apply filter if provided
        if filter_condition:
            df = df.filter(filter_condition)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract data from staging area."""
        staging_config = self.config['extraction']['staging']
        
        df = (self.spark.read
              .format(staging_config['format'])
              .option("header", "true")
              .schema(self.source_schema)
              .load(staging_config['path']))
        
        # Filter by run_id and status
        df = df.filter(
            (df.run_id == self.run_id) & 
            (df.status == 'READY')
        )
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last successful run."""
        # Get last successful run timestamp
        last_run_time = self._get_last_run_time()
        
        source_config = self.config['extraction']['database']
        
        df = (self.spark.read
              .format(source_config['format'])
              .option("url", source_config['url'])
              .option("dbtable", source_config['table'])
              .option("user", source_config['user'])
              .option("password", source_config['password'])
              .option("driver", source_config['driver'])
              .load())
        
        # Filter records changed after last run
        if last_run_time:
            df = df.filter(df.changed_at > last_run_time)
            self.logger.log_info(
                component='EXTRACTOR',
                message=f"Incremental load from {last_run_time}"
            )
        else:
            self.logger.log_warning(
                component='EXTRACTOR',
                message="No previous run found, performing full load"
            )
        
        return df
    
    def _get_last_run_time(self) -> Optional[datetime]:
        """Retrieve the timestamp of the last successful run."""
        try:
            run_log_config = self.config['run_log']
            
            df = (self.spark.read
                  .format(run_log_config['format'])
                  .option("url", run_log_config['url'])
                  .option("dbtable", run_log_config['table'])
                  .option("user", run_log_config['user'])
                  .option("password", run_log_config['password'])
                  .option("driver", run_log_config['driver'])
                  .load())
            
            last_run = (df.filter(df.status == 'SUCCESS')
                       .orderBy(df.end_time.desc())
                       .select('end_time')
                       .first())
            
            return last_run['end_time'] if last_run else None
            
        except Exception as e:
            self.logger.log_warning(
                component='EXTRACTOR',
                message=f"Could not retrieve last run time: {str(e)}"
            )
            return None