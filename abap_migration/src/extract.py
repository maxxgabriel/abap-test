"""
ETL Extract Module - Data Extraction from various sources
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional
import logging
import yaml


class ETLExtractor:
    """Handles data extraction from multiple sources"""
    
    def __init__(self, spark: SparkSession, config: dict, run_id: str):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def get_source_schema(self) -> StructType:
        """Define schema for source data"""
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
        source_type: str = "DATABASE",
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """Main extraction method"""
        self.logger.info(f"Starting extraction - Source: {source_type}, Run ID: {self.run_id}")
        
        try:
            if source_type == "DATABASE":
                df = self.extract_from_database(filter_condition)
            elif source_type == "STAGING":
                df = self.extract_from_staging()
            elif source_type == "INCREMENTAL":
                df = self.extract_incremental()
            else:
                df = self.extract_from_database(filter_condition)
            
            # Apply max records limit
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract from database source"""
        source_config = self.config['sources']['database']
        
        df = self.spark.read \
            .format(source_config['format']) \
            .option("url", source_config['url']) \
            .option("dbtable", source_config['table']) \
            .option("user", source_config.get('user', '')) \
            .option("password", source_config.get('password', '')) \
            .load()
        
        if filter_condition:
            df = df.filter(filter_condition)
        
        return df
    
    def extract_from_staging(self) -> DataFrame:
        """Extract from staging area"""
        staging_path = self.config['sources']['staging']['path']
        
        df = self.spark.read \
            .format(self.config['sources']['staging']['format']) \
            .schema(self.get_source_schema()) \
            .option("header", "true") \
            .load(f"{staging_path}/{self.run_id}")
        
        return df.filter("status = 'READY'")
    
    def extract_incremental(self) -> DataFrame:
        """Extract only changed records since last run"""
        last_run_time = self._get_last_successful_run_time()
        
        if last_run_time:
            self.logger.info(f"Extracting incremental data since {last_run_time}")
            df = self.extract_from_database()
            return df.filter(f"changed_at > '{last_run_time}'")
        else:
            self.logger.warning("No previous run found, performing full extraction")
            return self.extract_from_database()
    
    def _get_last_successful_run_time(self) -> Optional[str]:
        """Get timestamp of last successful run"""
        try:
            log_path = self.config['monitoring']['run_log_path']
            df = self.spark.read.parquet(log_path)
            
            last_run = df.filter("status = 'SUCCESS'") \
                .orderBy("end_time", ascending=False) \
                .limit(1) \
                .collect()
            
            if last_run:
                return last_run[0]['end_time']
        except Exception as e:
            self.logger.warning(f"Could not retrieve last run time: {str(e)}")
        
        return None