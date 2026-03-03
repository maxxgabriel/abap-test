"""
PySpark ETL Extractor Module
Migrated from ABAP zcl_etl_extractor
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from typing import Optional
import yaml
from datetime import datetime
from src.logger import ETLLogger


class ETLExtractor:
    """Extract data from various sources"""
    
    def __init__(self, spark: SparkSession, config: dict, run_id: str, source_type: str = "DATABASE"):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.source_type = source_type
        self.logger = ETLLogger.get_instance()
        
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
    
    def extract_data(self, filter_expr: Optional[str] = None, max_records: int = 0) -> DataFrame:
        """Main extraction method"""
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Starting extraction - Source: {self.source_type}"
        )
        
        try:
            if self.source_type == "DATABASE":
                df = self.extract_from_database(filter_expr)
            elif self.source_type == "STAGING":
                df = self.extract_from_staging(self.run_id)
            elif self.source_type == "INCREMENTAL":
                df = self.extract_incremental()
            else:
                df = self.extract_from_database(filter_expr)
            
            # Apply max records limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.log_info(
                component="EXTRACTOR",
                message=f"Extracted {record_count} records"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="EXTRACTOR",
                message="Extraction failed",
                details=str(e)
            )
            raise
    
    def extract_from_database(self, filter_expr: Optional[str] = None) -> DataFrame:
        """Extract from source database table"""
        jdbc_config = self.config['source']['jdbc']
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", jdbc_config['table']) \
            .option("user", jdbc_config['user']) \
            .option("password", jdbc_config['password']) \
            .option("driver", jdbc_config['driver']) \
            .load()
        
        # Apply filter if provided
        if filter_expr:
            df = df.filter(f"status = '{filter_expr}'")
        
        return df.limit(1000)
    
    def extract_from_staging(self, run_id: str) -> DataFrame:
        """Extract from staging table"""
        jdbc_config = self.config['source']['jdbc']
        
        query = f"""
            (SELECT * FROM {jdbc_config['staging_table']} 
             WHERE run_id = '{run_id}' AND status = 'READY') AS staging
        """
        
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
        """Extract only changed records since last run"""
        jdbc_config = self.config['source']['jdbc']
        
        # Get last successful run time
        query = f"""
            (SELECT COALESCE(MAX(end_time), '1900-01-01 00:00:00') as last_run
             FROM {jdbc_config['log_table']} 
             WHERE status = 'SUCCESS') AS last_run
        """
        
        last_run_df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", query) \
            .option("user", jdbc_config['user']) \
            .option("password", jdbc_config['password']) \
            .option("driver", jdbc_config['driver']) \
            .load()
        
        last_run_time = last_run_df.first()['last_run']
        
        # Extract incremental data
        incremental_query = f"""
            (SELECT * FROM {jdbc_config['table']} 
             WHERE changed_at > '{last_run_time}') AS incremental
        """
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", incremental_query) \
            .option("user", jdbc_config['user']) \
            .option("password", jdbc_config['password']) \
            .option("driver", jdbc_config['driver']) \
            .load()
        
        return df