"""ETL Data Extraction Module"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import Optional, Dict, Any
from datetime import datetime
import logging


class ETLExtractor:
    """Extract data from various sources"""
    
    def __init__(self, spark: SparkSession, run_id: str, source_type: str = "DATABASE"):
        self.spark = spark
        self.run_id = run_id
        self.source_type = source_type
        self.logger = logging.getLogger(__name__)
        
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
        """Main extraction method"""
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
        query = "(SELECT * FROM etl_source_data) AS source"
        
        if filter_condition:
            query = f"(SELECT * FROM etl_source_data WHERE status = '{filter_condition}') AS source"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/etl_db") \
            .option("dbtable", query) \
            .option("user", "etl_user") \
            .option("password", "etl_password") \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract from staging area"""
        df = self.spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/etl_db") \
            .option("dbtable", f"(SELECT * FROM etl_staging WHERE run_id = '{self.run_id}' AND status = 'READY') AS staging") \
            .option("user", "etl_user") \
            .option("password", "etl_password") \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last run"""
        # Get last successful run time
        last_run_query = """
        (SELECT MAX(end_time) as last_run 
         FROM etl_run_log 
         WHERE status = 'SUCCESS') AS last_run
        """
        
        last_run_df = self.spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/etl_db") \
            .option("dbtable", last_run_query) \
            .option("user", "etl_user") \
            .option("password", "etl_password") \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        last_run_time = last_run_df.collect()[0]["last_run"]
        
        if last_run_time:
            query = f"""
            (SELECT * FROM etl_source_data 
             WHERE changed_at > '{last_run_time}') AS incremental
            """
        else:
            query = "(SELECT * FROM etl_source_data) AS incremental"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/etl_db") \
            .option("dbtable", query) \
            .option("user", "etl_user") \
            .option("password", "etl_password") \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        return df