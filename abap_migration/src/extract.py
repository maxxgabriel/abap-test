"""
Extract module for ETL pipeline
Handles data extraction from various sources with incremental load support
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, TimestampType
)
from datetime import datetime
from typing import Optional, Dict
import logging

from src.logger import ETLLogger


class ETLExtractor:
    """
    Data extraction class supporting multiple source types
    and incremental loading strategies
    """
    
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
        StructField("changed_by", StringType(), True)
    ])
    
    def __init__(
        self, 
        spark: SparkSession,
        source_type: str = "DATABASE",
        run_id: str = None
    ):
        """
        Initialize extractor
        
        Args:
            spark: SparkSession instance
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            run_id: Unique run identifier
        """
        self.spark = spark
        self.source_type = source_type.upper()
        self.run_id = run_id or f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.logger = ETLLogger.get_instance()
        
    def extract_data(
        self,
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Main extraction method routing to appropriate extractor
        
        Args:
            filter_condition: Optional filter SQL condition
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Starting extraction - Source: {self.source_type}"
        )
        
        try:
            if self.source_type == "DATABASE":
                df = self.extract_from_database(filter_condition)
            elif self.source_type == "STAGING":
                df = self.extract_from_staging(self.run_id)
            elif self.source_type == "INCREMENTAL":
                df = self.extract_incremental()
            else:
                self.logger.log_warning(
                    component="EXTRACTOR",
                    message=f"Unknown source type: {self.source_type}, defaulting to DATABASE"
                )
                df = self.extract_from_database(filter_condition)
            
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
    
    def extract_from_database(
        self,
        filter_condition: Optional[str] = None
    ) -> DataFrame:
        """
        Extract from database source
        
        Args:
            filter_condition: Optional SQL WHERE clause
            
        Returns:
            DataFrame with source data
        """
        query = "(SELECT * FROM etl_source_data"
        
        if filter_condition:
            query += f" WHERE {filter_condition}"
        
        query += " LIMIT 1000) as source"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", self.spark.conf.get("spark.etl.source.jdbc.url")) \
            .option("dbtable", query) \
            .option("user", self.spark.conf.get("spark.etl.source.jdbc.user")) \
            .option("password", self.spark.conf.get("spark.etl.source.jdbc.password")) \
            .option("driver", self.spark.conf.get("spark.etl.source.jdbc.driver")) \
            .load()
        
        return df
    
    def extract_from_staging(self, run_id: str) -> DataFrame:
        """
        Extract from staging area
        
        Args:
            run_id: Run identifier for staged data
            
        Returns:
            DataFrame with staged data
        """
        query = f"""(
            SELECT * FROM etl_staging 
            WHERE run_id = '{run_id}' 
            AND status = 'READY'
        ) as staging"""
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", self.spark.conf.get("spark.etl.source.jdbc.url")) \
            .option("dbtable", query) \
            .option("user", self.spark.conf.get("spark.etl.source.jdbc.user")) \
            .option("password", self.spark.conf.get("spark.etl.source.jdbc.password")) \
            .option("driver", self.spark.conf.get("spark.etl.source.jdbc.driver")) \
            .load()
        
        return df
    
    def extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run
        
        Returns:
            DataFrame with incremental data
        """
        # Get last successful run timestamp
        last_run_query = """(
            SELECT MAX(end_time) as last_run 
            FROM etl_run_log 
            WHERE status = 'SUCCESS'
        ) as last_run"""
        
        last_run_df = self.spark.read \
            .format("jdbc") \
            .option("url", self.spark.conf.get("spark.etl.source.jdbc.url")) \
            .option("dbtable", last_run_query) \
            .option("user", self.spark.conf.get("spark.etl.source.jdbc.user")) \
            .option("password", self.spark.conf.get("spark.etl.source.jdbc.password")) \
            .option("driver", self.spark.conf.get("spark.etl.source.jdbc.driver")) \
            .load()
        
        last_run_time = last_run_df.first()["last_run"]
        
        if last_run_time:
            query = f"""(
                SELECT * FROM etl_source_data 
                WHERE changed_at > TIMESTAMP '{last_run_time}'
            ) as incremental"""
        else:
            # First run - extract all
            query = "(SELECT * FROM etl_source_data) as incremental"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", self.spark.conf.get("spark.etl.source.jdbc.url")) \
            .option("dbtable", query) \
            .option("user", self.spark.conf.get("spark.etl.source.jdbc.user")) \
            .option("password", self.spark.conf.get("spark.etl.source.jdbc.password")) \
            .option("driver", self.spark.conf.get("spark.etl.source.jdbc.driver")) \
            .load()
        
        return df
    
    def get_source_schema(self) -> StructType:
        """Get the expected source schema"""
        return self.SOURCE_SCHEMA