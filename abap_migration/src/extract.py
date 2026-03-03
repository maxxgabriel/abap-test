"""Data extraction module for ETL pipeline."""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional
import logging


class ETLExtractor:
    """Handles data extraction from various sources."""
    
    SCHEMA = StructType([
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
    
    def __init__(self, spark: SparkSession, source_type: str, run_id: str):
        """
        Initialize extractor.
        
        Args:
            spark: SparkSession instance
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            run_id: Unique run identifier
        """
        self.spark = spark
        self.source_type = source_type.upper()
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
    
    def extract_data(
        self, 
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data based on source type.
        
        Args:
            filter_condition: SQL WHERE clause filter
            max_records: Maximum records to extract (0 for unlimited)
            
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
            self.logger.warning(f"Unknown source type: {self.source_type}, defaulting to DATABASE")
            df = self._extract_from_database(filter_condition)
        
        # Apply max records limit
        if max_records > 0:
            df = df.limit(max_records)
        
        record_count = df.count()
        self.logger.info(f"Extracted {record_count} records")
        
        return df
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract data from source database table."""
        self.logger.info("Extracting from database")
        
        query = "(SELECT * FROM zetl_source_data"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        query += " LIMIT 1000) AS source_data"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", self.spark.conf.get("spark.jdbc.url")) \
            .option("dbtable", query) \
            .option("user", self.spark.conf.get("spark.jdbc.user")) \
            .option("password", self.spark.conf.get("spark.jdbc.password")) \
            .option("driver", self.spark.conf.get("spark.jdbc.driver")) \
            .load()
        
        return df.select(
            "id", "name", "value", "status", "category",
            "source_system", "created_at", "created_by",
            "changed_at", "changed_by"
        )
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract data from staging area."""
        self.logger.info(f"Extracting from staging - Run ID: {self.run_id}")
        
        query = f"""(
            SELECT id, parsed_data 
            FROM zetl_staging 
            WHERE run_id = '{self.run_id}' AND status = 'READY'
        ) AS staging_data"""
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", self.spark.conf.get("spark.jdbc.url")) \
            .option("dbtable", query) \
            .option("user", self.spark.conf.get("spark.jdbc.user")) \
            .option("password", self.spark.conf.get("spark.jdbc.password")) \
            .option("driver", self.spark.conf.get("spark.jdbc.driver")) \
            .load()
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last successful run."""
        self.logger.info("Extracting incremental data")
        
        # Get last successful run timestamp
        last_run_query = """(
            SELECT MAX(end_time) as last_run 
            FROM zetl_run_log 
            WHERE status = 'SUCCESS'
        ) AS last_run"""
        
        last_run_df = self.spark.read \
            .format("jdbc") \
            .option("url", self.spark.conf.get("spark.jdbc.url")) \
            .option("dbtable", last_run_query) \
            .option("user", self.spark.conf.get("spark.jdbc.user")) \
            .option("password", self.spark.conf.get("spark.jdbc.password")) \
            .option("driver", self.spark.conf.get("spark.jdbc.driver")) \
            .load()
        
        last_run_time = last_run_df.first()["last_run"]
        
        if last_run_time:
            query = f"""(
                SELECT * FROM zetl_source_data 
                WHERE changed_at > '{last_run_time}'
            ) AS incremental_data"""
        else:
            # First run - extract all
            query = "(SELECT * FROM zetl_source_data LIMIT 1000) AS incremental_data"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", self.spark.conf.get("spark.jdbc.url")) \
            .option("dbtable", query) \
            .option("user", self.spark.conf.get("spark.jdbc.user")) \
            .option("password", self.spark.conf.get("spark.jdbc.password")) \
            .option("driver", self.spark.conf.get("spark.jdbc.driver")) \
            .load()
        
        return df.select(
            "id", "name", "value", "status", "category",
            "source_system", "created_at", "created_by",
            "changed_at", "changed_by"
        )