"""
ETL Extractor Module
Extracts data from various sources (database, staging, incremental)
Migrated from zcl_etl_extractor.abap
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from typing import Optional
import logging
from datetime import datetime


class ETLExtractor:
    """Extracts data from various sources for ETL processing"""
    
    def __init__(self, spark: SparkSession, source_type: str = "DATABASE", run_id: str = ""):
        """
        Initialize extractor
        
        Args:
            spark: SparkSession instance
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            run_id: Unique run identifier
        """
        self.spark = spark
        self.source_type = source_type if source_type else "DATABASE"
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
            StructField("changed_by", StringType(), True)
        ])
    
    def extract_data(self, filter_value: Optional[str] = None, max_records: int = 0) -> DataFrame:
        """
        Main extraction method
        
        Args:
            filter_value: Optional filter criteria
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.info(f"Starting extraction - Source: {self.source_type}")
        
        try:
            # Route to appropriate extraction method
            if self.source_type == "DATABASE":
                df = self.extract_from_database(filter_value)
            elif self.source_type == "STAGING":
                df = self.extract_from_staging(self.run_id)
            elif self.source_type == "INCREMENTAL":
                last_run_time = self._get_last_run_time()
                if last_run_time:
                    df = self.extract_incremental(last_run_time)
                else:
                    df = self.extract_from_database(filter_value)
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
        # Read from source table (adjust connection details as needed)
        df = self.spark.read \
            .format("jdbc") \
            .option("url", self._get_jdbc_url()) \
            .option("dbtable", "zetl_source_data") \
            .option("driver", self._get_jdbc_driver()) \
            .schema(self.get_source_schema()) \
            .load()
        
        # Apply filter if provided
        if filter_value:
            df = df.filter(col("status") == lit(filter_value))
        
        return df.limit(1000)
    
    def extract_from_staging(self, run_id: str) -> DataFrame:
        """
        Extract from staging area
        
        Args:
            run_id: Run identifier for staging data
            
        Returns:
            DataFrame with staged data
        """
        # Read from staging table
        df = self.spark.read \
            .format("jdbc") \
            .option("url", self._get_jdbc_url()) \
            .option("dbtable", "zetl_staging") \
            .option("driver", self._get_jdbc_driver()) \
            .load()
        
        # Filter by run_id and status
        df = df.filter(
            (col("run_id") == lit(run_id)) & 
            (col("status") == lit("READY"))
        )
        
        # Project to source schema
        return df.select(
            col("id"),
            lit("STAGED").alias("status")
        )
    
    def extract_incremental(self, last_run_time: datetime) -> DataFrame:
        """
        Extract only changed records since last run
        
        Args:
            last_run_time: Timestamp of last successful run
            
        Returns:
            DataFrame with incremental data
        """
        df = self.spark.read \
            .format("jdbc") \
            .option("url", self._get_jdbc_url()) \
            .option("dbtable", "zetl_source_data") \
            .option("driver", self._get_jdbc_driver()) \
            .schema(self.get_source_schema()) \
            .load()
        
        # Filter by changed_at timestamp
        return df.filter(col("changed_at") > lit(last_run_time))
    
    def _get_last_run_time(self) -> Optional[datetime]:
        """Get timestamp of last successful run"""
        try:
            df = self.spark.read \
                .format("jdbc") \
                .option("url", self._get_jdbc_url()) \
                .option("dbtable", "zetl_run_log") \
                .option("driver", self._get_jdbc_driver()) \
                .load()
            
            last_run = df.filter(col("status") == lit("SUCCESS")) \
                .orderBy(col("end_time").desc()) \
                .select("end_time") \
                .first()
            
            return last_run["end_time"] if last_run else None
            
        except Exception as e:
            self.logger.warning(f"Could not retrieve last run time: {str(e)}")
            return None
    
    def _get_jdbc_url(self) -> str:
        """Get JDBC connection URL from config"""
        # Load from config - placeholder
        return "jdbc:postgresql://localhost:5432/etl_db"
    
    def _get_jdbc_driver(self) -> str:
        """Get JDBC driver class"""
        return "org.postgresql.Driver"