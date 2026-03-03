"""
PySpark ETL Extractor Module
Migrated from zcl_etl_extractor.abap
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from typing import Optional
import logging
from datetime import datetime


class ETLExtractor:
    """Extract data from various sources for ETL processing."""
    
    def __init__(self, source_type: str = "DATABASE", run_id: str = None, spark: SparkSession = None):
        """
        Initialize ETL Extractor.
        
        Args:
            source_type: Type of data source (DATABASE, STAGING, INCREMENTAL)
            run_id: Unique run identifier
            spark: SparkSession instance
        """
        self.source_type = source_type
        self.run_id = run_id or self._generate_run_id()
        self.spark = spark or SparkSession.builder.getOrCreate()
        self.logger = logging.getLogger(__name__)
        
    def _generate_run_id(self) -> str:
        """Generate a unique run ID."""
        return f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def get_source_schema(self) -> StructType:
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
            StructField("changed_by", StringType(), True),
        ])
    
    def extract_data(self, filter_condition: Optional[str] = None, max_records: int = 0) -> DataFrame:
        """
        Extract data based on source type.
        
        Args:
            filter_condition: SQL filter condition
            max_records: Maximum number of records to extract (0 = no limit)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.info(f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}")
        
        try:
            if self.source_type == "DATABASE":
                df = self.extract_from_database(filter_condition)
            elif self.source_type == "STAGING":
                df = self.extract_from_staging(self.run_id)
            elif self.source_type == "INCREMENTAL":
                last_run_time = self._get_last_successful_run_time()
                if last_run_time:
                    df = self.extract_incremental(last_run_time)
                else:
                    df = self.extract_from_database(filter_condition)
            else:
                df = self.extract_from_database(filter_condition)
            
            # Apply max records limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract from source database table.
        
        Args:
            filter_condition: SQL WHERE clause condition
            
        Returns:
            DataFrame with source data
        """
        from pyspark.sql import functions as F
        
        # Read from source table (adjust path/connection as needed)
        df = self.spark.read \
            .format("jdbc") \
            .option("url", self._get_jdbc_url()) \
            .option("dbtable", "zetl_source_data") \
            .option("driver", self._get_jdbc_driver()) \
            .schema(self.get_source_schema()) \
            .load()
        
        # Apply filter if provided
        if filter_condition:
            df = df.filter(filter_condition)
        
        return df
    
    def extract_from_staging(self, run_id: str) -> DataFrame:
        """
        Extract from staging table.
        
        Args:
            run_id: Run identifier for staged data
            
        Returns:
            DataFrame with staged data
        """
        staging_schema = StructType([
            StructField("id", StringType(), False),
            StructField("run_id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("raw_data", StringType(), True),
        ])
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", self._get_jdbc_url()) \
            .option("dbtable", "zetl_staging") \
            .option("driver", self._get_jdbc_driver()) \
            .schema(staging_schema) \
            .load() \
            .filter(f"run_id = '{run_id}' AND status = 'READY'")
        
        return df
    
    def extract_incremental(self, last_run_time: datetime) -> DataFrame:
        """
        Extract only changed records since last run.
        
        Args:
            last_run_time: Timestamp of last successful run
            
        Returns:
            DataFrame with incremental data
        """
        from pyspark.sql import functions as F
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", self._get_jdbc_url()) \
            .option("dbtable", "zetl_source_data") \
            .option("driver", self._get_jdbc_driver()) \
            .schema(self.get_source_schema()) \
            .load() \
            .filter(F.col("changed_at") > F.lit(last_run_time))
        
        return df
    
    def _get_last_successful_run_time(self) -> Optional[datetime]:
        """Get timestamp of last successful ETL run."""
        try:
            run_log_df = self.spark.read \
                .format("jdbc") \
                .option("url", self._get_jdbc_url()) \
                .option("dbtable", "zetl_run_log") \
                .option("driver", self._get_jdbc_driver()) \
                .load() \
                .filter("status = 'SUCCESS'") \
                .orderBy("end_time", ascending=False) \
                .limit(1)
            
            if run_log_df.count() > 0:
                return run_log_df.select("end_time").first()[0]
            return None
        except Exception as e:
            self.logger.warning(f"Could not retrieve last run time: {str(e)}")
            return None
    
    def _get_jdbc_url(self) -> str:
        """Get JDBC connection URL from config."""
        # This should come from config - placeholder for now
        return "jdbc:postgresql://localhost:5432/etl_db"
    
    def _get_jdbc_driver(self) -> str:
        """Get JDBC driver class name."""
        return "org.postgresql.Driver"