"""
ETL Extraction Module
Handles data extraction from various sources
"""
from typing import Optional, Dict, Any
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType

from src.utils.logger import ETLLogger


class ETLExtractor:
    """
    Handles data extraction from various sources
    Supports database, file, and incremental extraction
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Dict[str, Any],
        source_type: str = "DATABASE",
        run_id: str = ""
    ):
        """
        Initialize extractor
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.source_type = source_type
        self.run_id = run_id
        self.logger = ETLLogger(run_id=run_id)
        
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Extractor initialized - Source: {source_type}"
        )
    
    def extract_data(
        self,
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data from configured source
        
        Args:
            filter_condition: Optional SQL filter condition
            max_records: Maximum records to extract (0 for unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Starting extraction - Source: {self.source_type}"
        )
        
        try:
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
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract data from database"""
        source_table = self.config.get("source_table", "etl_source_data")
        
        # Define source schema
        schema = StructType([
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
        
        # Read from database
        df = self.spark.read \
            .format(self.config.get("db_format", "jdbc")) \
            .option("url", self.config["db_url"]) \
            .option("dbtable", source_table) \
            .option("user", self.config["db_user"]) \
            .option("password", self.config["db_password"]) \
            .option("driver", self.config.get("db_driver", "org.postgresql.Driver")) \
            .load()
        
        # Apply filter if provided
        if filter_condition:
            df = df.filter(filter_condition)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract data from staging area"""
        staging_table = self.config.get("staging_table", "etl_staging")
        
        df = self.spark.read \
            .format(self.config.get("db_format", "jdbc")) \
            .option("url", self.config["db_url"]) \
            .option("dbtable", staging_table) \
            .option("user", self.config["db_user"]) \
            .option("password", self.config["db_password"]) \
            .load() \
            .filter(f"run_id = '{self.run_id}' AND status = 'READY'")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed data since last successful run"""
        # Get last successful run time
        last_run_time = self._get_last_run_time()
        
        if last_run_time is None:
            self.logger.log_warning(
                component="EXTRACTOR",
                message="No previous run found, performing full extraction"
            )
            return self._extract_from_database()
        
        # Extract only changed records
        df = self._extract_from_database(
            filter_condition=f"changed_at > '{last_run_time}'"
        )
        
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Incremental extraction since {last_run_time}"
        )
        
        return df
    
    def _get_last_run_time(self) -> Optional[datetime]:
        """Get timestamp of last successful run"""
        try:
            run_log_table = self.config.get("run_log_table", "etl_run_log")
            
            df_last_run = self.spark.read \
                .format(self.config.get("db_format", "jdbc")) \
                .option("url", self.config["db_url"]) \
                .option("dbtable", run_log_table) \
                .option("user", self.config["db_user"]) \
                .option("password", self.config["db_password"]) \
                .load() \
                .filter("status = 'SUCCESS'") \
                .orderBy("end_time", ascending=False) \
                .limit(1)
            
            if df_last_run.count() > 0:
                return df_last_run.first()["end_time"]
            
            return None
            
        except Exception as e:
            self.logger.log_warning(
                component="EXTRACTOR",
                message=f"Failed to get last run time: {str(e)}"
            )
            return None