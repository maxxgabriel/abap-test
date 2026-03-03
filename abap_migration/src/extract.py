"""
ETL Extractor module.
Handles data extraction from various sources.
"""

from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType

from src.utils.logger import ETLLogger
from src.utils.config import ConfigManager


class ETLExtractor:
    """
    Data extraction class.
    Converts ABAP zcl_etl_extractor class to PySpark.
    """
    
    # Define source data schema
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
        config: ConfigManager,
        source_type: str = "DATABASE",
        run_id: str = ""
    ):
        self.spark = spark
        self.config = config
        self.source_type = source_type
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
        
    def extract_data(
        self,
        filter_expr: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Main extraction method.
        Replaces extract_data ABAP method.
        """
        self.logger.log_info(
            "EXTRACTOR",
            f"Starting extraction - Source: {self.source_type}"
        )
        
        try:
            # Route to appropriate extraction method
            if self.source_type == "DATABASE":
                df = self._extract_from_database(filter_expr)
            elif self.source_type == "STAGING":
                df = self._extract_from_staging(self.run_id)
            elif self.source_type == "INCREMENTAL":
                df = self._extract_incremental()
            else:
                df = self._extract_from_database(filter_expr)
            
            # Apply max records limit
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.log_info(
                "EXTRACTOR",
                f"Extracted {record_count} records"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                "EXTRACTOR",
                "Extraction failed",
                details=str(e)
            )
            raise
    
    def _extract_from_database(self, filter_expr: Optional[str]) -> DataFrame:
        """Extract from source database table."""
        db_config = self.config.get_database_config("source")
        table_name = db_config.get("table", "etl_source_data")
        
        # Read from database
        df = (self.spark.read
              .format("jdbc")
              .option("url", db_config["url"])
              .option("dbtable", table_name)
              .option("user", db_config.get("user", ""))
              .option("password", db_config.get("password", ""))
              .option("driver", db_config.get("driver", "org.postgresql.Driver"))
              .load())
        
        # Apply filter if provided
        if filter_expr:
            df = df.filter(filter_expr)
        
        return df
    
    def _extract_from_staging(self, run_id: str) -> DataFrame:
        """Extract from staging area."""
        staging_path = self.config.get("staging.path", "/data/staging")
        
        df = (self.spark.read
              .format("parquet")
              .load(f"{staging_path}/{run_id}")
              .filter("status = 'READY'"))
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last run."""
        # Get last successful run time
        monitor_table = self.config.get("monitoring.table", "etl_run_log")
        
        last_run_df = (self.spark.read
                       .format("jdbc")
                       .option("url", self.config.get_database_config("monitoring")["url"])
                       .option("dbtable", monitor_table)
                       .load()
                       .filter("status = 'SUCCESS'")
                       .orderBy("end_time", ascending=False)
                       .limit(1))
        
        if last_run_df.count() == 0:
            # No previous run, do full extract
            return self._extract_from_database(None)
        
        last_run_time = last_run_df.select("end_time").first()[0]
        
        # Extract changed records
        db_config = self.config.get_database_config("source")
        table_name = db_config.get("table", "etl_source_data")
        
        df = (self.spark.read
              .format("jdbc")
              .option("url", db_config["url"])
              .option("dbtable", table_name)
              .load()
              .filter(f"changed_at > '{last_run_time}'"))
        
        return df