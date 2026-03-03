"""
ETL Extractor Module
Handles data extraction from various sources
"""
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime

from src.utils.logger import ETLLogger


class ETLExtractor:
    """Extracts data from various sources"""
    
    def __init__(self, spark: SparkSession, config: dict, run_id: str):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger(run_id=run_id)
    
    def extract_data(
        self,
        source_type: str = "DATABASE",
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data from source
        
        Args:
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            filter_condition: Optional SQL filter condition
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Starting extraction - Source: {source_type}"
        )
        
        if source_type == "DATABASE":
            df = self._extract_from_database(filter_condition)
        elif source_type == "STAGING":
            df = self._extract_from_staging()
        elif source_type == "INCREMENTAL":
            df = self._extract_incremental()
        else:
            df = self._extract_from_database(filter_condition)
        
        if df is not None and max_records > 0:
            df = df.limit(max_records)
        
        count = df.count() if df is not None else 0
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Extracted {count} records"
        )
        
        return df
    
    def _extract_from_database(self, filter_condition: Optional[str]) -> DataFrame:
        """Extract from database source"""
        query = f"SELECT * FROM {self.config['database']['source_table']}"
        
        if filter_condition:
            query += f" WHERE {filter_condition}"
        
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.config["database"]["url"]) \
            .option("query", query) \
            .option("user", self.config["database"]["user"]) \
            .option("password", self.config["database"]["password"]) \
            .load()
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract from staging area"""
        return self.spark.read \
            .format("parquet") \
            .load(self.config["staging"]["path"])
    
    def _extract_incremental(self) -> DataFrame:
        """Extract incremental data based on last run"""
        # Get last successful run time
        last_run_time = self._get_last_run_time()
        
        query = f"""
        SELECT * FROM {self.config['database']['source_table']}
        WHERE changed_at > '{last_run_time}'
        """
        
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.config["database"]["url"]) \
            .option("query", query) \
            .option("user", self.config["database"]["user"]) \
            .option("password", self.config["database"]["password"]) \
            .load()
    
    def _get_last_run_time(self) -> str:
        """Get timestamp of last successful run"""
        try:
            query = f"""
            SELECT MAX(end_time) as last_run
            FROM {self.config['database']['run_log_table']}
            WHERE status = 'SUCCESS'
            """
            
            result = self.spark.read \
                .format("jdbc") \
                .option("url", self.config["database"]["url"]) \
                .option("query", query) \
                .option("user", self.config["database"]["user"]) \
                .option("password", self.config["database"]["password"]) \
                .load()
            
            last_run = result.collect()[0]["last_run"]
            return last_run.strftime("%Y-%m-%d %H:%M:%S") if last_run else "1900-01-01 00:00:00"
            
        except Exception as e:
            self.logger.log_warning(
                component="EXTRACTOR",
                message="Could not determine last run time, using default",
                details=str(e)
            )
            return "1900-01-01 00:00:00"