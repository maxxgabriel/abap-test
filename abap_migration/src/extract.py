"""
Data Extraction Module
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from pyspark.sql.functions import col, lit
from typing import Optional
from datetime import datetime

from src.logger import ETLLogger
from src.config import Config


def get_source_schema() -> StructType:
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


class DataExtractor:
    """Handles data extraction from various sources"""
    
    def __init__(
        self,
        spark: SparkSession,
        source_type: str = "DATABASE",
        run_id: str = None,
        config: Config = None
    ):
        self.spark = spark
        self.source_type = source_type
        self.run_id = run_id or datetime.now().strftime("%Y%m%d%H%M%S")
        self.config = config or Config()
        self.logger = ETLLogger.get_instance()
    
    def extract_data(
        self,
        filter_expr: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data based on source type
        
        Args:
            filter_expr: SQL filter expression
            max_records: Maximum records to extract (0 = unlimited)
        
        Returns:
            Extracted DataFrame
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Starting extraction - Source: {self.source_type}"
        )
        
        try:
            if self.source_type == "DATABASE":
                df = self._extract_from_database(filter_expr)
            elif self.source_type == "STAGING":
                df = self._extract_from_staging()
            elif self.source_type == "INCREMENTAL":
                df = self._extract_incremental()
            else:
                df = self._extract_from_database(filter_expr)
            
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
    
    def _extract_from_database(self, filter_expr: Optional[str] = None) -> DataFrame:
        """Extract from database source"""
        source_path = self.config.get_source_path()
        
        df = self.spark.read \
            .format(self.config.get("SOURCE_FORMAT", "delta")) \
            .load(source_path)
        
        if filter_expr:
            df = df.filter(filter_expr)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract from staging area"""
        staging_path = self.config.get("STAGING_PATH")
        
        df = self.spark.read \
            .format("delta") \
            .load(staging_path) \
            .filter(col("run_id") == self.run_id) \
            .filter(col("status") == "READY")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract incremental changes"""
        source_path = self.config.get_source_path()
        
        # Get last successful run time
        last_run_time = self._get_last_run_time()
        
        df = self.spark.read \
            .format("delta") \
            .load(source_path)
        
        if last_run_time:
            df = df.filter(col("changed_at") > lit(last_run_time))
        
        return df
    
    def _get_last_run_time(self) -> Optional[datetime]:
        """Get timestamp of last successful run"""
        try:
            run_log_path = self.config.get("RUN_LOG_PATH")
            
            last_run = self.spark.read \
                .format("delta") \
                .load(run_log_path) \
                .filter(col("status") == "SUCCESS") \
                .orderBy(col("end_time").desc()) \
                .limit(1) \
                .select("end_time") \
                .collect()
            
            if last_run:
                return last_run[0]["end_time"]
            
        except Exception:
            pass
        
        return None