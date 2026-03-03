"""
ETL Data Extraction Module
Handles extraction from various data sources
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional
from src.logger import ETLLogger


class ETLExtractor:
    """Data extraction component"""
    
    def __init__(self, spark: SparkSession, config: dict, run_id: str):
        """
        Initialize extractor
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger(run_id)
    
    def get_source_schema(self) -> StructType:
        """
        Define source data schema
        
        Returns:
            StructType schema
        """
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
        
        try:
            if source_type == "DATABASE":
                df = self._extract_from_database(filter_condition)
            elif source_type == "STAGING":
                df = self._extract_from_staging()
            elif source_type == "INCREMENTAL":
                df = self._extract_incremental()
            else:
                df = self._extract_from_database(filter_condition)
            
            # Apply max records limit
            if max_records > 0:
                df = df.limit(max_records)
            
            count = df.count()
            self.logger.log_info(
                component="EXTRACTOR",
                message=f"Extracted {count} records"
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
        """Extract from source database table"""
        table_name = self.config['tables']['source']
        
        df = self.spark.table(table_name)
        
        if filter_condition:
            df = df.filter(filter_condition)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract from staging area"""
        staging_table = self.config['tables']['staging']
        
        df = self.spark.table(staging_table) \
            .filter(f"run_id = '{self.run_id}' AND status = 'READY'")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last run"""
        # Get last successful run time
        run_log_table = self.config['tables']['run_log']
        
        last_run = self.spark.sql(f"""
            SELECT MAX(end_time) as last_run_time
            FROM {run_log_table}
            WHERE status = 'SUCCESS'
        """).first()
        
        if last_run and last_run['last_run_time']:
            last_run_time = last_run['last_run_time']
            
            source_table = self.config['tables']['source']
            df = self.spark.table(source_table) \
                .filter(f"changed_at > timestamp'{last_run_time}'")
        else:
            # No previous run, extract all
            df = self._extract_from_database()
        
        return df