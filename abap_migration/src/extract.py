"""
ETL Extractor - Handles data extraction from various sources
"""
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType

from src.logger import ETLLogger
from src.exceptions import ETLExtractionError


class ETLExtractor:
    """Extracts data from various sources"""
    
    def __init__(
        self,
        spark: SparkSession,
        source_type: str = "DATABASE",
        run_id: str = "",
        logger: Optional[ETLLogger] = None
    ):
        self.spark = spark
        self.source_type = source_type
        self.run_id = run_id
        self.logger = logger or ETLLogger()
        
    def extract_data(
        self,
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """Extract data based on source type"""
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
            raise ETLExtractionError(f"Extraction failed: {str(e)}") from e
    
    def _extract_from_database(self, filter_condition: Optional[str]) -> DataFrame:
        """Extract from database table"""
        query = "SELECT * FROM etl_source_data"
        
        if filter_condition:
            query += f" WHERE {filter_condition}"
        
        return self.spark.sql(query)
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract from staging area"""
        return self.spark.table("etl_staging").filter(
            f"run_id = '{self.run_id}' AND status = 'READY'"
        )
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records"""
        # Get last successful run time
        last_run_df = self.spark.sql("""
            SELECT MAX(end_time) as last_run_time
            FROM etl_run_log
            WHERE status = 'SUCCESS'
        """)
        
        last_run_time = last_run_df.first().last_run_time
        
        if last_run_time:
            return self.spark.table("etl_source_data").filter(
                f"changed_at > '{last_run_time}'"
            )
        else:
            return self._extract_from_database(None)