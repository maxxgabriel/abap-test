"""
Data Extraction Module
Handles extraction from various data sources
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from typing import Optional, Dict, Any
import logging


class Extractor:
    """Handles data extraction from various sources"""
    
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
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize Extractor
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def extract_data(
        self,
        source_type: str = "database",
        filter_clause: Optional[str] = None,
        max_records: int = 0
    ) -> Optional[DataFrame]:
        """
        Extract data from configured source
        
        Args:
            source_type: Type of source (database, staging, incremental)
            filter_clause: Optional SQL filter clause
            max_records: Maximum records to extract (0 for unlimited)
            
        Returns:
            DataFrame with extracted data or None
        """
        self.logger.info(f"Starting extraction - Source: {source_type}")
        
        try:
            if source_type == "database":
                df = self._extract_from_database(filter_clause)
            elif source_type == "staging":
                df = self._extract_from_staging()
            elif source_type == "incremental":
                df = self._extract_incremental(filter_clause)
            else:
                df = self._extract_from_database(filter_clause)
            
            if df is None:
                return None
            
            # Apply max records limit
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}", exc_info=True)
            raise
    
    def _extract_from_database(self, filter_clause: Optional[str] = None) -> DataFrame:
        """Extract from database source"""
        source_config = self.config.get("source", {})
        
        # Build JDBC connection properties
        jdbc_url = source_config.get("jdbc_url")
        table_name = source_config.get("table_name", "etl_source_data")
        
        connection_props = {
            "user": source_config.get("user"),
            "password": source_config.get("password"),
            "driver": source_config.get("driver", "org.postgresql.Driver")
        }
        
        # Read from database
        df = self.spark.read.jdbc(
            url=jdbc_url,
            table=table_name,
            properties=connection_props
        )
        
        # Apply filter if provided
        if filter_clause:
            df = df.filter(filter_clause)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract from staging area"""
        staging_config = self.config.get("staging", {})
        staging_path = staging_config.get("path")
        
        # Read from staging (assuming Parquet format)
        df = self.spark.read.parquet(staging_path)
        
        # Filter for current run
        df = df.filter(f"run_id = '{self.run_id}' AND status = 'READY'")
        
        return df
    
    def _extract_incremental(self, filter_clause: Optional[str] = None) -> DataFrame:
        """Extract incremental changes"""
        source_config = self.config.get("source", {})
        
        jdbc_url = source_config.get("jdbc_url")
        table_name = source_config.get("table_name", "etl_source_data")
        
        connection_props = {
            "user": source_config.get("user"),
            "password": source_config.get("password"),
            "driver": source_config.get("driver", "org.postgresql.Driver")
        }
        
        # Read with incremental filter
        df = self.spark.read.jdbc(
            url=jdbc_url,
            table=table_name,
            properties=connection_props
        )
        
        # Apply incremental filter
        if filter_clause:
            df = df.filter(filter_clause)
        else:
            # Default: extract changes from last 24 hours
            df = df.filter("changed_at > current_timestamp() - interval 1 day")
        
        return df