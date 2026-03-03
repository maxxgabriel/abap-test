"""
Data extraction module for ETL pipeline.
Handles extraction from various source systems including databases, files, and APIs.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, 
    TimestampType, IntegerType
)
from src.logger import ETLLogger


class DataExtractor:
    """
    Extracts data from various source systems.
    Supports full, incremental, and filtered extraction modes.
    """
    
    SOURCE_SCHEMA = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("value", DecimalType(15, 2), False),
        StructField("status", StringType(), False),
        StructField("category", StringType(), False),
        StructField("source_system", StringType(), True),
        StructField("created_at", TimestampType(), False),
        StructField("created_by", StringType(), False),
        StructField("changed_at", TimestampType(), True),
        StructField("changed_by", StringType(), True)
    ])
    
    def __init__(
        self, 
        spark: SparkSession,
        config: Dict[str, Any],
        run_id: str
    ):
        """
        Initialize extractor.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
        self.source_type = config.get("source_type", "database")
        
    def extract_data(
        self,
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data based on source type.
        
        Args:
            filter_condition: Optional filter SQL condition
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}"
        )
        
        try:
            if self.source_type == "DATABASE":
                df = self._extract_from_database(filter_condition)
            elif self.source_type == "STAGING":
                df = self._extract_from_staging()
            elif self.source_type == "INCREMENTAL":
                df = self._extract_incremental()
            elif self.source_type == "FILE":
                df = self._extract_from_file()
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
    
    def _extract_from_database(
        self,
        filter_condition: Optional[str] = None
    ) -> DataFrame:
        """Extract from database table."""
        jdbc_config = self.config.get("jdbc", {})
        table_name = jdbc_config.get("source_table", "etl_source_data")
        
        # Build query
        query = f"SELECT * FROM {table_name}"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        query += " LIMIT 10000"  # Safety limit
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config.get("url")) \
            .option("dbtable", f"({query}) AS source") \
            .option("user", jdbc_config.get("user")) \
            .option("password", jdbc_config.get("password")) \
            .option("driver", jdbc_config.get("driver", "org.postgresql.Driver")) \
            .load()
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract from staging area."""
        staging_path = self.config.get("staging_path", "/data/staging")
        
        df = self.spark.read \
            .schema(self.SOURCE_SCHEMA) \
            .parquet(f"{staging_path}/run_id={self.run_id}")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last successful run."""
        # Get last successful run timestamp
        last_run_time = self._get_last_run_time()
        
        jdbc_config = self.config.get("jdbc", {})
        table_name = jdbc_config.get("source_table", "etl_source_data")
        
        query = f"""
            SELECT * FROM {table_name}
            WHERE changed_at > '{last_run_time.isoformat()}'
            ORDER BY changed_at
        """
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config.get("url")) \
            .option("dbtable", f"({query}) AS source") \
            .option("user", jdbc_config.get("user")) \
            .option("password", jdbc_config.get("password")) \
            .load()
        
        return df
    
    def _extract_from_file(self) -> DataFrame:
        """Extract from file source."""
        file_config = self.config.get("file", {})
        file_path = file_config.get("path")
        file_format = file_config.get("format", "csv")
        
        reader = self.spark.read.schema(self.SOURCE_SCHEMA)
        
        if file_format == "csv":
            df = reader \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .csv(file_path)
        elif file_format == "parquet":
            df = reader.parquet(file_path)
        elif file_format == "json":
            df = reader.json(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        return df
    
    def _get_last_run_time(self) -> datetime:
        """Get timestamp of last successful run."""
        jdbc_config = self.config.get("jdbc", {})
        
        query = """
            SELECT MAX(end_time) as last_run
            FROM etl_run_log
            WHERE status = 'SUCCESS'
        """
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config.get("url")) \
            .option("dbtable", f"({query}) AS runs") \
            .option("user", jdbc_config.get("user")) \
            .option("password", jdbc_config.get("password")) \
            .load()
        
        result = df.first()
        if result and result["last_run"]:
            return result["last_run"]
        else:
            # Default to beginning of time
            return datetime(2000, 1, 1)