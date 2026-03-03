"""
PySpark Data Extraction Module with Incremental Load Support
Implements DataFrame-based extraction with filtering logic for delta processing
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from pyspark.sql import functions as F
from datetime import datetime
from typing import Optional, Dict, Any
import logging


class SparkExtractor:
    """
    Handles data extraction from various sources with incremental load support
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize extractor with Spark session and configuration
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def get_source_schema(self) -> StructType:
        """
        Define schema for source data
        
        Returns:
            StructType schema definition
        """
        return StructType([
            StructField("id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("value", DecimalType(15, 2), nullable=False),
            StructField("status", StringType(), nullable=True),
            StructField("category", StringType(), nullable=True),
            StructField("source_system", StringType(), nullable=True),
            StructField("created_at", TimestampType(), nullable=True),
            StructField("created_by", StringType(), nullable=True),
            StructField("changed_at", TimestampType(), nullable=True),
            StructField("changed_by", StringType(), nullable=True)
        ])
    
    def extract_data(
        self,
        source_type: str = "database",
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Main extraction method routing to appropriate source handler
        
        Args:
            source_type: Type of source (database, staging, incremental)
            filter_condition: Optional filter string
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.info(f"Starting extraction - Source: {source_type}, Run ID: {self.run_id}")
        
        if source_type.lower() == "database":
            df = self._extract_from_database(filter_condition)
        elif source_type.lower() == "staging":
            df = self._extract_from_staging()
        elif source_type.lower() == "incremental":
            df = self._extract_incremental()
        else:
            self.logger.warning(f"Unknown source type: {source_type}, defaulting to database")
            df = self._extract_from_database(filter_condition)
        
        # Apply max records limit if specified
        if max_records > 0:
            df = df.limit(max_records)
        
        record_count = df.count()
        self.logger.info(f"Extracted {record_count} records")
        
        return df
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract data from database source
        
        Args:
            filter_condition: SQL WHERE clause condition
            
        Returns:
            DataFrame with database records
        """
        self.logger.info("Extracting from database source")
        
        jdbc_config = self.config.get("database", {})
        table_name = jdbc_config.get("source_table", "etl_source_data")
        
        # Build query
        query = f"(SELECT * FROM {table_name}"
        
        if filter_condition:
            query += f" WHERE {filter_condition}"
        
        query += ") as source_data"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config.get("url")) \
            .option("dbtable", query) \
            .option("user", jdbc_config.get("user")) \
            .option("password", jdbc_config.get("password")) \
            .option("driver", jdbc_config.get("driver", "org.postgresql.Driver")) \
            .load()
        
        # Ensure schema compliance
        df = self._apply_schema_transformations(df)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """
        Extract data from staging area
        
        Returns:
            DataFrame with staging records
        """
        self.logger.info(f"Extracting from staging - Run ID: {self.run_id}")
        
        staging_config = self.config.get("staging", {})
        staging_path = staging_config.get("path", "/data/staging")
        
        # Read from staging path (could be parquet, CSV, etc.)
        file_format = staging_config.get("format", "parquet")
        
        df = self.spark.read \
            .format(file_format) \
            .option("header", "true") \
            .load(f"{staging_path}/run_id={self.run_id}")
        
        # Filter for ready status
        df = df.filter(F.col("status") == "READY")
        
        df = self._apply_schema_transformations(df)
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run
        
        Returns:
            DataFrame with incremental records
        """
        self.logger.info("Extracting incremental data")
        
        # Get last successful run timestamp
        last_run_time = self._get_last_run_timestamp()
        
        if last_run_time is None:
            self.logger.warning("No previous run found, performing full extract")
            return self._extract_from_database()
        
        self.logger.info(f"Extracting records changed after: {last_run_time}")
        
        jdbc_config = self.config.get("database", {})
        table_name = jdbc_config.get("source_table", "etl_source_data")
        
        # Query for changed records
        query = f"""(
            SELECT * FROM {table_name}
            WHERE changed_at > '{last_run_time}'
        ) as incremental_data"""
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config.get("url")) \
            .option("dbtable", query) \
            .option("user", jdbc_config.get("user")) \
            .option("password", jdbc_config.get("password")) \
            .option("driver", jdbc_config.get("driver", "org.postgresql.Driver")) \
            .load()
        
        df = self._apply_schema_transformations(df)
        
        return df
    
    def _get_last_run_timestamp(self) -> Optional[str]:
        """
        Retrieve timestamp of last successful ETL run
        
        Returns:
            Timestamp string or None if no previous runs
        """
        jdbc_config = self.config.get("database", {})
        
        try:
            run_log_df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_config.get("url")) \
                .option("dbtable", "(SELECT MAX(end_time) as last_run FROM etl_run_log WHERE status = 'SUCCESS') as log") \
                .option("user", jdbc_config.get("user")) \
                .option("password", jdbc_config.get("password")) \
                .option("driver", jdbc_config.get("driver")) \
                .load()
            
            last_run = run_log_df.first()
            
            if last_run and last_run["last_run"]:
                return last_run["last_run"]
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error retrieving last run timestamp: {str(e)}")
            return None
    
    def _apply_schema_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply schema transformations and type conversions
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with correct schema
        """
        # Cast columns to correct types
        df = df.withColumn("value", F.col("value").cast(DecimalType(15, 2))) \
               .withColumn("created_at", F.col("created_at").cast(TimestampType())) \
               .withColumn("changed_at", F.col("changed_at").cast(TimestampType()))
        
        # Handle nulls and defaults
        df = df.fillna({
            "status": "UNKNOWN",
            "category": "UNCATEGORIZED",
            "source_system": "UNKNOWN"
        })
        
        return df