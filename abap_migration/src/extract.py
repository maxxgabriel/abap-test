"""
PySpark Data Extraction Module with Incremental Load Support
Extracts data from source systems with filtering logic for delta processing.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, 
    TimestampType, IntegerType
)
from pyspark.sql import functions as F
from typing import Optional, Dict, Any
from datetime import datetime
import logging


class DataExtractor:
    """Handles data extraction with support for full and incremental loads."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the data extractor.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
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
    
    def extract_data(
        self,
        source_type: str = "database",
        filter_expr: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data based on source type.
        
        Args:
            source_type: Type of source (database, staging, incremental)
            filter_expr: Optional filter expression
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame containing extracted data
        """
        self.logger.info(
            f"Starting extraction - Source: {source_type}, Run ID: {self.run_id}"
        )
        
        try:
            if source_type.lower() == "database":
                df = self._extract_from_database(filter_expr)
            elif source_type.lower() == "staging":
                df = self._extract_from_staging()
            elif source_type.lower() == "incremental":
                df = self._extract_incremental()
            else:
                self.logger.warning(f"Unknown source type: {source_type}, defaulting to database")
                df = self._extract_from_database(filter_expr)
            
            # Apply max records limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _extract_from_database(self, filter_expr: Optional[str] = None) -> DataFrame:
        """
        Extract data from database source.
        
        Args:
            filter_expr: Optional SQL WHERE clause filter
            
        Returns:
            DataFrame with extracted data
        """
        source_config = self.config["sources"]["database"]
        
        # Build JDBC options
        jdbc_options = {
            "url": source_config["jdbc_url"],
            "dbtable": source_config["table"],
            "user": source_config.get("user", ""),
            "password": source_config.get("password", ""),
            "driver": source_config.get("driver", "org.postgresql.Driver")
        }
        
        # Add filter if provided
        if filter_expr:
            jdbc_options["dbtable"] = f"(SELECT * FROM {source_config['table']} WHERE {filter_expr}) AS filtered_data"
        
        self.logger.info(f"Reading from database: {source_config['table']}")
        
        df = self.spark.read \
            .format("jdbc") \
            .options(**jdbc_options) \
            .load()
        
        # Ensure schema compliance
        df = self._apply_schema_conversion(df)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """
        Extract data from staging area.
        
        Returns:
            DataFrame with staged data
        """
        staging_config = self.config["sources"]["staging"]
        staging_path = staging_config["path"]
        
        self.logger.info(f"Reading from staging: {staging_path}")
        
        df = self.spark.read \
            .format(staging_config.get("format", "parquet")) \
            .schema(self.get_source_schema()) \
            .load(staging_path)
        
        # Filter by run_id if applicable
        if "run_id" in df.columns:
            df = df.filter(F.col("run_id") == self.run_id)
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """
        Extract only records changed since last successful run.
        
        Returns:
            DataFrame with incremental data
        """
        self.logger.info("Performing incremental extraction")
        
        # Get last successful run timestamp
        last_run_time = self._get_last_successful_run_time()
        
        if last_run_time is None:
            self.logger.warning("No previous successful run found, performing full extraction")
            return self._extract_from_database()
        
        self.logger.info(f"Extracting records changed after: {last_run_time}")
        
        # Extract with timestamp filter
        filter_expr = f"changed_at > '{last_run_time}'"
        return self._extract_from_database(filter_expr)
    
    def _get_last_successful_run_time(self) -> Optional[datetime]:
        """
        Retrieve the timestamp of the last successful ETL run.
        
        Returns:
            Timestamp of last successful run or None
        """
        try:
            run_log_config = self.config["metadata"]["run_log"]
            
            run_log_df = self.spark.read \
                .format("jdbc") \
                .option("url", run_log_config["jdbc_url"]) \
                .option("dbtable", run_log_config["table"]) \
                .option("user", run_log_config.get("user", "")) \
                .option("password", run_log_config.get("password", "")) \
                .load()
            
            # Get most recent successful run
            last_run = run_log_df \
                .filter(F.col("status") == "SUCCESS") \
                .orderBy(F.col("end_time").desc()) \
                .select("end_time") \
                .first()
            
            return last_run["end_time"] if last_run else None
            
        except Exception as e:
            self.logger.warning(f"Could not retrieve last run time: {str(e)}")
            return None
    
    def _apply_schema_conversion(self, df: DataFrame) -> DataFrame:
        """
        Apply data type conversions to ensure schema compliance.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with converted schema
        """
        # Convert string timestamps to TimestampType if needed
        if "created_at" in df.columns and df.schema["created_at"].dataType != TimestampType():
            df = df.withColumn("created_at", F.to_timestamp("created_at"))
        
        if "changed_at" in df.columns and df.schema["changed_at"].dataType != TimestampType():
            df = df.withColumn("changed_at", F.to_timestamp("changed_at"))
        
        # Convert decimal values
        if "value" in df.columns:
            df = df.withColumn("value", F.col("value").cast(DecimalType(15, 2)))
        
        return df
    
    def extract_with_partitioning(
        self,
        partition_column: str = "created_at",
        num_partitions: int = 10
    ) -> DataFrame:
        """
        Extract data with partitioning for better performance.
        
        Args:
            partition_column: Column to partition by
            num_partitions: Number of partitions
            
        Returns:
            Partitioned DataFrame
        """
        df = self.extract_data()
        
        return df.repartition(num_partitions, partition_column)


def create_extractor(
    spark: SparkSession,
    config: Dict[str, Any],
    run_id: str
) -> DataExtractor:
    """
    Factory function to create a DataExtractor instance.
    
    Args:
        spark: Active SparkSession
        config: Configuration dictionary
        run_id: Run identifier
        
    Returns:
        Configured DataExtractor instance
    """
    return DataExtractor(spark, config, run_id)