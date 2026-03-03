"""
PySpark Data Extraction Module with Incremental Load Support
Provides DataFrame-based extraction with filtering and data type conversion
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from pyspark.sql import functions as F
from typing import Optional, Dict, Any
from datetime import datetime
import logging

from src.logger import ETLLogger
from src.config import ETLConfig


class ETLExtractor:
    """
    Extract data from source systems with support for full and incremental loads
    """
    
    def __init__(self, spark: SparkSession, config: ETLConfig, run_id: str):
        """
        Initialize extractor
        
        Args:
            spark: Active SparkSession
            config: ETL configuration object
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger.get_logger("EXTRACTOR")
        
    def get_source_schema(self) -> StructType:
        """
        Define source data schema
        
        Returns:
            StructType schema definition
        """
        return StructType([
            StructField("id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("value", DecimalType(15, 2), nullable=True),
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
        source_type: str = "DATABASE",
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Main extraction method with routing logic
        
        Args:
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            filter_condition: Optional SQL filter condition
            max_records: Maximum records to extract (0 = no limit)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.info(
            f"Starting extraction - Source: {source_type}, Run ID: {self.run_id}"
        )
        
        try:
            # Route to appropriate extraction method
            if source_type == "DATABASE":
                df = self._extract_from_database(filter_condition)
            elif source_type == "STAGING":
                df = self._extract_from_staging()
            elif source_type == "INCREMENTAL":
                df = self._extract_incremental()
            else:
                self.logger.warning(f"Unknown source type '{source_type}', defaulting to DATABASE")
                df = self._extract_from_database(filter_condition)
            
            # Apply record limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}", exc_info=True)
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract data from database source
        
        Args:
            filter_condition: Optional WHERE clause filter
            
        Returns:
            DataFrame with database data
        """
        self.logger.info("Extracting from database")
        
        source_config = self.config.get_source_config()
        
        # Build read options
        read_options = {
            "url": source_config["jdbc_url"],
            "dbtable": source_config["table_name"],
            "user": source_config.get("username", ""),
            "password": source_config.get("password", ""),
            "driver": source_config.get("driver", "org.postgresql.Driver")
        }
        
        # Apply filter if specified
        if filter_condition:
            query = f"(SELECT * FROM {source_config['table_name']} WHERE {filter_condition}) AS filtered_data"
            read_options["dbtable"] = query
        
        # Read from database
        df = self.spark.read \
            .format("jdbc") \
            .options(**read_options) \
            .load()
        
        # Apply schema validation and type conversion
        df = self._apply_schema_conversion(df)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """
        Extract data from staging area
        
        Returns:
            DataFrame with staging data
        """
        self.logger.info(f"Extracting from staging - Run ID: {self.run_id}")
        
        staging_config = self.config.get_staging_config()
        
        # Read from staging location
        df = self.spark.read \
            .format(staging_config.get("format", "parquet")) \
            .load(staging_config["path"])
        
        # Filter by run_id and status
        df = df.filter(
            (F.col("run_id") == self.run_id) & 
            (F.col("status") == "READY")
        )
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run
        
        Returns:
            DataFrame with incremental data
        """
        self.logger.info("Extracting incremental data")
        
        # Get last successful run timestamp
        last_run_time = self._get_last_run_timestamp()
        
        if last_run_time is None:
            self.logger.warning("No previous successful run found, performing full extraction")
            return self._extract_from_database()
        
        self.logger.info(f"Extracting records changed after {last_run_time}")
        
        source_config = self.config.get_source_config()
        
        # Query for changed records
        query = f"""
            (SELECT * FROM {source_config['table_name']} 
             WHERE changed_at > '{last_run_time}') AS incremental_data
        """
        
        read_options = {
            "url": source_config["jdbc_url"],
            "dbtable": query,
            "user": source_config.get("username", ""),
            "password": source_config.get("password", ""),
            "driver": source_config.get("driver", "org.postgresql.Driver")
        }
        
        df = self.spark.read \
            .format("jdbc") \
            .options(**read_options) \
            .load()
        
        df = self._apply_schema_conversion(df)
        
        return df
    
    def _apply_schema_conversion(self, df: DataFrame) -> DataFrame:
        """
        Apply schema conversion and data type validation
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with converted schema
        """
        target_schema = self.get_source_schema()
        
        # Convert columns to match target schema
        for field in target_schema.fields:
            if field.name in df.columns:
                df = df.withColumn(
                    field.name,
                    F.col(field.name).cast(field.dataType)
                )
        
        # Add missing columns with null values
        for field in target_schema.fields:
            if field.name not in df.columns:
                df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
        
        # Select only schema columns in correct order
        df = df.select([field.name for field in target_schema.fields])
        
        return df
    
    def _get_last_run_timestamp(self) -> Optional[str]:
        """
        Retrieve timestamp of last successful ETL run
        
        Returns:
            Timestamp string or None if no previous run
        """
        try:
            run_log_config = self.config.get_run_log_config()
            
            # Read run log
            df = self.spark.read \
                .format("jdbc") \
                .options(
                    url=run_log_config["jdbc_url"],
                    dbtable=run_log_config["table_name"],
                    user=run_log_config.get("username", ""),
                    password=run_log_config.get("password", ""),
                    driver=run_log_config.get("driver", "org.postgresql.Driver")
                ) \
                .load()
            
            # Get last successful run
            last_run = df.filter(F.col("status") == "SUCCESS") \
                .orderBy(F.col("end_time").desc()) \
                .select("end_time") \
                .first()
            
            if last_run:
                return last_run["end_time"].isoformat()
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Could not retrieve last run timestamp: {str(e)}")
            return None
    
    def extract_with_metadata(
        self,
        source_type: str = "DATABASE",
        filter_condition: Optional[str] = None
    ) -> DataFrame:
        """
        Extract data with additional extraction metadata
        
        Args:
            source_type: Type of source
            filter_condition: Optional filter
            
        Returns:
            DataFrame with extraction metadata columns
        """
        df = self.extract_data(source_type, filter_condition)
        
        # Add extraction metadata
        df = df.withColumn("extraction_timestamp", F.current_timestamp()) \
            .withColumn("extraction_run_id", F.lit(self.run_id)) \
            .withColumn("extraction_source", F.lit(source_type))
        
        return df