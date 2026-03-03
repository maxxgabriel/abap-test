"""
ETL Data Extraction Module
Extracts data from various sources with comprehensive error handling
"""
import logging
from typing import List, Dict, Optional
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, 
    TimestampType, IntegerType
)
from pyspark.sql import functions as F
import yaml


class ETLExtractor:
    """Handles data extraction from various sources"""
    
    SCHEMA = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("value", DecimalType(15, 2), False),
        StructField("status", StringType(), True),
        StructField("category", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("created_by", StringType(), True),
        StructField("changed_at", TimestampType(), True),
        StructField("changed_by", StringType(), True)
    ])
    
    def __init__(self, spark: SparkSession, config: Dict, run_id: str):
        """
        Initialize extractor
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def extract_data(
        self, 
        source_type: str = "DATABASE",
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data based on source type
        
        Args:
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            filter_condition: Optional filter SQL condition
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
            
        Raises:
            ValueError: If source type is invalid
            RuntimeError: If extraction fails
        """
        self.logger.info(
            f"Starting extraction - Source: {source_type}, "
            f"Run ID: {self.run_id}"
        )
        
        try:
            if source_type.upper() == "DATABASE":
                df = self._extract_from_database(filter_condition)
            elif source_type.upper() == "STAGING":
                df = self._extract_from_staging()
            elif source_type.upper() == "INCREMENTAL":
                df = self._extract_incremental()
            else:
                df = self._extract_from_database(filter_condition)
                
            # Apply max records limit
            if max_records > 0:
                df = df.limit(max_records)
                
            record_count = df.count()
            self.logger.info(f"Extracted {record_count} records")
            
            # Add extraction metadata
            df = df.withColumn("extraction_timestamp", F.current_timestamp())
            df = df.withColumn("extraction_run_id", F.lit(self.run_id))
            
            return df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise RuntimeError(f"Data extraction failed: {str(e)}") from e
    
    def _extract_from_database(
        self, 
        filter_condition: Optional[str] = None
    ) -> DataFrame:
        """
        Extract from database source
        
        Args:
            filter_condition: Optional SQL WHERE clause
            
        Returns:
            DataFrame with database data
        """
        source_config = self.config['source']['database']
        
        # Build read options
        read_options = {
            "url": source_config['jdbc_url'],
            "dbtable": source_config['table'],
            "user": source_config.get('user', ''),
            "password": source_config.get('password', ''),
            "driver": source_config.get('driver', 'org.postgresql.Driver')
        }
        
        # Apply filter if provided
        if filter_condition:
            read_options["dbtable"] = (
                f"(SELECT * FROM {source_config['table']} "
                f"WHERE {filter_condition}) AS filtered"
            )
        
        df = self.spark.read.format("jdbc").options(**read_options).load()
        
        # Ensure schema compliance
        return self._align_schema(df)
    
    def _extract_from_staging(self) -> DataFrame:
        """
        Extract from staging area
        
        Returns:
            DataFrame with staging data
        """
        staging_config = self.config['source']['staging']
        staging_path = staging_config['path']
        
        df = self.spark.read \
            .format(staging_config.get('format', 'parquet')) \
            .load(f"{staging_path}/run_id={self.run_id}")
        
        return self._align_schema(df)
    
    def _extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run
        
        Returns:
            DataFrame with incremental data
        """
        # Get last successful run timestamp
        last_run_time = self._get_last_run_time()
        
        if last_run_time:
            filter_condition = f"changed_at > '{last_run_time}'"
            self.logger.info(
                f"Incremental extraction from {last_run_time}"
            )
            return self._extract_from_database(filter_condition)
        else:
            self.logger.warning(
                "No previous run found, performing full extraction"
            )
            return self._extract_from_database()
    
    def _get_last_run_time(self) -> Optional[str]:
        """
        Get timestamp of last successful run
        
        Returns:
            Timestamp string or None if no previous run
        """
        try:
            log_config = self.config.get('metadata', {}).get('run_log', {})
            
            if not log_config:
                return None
            
            df = self.spark.read \
                .format("jdbc") \
                .options(**log_config) \
                .load()
            
            last_run = df.filter(F.col("status") == "SUCCESS") \
                .orderBy(F.col("end_time").desc()) \
                .select("end_time") \
                .first()
            
            if last_run:
                return last_run['end_time'].isoformat()
            
            return None
            
        except Exception as e:
            self.logger.warning(
                f"Could not retrieve last run time: {str(e)}"
            )
            return None
    
    def _align_schema(self, df: DataFrame) -> DataFrame:
        """
        Align DataFrame to expected schema
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with aligned schema
        """
        # Select and cast columns to match schema
        select_exprs = []
        
        for field in self.SCHEMA.fields:
            if field.name in df.columns:
                select_exprs.append(
                    F.col(field.name).cast(field.dataType).alias(field.name)
                )
            else:
                # Add null for missing columns
                select_exprs.append(F.lit(None).cast(field.dataType).alias(field.name))
        
        return df.select(*select_exprs)