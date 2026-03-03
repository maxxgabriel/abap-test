"""
PySpark ETL Extraction Module
Handles database-to-DataFrame conversion, incremental load filtering, and data type transformations.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from pyspark.sql import functions as F
from typing import Optional, Dict, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class ETLExtractor:
    """Extract data from various sources with support for incremental loading."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the extractor.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.source_type = config.get('source_type', 'database')
        
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
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data based on source type.
        
        Args:
            filter_condition: SQL WHERE clause filter
            max_records: Maximum number of records to extract (0 = unlimited)
            
        Returns:
            DataFrame containing extracted data
        """
        logger.info(f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}")
        
        try:
            if self.source_type.upper() == 'DATABASE':
                df = self.extract_from_database(filter_condition)
            elif self.source_type.upper() == 'STAGING':
                df = self.extract_from_staging()
            elif self.source_type.upper() == 'INCREMENTAL':
                df = self.extract_incremental()
            else:
                df = self.extract_from_database(filter_condition)
            
            # Apply record limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract data from database table.
        
        Args:
            filter_condition: SQL WHERE clause filter
            
        Returns:
            DataFrame with extracted data
        """
        jdbc_config = self.config.get('jdbc', {})
        table_name = self.config.get('source_table', 'etl_source_data')
        
        # Build JDBC URL
        jdbc_url = (
            f"jdbc:{jdbc_config.get('driver', 'postgresql')}://"
            f"{jdbc_config.get('host', 'localhost')}:"
            f"{jdbc_config.get('port', '5432')}/"
            f"{jdbc_config.get('database', 'etl_db')}"
        )
        
        connection_properties = {
            "user": jdbc_config.get('user', 'etl_user'),
            "password": jdbc_config.get('password', ''),
            "driver": jdbc_config.get('driver_class', 'org.postgresql.Driver')
        }
        
        # Build query
        query = f"(SELECT * FROM {table_name}"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        query += " LIMIT 1000) AS source_data"
        
        # Read from database
        df = self.spark.read.jdbc(
            url=jdbc_url,
            table=query,
            properties=connection_properties
        )
        
        # Ensure schema compliance
        df = self._apply_schema_conversions(df)
        
        return df
    
    def extract_from_staging(self) -> DataFrame:
        """
        Extract data from staging area.
        
        Returns:
            DataFrame with staged data
        """
        staging_path = self.config.get('staging_path', '/data/staging')
        file_format = self.config.get('staging_format', 'parquet')
        
        df = self.spark.read.format(file_format).load(staging_path)
        
        # Filter by run_id and status
        df = df.filter(
            (F.col("run_id") == self.run_id) & 
            (F.col("status") == "READY")
        )
        
        return self._apply_schema_conversions(df)
    
    def extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run.
        
        Returns:
            DataFrame with incremental data
        """
        # Get last successful run timestamp
        last_run_time = self._get_last_successful_run_time()
        
        if last_run_time:
            logger.info(f"Extracting incremental data since: {last_run_time}")
            filter_condition = f"changed_at > '{last_run_time}'"
            return self.extract_from_database(filter_condition)
        else:
            logger.warning("No previous successful run found, performing full extraction")
            return self.extract_from_database()
    
    def _get_last_successful_run_time(self) -> Optional[datetime]:
        """
        Get timestamp of last successful ETL run.
        
        Returns:
            Timestamp of last successful run or None
        """
        try:
            run_log_path = self.config.get('run_log_path', '/data/run_log')
            
            if self.spark._jsc.sc().textFile(run_log_path).isEmpty():
                return None
            
            run_log_df = self.spark.read.parquet(run_log_path)
            
            last_run = run_log_df.filter(
                F.col("status") == "SUCCESS"
            ).orderBy(
                F.col("end_time").desc()
            ).limit(1).collect()
            
            if last_run:
                return last_run[0]['end_time']
            
            return None
            
        except Exception as e:
            logger.warning(f"Could not retrieve last run time: {str(e)}")
            return None
    
    def _apply_schema_conversions(self, df: DataFrame) -> DataFrame:
        """
        Apply necessary data type conversions to match expected schema.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with corrected schema
        """
        expected_schema = self.get_source_schema()
        
        for field in expected_schema.fields:
            if field.name in df.columns:
                df = df.withColumn(
                    field.name,
                    F.col(field.name).cast(field.dataType)
                )
        
        return df


def create_extractor(config: Dict[str, Any], run_id: str) -> ETLExtractor:
    """
    Factory function to create an ETLExtractor instance.
    
    Args:
        config: Configuration dictionary
        run_id: Unique run identifier
        
    Returns:
        Configured ETLExtractor instance
    """
    spark = SparkSession.builder.getOrCreate()
    return ETLExtractor(spark, config, run_id)