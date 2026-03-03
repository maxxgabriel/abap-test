"""
PySpark Data Extraction Module with Incremental Load Support
Handles extraction from various source systems with filtering and delta processing
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional, Dict, Any
import logging


class DataExtractor:
    """Extract data from source systems with incremental load capabilities"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the data extractor
        
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
        """
        Define the schema for source data
        
        Returns:
            StructType schema definition
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
            StructField("changed_by", StringType(), True),
        ])
    
    def extract_data(
        self,
        source_type: str = "DATABASE",
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Main extraction method that routes to appropriate extraction logic
        
        Args:
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            filter_condition: Optional SQL filter condition
            max_records: Maximum number of records to extract (0 = no limit)
            
        Returns:
            Extracted DataFrame
        """
        self.logger.info(f"Starting extraction - Source: {source_type}, Run ID: {self.run_id}")
        
        if source_type == "DATABASE":
            df = self._extract_from_database(filter_condition)
        elif source_type == "STAGING":
            df = self._extract_from_staging()
        elif source_type == "INCREMENTAL":
            df = self._extract_incremental()
        else:
            self.logger.warning(f"Unknown source type {source_type}, defaulting to DATABASE")
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
            filter_condition: Optional SQL filter condition
            
        Returns:
            DataFrame with extracted data
        """
        source_config = self.config['source']['database']
        
        # Build JDBC URL
        jdbc_url = (
            f"jdbc:{source_config['type']}://"
            f"{source_config['host']}:{source_config['port']}/"
            f"{source_config['database']}"
        )
        
        # Base read options
        read_options = {
            "url": jdbc_url,
            "dbtable": source_config['table'],
            "user": source_config['user'],
            "password": source_config['password'],
            "driver": source_config['driver']
        }
        
        # Add filter if provided
        if filter_condition:
            query = f"(SELECT * FROM {source_config['table']} WHERE {filter_condition}) as filtered_data"
            read_options['dbtable'] = query
        
        self.logger.info(f"Reading from database: {source_config['table']}")
        
        df = self.spark.read.format("jdbc").options(**read_options).load()
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """
        Extract data from staging area
        
        Returns:
            DataFrame with staged data
        """
        staging_config = self.config['source']['staging']
        staging_path = staging_config['path']
        
        self.logger.info(f"Reading from staging: {staging_path}")
        
        # Read from staging (assuming parquet format)
        df = (self.spark.read
              .format(staging_config.get('format', 'parquet'))
              .load(f"{staging_path}/run_id={self.run_id}"))
        
        # Filter for ready records
        df = df.filter(df.status == 'READY')
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run
        
        Returns:
            DataFrame with incremental data
        """
        self.logger.info("Performing incremental extraction")
        
        # Get last successful run timestamp from run log
        last_run_time = self._get_last_run_time()
        
        if last_run_time is None:
            self.logger.warning("No previous run found, performing full extraction")
            return self._extract_from_database()
        
        self.logger.info(f"Extracting records changed after: {last_run_time}")
        
        source_config = self.config['source']['database']
        
        jdbc_url = (
            f"jdbc:{source_config['type']}://"
            f"{source_config['host']}:{source_config['port']}/"
            f"{source_config['database']}"
        )
        
        # Query only changed records
        query = (
            f"(SELECT * FROM {source_config['table']} "
            f"WHERE changed_at > TIMESTAMP '{last_run_time}') as incremental_data"
        )
        
        df = (self.spark.read
              .format("jdbc")
              .option("url", jdbc_url)
              .option("dbtable", query)
              .option("user", source_config['user'])
              .option("password", source_config['password'])
              .option("driver", source_config['driver'])
              .load())
        
        return df
    
    def _get_last_run_time(self) -> Optional[str]:
        """
        Retrieve the timestamp of the last successful ETL run
        
        Returns:
            Timestamp string or None if no previous run
        """
        try:
            log_config = self.config['logging']['database']
            
            jdbc_url = (
                f"jdbc:{log_config['type']}://"
                f"{log_config['host']}:{log_config['port']}/"
                f"{log_config['database']}"
            )
            
            query = """
                (SELECT MAX(end_time) as last_run_time 
                 FROM etl_run_log 
                 WHERE status = 'SUCCESS') as last_run
            """
            
            df = (self.spark.read
                  .format("jdbc")
                  .option("url", jdbc_url)
                  .option("dbtable", query)
                  .option("user", log_config['user'])
                  .option("password", log_config['password'])
                  .option("driver", log_config['driver'])
                  .load())
            
            result = df.first()
            if result and result.last_run_time:
                return str(result.last_run_time)
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error retrieving last run time: {str(e)}")
            return None
    
    def extract_with_partitions(
        self,
        partition_column: str = "id",
        num_partitions: int = 10
    ) -> DataFrame:
        """
        Extract data with partitioning for parallel processing
        
        Args:
            partition_column: Column to use for partitioning
            num_partitions: Number of partitions to create
            
        Returns:
            Partitioned DataFrame
        """
        source_config = self.config['source']['database']
        
        jdbc_url = (
            f"jdbc:{source_config['type']}://"
            f"{source_config['host']}:{source_config['port']}/"
            f"{source_config['database']}"
        )
        
        df = (self.spark.read
              .format("jdbc")
              .option("url", jdbc_url)
              .option("dbtable", source_config['table'])
              .option("user", source_config['user'])
              .option("password", source_config['password'])
              .option("driver", source_config['driver'])
              .option("partitionColumn", partition_column)
              .option("numPartitions", num_partitions)
              .load())
        
        return df