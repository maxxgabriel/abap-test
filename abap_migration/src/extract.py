"""
PySpark Data Extraction Module with Incremental Load Support
Handles extraction from various source systems with filtering and delta processing
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from pyspark.sql import functions as F
from datetime import datetime
from typing import Optional, Dict, Any
import yaml
import logging


class DataExtractor:
    """
    DataFrame-based extraction module supporting incremental loads and data type conversion
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the DataExtractor
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
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
            StructField("changed_by", StringType(), True)
        ])
    
    def extract_data(
        self,
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Main extraction method with support for different source types
        
        Args:
            filter_condition: Optional SQL filter condition
            max_records: Maximum number of records to extract (0 = no limit)
            
        Returns:
            Extracted DataFrame
        """
        source_type = self.config.get('source_type', 'DATABASE')
        
        self.logger.info(f"Starting extraction - Source: {source_type}, Run ID: {self.run_id}")
        
        try:
            if source_type == 'DATABASE':
                df = self._extract_from_database(filter_condition)
            elif source_type == 'STAGING':
                df = self._extract_from_staging()
            elif source_type == 'INCREMENTAL':
                df = self._extract_incremental()
            else:
                df = self._extract_from_database(filter_condition)
            
            # Apply max records limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract data from database source
        
        Args:
            filter_condition: Optional filter condition
            
        Returns:
            Extracted DataFrame
        """
        source_config = self.config['source']
        table_name = source_config['table']
        
        # Build read options
        read_options = {
            "url": source_config['jdbc_url'],
            "dbtable": table_name,
            "driver": source_config.get('driver', 'org.postgresql.Driver'),
            "user": source_config.get('user', ''),
            "password": source_config.get('password', '')
        }
        
        # Add filter if provided
        if filter_condition:
            read_options["dbtable"] = f"(SELECT * FROM {table_name} WHERE {filter_condition}) as filtered_data"
        
        df = self.spark.read \
            .format("jdbc") \
            .options(**read_options) \
            .load()
        
        # Apply schema conversion
        df = self._apply_type_conversion(df)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """
        Extract data from staging area
        
        Returns:
            Extracted DataFrame
        """
        staging_config = self.config['staging']
        staging_path = staging_config['path']
        
        df = self.spark.read \
            .format(staging_config.get('format', 'parquet')) \
            .load(f"{staging_path}/{self.run_id}")
        
        # Filter ready records
        df = df.filter(F.col("status") == "READY")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run
        
        Returns:
            Incremental DataFrame
        """
        # Get last successful run timestamp
        last_run_time = self._get_last_run_timestamp()
        
        if last_run_time is None:
            self.logger.warning("No previous run found, performing full extraction")
            return self._extract_from_database()
        
        # Extract records changed since last run
        filter_condition = f"changed_at > '{last_run_time}'"
        df = self._extract_from_database(filter_condition)
        
        self.logger.info(f"Incremental extraction from timestamp: {last_run_time}")
        
        return df
    
    def _get_last_run_timestamp(self) -> Optional[str]:
        """
        Retrieve the timestamp of the last successful ETL run
        
        Returns:
            Timestamp string or None
        """
        try:
            run_log_config = self.config.get('run_log', {})
            
            # Read from run log table/file
            if 'jdbc_url' in run_log_config:
                # From database
                log_df = self.spark.read \
                    .format("jdbc") \
                    .options(
                        url=run_log_config['jdbc_url'],
                        dbtable="(SELECT MAX(end_time) as last_run FROM etl_run_log WHERE status = 'SUCCESS') as log",
                        driver=run_log_config.get('driver', 'org.postgresql.Driver')
                    ) \
                    .load()
            else:
                # From file
                log_path = run_log_config.get('path', '/tmp/etl_run_log')
                log_df = self.spark.read.parquet(log_path) \
                    .filter(F.col("status") == "SUCCESS") \
                    .agg(F.max("end_time").alias("last_run"))
            
            result = log_df.first()
            return result['last_run'] if result and result['last_run'] else None
            
        except Exception as e:
            self.logger.warning(f"Could not retrieve last run timestamp: {str(e)}")
            return None
    
    def _apply_type_conversion(self, df: DataFrame) -> DataFrame:
        """
        Apply proper type conversions to the DataFrame
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with corrected types
        """
        schema = self.get_source_schema()
        
        # Build select expression with proper casting
        select_exprs = []
        for field in schema.fields:
            if field.name in df.columns:
                select_exprs.append(F.col(field.name).cast(field.dataType).alias(field.name))
            else:
                select_exprs.append(F.lit(None).cast(field.dataType).alias(field.name))
        
        return df.select(*select_exprs)
    
    def extract_with_partitioning(
        self,
        partition_column: str = "created_at",
        num_partitions: int = 10
    ) -> DataFrame:
        """
        Extract data with partitioning for better performance
        
        Args:
            partition_column: Column to partition by
            num_partitions: Number of partitions
            
        Returns:
            Partitioned DataFrame
        """
        df = self.extract_data()
        
        return df.repartition(num_partitions, partition_column)
    
    def validate_extraction(self, df: DataFrame) -> bool:
        """
        Validate the extracted data
        
        Args:
            df: Extracted DataFrame
            
        Returns:
            True if validation passes
        """
        # Check if DataFrame is not empty
        if df.count() == 0:
            self.logger.warning("Extraction resulted in empty DataFrame")
            return False
        
        # Check for required columns
        required_columns = ['id', 'name']
        missing_columns = set(required_columns) - set(df.columns)
        
        if missing_columns:
            self.logger.error(f"Missing required columns: {missing_columns}")
            return False
        
        # Check for null IDs
        null_id_count = df.filter(F.col("id").isNull()).count()
        if null_id_count > 0:
            self.logger.warning(f"Found {null_id_count} records with null IDs")
        
        return True


def create_extractor(config_path: str, run_id: str) -> DataExtractor:
    """
    Factory function to create a DataExtractor instance
    
    Args:
        config_path: Path to configuration file
        run_id: Unique run identifier
        
    Returns:
        DataExtractor instance
    """
    # Load configuration
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Create SparkSession
    spark = SparkSession.builder \
        .appName(f"ETL_Extract_{run_id}") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    return DataExtractor(spark, config, run_id)