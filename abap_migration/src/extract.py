"""
Extract module for ETL pipeline.
Handles data extraction from various sources.
"""
from typing import Optional, Dict, Any, List
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
import logging

logger = logging.getLogger(__name__)


class ETLExtractor:
    """Extract data from source systems."""
    
    SOURCE_SCHEMA = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("value", DecimalType(15, 2), False),
        StructField("status", StringType(), False),
        StructField("category", StringType(), False),
        StructField("source_system", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("created_by", StringType(), False),
        StructField("changed_at", TimestampType(), False),
        StructField("changed_by", StringType(), False)
    ])
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
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
        self.source_type = config.get('source_type', 'DATABASE')
        
    def extract_data(
        self, 
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data from configured source.
        
        Args:
            filter_condition: Optional SQL filter condition
            max_records: Maximum number of records to extract (0 = unlimited)
            
        Returns:
            DataFrame containing extracted data
        """
        logger.info(f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}")
        
        try:
            if self.source_type == 'DATABASE':
                df = self._extract_from_database(filter_condition)
            elif self.source_type == 'STAGING':
                df = self._extract_from_staging()
            elif self.source_type == 'INCREMENTAL':
                df = self._extract_incremental()
            else:
                df = self._extract_from_database(filter_condition)
            
            # Apply max records limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract data from database source."""
        jdbc_config = self.config['source']['database']
        
        query = f"SELECT * FROM {jdbc_config['table']}"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        query += f" LIMIT {jdbc_config.get('max_rows', 1000)}"
        
        df = (self.spark.read
              .format("jdbc")
              .option("url", jdbc_config['url'])
              .option("dbtable", f"({query}) AS source_data")
              .option("user", jdbc_config['user'])
              .option("password", jdbc_config['password'])
              .option("driver", jdbc_config['driver'])
              .load())
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract data from staging area."""
        staging_path = self.config['source']['staging']['path']
        file_format = self.config['source']['staging']['format']
        
        logger.info(f"Reading from staging: {staging_path}")
        
        df = (self.spark.read
              .format(file_format)
              .option("header", "true")
              .schema(self.SOURCE_SCHEMA)
              .load(f"{staging_path}/run_id={self.run_id}"))
        
        return df.filter("status = 'READY'")
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last run."""
        # Get last successful run timestamp
        run_log_path = self.config['metadata']['run_log_path']
        
        try:
            run_log_df = (self.spark.read
                          .format("delta")
                          .load(run_log_path))
            
            last_run_time = (run_log_df
                            .filter("status = 'SUCCESS'")
                            .agg({"end_time": "max"})
                            .collect()[0][0])
            
            if last_run_time:
                logger.info(f"Extracting incremental data since {last_run_time}")
                return self._extract_from_database(f"changed_at > '{last_run_time}'")
            else:
                logger.warning("No previous successful run found, performing full extraction")
                return self._extract_from_database()
                
        except Exception as e:
            logger.warning(f"Could not read run log: {str(e)}, performing full extraction")
            return self._extract_from_database()
    
    def extract_from_parquet(self, path: str) -> DataFrame:
        """Extract data from Parquet files."""
        logger.info(f"Reading from Parquet: {path}")
        return self.spark.read.schema(self.SOURCE_SCHEMA).parquet(path)
    
    def extract_from_csv(self, path: str) -> DataFrame:
        """Extract data from CSV files."""
        logger.info(f"Reading from CSV: {path}")
        return (self.spark.read
                .schema(self.SOURCE_SCHEMA)
                .option("header", "true")
                .csv(path))