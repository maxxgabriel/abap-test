"""
PySpark ETL Extractor Module
Migrated from ABAP zcl_etl_extractor
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
from typing import Optional, Dict, Any
import yaml
from datetime import datetime, timedelta
from src.logger import ETLLogger


class ETLExtractor:
    """Extract data from various sources using PySpark"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize extractor
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
        self.source_type = config.get('source_type', 'DATABASE')
        
    def extract_data(
        self,
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Main extraction method with source type routing
        
        Args:
            filter_condition: Optional filter string
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.log_info(
            component='EXTRACTOR',
            message=f'Starting extraction - Source: {self.source_type}'
        )
        
        try:
            # Route to appropriate extraction method
            if self.source_type == 'DATABASE':
                df = self._extract_from_database(filter_condition)
            elif self.source_type == 'STAGING':
                df = self._extract_from_staging(self.run_id)
            elif self.source_type == 'INCREMENTAL':
                df = self._extract_incremental()
            else:
                df = self._extract_from_database(filter_condition)
            
            # Apply max records limit
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.log_info(
                component='EXTRACTOR',
                message=f'Extracted {record_count} records'
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component='EXTRACTOR',
                message='Extraction failed',
                details=str(e)
            )
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract from database source
        
        Args:
            filter_condition: SQL WHERE clause condition
            
        Returns:
            DataFrame with source data
        """
        db_config = self.config['database']
        table_name = db_config['source_table']
        
        # Build query
        if filter_condition:
            query = f"(SELECT * FROM {table_name} WHERE {filter_condition}) AS source"
        else:
            query = f"(SELECT * FROM {table_name} LIMIT 1000) AS source"
        
        # Read from database
        df = self.spark.read \
            .format("jdbc") \
            .option("url", db_config['jdbc_url']) \
            .option("dbtable", query) \
            .option("user", db_config['user']) \
            .option("password", db_config['password']) \
            .option("driver", db_config['driver']) \
            .load()
        
        return df
    
    def _extract_from_staging(self, run_id: str) -> DataFrame:
        """
        Extract from staging area
        
        Args:
            run_id: Run identifier for staged data
            
        Returns:
            DataFrame with staged data
        """
        staging_path = self.config['staging']['path']
        
        df = self.spark.read \
            .format(self.config['staging']['format']) \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(f"{staging_path}/run_id={run_id}")
        
        return df.filter(col("status") == "READY")
    
    def _extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run
        
        Returns:
            DataFrame with incremental data
        """
        # Get last successful run timestamp
        last_run_time = self._get_last_successful_run_time()
        
        if last_run_time:
            # Extract only records changed after last run
            filter_condition = f"changed_at > '{last_run_time}'"
            return self._extract_from_database(filter_condition)
        else:
            # No previous run - extract all
            return self._extract_from_database()
    
    def _get_last_successful_run_time(self) -> Optional[str]:
        """
        Get timestamp of last successful run
        
        Returns:
            ISO format timestamp string or None
        """
        try:
            run_log_table = self.config['database']['run_log_table']
            query = f"""
                (SELECT end_time 
                 FROM {run_log_table} 
                 WHERE status = 'SUCCESS' 
                 ORDER BY end_time DESC 
                 LIMIT 1) AS last_run
            """
            
            df = self.spark.read \
                .format("jdbc") \
                .option("url", self.config['database']['jdbc_url']) \
                .option("dbtable", query) \
                .option("user", self.config['database']['user']) \
                .option("password", self.config['database']['password']) \
                .load()
            
            if df.count() > 0:
                return df.first()['end_time']
            return None
            
        except Exception:
            return None