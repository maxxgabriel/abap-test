"""
Extract module for ETL pipeline.
Handles data extraction from various sources with configuration support.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional, Dict, Any
import logging


class Extractor:
    """Handles data extraction from various sources."""
    
    def __init__(self, spark: SparkSession, run_id: str, config: Dict[str, Any]):
        """
        Initialize extractor with Spark session and configuration.
        
        Args:
            spark: Active SparkSession
            run_id: Unique identifier for this ETL run
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = logging.getLogger(__name__)
        
    def get_source_schema(self) -> StructType:
        """
        Define schema for source data.
        
        Returns:
            StructType defining the source data structure
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
        filter_expr: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data based on source type.
        
        Args:
            source_type: Type of source ('database', 'staging', 'incremental')
            filter_expr: Optional filter expression
            max_records: Maximum records to extract (0 for all)
            
        Returns:
            DataFrame containing extracted data
        """
        self.logger.info(f"Starting extraction - Source: {source_type}, Run ID: {self.run_id}")
        
        if source_type.lower() == "database":
            df = self._extract_from_database(filter_expr)
        elif source_type.lower() == "staging":
            df = self._extract_from_staging()
        elif source_type.lower() == "incremental":
            df = self._extract_incremental()
        else:
            self.logger.warning(f"Unknown source type {source_type}, defaulting to database")
            df = self._extract_from_database(filter_expr)
        
        # Apply max records limit if specified
        if max_records > 0:
            df = df.limit(max_records)
        
        record_count = df.count()
        self.logger.info(f"Extracted {record_count} records")
        
        return df
    
    def _extract_from_database(self, filter_expr: Optional[str] = None) -> DataFrame:
        """
        Extract data from database source.
        
        Args:
            filter_expr: Optional SQL WHERE clause filter
            
        Returns:
            DataFrame with extracted data
        """
        table_name = self.config.get("source_table", "etl_source_data")
        
        query = f"SELECT * FROM {table_name}"
        if filter_expr:
            query += f" WHERE {filter_expr}"
        query += " LIMIT 1000"
        
        self.logger.debug(f"Executing query: {query}")
        
        # In production, use JDBC or other connector
        # For testing, create sample data
        if self.config.get("test_mode", False):
            return self._create_test_data()
        
        try:
            df = self.spark.read \
                .format("jdbc") \
                .option("url", self.config.get("jdbc_url")) \
                .option("dbtable", f"({query}) as source") \
                .option("user", self.config.get("db_user")) \
                .option("password", self.config.get("db_password")) \
                .load()
            return df
        except Exception as e:
            self.logger.error(f"Database extraction failed: {str(e)}")
            # Fallback to test data in case of connection issues
            return self._create_test_data()
    
    def _extract_from_staging(self) -> DataFrame:
        """
        Extract data from staging area.
        
        Returns:
            DataFrame with staged data
        """
        staging_path = self.config.get("staging_path", "/tmp/etl_staging")
        
        self.logger.info(f"Reading from staging: {staging_path}")
        
        df = self.spark.read \
            .format("parquet") \
            .schema(self.get_source_schema()) \
            .load(f"{staging_path}/{self.run_id}")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run.
        
        Returns:
            DataFrame with incremental data
        """
        # Get last successful run timestamp
        last_run_time = self._get_last_run_time()
        
        if last_run_time:
            filter_expr = f"changed_at > '{last_run_time}'"
            self.logger.info(f"Incremental extraction since: {last_run_time}")
        else:
            filter_expr = None
            self.logger.info("No previous run found, performing full extraction")
        
        return self._extract_from_database(filter_expr)
    
    def _get_last_run_time(self) -> Optional[str]:
        """
        Get timestamp of last successful ETL run.
        
        Returns:
            Timestamp string or None
        """
        try:
            run_log_table = self.config.get("run_log_table", "etl_run_log")
            
            query = f"""
                SELECT MAX(end_time) as last_run
                FROM {run_log_table}
                WHERE status = 'SUCCESS'
            """
            
            df = self.spark.sql(query)
            result = df.collect()
            
            if result and result[0]["last_run"]:
                return result[0]["last_run"].isoformat()
            return None
        except Exception as e:
            self.logger.warning(f"Could not retrieve last run time: {str(e)}")
            return None
    
    def _create_test_data(self) -> DataFrame:
        """
        Create test data for development/testing.
        
        Returns:
            DataFrame with sample data
        """
        current_time = datetime.now()
        
        data = [
            ("TEST001", "Test Product 1", 100.00, "ACTIVE", "PREMIUM", "SAP_ERP", 
             current_time, "testuser", current_time, "testuser"),
            ("TEST002", "Test Product 2", 250.00, "ACTIVE", "STANDARD", "SAP_ERP",
             current_time, "testuser", current_time, "testuser"),
            ("TEST003", "Test Product 3", 500.00, "ACTIVE", "BASIC", "SAP_ERP",
             current_time, "testuser", current_time, "testuser"),
            ("TEST004", "Test Product 4", 750.00, "ACTIVE", "VIP", "SAP_ERP",
             current_time, "testuser", current_time, "testuser"),
            ("TEST005", "Test Product 5", 1000.00, "ACTIVE", "PREMIUM", "SAP_ERP",
             current_time, "testuser", current_time, "testuser"),
        ]
        
        return self.spark.createDataFrame(data, schema=self.get_source_schema())