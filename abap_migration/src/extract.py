"""
ETL Extraction Module
Implements PySpark-based extraction with database-to-DataFrame conversion,
incremental load filtering, and data type transformations.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from pyspark.sql.functions import col, lit, current_timestamp
from typing import Optional, Dict, Any
import logging
from datetime import datetime

from src.logger import ETLLogger


class ETLExtractor:
    """Handles data extraction from various sources with incremental load support."""
    
    # Define source data schema
    SOURCE_SCHEMA = StructType([
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
    
    def __init__(
        self,
        spark: SparkSession,
        source_type: str = "DATABASE",
        run_id: str = None,
        config: Dict[str, Any] = None
    ):
        """
        Initialize the ETL Extractor.
        
        Args:
            spark: SparkSession instance
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            run_id: Unique identifier for this ETL run
            config: Configuration dictionary
        """
        self.spark = spark
        self.source_type = source_type.upper()
        self.run_id = run_id or self._generate_run_id()
        self.config = config or {}
        self.logger = ETLLogger.get_instance()
        
    def _generate_run_id(self) -> str:
        """Generate a unique run ID."""
        return f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def extract_data(
        self,
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data based on source type.
        
        Args:
            filter_condition: SQL WHERE clause filter
            max_records: Maximum number of records to extract (0 = no limit)
            
        Returns:
            DataFrame with extracted data
            
        Raises:
            Exception: If extraction fails
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}"
        )
        
        try:
            if self.source_type == "DATABASE":
                df = self.extract_from_database(filter_condition)
            elif self.source_type == "STAGING":
                df = self.extract_from_staging(self.run_id)
            elif self.source_type == "INCREMENTAL":
                last_run_time = self._get_last_successful_run_time()
                if last_run_time:
                    df = self.extract_incremental(last_run_time)
                else:
                    self.logger.log_warning(
                        component="EXTRACTOR",
                        message="No previous successful run found, performing full extraction"
                    )
                    df = self.extract_from_database(filter_condition)
            else:
                df = self.extract_from_database(filter_condition)
            
            # Apply max records limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.log_info(
                component="EXTRACTOR",
                message=f"Extracted {record_count} records"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="EXTRACTOR",
                message="Extraction failed",
                details=str(e)
            )
            raise
    
    def extract_from_database(
        self,
        filter_condition: Optional[str] = None
    ) -> DataFrame:
        """
        Extract data from database source.
        
        Args:
            filter_condition: SQL WHERE clause filter
            
        Returns:
            DataFrame with extracted data
        """
        jdbc_config = self.config.get("source_jdbc", {})
        
        # Build query
        table_name = jdbc_config.get("table", "etl_source_data")
        
        if filter_condition:
            query = f"(SELECT * FROM {table_name} WHERE {filter_condition}) as filtered_data"
        else:
            query = f"(SELECT * FROM {table_name} LIMIT 1000) as source_data"
        
        # Read from database
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config.get("url")) \
            .option("dbtable", query) \
            .option("user", jdbc_config.get("user")) \
            .option("password", jdbc_config.get("password")) \
            .option("driver", jdbc_config.get("driver", "org.postgresql.Driver")) \
            .schema(self.SOURCE_SCHEMA) \
            .load()
        
        return df
    
    def extract_from_staging(self, run_id: str) -> DataFrame:
        """
        Extract data from staging area.
        
        Args:
            run_id: Run ID to extract from staging
            
        Returns:
            DataFrame with extracted data
        """
        staging_config = self.config.get("staging", {})
        staging_path = staging_config.get("path", "/tmp/etl_staging")
        
        # Read from staging (assuming parquet format)
        df = self.spark.read \
            .parquet(f"{staging_path}/run_id={run_id}") \
            .filter(col("status") == "READY")
        
        return df
    
    def extract_incremental(self, last_run_time: datetime) -> DataFrame:
        """
        Extract only records changed since last run.
        
        Args:
            last_run_time: Timestamp of last successful run
            
        Returns:
            DataFrame with changed records
        """
        jdbc_config = self.config.get("source_jdbc", {})
        table_name = jdbc_config.get("table", "etl_source_data")
        
        # Query for incremental changes
        query = f"""(
            SELECT * FROM {table_name}
            WHERE changed_at > '{last_run_time.strftime('%Y-%m-%d %H:%M:%S')}'
        ) as incremental_data"""
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config.get("url")) \
            .option("dbtable", query) \
            .option("user", jdbc_config.get("user")) \
            .option("password", jdbc_config.get("password")) \
            .option("driver", jdbc_config.get("driver", "org.postgresql.Driver")) \
            .schema(self.SOURCE_SCHEMA) \
            .load()
        
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Incremental extraction from {last_run_time}"
        )
        
        return df
    
    def _get_last_successful_run_time(self) -> Optional[datetime]:
        """
        Get timestamp of last successful ETL run.
        
        Returns:
            Datetime of last successful run or None
        """
        try:
            jdbc_config = self.config.get("source_jdbc", {})
            
            query = """(
                SELECT MAX(end_time) as last_run
                FROM etl_run_log
                WHERE status = 'SUCCESS'
            ) as last_run_query"""
            
            df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_config.get("url")) \
                .option("dbtable", query) \
                .option("user", jdbc_config.get("user")) \
                .option("password", jdbc_config.get("password")) \
                .option("driver", jdbc_config.get("driver")) \
                .load()
            
            result = df.collect()
            if result and result[0]["last_run"]:
                return result[0]["last_run"]
            
            return None
            
        except Exception as e:
            self.logger.log_warning(
                component="EXTRACTOR",
                message="Could not retrieve last run time",
                details=str(e)
            )
            return None