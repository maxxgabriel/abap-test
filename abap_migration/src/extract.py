"""
Data extraction module for ETL pipeline.
Handles extraction from various data sources.
"""
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType

from src.utils.logger import ETLLogger
from src.config_manager import ConfigManager


class ETLExtractor:
    """Extract data from various sources."""
    
    # Source data schema
    SOURCE_SCHEMA = StructType([
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
    
    def __init__(
        self,
        spark: SparkSession,
        source_type: str = "DATABASE",
        run_id: str = None,
        config: ConfigManager = None
    ):
        self.spark = spark
        self.source_type = source_type
        self.run_id = run_id
        self.config = config or ConfigManager()
        self.logger = ETLLogger(run_id=run_id)
    
    def extract_data(
        self,
        filter_expr: Optional[str] = None,
        max_records: int = 0
    ) -> Optional[DataFrame]:
        """
        Extract data based on source type.
        
        Args:
            filter_expr: SQL filter expression
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame containing extracted data
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Starting extraction - Source: {self.source_type}"
        )
        
        try:
            if self.source_type == "DATABASE":
                df = self.extract_from_database(filter_expr)
            elif self.source_type == "STAGING":
                df = self.extract_from_staging(self.run_id)
            elif self.source_type == "INCREMENTAL":
                df = self.extract_incremental()
            else:
                df = self.extract_from_database(filter_expr)
            
            # Apply max records limit
            if df and max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count() if df else 0
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
    
    def extract_from_database(self, filter_expr: Optional[str] = None) -> DataFrame:
        """Extract from database table."""
        db_config = self.config.get("database", {})
        
        source_table = db_config.get("source_table", "etl_source_data")
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", db_config.get("url")) \
            .option("dbtable", source_table) \
            .option("user", db_config.get("user")) \
            .option("password", db_config.get("password")) \
            .option("driver", db_config.get("driver", "org.postgresql.Driver")) \
            .load()
        
        # Apply filter if specified
        if filter_expr:
            df = df.filter(filter_expr)
        
        return df
    
    def extract_from_staging(self, run_id: str) -> DataFrame:
        """Extract from staging area."""
        db_config = self.config.get("database", {})
        staging_table = db_config.get("staging_table", "etl_staging")
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", db_config.get("url")) \
            .option("dbtable", staging_table) \
            .option("user", db_config.get("user")) \
            .option("password", db_config.get("password")) \
            .load() \
            .filter(f"run_id = '{run_id}' AND status = 'READY'")
        
        return df
    
    def extract_incremental(self) -> DataFrame:
        """Extract only changed records since last successful run."""
        db_config = self.config.get("database", {})
        
        # Get last successful run time
        run_log_table = db_config.get("run_log_table", "etl_run_log")
        
        last_run_df = self.spark.read \
            .format("jdbc") \
            .option("url", db_config.get("url")) \
            .option("dbtable", run_log_table) \
            .option("user", db_config.get("user")) \
            .option("password", db_config.get("password")) \
            .load() \
            .filter("status = 'SUCCESS'") \
            .orderBy("end_time", ascending=False) \
            .limit(1)
        
        if last_run_df.count() > 0:
            last_run_time = last_run_df.select("end_time").first()[0]
            filter_expr = f"changed_at > '{last_run_time}'"
        else:
            filter_expr = None
        
        return self.extract_from_database(filter_expr)