"""
PySpark Data Extractor with Delta Lake Integration

Replaces ABAP zcl_etl_extractor class with PySpark DataFrame operations.
Converts SQL WHERE filters to DataFrame.filter(), row limits to DataFrame.limit(),
and implements timestamp-based incremental logic with Delta Lake time travel.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, max as spark_max, 
    to_timestamp, unix_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    TimestampType, LongType
)
from typing import Optional, Dict, Any
from datetime import datetime
from delta import DeltaTable
import logging

from src.logger import ETLLogger
from src.config_manager import ConfigManager


class ETLExtractor:
    """
    Extract data from various sources using PySpark DataFrame API.
    Supports database, staging, and incremental extraction modes.
    """
    
    # Define schema for source data
    SOURCE_SCHEMA = StructType([
        StructField("id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("value", DecimalType(15, 2), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("category", StringType(), nullable=True),
        StructField("source_system", StringType(), nullable=True),
        StructField("created_at", TimestampType(), nullable=True),
        StructField("created_by", StringType(), nullable=True),
        StructField("changed_at", TimestampType(), nullable=True),
        StructField("changed_by", StringType(), nullable=True),
    ])
    
    def __init__(
        self,
        spark: SparkSession,
        config: ConfigManager,
        source_type: str = "DATABASE",
        run_id: str = None
    ):
        """
        Initialize the ETL Extractor.
        
        Args:
            spark: Active SparkSession
            config: Configuration manager instance
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.source_type = source_type.upper()
        self.run_id = run_id or self._generate_run_id()
        self.logger = ETLLogger.get_logger(__name__)
        
        self.logger.info(
            f"ETL Extractor initialized - Source: {self.source_type}, "
            f"Run ID: {self.run_id}"
        )
    
    def extract_data(
        self,
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Main extraction method - routes to appropriate extraction logic.
        
        Args:
            filter_condition: Optional filter string (e.g., "status='ACTIVE'")
            max_records: Maximum number of records to extract (0 = unlimited)
            
        Returns:
            DataFrame containing extracted data
            
        Raises:
            ValueError: If source type is invalid
            Exception: For extraction errors
        """
        self.logger.info(
            f"Starting extraction - Source: {self.source_type}, "
            f"Filter: {filter_condition}, Max Records: {max_records}"
        )
        
        try:
            # Route to appropriate extraction method
            if self.source_type == "DATABASE":
                df = self._extract_from_database(filter_condition)
            elif self.source_type == "STAGING":
                df = self._extract_from_staging(self.run_id)
            elif self.source_type == "INCREMENTAL":
                last_run_time = self._get_last_successful_run_time()
                if last_run_time:
                    df = self._extract_incremental(last_run_time)
                else:
                    self.logger.warning(
                        "No previous successful run found, "
                        "performing full extraction"
                    )
                    df = self._extract_from_database(filter_condition)
            else:
                raise ValueError(f"Invalid source type: {self.source_type}")
            
            # Apply row limit using DataFrame.limit()
            if max_records > 0:
                df = df.limit(max_records)
                self.logger.info(f"Applied limit of {max_records} records")
            
            record_count = df.count()
            self.logger.info(f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}", exc_info=True)
            raise
    
    def _extract_from_database(
        self,
        filter_condition: Optional[str] = None
    ) -> DataFrame:
        """
        Extract data from database source using JDBC.
        Replaces ABAP SELECT with DataFrame operations.
        
        Args:
            filter_condition: SQL WHERE clause conditions
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.info("Extracting from database source")
        
        # Get database configuration
        db_config = self.config.get("source.database")
        
        # Build JDBC options
        jdbc_options = {
            "url": db_config["url"],
            "dbtable": db_config["table"],
            "driver": db_config["driver"],
            "user": db_config["user"],
            "password": db_config["password"],
            "fetchsize": str(db_config.get("fetch_size", 10000)),
        }
        
        # Add partitioning for parallel reads
        if "partition_column" in db_config:
            jdbc_options.update({
                "partitionColumn": db_config["partition_column"],
                "numPartitions": str(db_config.get("num_partitions", 4)),
                "lowerBound": "1",
                "upperBound": "1000000",
            })
        
        # Read from database
        df = self.spark.read.format("jdbc").options(**jdbc_options).load()
        
        # Apply filter using DataFrame.filter() instead of SQL WHERE
        if filter_condition:
            df = df.filter(filter_condition)
            self.logger.info(f"Applied filter: {filter_condition}")
        
        # Ensure schema matches expected structure
        df = self._validate_schema(df)
        
        return df
    
    def _extract_from_staging(self, run_id: str) -> DataFrame:
        """
        Extract data from Delta Lake staging area.
        
        Args:
            run_id: Run identifier to filter staging data
            
        Returns:
            DataFrame with staged data
        """
        self.logger.info(f"Extracting from staging - Run ID: {run_id}")
        
        staging_path = self.config.get("source.staging.path")
        
        # Read from Delta Lake staging table
        df = (
            self.spark.read
            .format("delta")
            .load(staging_path)
            .filter(col("run_id") == run_id)
            .filter(col("status") == "READY")
        )
        
        self.logger.info(f"Loaded staging data from {staging_path}")
        
        return df
    
    def _extract_incremental(self, last_run_time: datetime) -> DataFrame:
        """
        Extract only changed records since last run using timestamp filtering.
        Implements Delta Lake time travel for incremental loads.
        
        Args:
            last_run_time: Timestamp of last successful run
            
        Returns:
            DataFrame with incremental data
        """
        self.logger.info(
            f"Extracting incremental data since {last_run_time}"
        )
        
        # Get database configuration
        db_config = self.config.get("source.database")
        watermark_column = self.config.get(
            "source.incremental.watermark_column",
            "changed_at"
        )
        
        # Build JDBC options
        jdbc_options = {
            "url": db_config["url"],
            "driver": db_config["driver"],
            "user": db_config["user"],
            "password": db_config["password"],
        }
        
        # Use pushdown predicate for efficient incremental extraction
        query = f"""
            (SELECT * FROM {db_config['table']} 
             WHERE {watermark_column} > '{last_run_time}') AS incremental_data
        """
        jdbc_options["dbtable"] = query
        
        df = self.spark.read.format("jdbc").options(**jdbc_options).load()
        
        # Alternative: Use Delta Lake time travel if source is Delta
        if db_config.get("format") == "delta":
            df = self._extract_using_time_travel(
                db_config["path"],
                last_run_time,
                watermark_column
            )
        
        record_count = df.count()
        self.logger.info(
            f"Extracted {record_count} incremental records "
            f"changed after {last_run_time}"
        )
        
        return df
    
    def _extract_using_time_travel(
        self,
        delta_path: str,
        timestamp: datetime,
        watermark_column: str
    ) -> DataFrame:
        """
        Extract data using Delta Lake time travel features.
        
        Args:
            delta_path: Path to Delta table
            timestamp: Point in time for extraction
            watermark_column: Column to filter on timestamp
            
        Returns:
            DataFrame with time-traveled data
        """
        self.logger.info(
            f"Using Delta time travel to extract changes since {timestamp}"
        )
        
        # Read current version
        current_df = self.spark.read.format("delta").load(delta_path)
        
        # Read version at last run time (time travel)
        previous_df = (
            self.spark.read
            .format("delta")
            .option("timestampAsOf", timestamp.strftime("%Y-%m-%d %H:%M:%S"))
            .load(delta_path)
        )
        
        # Get changed records using DataFrame operations
        # Records in current but not in previous, or changed values
        changed_df = (
            current_df
            .filter(col(watermark_column) > lit(timestamp))
        )
        
        return changed_df
    
    def _get_last_successful_run_time(self) -> Optional[datetime]:
        """
        Get timestamp of last successful ETL run from run log.
        
        Returns:
            Datetime of last successful run, or None if no previous runs
        """
        try:
            run_log_table = self.config.get(
                "incremental.last_run_table",
                "etl_run_log"
            )
            
            # Read run log from Delta Lake
            run_log_path = f"/data/logs/{run_log_table}"
            
            if DeltaTable.isDeltaTable(self.spark, run_log_path):
                df = (
                    self.spark.read
                    .format("delta")
                    .load(run_log_path)
                    .filter(col("status") == "SUCCESS")
                    .select(spark_max("end_time").alias("last_run"))
                )
                
                result = df.first()
                if result and result["last_run"]:
                    last_run = result["last_run"]
                    self.logger.info(f"Last successful run: {last_run}")
                    return last_run
            
            self.logger.info("No previous successful run found")
            return None
            
        except Exception as e:
            self.logger.warning(
                f"Could not retrieve last run time: {str(e)}"
            )
            return None
    
    def _validate_schema(self, df: DataFrame) -> DataFrame:
        """
        Validate and cast DataFrame to expected schema.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with validated schema
        """
        try:
            # Select and cast columns to match expected schema
            validated_df = df.select([
                col(field.name).cast(field.dataType).alias(field.name)
                if field.name in df.columns
                else lit(None).cast(field.dataType).alias(field.name)
                for field in self.SOURCE_SCHEMA.fields
            ])
            
            self.logger.info("Schema validation successful")
            return validated_df
            
        except Exception as e:
            self.logger.error(f"Schema validation failed: {str(e)}")
            raise
    
    def _generate_run_id(self) -> str:
        """
        Generate unique run identifier.
        
        Returns:
            Unique run ID string
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"RUN_{timestamp}"
    
    def get_extraction_metadata(self, df: DataFrame) -> Dict[str, Any]:
        """
        Get metadata about extracted data.
        
        Args:
            df: Extracted DataFrame
            
        Returns:
            Dictionary with extraction metadata
        """
        return {
            "run_id": self.run_id,
            "source_type": self.source_type,
            "record_count": df.count(),
            "schema": df.schema.simpleString(),
            "extraction_time": datetime.now().isoformat(),
            "columns": df.columns,
        }


def create_extractor(
    spark: SparkSession,
    config_path: str = "config.yaml",
    source_type: str = "DATABASE",
    run_id: Optional[str] = None
) -> ETLExtractor:
    """
    Factory function to create ETL Extractor instance.
    
    Args:
        spark: SparkSession instance
        config_path: Path to configuration file
        source_type: Type of data source
        run_id: Optional run identifier
        
    Returns:
        Configured ETLExtractor instance
    """
    config = ConfigManager(config_path)
    return ETLExtractor(spark, config, source_type, run_id)