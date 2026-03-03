"""
Data Extraction Module
Extracts data from various sources with support for full and incremental loads.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from typing import Optional
from datetime import datetime

from src.config_manager import ConfigManager
from src.logger import ETLLogger


class DataExtractor:
    """
    Extract data from various sources with filtering and incremental support.
    """

    def __init__(self, spark: SparkSession, config_manager: ConfigManager, run_id: str):
        """
        Initialize Data Extractor.

        Args:
            spark: SparkSession instance
            config_manager: Configuration manager
            run_id: Unique ETL run identifier
        """
        self.spark = spark
        self.config = config_manager
        self.run_id = run_id
        self.logger = ETLLogger(component="EXTRACTOR")

    def extract_data(
        self,
        source_type: str = "DATABASE",
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data based on source type.

        Args:
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            filter_condition: Optional filter condition
            max_records: Maximum records to extract (0 = unlimited)

        Returns:
            Extracted DataFrame
        """
        self.logger.log_info(f"Starting extraction - Source: {source_type}")

        try:
            if source_type == "DATABASE":
                data = self._extract_from_database(filter_condition)
            elif source_type == "STAGING":
                data = self._extract_from_staging()
            elif source_type == "INCREMENTAL":
                data = self._extract_incremental()
            else:
                data = self._extract_from_database(filter_condition)

            # Apply max records limit
            if max_records > 0:
                data = data.limit(max_records)

            record_count = data.count()
            self.logger.log_info(f"Extracted {record_count} records")

            return data

        except Exception as e:
            self.logger.log_error(f"Extraction failed: {str(e)}")
            raise

    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract from source database/table"""
        source_path = self.config.get("source.database_path")

        data = self.spark.read \
            .format(self.config.get("source.format", "delta")) \
            .load(source_path)

        if filter_condition:
            data = data.filter(filter_condition)

        return data

    def _extract_from_staging(self) -> DataFrame:
        """Extract from staging area"""
        staging_path = self.config.get("source.staging_path")

        return self.spark.read \
            .format("delta") \
            .load(staging_path) \
            .filter(col("run_id") == self.run_id) \
            .filter(col("status") == "READY")

    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last successful run"""
        # Get last successful run timestamp
        last_run_time = self._get_last_run_time()

        source_path = self.config.get("source.database_path")

        data = self.spark.read \
            .format("delta") \
            .load(source_path)

        if last_run_time:
            data = data.filter(col("changed_at") > lit(last_run_time))
            self.logger.log_info(f"Incremental load from {last_run_time}")
        else:
            self.logger.log_info("No previous run found - performing full load")

        return data

    def _get_last_run_time(self) -> Optional[datetime]:
        """Get timestamp of last successful run"""
        try:
            run_log_path = self.config.get("metadata.run_log_path")
            run_log = self.spark.read.format("delta").load(run_log_path)

            last_run = run_log \
                .filter(col("status") == "SUCCESS") \
                .orderBy(col("end_time").desc()) \
                .limit(1) \
                .collect()

            if last_run:
                return last_run[0]["end_time"]
        except Exception as e:
            self.logger.log_warning(f"Could not get last run time: {str(e)}")

        return None

    @staticmethod
    def get_source_schema() -> StructType:
        """Define source data schema"""
        return StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("value", DecimalType(15, 2), True),
            StructField("status", StringType(), True),
            StructField("category", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", StringType(), True),
            StructField("changed_at", TimestampType(), True),
            StructField("changed_by", StringType(), True)
        ])