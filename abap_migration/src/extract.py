"""
ETL Data Extraction Module

Migrated from ABAP zcl_etl_extractor
Handles data extraction from various sources including database, staging, and incremental loads.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from typing import Optional
from datetime import datetime
import logging

from src.logger import ETLLogger


class ETLExtractor:
    """
    Data extractor supporting multiple source types:
    - DATABASE: Full extraction from source tables
    - STAGING: Extract from staging area
    - INCREMENTAL: Extract only changed records since last run
    """

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

    def __init__(self, spark: SparkSession, source_type: str, run_id: str, config: dict):
        """
        Initialize extractor

        Args:
            spark: SparkSession instance
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.source_type = source_type.upper()
        self.run_id = run_id
        self.config = config
        self.logger = ETLLogger.get_instance()

    def extract_data(self, filter_condition: Optional[str] = None, max_records: int = 0) -> DataFrame:
        """
        Extract data based on source type

        Args:
            filter_condition: Optional SQL WHERE clause
            max_records: Maximum records to extract (0 = unlimited)

        Returns:
            DataFrame with extracted data
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}"
        )

        try:
            if self.source_type == "DATABASE":
                df = self._extract_from_database(filter_condition)
            elif self.source_type == "STAGING":
                df = self._extract_from_staging()
            elif self.source_type == "INCREMENTAL":
                df = self._extract_incremental()
            else:
                self.logger.log_warning(
                    component="EXTRACTOR",
                    message=f"Unknown source type: {self.source_type}, defaulting to DATABASE"
                )
                df = self._extract_from_database(filter_condition)

            # Apply max records limit
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

    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract from source database table"""
        source_table = self.config.get("source_table", "etl_source_data")

        query = f"SELECT * FROM {source_table}"
        if filter_condition:
            query += f" WHERE {filter_condition}"

        df = self.spark.read \
            .format("jdbc") \
            .option("url", self.config["jdbc_url"]) \
            .option("dbtable", f"({query}) as src") \
            .option("user", self.config.get("db_user", "")) \
            .option("password", self.config.get("db_password", "")) \
            .option("driver", self.config.get("jdbc_driver", "org.postgresql.Driver")) \
            .load()

        return df

    def _extract_from_staging(self) -> DataFrame:
        """Extract from staging area"""
        staging_path = self.config.get("staging_path", f"/data/staging/{self.run_id}")

        df = self.spark.read \
            .format(self.config.get("staging_format", "parquet")) \
            .schema(self.SOURCE_SCHEMA) \
            .load(staging_path)

        df = df.filter(df.status == "READY")

        return df

    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last successful run"""
        # Get last successful run timestamp
        run_log_table = self.config.get("run_log_table", "etl_run_log")

        last_run_query = f"""
            SELECT MAX(end_time) as last_run_time 
            FROM {run_log_table} 
            WHERE status = 'SUCCESS'
        """

        last_run_df = self.spark.read \
            .format("jdbc") \
            .option("url", self.config["jdbc_url"]) \
            .option("dbtable", f"({last_run_query}) as lr") \
            .option("user", self.config.get("db_user", "")) \
            .option("password", self.config.get("db_password", "")) \
            .load()

        last_run_time = last_run_df.first()["last_run_time"] if last_run_df.count() > 0 else None

        if last_run_time:
            filter_condition = f"changed_at > '{last_run_time}'"
            return self._extract_from_database(filter_condition)
        else:
            self.logger.log_warning(
                component="EXTRACTOR",
                message="No previous successful run found, performing full extraction"
            )
            return self._extract_from_database()