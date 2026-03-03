"""
Schema definitions for ETL data quality framework
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, LongType, DecimalType
)


def get_source_schema() -> StructType:
    """
    Schema for source data (extracted from database)
    """
    return StructType([
        StructField("id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("value", DoubleType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("category", StringType(), nullable=True),
        StructField("source_system", StringType(), nullable=True),
        StructField("created_at", TimestampType(), nullable=True),
        StructField("created_by", StringType(), nullable=True),
        StructField("changed_at", TimestampType(), nullable=True),
        StructField("changed_by", StringType(), nullable=True)
    ])


def get_transformed_schema() -> StructType:
    """
    Schema for transformed data (ready for loading)
    """
    return StructType([
        StructField("id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("value", DoubleType(), nullable=False),
        StructField("transformed_value", DoubleType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("priority", IntegerType(), nullable=False),
        StructField("etl_run_id", StringType(), nullable=False),
        StructField("processed_at", TimestampType(), nullable=False),
        StructField("processed_by", StringType(), nullable=False)
    ])


def get_quality_check_schema() -> StructType:
    """
    Schema for quality check results
    """
    return StructType([
        StructField("check_name", StringType(), nullable=False),
        StructField("check_type", StringType(), nullable=False),
        StructField("passed", StringType(), nullable=False),
        StructField("failed_count", IntegerType(), nullable=False),
        StructField("message", StringType(), nullable=True),
        StructField("run_id", StringType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False)
    ])


def get_data_profile_schema() -> StructType:
    """
    Schema for data profile results
    """
    return StructType([
        StructField("total_records", LongType(), nullable=False),
        StructField("null_count", LongType(), nullable=False),
        StructField("duplicate_count", LongType(), nullable=False),
        StructField("min_value", DoubleType(), nullable=True),
        StructField("max_value", DoubleType(), nullable=True),
        StructField("avg_value", DoubleType(), nullable=True),
        StructField("std_deviation", DoubleType(), nullable=True),
        StructField("unique_categories", IntegerType(), nullable=True),
        StructField("run_id", StringType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False)
    ])