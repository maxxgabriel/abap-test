"""
ETL Data Transformation Module

Migrated from ABAP zcl_etl_transformer
Applies business rules, enrichment, and validation to extracted data.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, upper, trim, when, lit, current_timestamp,
    regexp_replace, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import Tuple, List
import re

from src.logger import ETLLogger


class ETLTransformer:
    """
    Data transformer applying business logic and enrichment
    """

    TRANSFORMED_SCHEMA = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("value", DecimalType(15, 2), True),
        StructField("transformed_value", DecimalType(15, 2), True),
        StructField("status", StringType(), True),
        StructField("category", StringType(), True),
        StructField("priority", IntegerType(), True),
        StructField("etl_run_id", StringType(), True),
        StructField("processed_at", TimestampType(), True),
        StructField("processed_by", StringType(), True),
    ])

    def __init__(self, spark: SparkSession, run_id: str, config: dict):
        """
        Initialize transformer

        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = ETLLogger.get_instance()

    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply transformations to source data

        Args:
            source_df: Source DataFrame

        Returns:
            Transformed DataFrame
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message=f"Starting transformation for run {self.run_id}"
        )

        try:
            # Basic transformations
            df = source_df.select(
                col("id"),
                upper(trim(col("name"))).alias("name"),
                col("value"),
                self._calculate_transformed_value(col("value"), col("category")).alias("transformed_value"),
                lit("TRANSFORMED").alias("status"),
                col("category"),
                self._calculate_priority(col("value"), col("category")).alias("priority"),
                lit(self.run_id).alias("etl_run_id"),
                current_timestamp().alias("processed_at"),
                lit(self.config.get("user", "ETL_SYSTEM")).alias("processed_by")
            )

            # Apply category-specific rules
            df = self._apply_category_rules(df)

            # Apply business rules
            df = self._apply_business_rules(df)

            # Enrich data
            df = self._enrich_data(df)

            # Clean name field
            df = df.withColumn("name", regexp_replace(col("name"), r'\s+', ' '))

            record_count = df.count()
            self.logger.log_info(
                component="TRANSFORMER",
                message=f"Transformed {record_count} records"
            )

            return df

        except Exception as e:
            self.logger.log_error(
                component="TRANSFORMER",
                message="Transformation failed",
                details=str(e)
            )
            raise

    def _calculate_transformed_value(self, value_col, category_col):
        """Calculate derived value based on category"""
        return when(category_col == "PREMIUM", value_col * 1.5) \
            .when(category_col == "VIP", value_col * 2.0) \
            .when(category_col == "STANDARD", value_col * 1.2) \
            .otherwise(value_col)

    def _calculate_priority(self, value_col, category_col):
        """Calculate priority based on value and category"""
        return when(value_col >= 1000, lit(1)) \
            .when(value_col >= 750, lit(2)) \
            .when(value_col >= 500, lit(3)) \
            .when(value_col >= 250, lit(4)) \
            .otherwise(lit(5))

    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """Apply category-specific business rules"""
        # Handle uncategorized items
        df = df.withColumn(
            "category",
            coalesce(col("category"), lit("UNCATEGORIZED"))
        )

        return df

    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply general business rules"""
        self.logger.log_info(
            component="TRANSFORMER",
            message="Applying business rules"
        )

        # Rule 1: Set status based on transformed value
        df = df.withColumn(
            "status",
            when(col("value").isNull(), lit("INVALID"))
            .when(col("transformed_value") >= 750, lit("HIGH_VALUE"))
            .when(col("transformed_value") >= 300, lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )

        # Rule 2: Priority override for high-value items
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, lit(1))
            .otherwise(col("priority"))
        )

        return df

    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional calculated fields"""
        # Load enrichment configuration if available
        premium_multiplier = float(self.config.get("premium_multiplier", 1.2))

        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("transformed_value") * premium_multiplier)
            .otherwise(col("transformed_value"))
        )

        return df

    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []

        # Rule 1: ID is required
        null_ids = df.filter(col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with missing ID")

        # Rule 2: Name is required
        null_names = df.filter(col("name").isNull() | (col("name") == "")).count()
        if null_names > 0:
            errors.append(f"{null_names} records with missing name")

        # Rule 3: Value must be non-negative
        negative_values = df.filter(col("value") < 0).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative values")

        # Rule 4: Priority must be 1-5
        invalid_priority = df.filter((col("priority") < 1) | (col("priority") > 5)).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")

        is_valid = len(errors) == 0

        if not is_valid:
            self.logger.log_warning(
                component="TRANSFORMER",
                message=f"Validation failed with {len(errors)} error(s)"
            )

        return is_valid, errors