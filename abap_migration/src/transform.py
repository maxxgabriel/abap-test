"""
Data Transformation Module
Applies business rules, enrichments, and validations.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, 
    current_timestamp, expr, coalesce
)
from pyspark.sql.types import IntegerType, DecimalType
from typing import Tuple, List

from src.config_manager import ConfigManager
from src.logger import ETLLogger


class DataTransformer:
    """
    Transform and enrich data with business rules.
    """

    def __init__(self, config_manager: ConfigManager, run_id: str):
        """
        Initialize Data Transformer.

        Args:
            config_manager: Configuration manager
            run_id: Unique ETL run identifier
        """
        self.config = config_manager
        self.run_id = run_id
        self.logger = ETLLogger(component="TRANSFORMER")

    def transform_data(self, source_data: DataFrame) -> DataFrame:
        """
        Apply transformations to source data.

        Args:
            source_data: Source DataFrame

        Returns:
            Transformed DataFrame
        """
        self.logger.log_info("Starting transformation")

        # Clean and normalize names
        transformed = source_data.withColumn(
            "name",
            upper(trim(regexp_replace(col("name"), "\\s+", " ")))
        )

        # Calculate derived values
        transformed = self._calculate_derived_values(transformed)

        # Calculate priority
        transformed = self._calculate_priority(transformed)

        # Apply category rules
        transformed = self._apply_category_rules(transformed)

        # Apply business rules
        transformed = self._apply_business_rules(transformed)

        # Add ETL metadata
        transformed = transformed.withColumn("etl_run_id", lit(self.run_id)) \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("processed_by", lit("spark_etl"))

        record_count = transformed.count()
        self.logger.log_info(f"Transformed {record_count} records")

        return transformed

    def _calculate_derived_values(self, data: DataFrame) -> DataFrame:
        """Calculate transformed values based on category"""
        premium_multiplier = self.config.get("business_rules.premium_multiplier", 1.5)

        return data.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("value") * premium_multiplier)
            .when(col("category") == "VIP", col("value") * 1.3)
            .otherwise(col("value"))
        )

    def _calculate_priority(self, data: DataFrame) -> DataFrame:
        """Calculate priority based on value and category"""
        return data.withColumn(
            "priority",
            when(col("value") >= 1000, lit(1))
            .when((col("value") >= 750) | (col("category") == "VIP"), lit(2))
            .when(col("value") >= 500, lit(3))
            .when(col("value") >= 300, lit(4))
            .otherwise(lit(5))
            .cast(IntegerType())
        )

    def _apply_category_rules(self, data: DataFrame) -> DataFrame:
        """Apply category-specific rules"""
        return data.withColumn(
            "category",
            coalesce(col("category"), lit("UNCATEGORIZED"))
        )

    def _apply_business_rules(self, data: DataFrame) -> DataFrame:
        """Apply business rules to determine status"""
        return data.withColumn(
            "status",
            when(col("value").isNull(), lit("INVALID"))
            .when(col("transformed_value") >= 750, lit("HIGH_VALUE"))
            .when(col("transformed_value") >= 300, lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )

    def validate_data(self, data: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.

        Args:
            data: Transformed DataFrame

        Returns:
            Tuple of (is_valid, list of errors)
        """
        errors = []

        # Check for null IDs
        null_ids = data.filter(col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null ID")

        # Check for null names
        null_names = data.filter(col("name").isNull()).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null name")

        # Check for negative values
        negative_values = data.filter(col("value") < 0).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative values")

        # Check priority range
        invalid_priority = data.filter((col("priority") < 1) | (col("priority") > 5)).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")

        is_valid = len(errors) == 0

        if is_valid:
            self.logger.log_info("Validation passed")
        else:
            self.logger.log_error(f"Validation failed with {len(errors)} errors")

        return is_valid, errors