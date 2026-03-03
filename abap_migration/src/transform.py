"""
Data transformation module for ETL pipeline.
Applies business rules, enrichment, and validation.
"""

from typing import List, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, current_timestamp,
    current_user, lit, udf, concat_ws
)
from pyspark.sql.types import IntegerType, DecimalType
import logging

logger = logging.getLogger(__name__)


class ETLTransformer:
    """Handles data transformation and validation."""
    
    def __init__(self, spark: SparkSession, run_id: str, config: dict):
        """
        Initialize transformer.
        
        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        logger.info(f"Transformer initialized - Run ID: {run_id}")
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply complete transformation pipeline.
        
        Args:
            source_df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Starting transformation")
        
        # Basic transformations
        df = self._apply_basic_transformations(source_df)
        
        # Calculate derived values
        df = self._calculate_derived_values(df)
        
        # Calculate priority
        df = self._calculate_priority(df)
        
        # Apply business rules
        df = self._apply_business_rules(df)
        
        # Enrich data
        df = self._enrich_data(df)
        
        # Add metadata
        df = self._add_metadata(df)
        
        record_count = df.count()
        logger.info(f"Transformed {record_count} records")
        
        return df
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic data cleaning and formatting."""
        logger.info("Applying basic transformations")
        
        return (df
                .withColumn("name", upper(trim(col("name"))))
                .withColumn("name", regexp_replace(col("name"), r"\s+", " "))
                .withColumn("status", lit("TRANSFORMED")))
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """Calculate transformed values based on category."""
        logger.info("Calculating derived values")
        
        multipliers = self.config["business_rules"]["category_multipliers"]
        
        # Build when/otherwise chain for category-based calculation
        transform_expr = when(col("category") == "PREMIUM", 
                             col("value") * multipliers.get("PREMIUM", 1.5))
        
        for category, multiplier in multipliers.items():
            if category != "PREMIUM":
                transform_expr = transform_expr.when(
                    col("category") == category, 
                    col("value") * multiplier
                )
        
        transform_expr = transform_expr.otherwise(col("value"))
        
        return df.withColumn("transformed_value", transform_expr.cast(DecimalType(15, 2)))
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """Calculate priority based on value and category."""
        logger.info("Calculating priority")
        
        thresholds = self.config["business_rules"]["priority_thresholds"]
        
        priority_expr = (
            when(col("transformed_value") >= thresholds["critical"], 1)
            .when(col("transformed_value") >= thresholds["high"], 2)
            .when(col("transformed_value") >= thresholds["medium"], 3)
            .when(col("transformed_value") >= thresholds["low"], 4)
            .otherwise(5)
        )
        
        return df.withColumn("priority", priority_expr.cast(IntegerType()))
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules and status determination."""
        logger.info("Applying business rules")
        
        # Rule 1: Set status based on transformed value
        status_expr = (
            when(col("value").isNull(), "INVALID")
            .when(col("transformed_value") >= 750, "HIGH_VALUE")
            .when(col("transformed_value") >= 300, "MEDIUM_VALUE")
            .otherwise("LOW_VALUE")
        )
        
        df = df.withColumn("status", status_expr)
        
        # Rule 2: Override priority for high-value items
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, 1)
            .otherwise(col("priority"))
        )
        
        # Rule 3: Ensure category has value
        df = df.withColumn(
            "category",
            when(col("category").isNull() | (col("category") == ""), "UNCATEGORIZED")
            .otherwise(col("category"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional attributes."""
        logger.info("Enriching data")
        
        # Load enrichment config if available
        if self.config.get("enrichment", {}).get("enabled", True):
            # Add premium boost for PREMIUM category
            df = df.withColumn(
                "transformed_value",
                when(col("category") == "PREMIUM", 
                     col("transformed_value") * 1.2)
                .otherwise(col("transformed_value"))
            )
        
        return df
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """Add ETL metadata columns."""
        return (df
                .withColumn("etl_run_id", lit(self.run_id))
                .withColumn("processed_at", current_timestamp())
                .withColumn("processed_by", lit(current_user())))
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, error_messages)
        """
        logger.info("Validating transformed data")
        
        errors = []
        
        # Validation 1: Check for required fields
        null_ids = df.filter(col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null ID")
        
        null_names = df.filter(col("name").isNull() | (col("name") == "")).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null/empty name")
        
        # Validation 2: Check value ranges
        negative_values = df.filter(
            (col("value") < 0) | (col("transformed_value") < 0)
        ).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative values")
        
        # Validation 3: Check priority range
        invalid_priority = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        # Validation 4: Check for duplicates
        total_count = df.count()
        unique_count = df.select("id").distinct().count()
        if total_count != unique_count:
            errors.append(f"{total_count - unique_count} duplicate IDs found")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            logger.info("Validation passed")
        else:
            logger.error(f"Validation failed with {len(errors)} errors")
            for error in errors:
                logger.error(f"  - {error}")
        
        return is_valid, errors