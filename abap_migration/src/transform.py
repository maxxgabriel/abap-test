"""
ETL Data Transformation Module

This module handles data transformations including business rules,
data enrichment, validation, and derived calculations.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, current_timestamp,
    current_user, coalesce
)
from pyspark.sql.types import DecimalType, IntegerType
from typing import List, Tuple, Dict, Any
import logging

logger = logging.getLogger(__name__)


class ETLTransformer:
    """
    Handles data transformation, validation, and enrichment.
    """
    
    def __init__(self, run_id: str):
        """
        Initialize the transformer.
        
        Args:
            run_id: Unique run identifier
        """
        self.run_id = run_id
        logger.info(f"ETLTransformer initialized - Run ID: {run_id}")
    
    def transform_data(self, df: DataFrame, config: Dict[str, Any]) -> DataFrame:
        """
        Main transformation method that applies all transformation logic.
        
        Args:
            df: Source DataFrame
            config: Configuration dictionary
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Starting transformation")
        
        # Step 1: Basic transformations
        df_transformed = self._apply_basic_transformations(df)
        
        # Step 2: Calculate derived values
        df_transformed = self._calculate_derived_values(df_transformed, config)
        
        # Step 3: Calculate priority
        df_transformed = self._calculate_priority(df_transformed)
        
        # Step 4: Apply business rules
        df_transformed = self._apply_business_rules(df_transformed)
        
        # Step 5: Apply category-specific rules
        df_transformed = self._apply_category_rules(df_transformed, config)
        
        # Step 6: Enrich data
        df_transformed = self._enrich_data(df_transformed, config)
        
        # Step 7: Add ETL metadata
        df_transformed = self._add_etl_metadata(df_transformed)
        
        record_count = df_transformed.count()
        logger.info(f"Transformed {record_count} records")
        
        return df_transformed
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply basic data cleaning and formatting.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        logger.info("Applying basic transformations")
        
        return df.select(
            col("id"),
            # Clean and normalize name
            upper(trim(regexp_replace(col("name"), "\\s+", " "))).alias("name"),
            col("value"),
            col("status"),
            coalesce(col("category"), lit("UNCATEGORIZED")).alias("category"),
            col("source_system"),
            col("created_at"),
            col("created_by"),
            col("changed_at"),
            col("changed_by")
        )
    
    def _calculate_derived_values(self, df: DataFrame, config: Dict[str, Any]) -> DataFrame:
        """
        Calculate transformed values based on business logic.
        
        Args:
            df: Input DataFrame
            config: Configuration with multipliers
            
        Returns:
            DataFrame with transformed_value column
        """
        logger.info("Calculating derived values")
        
        # Get multipliers from config
        premium_multiplier = float(config.get("premium_multiplier", 1.5))
        standard_multiplier = float(config.get("standard_multiplier", 1.2))
        
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("value") * premium_multiplier)
            .when(col("category") == "STANDARD", col("value") * standard_multiplier)
            .otherwise(col("value"))
        )
        
        return df
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """
        Calculate priority based on value and category.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with priority column
        """
        logger.info("Calculating priority")
        
        df = df.withColumn(
            "priority",
            when((col("transformed_value") >= 1000) | (col("category") == "VIP"), lit(1))
            .when(col("transformed_value") >= 750, lit(2))
            .when(col("transformed_value") >= 500, lit(3))
            .when(col("transformed_value") >= 250, lit(4))
            .otherwise(lit(5))
            .cast(IntegerType())
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to set status and classifications.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with business rules applied
        """
        logger.info("Applying business rules")
        
        df = df.withColumn(
            "status",
            when(col("value").isNull() | (col("value") == 0), lit("INVALID"))
            .when(col("transformed_value") >= 750, lit("HIGH_VALUE"))
            .when(col("transformed_value") >= 300, lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )
        
        return df
    
    def _apply_category_rules(self, df: DataFrame, config: Dict[str, Any]) -> DataFrame:
        """
        Apply category-specific transformation rules.
        
        Args:
            df: Input DataFrame
            config: Configuration
            
        Returns:
            DataFrame with category rules applied
        """
        logger.info("Applying category-specific rules")
        
        # Override priority for premium and VIP categories
        df = df.withColumn(
            "priority",
            when((col("category") == "PREMIUM") & (col("transformed_value") >= 1000), lit(1))
            .when(col("category") == "VIP", lit(1))
            .otherwise(col("priority"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame, config: Dict[str, Any]) -> DataFrame:
        """
        Enrich data with additional calculated fields and lookups.
        
        Args:
            df: Input DataFrame
            config: Configuration
            
        Returns:
            Enriched DataFrame
        """
        logger.info("Enriching data")
        
        # Add value tier classification
        df = df.withColumn(
            "value_tier",
            when(col("transformed_value") >= 1000, lit("TIER_1"))
            .when(col("transformed_value") >= 500, lit("TIER_2"))
            .when(col("transformed_value") >= 250, lit("TIER_3"))
            .otherwise(lit("TIER_4"))
        )
        
        # Add processing flags
        df = df.withColumn(
            "requires_review",
            when((col("status") == "HIGH_VALUE") & (col("priority") == 1), lit(True))
            .otherwise(lit(False))
        )
        
        return df
    
    def _add_etl_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add ETL processing metadata to records.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with ETL metadata
        """
        logger.info("Adding ETL metadata")
        
        return df \
            .withColumn("etl_run_id", lit(self.run_id)) \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("processed_by", lit("spark_etl"))
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data against business rules.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        logger.info("Validating transformed data")
        
        errors = []
        
        # Validation 1: Check for null IDs
        null_id_count = df.filter(col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with null IDs")
        
        # Validation 2: Check for null names
        null_name_count = df.filter(col("name").isNull() | (col("name") == "")).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with null or empty names")
        
        # Validation 3: Check for negative values
        negative_value_count = df.filter((col("value") < 0) | (col("transformed_value") < 0)).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative values")
        
        # Validation 4: Check priority range
        invalid_priority_count = df.filter((col("priority") < 1) | (col("priority") > 5)).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        # Validation 5: Check for empty categories
        empty_category_count = df.filter(col("category").isNull() | (col("category") == "")).count()
        if empty_category_count > 0:
            errors.append(f"{empty_category_count} records with empty categories")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            logger.info("Validation passed")
        else:
            logger.warning(f"Validation failed with {len(errors)} errors")
            for error in errors:
                logger.warning(f"  - {error}")
        
        return is_valid, errors


def create_transformer(run_id: str) -> ETLTransformer:
    """
    Factory function to create an ETLTransformer instance.
    
    Args:
        run_id: Run identifier
        
    Returns:
        ETLTransformer instance
    """
    return ETLTransformer(run_id)