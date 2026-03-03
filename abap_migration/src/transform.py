"""
ETL Data Transformation Module
Applies business rules, value mappings, data validation, and priority/category classification.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, current_timestamp,
    current_user, coalesce, udf
)
from typing import Dict, Any, List, Tuple
from decimal import Decimal
import logging


class ETLTransformer:
    """Handles data transformation and business rule application."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the transformer.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def get_transformed_schema(self) -> StructType:
        """Define the schema for transformed data."""
        return StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("value", DecimalType(15, 2), False),
            StructField("transformed_value", DecimalType(15, 2), True),
            StructField("status", StringType(), True),
            StructField("category", StringType(), True),
            StructField("priority", IntegerType(), True),
            StructField("etl_run_id", StringType(), True),
            StructField("processed_at", TimestampType(), True),
            StructField("processed_by", StringType(), True),
        ])
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply all transformations to the data.
        
        Args:
            df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info(f"Starting transformation for run {self.run_id}")
        
        try:
            # Step 1: Basic transformations
            df = self._apply_basic_transformations(df)
            
            # Step 2: Calculate derived values
            df = self._calculate_derived_values(df)
            
            # Step 3: Apply category-specific rules
            df = self._apply_category_rules(df)
            
            # Step 4: Calculate priority
            df = self._calculate_priority(df)
            
            # Step 5: Apply business rules
            df = self._apply_business_rules(df)
            
            # Step 6: Enrich data
            df = self._enrich_data(df)
            
            # Step 7: Add metadata
            df = self._add_metadata(df)
            
            record_count = df.count()
            self.logger.info(f"Transformed {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}")
            raise
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic data cleaning and normalization."""
        self.logger.info("Applying basic transformations")
        
        # Normalize name: uppercase, trim, remove extra spaces
        df = df.withColumn("name",
                          upper(trim(regexp_replace(col("name"), "\\s+", " "))))
        
        # Ensure value is not null (replace with 0)
        df = df.withColumn("value", coalesce(col("value"), lit(0)))
        
        return df
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """Calculate transformed values based on category and value."""
        self.logger.info("Calculating derived values")
        
        # Get multipliers from config
        multipliers = self.config["transformation"]["value_multipliers"]
        
        # Apply category-specific multipliers
        transformed_value = col("value")
        for category, multiplier in multipliers.items():
            transformed_value = when(
                col("category") == category,
                col("value") * multiplier
            ).otherwise(transformed_value)
        
        df = df.withColumn("transformed_value", transformed_value)
        
        return df
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """Apply category-specific business rules."""
        self.logger.info("Applying category rules")
        
        # Validate and normalize categories
        valid_categories = self.config["transformation"]["valid_categories"]
        
        df = df.withColumn(
            "category",
            when(col("category").isin(valid_categories), col("category"))
            .otherwise(lit("UNCATEGORIZED"))
        )
        
        # Apply premium category bonus
        if self.config["transformation"]["premium_bonus_enabled"]:
            bonus = self.config["transformation"]["premium_bonus"]
            df = df.withColumn(
                "transformed_value",
                when(col("category") == "PREMIUM",
                     col("transformed_value") * (1 + bonus))
                .otherwise(col("transformed_value"))
            )
        
        return df
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """Calculate priority based on value and category."""
        self.logger.info("Calculating priority")
        
        thresholds = self.config["transformation"]["priority_thresholds"]
        
        # Priority based on transformed_value
        priority = (
            when(col("transformed_value") >= thresholds["critical"], lit(1))
            .when(col("transformed_value") >= thresholds["high"], lit(2))
            .when(col("transformed_value") >= thresholds["medium"], lit(3))
            .when(col("transformed_value") >= thresholds["low"], lit(4))
            .otherwise(lit(5))
        )
        
        # Override for VIP category
        priority = when(col("category") == "VIP", lit(1)).otherwise(priority)
        
        df = df.withColumn("priority", priority)
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply comprehensive business rules."""
        self.logger.info("Applying business rules")
        
        # Rule 1: Set status based on value
        df = df.withColumn(
            "status",
            when(col("value").isNull() | (col("value") == 0), lit("INVALID"))
            .when(col("transformed_value") >= 750, lit("HIGH_VALUE"))
            .when(col("transformed_value") >= 300, lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )
        
        # Rule 2: Priority override for high value items
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, lit(1))
            .otherwise(col("priority"))
        )
        
        # Rule 3: Mark suspicious values
        if self.config["transformation"]["flag_outliers"]:
            max_threshold = self.config["transformation"]["max_value_threshold"]
            df = df.withColumn(
                "status",
                when(col("transformed_value") > max_threshold, lit("SUSPICIOUS"))
                .otherwise(col("status"))
            )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional attributes."""
        self.logger.info("Enriching data")
        
        # Add value range classification
        df = df.withColumn(
            "value_range",
            when(col("transformed_value") < 100, lit("SMALL"))
            .when(col("transformed_value") < 500, lit("MEDIUM"))
            .when(col("transformed_value") < 1000, lit("LARGE"))
            .otherwise(lit("EXTRA_LARGE"))
        )
        
        # Add category tier
        df = df.withColumn(
            "category_tier",
            when(col("category").isin(["PREMIUM", "VIP"]), lit("TIER_1"))
            .when(col("category") == "STANDARD", lit("TIER_2"))
            .otherwise(lit("TIER_3"))
        )
        
        return df
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """Add processing metadata."""
        return df.withColumn("etl_run_id", lit(self.run_id)) \
                 .withColumn("processed_at", current_timestamp()) \
                 .withColumn("processed_by", lit(current_user()))
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        self.logger.info("Validating transformed data")
        errors = []
        
        # Validation 1: Check for required fields
        null_ids = df.filter(col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null ID")
        
        null_names = df.filter(col("name").isNull()).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null name")
        
        # Validation 2: Check for invalid values
        invalid_values = df.filter((col("value") < 0) | (col("transformed_value") < 0)).count()
        if invalid_values > 0:
            errors.append(f"{invalid_values} records with negative values")
        
        # Validation 3: Check priority range
        invalid_priority = df.filter((col("priority") < 1) | (col("priority") > 5)).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        # Validation 4: Check categories
        valid_categories = self.config["transformation"]["valid_categories"] + ["UNCATEGORIZED"]
        invalid_categories = df.filter(~col("category").isin(valid_categories)).count()
        if invalid_categories > 0:
            errors.append(f"{invalid_categories} records with invalid category")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Validation passed")
        else:
            self.logger.warning(f"Validation failed: {'; '.join(errors)}")
        
        return is_valid, errors
    
    def apply_value_mappings(self, df: DataFrame, mapping_dict: Dict[str, str]) -> DataFrame:
        """
        Apply value mappings to specified columns.
        
        Args:
            df: DataFrame to transform
            mapping_dict: Dictionary of column -> mapping rules
            
        Returns:
            DataFrame with mappings applied
        """
        for column, mappings in mapping_dict.items():
            if column in df.columns:
                mapping_expr = col(column)
                for old_value, new_value in mappings.items():
                    mapping_expr = when(col(column) == old_value, lit(new_value)).otherwise(mapping_expr)
                df = df.withColumn(column, mapping_expr)
        
        return df