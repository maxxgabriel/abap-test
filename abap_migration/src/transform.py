"""
PySpark ETL Transformer Module
Transforms data with business rules, enrichment, and validation.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, current_timestamp,
    current_user, udf
)
from pyspark.sql.types import IntegerType, DecimalType
from typing import List, Tuple
import logging

logger = logging.getLogger(__name__)


class ETLTransformer:
    """Handles data transformation logic"""
    
    def __init__(self, config: dict, run_id: str):
        """
        Initialize transformer with configuration
        
        Args:
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.config = config
        self.run_id = run_id
        self.rules = config.get('transformation_rules', {})
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Main transformation method - applies all transformations
        
        Args:
            df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Starting transformation")
        
        # Basic transformations
        df_transformed = df.select(
            col("id"),
            upper(trim(regexp_replace(col("name"), r'\s+', ' '))).alias("name"),
            col("value"),
            self.calculate_derived_value(col("value"), col("category")).alias("transformed_value"),
            lit("TRANSFORMED").alias("status"),
            col("category"),
            self.calculate_priority(col("value"), col("category")).alias("priority"),
            lit(self.run_id).alias("etl_run_id"),
            current_timestamp().alias("processed_at"),
            current_user().alias("processed_by")
        )
        
        # Apply business rules
        df_transformed = self.apply_business_rules(df_transformed)
        
        # Enrich data
        df_transformed = self.enrich_data(df_transformed)
        
        record_count = df_transformed.count()
        logger.info(f"Transformed {record_count} records")
        
        return df_transformed
    
    def calculate_derived_value(self, value_col, category_col):
        """
        Calculate derived value based on category
        
        Args:
            value_col: Value column
            category_col: Category column
            
        Returns:
            Column expression for derived value
        """
        premium_multiplier = float(self.rules.get('premium_multiplier', 1.5))
        vip_multiplier = float(self.rules.get('vip_multiplier', 2.0))
        
        return when(category_col == "PREMIUM", value_col * premium_multiplier) \
            .when(category_col == "VIP", value_col * vip_multiplier) \
            .otherwise(value_col)
    
    def calculate_priority(self, value_col, category_col):
        """
        Calculate priority based on value and category
        
        Args:
            value_col: Value column
            category_col: Category column
            
        Returns:
            Column expression for priority (1-5)
        """
        return when(value_col >= 1000, lit(1)) \
            .when((value_col >= 750) & (category_col == "VIP"), lit(1)) \
            .when(value_col >= 750, lit(2)) \
            .when(value_col >= 500, lit(3)) \
            .when(value_col >= 250, lit(4)) \
            .otherwise(lit(5))
    
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to transformed data
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            DataFrame with business rules applied
        """
        logger.info("Applying business rules")
        
        df = df.withColumn(
            "status",
            when(col("value").isNull(), lit("INVALID"))
            .when(col("transformed_value") >= 750, lit("HIGH_VALUE"))
            .when(col("transformed_value") >= 300, lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )
        
        # Priority override for high value items
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, lit(1))
            .otherwise(col("priority"))
        )
        
        # Category validation
        df = df.withColumn(
            "category",
            when(col("category").isNull(), lit("UNCATEGORIZED"))
            .otherwise(col("category"))
        )
        
        return df
    
    def enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional attributes
        
        Args:
            df: DataFrame to enrich
            
        Returns:
            Enriched DataFrame
        """
        logger.info("Enriching data")
        
        # Add calculated fields based on category
        df = df.withColumn(
            "value_tier",
            when(col("transformed_value") >= 1000, lit("PLATINUM"))
            .when(col("transformed_value") >= 500, lit("GOLD"))
            .when(col("transformed_value") >= 250, lit("SILVER"))
            .otherwise(lit("BRONZE"))
        )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        logger.info("Validating transformed data")
        errors = []
        
        # Check for null IDs
        null_id_count = df.filter(col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with null ID")
        
        # Check for null names
        null_name_count = df.filter(col("name").isNull()).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with null name")
        
        # Check for invalid values
        invalid_value_count = df.filter(col("value") < 0).count()
        if invalid_value_count > 0:
            errors.append(f"{invalid_value_count} records with negative values")
        
        # Check priority range
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            logger.info("Data validation passed")
        else:
            logger.error(f"Data validation failed: {errors}")
        
        return is_valid, errors