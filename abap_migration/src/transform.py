"""
ETL Data Transformation Module
Transforms extracted data by applying business rules, enrichment, and validation.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, upper, trim, when, lit, current_timestamp, 
    regexp_replace, concat_ws, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import List, Tuple
from datetime import datetime
from src.logger import ETLLogger


class ETLTransformer:
    """Handles data transformation and validation."""
    
    # Define schema for transformed data
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
        StructField("processed_by", StringType(), True)
    ])
    
    def __init__(self, run_id: str, config: dict = None):
        """
        Initialize the transformer.
        
        Args:
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.run_id = run_id
        self.config = config or {}
        self.logger = ETLLogger.get_instance()
        
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Transform source data with all business rules.
        
        Args:
            source_df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Starting transformation"
        )
        
        # Apply transformations
        df = source_df
        
        # Basic transformations
        df = self._apply_basic_transformations(df)
        
        # Calculate derived values
        df = self._calculate_derived_values(df)
        
        # Apply business rules
        df = self._apply_business_rules(df)
        
        # Apply category-specific rules
        df = self._apply_category_rules(df)
        
        # Enrich data
        df = self._enrich_data(df)
        
        # Add metadata
        df = self._add_metadata(df)
        
        record_count = df.count()
        self.logger.log_info(
            component="TRANSFORMER",
            message=f"Transformed {record_count} records"
        )
        
        return df
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic data cleansing transformations."""
        return df.withColumn(
            "name", 
            upper(trim(regexp_replace(col("name"), "\\s+", " ")))
        )
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate derived values based on business logic.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with calculated values
        """
        # Get transformation multipliers from config
        transform_config = self.config.get("transformation", {})
        premium_mult = transform_config.get("premium_multiplier", 1.5)
        standard_mult = transform_config.get("standard_multiplier", 1.2)
        
        # Calculate transformed value based on category
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("value") * premium_mult)
            .when(col("category") == "STANDARD", col("value") * standard_mult)
            .when(col("category") == "VIP", col("value") * 1.8)
            .otherwise(col("value"))
        )
        
        # Calculate priority
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, 1)
            .when(col("transformed_value") >= 750, 2)
            .when(col("transformed_value") >= 300, 3)
            .when(col("transformed_value") >= 100, 4)
            .otherwise(5)
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to the data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with business rules applied
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Applying business rules"
        )
        
        # Rule 1: Set status based on value
        df = df.withColumn(
            "status",
            when(col("value").isNull(), "INVALID")
            .when(col("transformed_value") >= 750, "HIGH_VALUE")
            .when(col("transformed_value") >= 300, "MEDIUM_VALUE")
            .otherwise("LOW_VALUE")
        )
        
        # Rule 2: Priority override for high value items
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, 1)
            .otherwise(col("priority"))
        )
        
        # Rule 3: Category validation - set default for null
        df = df.withColumn(
            "category",
            coalesce(col("category"), lit("UNCATEGORIZED"))
        )
        
        return df
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply category-specific transformation rules.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with category rules applied
        """
        # Premium category gets additional boost
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("transformed_value") * 1.2)
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional information.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Enriched DataFrame
        """
        # Add enrichment logic based on config
        # For example: add region, department, etc.
        return df
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add ETL metadata fields.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with metadata
        """
        import os
        
        df = df.withColumn("etl_run_id", lit(self.run_id))
        df = df.withColumn("processed_at", current_timestamp())
        df = df.withColumn("processed_by", lit(os.environ.get("USER", "system")))
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        errors = []
        
        # Validation rule 1: ID is required
        null_id_count = df.filter(col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with missing ID")
        
        # Validation rule 2: Name is required
        null_name_count = df.filter(col("name").isNull()).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with missing name")
        
        # Validation rule 3: Value must be positive
        negative_value_count = df.filter(col("value") < 0).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative values")
        
        # Validation rule 4: Priority must be 1-5
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if not is_valid:
            self.logger.log_warning(
                component="TRANSFORMER",
                message=f"Validation failed with {len(errors)} errors"
            )
        
        return is_valid, errors