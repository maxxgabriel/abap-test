"""
ETL Data Transformation Module
Transforms extracted data by applying business rules, enrichment, and validation.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, current_timestamp,
    current_user, udf
)
from pyspark.sql.types import IntegerType, DecimalType, StringType
from typing import Tuple, List
import logging


class ETLTransformer:
    """Handles data transformation with business rules and validation."""
    
    def __init__(self, spark: SparkSession, run_id: str):
        """
        Initialize the transformer.
        
        Args:
            spark: Active SparkSession
            run_id: Unique run identifier
        """
        self.spark = spark
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply complete transformation pipeline.
        
        Args:
            df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        # Basic transformations
        df_transformed = df \
            .withColumn("name", upper(trim(col("name")))) \
            .withColumn("transformed_value", self._calculate_derived_value(col("value"), col("category"))) \
            .withColumn("status", lit("TRANSFORMED")) \
            .withColumn("priority", self._calculate_priority_udf(col("value"), col("category"))) \
            .withColumn("etl_run_id", lit(self.run_id)) \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("processed_by", current_user())
        
        # Apply category-specific rules
        df_transformed = self._apply_category_rules(df_transformed)
        
        # Apply business rules
        df_transformed = self.apply_business_rules(df_transformed)
        
        # Enrich data
        df_transformed = self.enrich_data(df_transformed)
        
        record_count = df_transformed.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return df_transformed
    
    def _calculate_derived_value(self, value_col, category_col):
        """Calculate derived/transformed value based on category."""
        return when(category_col == "PREMIUM", value_col * 1.5) \
               .when(category_col == "VIP", value_col * 2.0) \
               .when(category_col == "STANDARD", value_col * 1.2) \
               .otherwise(value_col)
    
    def _calculate_priority_udf(self, value_col, category_col):
        """Calculate priority based on value and category."""
        return when(value_col >= 1000, 1) \
               .when((value_col >= 500) & (category_col == "PREMIUM"), 1) \
               .when(value_col >= 500, 2) \
               .when(value_col >= 250, 3) \
               .when(value_col >= 100, 4) \
               .otherwise(5)
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """Apply category-specific transformation rules."""
        return df.withColumn(
            "category",
            when(col("category").isNull(), "UNCATEGORIZED")
            .otherwise(col("category"))
        )
    
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to transformed data.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            DataFrame with business rules applied
        """
        self.logger.info("Applying business rules")
        
        # Rule 1: Set status based on transformed value
        df = df.withColumn(
            "status",
            when(col("value").isNull(), "INVALID")
            .when(col("transformed_value") >= 750, "HIGH_VALUE")
            .when(col("transformed_value") >= 300, "MEDIUM_VALUE")
            .otherwise("LOW_VALUE")
        )
        
        # Rule 2: Priority override for high-value items
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, 1)
            .otherwise(col("priority"))
        )
        
        # Rule 3: Name normalization - remove multiple spaces
        df = df.withColumn(
            "name",
            trim(regexp_replace(col("name"), "\\s+", " "))
        )
        
        return df
    
    def enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional calculated fields.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Enriched DataFrame
        """
        self.logger.info("Enriching data")
        
        # Premium category gets additional 20% boost
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM",
                 col("transformed_value") * 1.2)
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, validation_errors)
        """
        self.logger.info("Validating data")
        validation_errors = []
        
        # Check for required fields
        null_ids = df.filter(col("id").isNull()).count()
        if null_ids > 0:
            validation_errors.append(f"{null_ids} records with missing ID")
        
        null_names = df.filter(col("name").isNull()).count()
        if null_names > 0:
            validation_errors.append(f"{null_names} records with missing name")
        
        # Check for positive values
        negative_values = df.filter(col("value") < 0).count()
        if negative_values > 0:
            validation_errors.append(f"{negative_values} records with negative values")
        
        # Check priority range
        invalid_priority = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority > 0:
            validation_errors.append(f"{invalid_priority} records with invalid priority")
        
        # Check for duplicates
        total_count = df.count()
        unique_count = df.select("id").distinct().count()
        if total_count != unique_count:
            validation_errors.append(f"{total_count - unique_count} duplicate IDs found")
        
        is_valid = len(validation_errors) == 0
        
        if is_valid:
            self.logger.info("Validation passed")
        else:
            self.logger.warning(f"Validation failed with {len(validation_errors)} errors")
        
        return is_valid, validation_errors
    
    def calculate_priority(self, value: float, category: str) -> int:
        """
        Calculate priority for a single record.
        
        Args:
            value: Record value
            category: Record category
            
        Returns:
            Priority (1-5)
        """
        if value >= 1000:
            return 1
        elif value >= 500 and category == "PREMIUM":
            return 1
        elif value >= 500:
            return 2
        elif value >= 250:
            return 3
        elif value >= 100:
            return 4
        else:
            return 5