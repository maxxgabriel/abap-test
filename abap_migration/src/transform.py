"""
Data Transformation Module
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, upper, trim, when, lit, current_timestamp,
    regexp_replace, coalesce
)
from pyspark.sql.types import IntegerType
from typing import Tuple, List

from src.logger import ETLLogger


class DataTransformer:
    """Handles data transformation logic"""
    
    def __init__(self, run_id: str):
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply transformations to source data
        
        Args:
            df: Source DataFrame
        
        Returns:
            Transformed DataFrame
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Starting transformation"
        )
        
        # Name normalization
        df = df.withColumn("name", upper(trim(col("name"))))
        df = df.withColumn("name", regexp_replace("name", "\\s+", " "))
        
        # Calculate derived values
        df = df.withColumn(
            "transformed_value",
            self._calculate_derived_value(col("value"), col("category"))
        )
        
        # Calculate priority
        df = df.withColumn(
            "priority",
            self._calculate_priority(col("value"), col("category"))
        )
        
        # Set status based on business rules
        df = self._apply_business_rules(df)
        
        # Add ETL metadata
        df = df.withColumn("etl_run_id", lit(self.run_id))
        df = df.withColumn("processed_at", current_timestamp())
        df = df.withColumn("processed_by", lit("pyspark_etl"))
        
        # Handle null categories
        df = df.withColumn(
            "category",
            coalesce(col("category"), lit("UNCATEGORIZED"))
        )
        
        count = df.count()
        self.logger.log_info(
            component="TRANSFORMER",
            message=f"Transformed {count} records"
        )
        
        return df
    
    def _calculate_derived_value(self, value_col, category_col):
        """Calculate transformed value based on category"""
        return when(category_col == "PREMIUM", value_col * 1.5) \
            .when(category_col == "VIP", value_col * 2.0) \
            .when(category_col == "STANDARD", value_col * 1.2) \
            .otherwise(value_col)
    
    def _calculate_priority(self, value_col, category_col):
        """Calculate priority based on value and category"""
        return when(value_col >= 1000, lit(1)) \
            .when(value_col >= 750, lit(2)) \
            .when(value_col >= 300, lit(3)) \
            .when(category_col == "VIP", lit(1)) \
            .otherwise(lit(5)) \
            .cast(IntegerType())
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to determine status"""
        df = df.withColumn(
            "status",
            when(col("value").isNull(), lit("INVALID"))
            .when(col("transformed_value") >= 750, lit("HIGH_VALUE"))
            .when(col("transformed_value") >= 300, lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data
        
        Args:
            df: DataFrame to validate
        
        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []
        
        # Check for required fields
        null_ids = df.filter(col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null ID")
        
        null_names = df.filter(col("name").isNull()).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null name")
        
        # Check for negative values
        negative_values = df.filter(col("value") < 0).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative values")
        
        # Check priority range
        invalid_priority = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.log_info(
                component="TRANSFORMER",
                message="Validation passed"
            )
        else:
            self.logger.log_error(
                component="TRANSFORMER",
                message="Validation failed",
                details="; ".join(errors)
            )
        
        return is_valid, errors