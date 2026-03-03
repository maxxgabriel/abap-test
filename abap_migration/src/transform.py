"""
Data Transformation Module
Handles business logic and data transformations.
"""

from typing import Dict, List, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, current_timestamp,
    when, lit, udf, round as spark_round
)
from pyspark.sql.types import IntegerType, StringType

from src.logger import ETLLogger
from src.config import ConfigManager


class Transformer:
    """Handles data transformation and business rules."""
    
    def __init__(self, spark: SparkSession, run_id: str):
        """
        Initialize transformer.
        
        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
        """
        self.spark = spark
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
        self.config = ConfigManager.get_instance()
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply transformations to source data.
        
        Args:
            source_df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Starting transformation"
        )
        
        # Apply transformations
        transformed_df = source_df \
            .withColumn("name", upper(trim(col("name")))) \
            .withColumn("name", regexp_replace(col("name"), r"\s+", " ")) \
            .withColumn(
                "transformed_value",
                self._calculate_derived_values(col("value"), col("category"))
            ) \
            .withColumn(
                "priority",
                self._calculate_priority(col("value"), col("category"))
            ) \
            .withColumn("status", lit("TRANSFORMED")) \
            .withColumn("etl_run_id", lit(self.run_id)) \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("processed_by", lit("spark_etl"))
        
        # Apply business rules
        transformed_df = self._apply_business_rules(transformed_df)
        
        # Enrich data
        transformed_df = self._enrich_data(transformed_df)
        
        record_count = transformed_df.count()
        self.logger.log_info(
            component="TRANSFORMER",
            message=f"Transformed {record_count} records"
        )
        
        return transformed_df.select(
            "id",
            "name",
            "value",
            "transformed_value",
            "status",
            "category",
            "priority",
            "etl_run_id",
            "processed_at",
            "processed_by"
        )
    
    def _calculate_derived_values(self, value_col, category_col):
        """Calculate derived/transformed values based on category."""
        return when(category_col == "PREMIUM", value_col * 1.5) \
            .when(category_col == "STANDARD", value_col * 1.2) \
            .when(category_col == "VIP", value_col * 1.8) \
            .otherwise(value_col)
    
    def _calculate_priority(self, value_col, category_col):
        """Calculate priority based on value and category."""
        return when(value_col >= 1000, lit(1)) \
            .when((value_col >= 750) | (category_col == "VIP"), lit(2)) \
            .when(value_col >= 500, lit(3)) \
            .when(value_col >= 250, lit(4)) \
            .otherwise(lit(5))
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to transformed data."""
        self.logger.log_info(
            component="TRANSFORMER",
            message="Applying business rules"
        )
        
        # Rule 1: Set status based on value
        df = df.withColumn(
            "status",
            when(col("value").isNull(), lit("INVALID"))
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
        
        # Rule 3: Category validation
        df = df.withColumn(
            "category",
            when(col("category").isNull(), lit("UNCATEGORIZED"))
            .otherwise(col("category"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional calculated fields."""
        # Load enrichment configuration
        premium_multiplier = self.config.get("premium_multiplier", 1.2)
        
        # Apply enrichment
        df = df.withColumn(
            "transformed_value",
            when(
                col("category") == "PREMIUM",
                spark_round(col("transformed_value") * premium_multiplier, 2)
            ).otherwise(col("transformed_value"))
        )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Dict[str, Any]:
        """
        Validate transformed data.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Dictionary with validation results
        """
        errors = []
        
        # Check for null IDs
        null_id_count = df.filter(col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with null ID")
        
        # Check for null names
        null_name_count = df.filter(col("name").isNull()).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with null name")
        
        # Check for negative values
        negative_value_count = df.filter(
            (col("value") < 0) | (col("transformed_value") < 0)
        ).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative values")
        
        # Check priority range
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if not is_valid:
            self.logger.log_warning(
                component="TRANSFORMER",
                message=f"Validation failed - {len(errors)} issues found"
            )
        
        return {
            "is_valid": is_valid,
            "errors": errors
        }