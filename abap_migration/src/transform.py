"""
Transform module for Delta Lake ETL pipeline.
Replaces ABAP transformer with PySpark DataFrame transformations.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, current_timestamp,
    lit, udf, coalesce
)
from pyspark.sql.types import IntegerType, DecimalType, StringType
from typing import Dict, Any, List, Tuple
import logging

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transform data using PySpark DataFrame operations."""
    
    def __init__(self, spark: SparkSession, run_id: str, config: Dict[str, Any]):
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
        
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply transformations to source data.
        
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
        
        # Apply category-specific rules
        df = self._apply_category_rules(df)
        
        # Apply business rules
        df = self._apply_business_rules(df)
        
        # Enrich data
        df = self._enrich_data(df)
        
        # Add ETL metadata
        df = df.withColumn("etl_run_id", lit(self.run_id)) \
               .withColumn("processed_at", current_timestamp()) \
               .withColumn("processed_by", lit("pyspark_etl"))
        
        logger.info(f"Transformed {df.count()} records")
        return df
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic field transformations."""
        return (df
                .withColumn("name", upper(trim(col("name"))))
                .withColumn("name", regexp_replace(col("name"), "\\s+", " "))
                .withColumn("status", lit("TRANSFORMED")))
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate transformed values based on category.
        
        Premium: value * 1.5
        Standard: value * 1.2
        VIP: value * 2.0
        Others: value * 1.0
        """
        multiplier_map = self.config.get('category_multipliers', {
            'PREMIUM': 1.5,
            'STANDARD': 1.2,
            'VIP': 2.0,
            'BASIC': 1.0,
            'TRIAL': 1.0
        })
        
        # Build WHEN clause for each category
        transform_expr = when(col("category") == "PREMIUM", col("value") * 1.5)
        for category, multiplier in multiplier_map.items():
            if category != "PREMIUM":
                transform_expr = transform_expr.when(
                    col("category") == category,
                    col("value") * multiplier
                )
        transform_expr = transform_expr.otherwise(col("value"))
        
        return df.withColumn("transformed_value", transform_expr.cast(DecimalType(15, 2)))
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """
        Calculate priority based on value and category.
        
        Priority levels:
        1 (Highest): transformed_value >= 1000 OR category = VIP
        2 (High): transformed_value >= 750
        3 (Medium): transformed_value >= 300
        4 (Low): transformed_value >= 100
        5 (Lowest): transformed_value < 100
        """
        priority_expr = (
            when((col("transformed_value") >= 1000) | (col("category") == "VIP"), 1)
            .when(col("transformed_value") >= 750, 2)
            .when(col("transformed_value") >= 300, 3)
            .when(col("transformed_value") >= 100, 4)
            .otherwise(5)
        )
        
        return df.withColumn("priority", priority_expr.cast(IntegerType()))
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """Apply category-specific business rules."""
        # Handle missing categories
        df = df.withColumn(
            "category",
            coalesce(col("category"), lit("UNCATEGORIZED"))
        )
        
        # Apply premium bonus
        premium_multiplier = self.config.get('premium_multiplier', 1.2)
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM",
                 col("transformed_value") * premium_multiplier)
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to transformed data."""
        # Rule 1: Set status based on value
        status_expr = (
            when(col("value").isNull(), "INVALID")
            .when(col("transformed_value") >= 750, "HIGH_VALUE")
            .when(col("transformed_value") >= 300, "MEDIUM_VALUE")
            .otherwise("LOW_VALUE")
        )
        df = df.withColumn("status", status_expr)
        
        # Rule 2: Priority override for high value items
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, 1)
            .otherwise(col("priority"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional computed fields."""
        # Add value band classification
        df = df.withColumn(
            "value_band",
            when(col("transformed_value") >= 1000, "PLATINUM")
            .when(col("transformed_value") >= 500, "GOLD")
            .when(col("transformed_value") >= 200, "SILVER")
            .otherwise("BRONZE")
        )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []
        
        # Rule 1: ID is required
        null_id_count = df.filter(col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with null ID")
        
        # Rule 2: Name is required
        null_name_count = df.filter(col("name").isNull()).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with null name")
        
        # Rule 3: Value must be positive
        negative_value_count = df.filter(col("value") < 0).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative value")
        
        # Rule 4: Priority must be 1-5
        invalid_priority = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            logger.info("Data validation passed")
        else:
            logger.error(f"Data validation failed: {errors}")
        
        return is_valid, errors