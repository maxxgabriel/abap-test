"""
Data transformation module for PySpark ETL pipeline.
Applies business rules, enrichment, and data validation.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, 
    current_timestamp, coalesce, round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import Dict, Any, Tuple, List
import logging


class DataTransformer:
    """Transforms extracted data according to business rules."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the transformer.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
    
    def get_target_schema(self) -> StructType:
        """Define the schema for transformed data."""
        return StructType([
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
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply all transformations to the data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        # Basic transformations
        df_transformed = df.select(
            col("id"),
            upper(trim(regexp_replace(col("name"), "\\s+", " "))).alias("name"),
            col("value"),
            self._calculate_transformed_value(col("value"), col("category")).alias("transformed_value"),
            lit("TRANSFORMED").alias("status"),
            coalesce(col("category"), lit("UNCATEGORIZED")).alias("category"),
            self._calculate_priority(col("value"), col("category")).alias("priority"),
            lit(self.run_id).alias("etl_run_id"),
            current_timestamp().alias("processed_at"),
            lit(self.config.get("processing", {}).get("user", "system")).alias("processed_by")
        )
        
        # Apply business rules
        df_transformed = self._apply_business_rules(df_transformed)
        
        # Enrich data
        df_transformed = self._enrich_data(df_transformed)
        
        record_count = df_transformed.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return df_transformed
    
    def _calculate_transformed_value(self, value_col, category_col):
        """Calculate derived value based on business logic."""
        multipliers = self.config.get("business_rules", {}).get("category_multipliers", {})
        
        # Apply category-specific multipliers
        result = value_col
        for category, multiplier in multipliers.items():
            result = when(
                category_col == category,
                value_col * multiplier
            ).otherwise(result)
        
        return spark_round(result, 2)
    
    def _calculate_priority(self, value_col, category_col):
        """Calculate priority based on value and category."""
        priority_thresholds = self.config.get("business_rules", {}).get("priority_thresholds", {})
        
        return (
            when(value_col >= priority_thresholds.get("critical", 1000), lit(1))
            .when(value_col >= priority_thresholds.get("high", 750), lit(2))
            .when(value_col >= priority_thresholds.get("medium", 300), lit(3))
            .when(value_col >= priority_thresholds.get("low", 100), lit(4))
            .otherwise(lit(5))
        )
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to the data."""
        self.logger.info("Applying business rules")
        
        value_thresholds = self.config.get("business_rules", {}).get("value_thresholds", {})
        
        # Rule 1: Set status based on transformed value
        df = df.withColumn(
            "status",
            when(col("value").isNull(), lit("INVALID"))
            .when(col("transformed_value") >= value_thresholds.get("high", 750), lit("HIGH_VALUE"))
            .when(col("transformed_value") >= value_thresholds.get("medium", 300), lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )
        
        # Rule 2: Priority override for high value items
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= value_thresholds.get("priority_override", 1000), lit(1))
            .otherwise(col("priority"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional calculated fields."""
        self.logger.info("Enriching data")
        
        # Apply premium category bonus if configured
        premium_bonus = self.config.get("business_rules", {}).get("premium_bonus", 1.2)
        
        df = df.withColumn(
            "transformed_value",
            when(
                col("category") == "PREMIUM",
                spark_round(col("transformed_value") * premium_bonus, 2)
            ).otherwise(col("transformed_value"))
        )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        self.logger.info("Validating data")
        errors = []
        
        # Validation 1: Check for null IDs
        null_id_count = df.filter(col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with null ID")
        
        # Validation 2: Check for null names
        null_name_count = df.filter(col("name").isNull()).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with null name")
        
        # Validation 3: Check for negative values
        negative_value_count = df.filter(
            (col("value") < 0) | (col("transformed_value") < 0)
        ).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative values")
        
        # Validation 4: Check priority range
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        # Validation 5: Check for empty categories
        empty_category_count = df.filter(col("category").isNull()).count()
        if empty_category_count > 0:
            errors.append(f"{empty_category_count} records with null category")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Data validation passed")
        else:
            self.logger.error(f"Data validation failed: {'; '.join(errors)}")
        
        return is_valid, errors