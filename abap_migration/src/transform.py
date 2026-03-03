"""
Data transformation module for PySpark ETL framework.
Applies business rules, enrichment, and data quality validations.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, current_timestamp,
    lit, coalesce, concat_ws, round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from typing import Dict, Any, List, Tuple
import logging


class DataTransformer:
    """Transforms raw data according to business rules and quality standards."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the data transformer.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def get_output_schema(self) -> StructType:
        """Define the schema for transformed data."""
        return StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("value", DecimalType(15, 2), True),
            StructField("transformed_value", DecimalType(15, 2), True),
            StructField("status", StringType(), True),
            StructField("category", StringType(), True),
            StructField("priority", IntegerType(), True),
            StructField("etl_run_id", StringType(), False),
            StructField("processed_at", TimestampType(), False),
            StructField("processed_by", StringType(), False)
        ])
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply all transformation steps to the input DataFrame.
        
        Args:
            df: Input DataFrame from extraction
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting data transformation")
        
        # Step 1: Basic transformations
        df_transformed = self._apply_basic_transformations(df)
        
        # Step 2: Calculate derived values
        df_transformed = self._calculate_derived_values(df_transformed)
        
        # Step 3: Apply business rules
        df_transformed = self._apply_business_rules(df_transformed)
        
        # Step 4: Enrich data
        df_transformed = self._enrich_data(df_transformed)
        
        # Step 5: Add metadata
        df_transformed = self._add_metadata(df_transformed)
        
        record_count = df_transformed.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return df_transformed
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic data cleaning and normalization."""
        return df.select(
            col("id"),
            upper(trim(regexp_replace(col("name"), "\\s+", " "))).alias("name"),
            col("value").cast(DecimalType(15, 2)),
            col("status"),
            coalesce(col("category"), lit("UNCATEGORIZED")).alias("category"),
            col("source_system"),
            col("created_at"),
            col("created_by")
        )
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """Calculate derived values based on business logic."""
        category_multipliers = self.config.get("transformation", {}).get("category_multipliers", {})
        
        # Apply category-specific multipliers
        transformed_value = col("value")
        for category, multiplier in category_multipliers.items():
            transformed_value = when(
                col("category") == category,
                col("value") * lit(multiplier)
            ).otherwise(transformed_value)
        
        df = df.withColumn("transformed_value", spark_round(transformed_value, 2))
        
        # Calculate priority based on value
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, lit(1))
            .when(col("transformed_value") >= 750, lit(2))
            .when(col("transformed_value") >= 300, lit(3))
            .when(col("transformed_value") >= 100, lit(4))
            .otherwise(lit(5))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to determine record status and handling."""
        # Rule 1: Set status based on transformed value
        df = df.withColumn(
            "status",
            when(col("value").isNull(), lit("INVALID"))
            .when(col("transformed_value") >= 750, lit("HIGH_VALUE"))
            .when(col("transformed_value") >= 300, lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )
        
        # Rule 2: Override priority for high-value items
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, lit(1))
            .otherwise(col("priority"))
        )
        
        # Rule 3: Validate category
        valid_categories = self.config.get("validation", {}).get("valid_categories", [])
        if valid_categories:
            df = df.withColumn(
                "category",
                when(col("category").isin(valid_categories), col("category"))
                .otherwise(lit("UNCATEGORIZED"))
            )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional calculated fields and lookups."""
        # Apply premium enhancement
        premium_multiplier = self.config.get("enrichment", {}).get("premium_multiplier", 1.2)
        
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("transformed_value") * lit(premium_multiplier))
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """Add ETL metadata to the DataFrame."""
        return df.withColumn("etl_run_id", lit(self.run_id)) \
                 .withColumn("processed_at", current_timestamp()) \
                 .withColumn("processed_by", lit(self.config.get("metadata", {}).get("user", "spark_etl")))
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data against business rules.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        errors = []
        
        # Check for required fields
        null_ids = df.filter(col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null ID")
        
        null_names = df.filter(col("name").isNull()).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null name")
        
        # Check for invalid values
        negative_values = df.filter(col("transformed_value") < 0).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative values")
        
        # Check priority range
        invalid_priorities = df.filter((col("priority") < 1) | (col("priority") > 5)).count()
        if invalid_priorities > 0:
            errors.append(f"{invalid_priorities} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Data validation passed")
        else:
            self.logger.error(f"Data validation failed: {errors}")
        
        return is_valid, errors