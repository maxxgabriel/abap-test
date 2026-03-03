"""
Data transformation module for ETL system.
Applies business rules, validations, and enrichment.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, current_timestamp,
    lit, udf, concat_ws, coalesce
)
from typing import List, Tuple
from datetime import datetime
from src.utils.logger import ETLLogger
from src.utils.config import Config


class DataTransformer:
    """Handles data transformation and business rule application."""
    
    def __init__(self, spark: SparkSession, config: Config, run_id: str):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
    
    def get_transformed_schema(self) -> StructType:
        """Define transformed data schema."""
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
            StructField("processed_by", StringType(), True),
        ])
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply complete transformation pipeline.
        
        Args:
            df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message=f"Starting transformation for run {self.run_id}"
        )
        
        try:
            # Step 1: Basic transformations
            df = self._apply_basic_transformations(df)
            
            # Step 2: Calculate derived values
            df = self._calculate_derived_values(df)
            
            # Step 3: Apply business rules
            df = self._apply_business_rules(df)
            
            # Step 4: Enrich data
            df = self._enrich_data(df)
            
            # Step 5: Add ETL metadata
            df = self._add_etl_metadata(df)
            
            record_count = df.count()
            
            self.logger.log_info(
                component="TRANSFORMER",
                message=f"Transformed {record_count} records"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="TRANSFORMER",
                message="Transformation failed",
                details=str(e)
            )
            raise
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic data cleaning and formatting."""
        df = df.withColumn("name", upper(trim(col("name"))))
        df = df.withColumn("name", regexp_replace(col("name"), r"\s+", " "))
        df = df.withColumn("category", coalesce(col("category"), lit("UNCATEGORIZED")))
        
        return df
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """Calculate transformed values based on category."""
        premium_multiplier = float(self.config.get("transformation.premium_multiplier", 1.5))
        
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("value") * premium_multiplier)
            .when(col("category") == "VIP", col("value") * 2.0)
            .when(col("category") == "STANDARD", col("value") * 1.2)
            .otherwise(col("value"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to set status and priority."""
        # Rule 1: Set status based on value
        df = df.withColumn(
            "status",
            when(col("value").isNull(), "INVALID")
            .when(col("transformed_value") >= 1000, "HIGH_VALUE")
            .when(col("transformed_value") >= 750, "HIGH_VALUE")
            .when(col("transformed_value") >= 300, "MEDIUM_VALUE")
            .otherwise("LOW_VALUE")
        )
        
        # Rule 2: Calculate priority (1=highest, 5=lowest)
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, 1)
            .when((col("transformed_value") >= 500) & (col("category") == "PREMIUM"), 1)
            .when(col("transformed_value") >= 500, 2)
            .when(col("transformed_value") >= 200, 3)
            .when(col("transformed_value") >= 100, 4)
            .otherwise(5)
        )
        
        # Rule 3: Priority override for high value items
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, 1)
            .otherwise(col("priority"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional information."""
        # Add enrichment based on configuration
        enable_enrichment = self.config.get("transformation.enable_enrichment", True)
        
        if enable_enrichment:
            # Example: Add calculated fields, lookup additional data
            df = df.withColumn(
                "value_tier",
                when(col("transformed_value") >= 1000, "TIER_1")
                .when(col("transformed_value") >= 500, "TIER_2")
                .when(col("transformed_value") >= 100, "TIER_3")
                .otherwise("TIER_4")
            )
        
        return df
    
    def _add_etl_metadata(self, df: DataFrame) -> DataFrame:
        """Add ETL processing metadata."""
        import os
        
        df = df.withColumn("etl_run_id", lit(self.run_id))
        df = df.withColumn("processed_at", current_timestamp())
        df = df.withColumn("processed_by", lit(os.environ.get("USER", "spark_etl")))
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        errors = []
        
        # Validation 1: Check for required fields
        null_ids = df.filter(col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null IDs")
        
        null_names = df.filter(col("name").isNull()).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null names")
        
        # Validation 2: Check for valid values
        negative_values = df.filter(
            (col("value") < 0) | (col("transformed_value") < 0)
        ).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative values")
        
        # Validation 3: Check priority range
        invalid_priority = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        # Validation 4: Check for duplicates
        total_count = df.count()
        unique_count = df.select("id").distinct().count()
        if total_count != unique_count:
            errors.append(f"{total_count - unique_count} duplicate IDs found")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.log_info(
                component="TRANSFORMER",
                message="Data validation passed"
            )
        else:
            self.logger.log_warning(
                component="TRANSFORMER",
                message=f"Data validation failed: {len(errors)} issues found"
            )
        
        return is_valid, errors