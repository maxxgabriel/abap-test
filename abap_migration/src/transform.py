"""
ETL Data Transformation Module
Applies business rules, enrichment, and data quality transformations.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, trim, when, lit, current_timestamp,
    udf, concat_ws, regexp_replace
)
from pyspark.sql.types import IntegerType, DecimalType
from datetime import datetime

from src.config_manager import ConfigManager
from src.logger import ETLLogger


class ETLTransformer:
    """Transform extracted data with business rules."""
    
    def __init__(self, spark: SparkSession, run_id: str):
        """
        Initialize transformer.
        
        Args:
            spark: Active Spark session
            run_id: Unique run identifier
        """
        self.spark = spark
        self.run_id = run_id
        self.config = ConfigManager()
        self.logger = ETLLogger(component="TRANSFORMER", run_id=run_id)
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply all transformations to source data.
        
        Args:
            source_df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.log_info("Starting transformation")
        
        try:
            # Basic transformations
            df = self._apply_basic_transformations(source_df)
            
            # Calculate derived values
            df = self._calculate_derived_values(df)
            
            # Apply business rules
            df = self._apply_business_rules(df)
            
            # Enrich data
            df = self._enrich_data(df)
            
            # Add metadata
            df = self._add_metadata(df)
            
            record_count = df.count()
            self.logger.log_info(f"Transformed {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.log_error(f"Transformation failed: {str(e)}")
            raise
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic data cleaning and normalization."""
        df = df.withColumn("name", upper(trim(col("name"))))
        df = df.withColumn("name", regexp_replace(col("name"), "\\s+", " "))
        
        # Handle nulls in category
        df = df.withColumn(
            "category",
            when(col("category").isNull(), lit("UNCATEGORIZED")).otherwise(col("category"))
        )
        
        return df
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """Calculate transformed values based on business logic."""
        # Get premium multiplier from config
        premium_multiplier = float(self.config.get("premium_multiplier", 1.5))
        
        # Calculate transformed_value based on category
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("value") * lit(premium_multiplier))
            .when(col("category") == "VIP", col("value") * lit(1.3))
            .otherwise(col("value"))
        )
        
        # Calculate priority
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
        """Apply business rules to data."""
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
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional information."""
        # Add enrichment based on category (example)
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("transformed_value") * lit(1.2))
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """Add ETL metadata columns."""
        df = df.withColumn("etl_run_id", lit(self.run_id))
        df = df.withColumn("processed_at", current_timestamp())
        df = df.withColumn("processed_by", lit("etl_system"))
        
        return df
    
    def validate_data(self, df: DataFrame) -> dict:
        """
        Validate transformed data.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Dictionary with validation results
        """
        self.logger.log_info("Validating transformed data")
        
        errors = []
        
        # Check for null IDs
        null_ids = df.filter(col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null ID")
        
        # Check for null names
        null_names = df.filter(col("name").isNull()).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null name")
        
        # Check for invalid values
        invalid_values = df.filter(col("value") < 0).count()
        if invalid_values > 0:
            errors.append(f"{invalid_values} records with negative value")
        
        # Check for invalid priorities
        invalid_priorities = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priorities > 0:
            errors.append(f"{invalid_priorities} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.log_info("Validation passed")
        else:
            self.logger.log_error(f"Validation failed: {len(errors)} error(s)")
        
        return {
            "is_valid": is_valid,
            "errors": errors,
            "total_records": df.count()
        }