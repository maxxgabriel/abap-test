"""
PySpark Data Transformation Module
Applies business rules, enrichment, and validation logic.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    TimestampType, IntegerType
)
from pyspark.sql.window import Window
from typing import Dict, Any, List, Tuple
import logging


class DataTransformer:
    """Handles data transformation with business rules and validation."""
    
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
        self.username = config.get("execution", {}).get("user", "spark_etl")
    
    def get_transformed_schema(self) -> StructType:
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
            StructField("processed_by", StringType(), False),
        ])
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply complete transformation pipeline.
        
        Args:
            source_df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting data transformation")
        
        try:
            # Step 1: Basic transformations
            df = self._apply_basic_transformations(source_df)
            
            # Step 2: Calculate derived values
            df = self._calculate_derived_values(df)
            
            # Step 3: Apply business rules
            df = self._apply_business_rules(df)
            
            # Step 4: Enrich data
            df = self._enrich_data(df)
            
            # Step 5: Add ETL metadata
            df = self._add_etl_metadata(df)
            
            record_count = df.count()
            self.logger.info(f"Transformed {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}")
            raise
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply basic data transformations.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with basic transformations applied
        """
        self.logger.info("Applying basic transformations")
        
        # Normalize name field
        df = df.withColumn(
            "name",
            F.upper(F.trim(F.regexp_replace("name", r"\s+", " ")))
        )
        
        # Handle null values with defaults
        df = df.fillna({
            "status": "UNKNOWN",
            "category": "UNCATEGORIZED",
            "value": 0.0
        })
        
        # Remove duplicate records based on ID
        df = df.dropDuplicates(["id"])
        
        return df
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate derived and transformed values.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with calculated values
        """
        self.logger.info("Calculating derived values")
        
        # Apply category-based multipliers
        category_multipliers = self.config["transformation"]["category_multipliers"]
        
        # Create a mapping expression
        multiplier_expr = F.when(F.col("category") == "PREMIUM", category_multipliers["PREMIUM"])
        for category, multiplier in category_multipliers.items():
            if category != "PREMIUM":
                multiplier_expr = multiplier_expr.when(
                    F.col("category") == category, multiplier
                )
        multiplier_expr = multiplier_expr.otherwise(1.0)
        
        # Calculate transformed value
        df = df.withColumn(
            "transformed_value",
            (F.col("value") * multiplier_expr).cast(DecimalType(15, 2))
        )
        
        # Calculate priority based on value
        df = df.withColumn(
            "priority",
            F.when(F.col("transformed_value") >= 1000, 1)
            .when(F.col("transformed_value") >= 750, 2)
            .when(F.col("transformed_value") >= 500, 3)
            .when(F.col("transformed_value") >= 250, 4)
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
        self.logger.info("Applying business rules")
        
        # Rule 1: Set status based on value
        df = df.withColumn(
            "status",
            F.when(F.col("value").isNull() | (F.col("value") == 0), "INVALID")
            .when(F.col("transformed_value") >= 750, "HIGH_VALUE")
            .when(F.col("transformed_value") >= 300, "MEDIUM_VALUE")
            .otherwise("LOW_VALUE")
        )
        
        # Rule 2: Override priority for high-value items
        df = df.withColumn(
            "priority",
            F.when(F.col("transformed_value") >= 1000, 1)
            .otherwise(F.col("priority"))
        )
        
        # Rule 3: Validate category values
        valid_categories = self.config["transformation"]["valid_categories"]
        df = df.withColumn(
            "category",
            F.when(F.col("category").isin(valid_categories), F.col("category"))
            .otherwise("UNCATEGORIZED")
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
        self.logger.info("Enriching data")
        
        # Load enrichment configuration if available
        if self.config.get("transformation", {}).get("enable_enrichment", False):
            # Example: Add calculated fields
            df = df.withColumn(
                "value_category",
                F.when(F.col("transformed_value") >= 1000, "VERY_HIGH")
                .when(F.col("transformed_value") >= 500, "HIGH")
                .when(F.col("transformed_value") >= 250, "MEDIUM")
                .otherwise("LOW")
            )
        
        return df
    
    def _add_etl_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add ETL processing metadata.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with ETL metadata
        """
        current_timestamp = F.current_timestamp()
        
        df = df.withColumn("etl_run_id", F.lit(self.run_id))
        df = df.withColumn("processed_at", current_timestamp)
        df = df.withColumn("processed_by", F.lit(self.username))
        
        # Select only required columns in correct order
        return df.select(
            "id", "name", "value", "transformed_value",
            "status", "category", "priority",
            "etl_run_id", "processed_at", "processed_by"
        )
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        self.logger.info("Validating transformed data")
        errors = []
        
        # Rule 1: ID is required
        null_ids = df.filter(F.col("id").isNull() | (F.col("id") == "")).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with missing ID")
        
        # Rule 2: Name is required
        null_names = df.filter(F.col("name").isNull() | (F.col("name") == "")).count()
        if null_names > 0:
            errors.append(f"{null_names} records with missing name")
        
        # Rule 3: Value must be non-negative
        negative_values = df.filter(F.col("value") < 0).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative values")
        
        # Rule 4: Priority must be 1-5
        invalid_priority = df.filter(
            (F.col("priority") < 1) | (F.col("priority") > 5)
        ).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        # Rule 5: Check for duplicates
        total_records = df.count()
        unique_ids = df.select("id").distinct().count()
        if total_records != unique_ids:
            errors.append(f"{total_records - unique_ids} duplicate IDs found")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Data validation passed")
        else:
            self.logger.error(f"Data validation failed with {len(errors)} errors")
            for error in errors:
                self.logger.error(f"  - {error}")
        
        return is_valid, errors
    
    def apply_data_quality_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply data quality rules and flag issues.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with quality flags
        """
        df = df.withColumn(
            "quality_flag",
            F.when(F.col("value").isNull(), "NULL_VALUE")
            .when(F.col("value") < 0, "NEGATIVE_VALUE")
            .when(F.col("name").isNull() | (F.col("name") == ""), "MISSING_NAME")
            .otherwise("VALID")
        )
        
        return df


def create_transformer(
    spark: SparkSession,
    config: Dict[str, Any],
    run_id: str
) -> DataTransformer:
    """
    Factory function to create a DataTransformer instance.
    
    Args:
        spark: Active SparkSession
        config: Configuration dictionary
        run_id: Run identifier
        
    Returns:
        Configured DataTransformer instance
    """
    return DataTransformer(spark, config, run_id)