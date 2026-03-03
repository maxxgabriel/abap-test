"""
Data transformation module for ETL pipeline.
Applies business rules, enrichment, and validation to extracted data.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import List, Tuple
import logging


class Transformer:
    """Handles data transformation and business rule application."""
    
    def __init__(self, spark: SparkSession, config: dict, run_id: str):
        """
        Initialize the transformer.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
    
    def get_transformed_schema(self) -> StructType:
        """Define the schema for transformed data."""
        return StructType([
            StructField("id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("value", DecimalType(15, 2), nullable=False),
            StructField("transformed_value", DecimalType(15, 2), nullable=False),
            StructField("status", StringType(), nullable=False),
            StructField("category", StringType(), nullable=False),
            StructField("priority", IntegerType(), nullable=False),
            StructField("etl_run_id", StringType(), nullable=False),
            StructField("processed_at", TimestampType(), nullable=False),
            StructField("processed_by", StringType(), nullable=False)
        ])
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply all transformations to the source data.
        
        Args:
            source_df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        # Basic transformations
        df = self._apply_basic_transformations(source_df)
        
        # Calculate derived values
        df = self._calculate_derived_values(df)
        
        # Calculate priority
        df = self._calculate_priority(df)
        
        # Apply category rules
        df = self._apply_category_rules(df)
        
        # Apply business rules
        df = self._apply_business_rules(df)
        
        # Enrich data
        df = self._enrich_data(df)
        
        record_count = df.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return df
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic field transformations."""
        return df.select(
            F.col("id"),
            F.upper(F.trim(F.col("name"))).alias("name"),
            F.col("value"),
            F.col("status"),
            F.col("category"),
            F.lit(self.run_id).alias("etl_run_id"),
            F.current_timestamp().alias("processed_at"),
            F.lit("pyspark_etl").alias("processed_by")
        )
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """Calculate transformed values based on business logic."""
        # Apply category-based multipliers
        category_multipliers = self.config.get("business_rules", {}).get("category_multipliers", {})
        
        df = df.withColumn(
            "transformed_value",
            F.when(F.col("category") == "PREMIUM", F.col("value") * F.lit(category_multipliers.get("PREMIUM", 1.5)))
            .when(F.col("category") == "VIP", F.col("value") * F.lit(category_multipliers.get("VIP", 2.0)))
            .when(F.col("category") == "STANDARD", F.col("value") * F.lit(category_multipliers.get("STANDARD", 1.2)))
            .when(F.col("category") == "BASIC", F.col("value") * F.lit(category_multipliers.get("BASIC", 1.0)))
            .otherwise(F.col("value"))
        )
        
        return df
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """Calculate priority based on value and category."""
        thresholds = self.config.get("business_rules", {}).get("priority_thresholds", {})
        
        df = df.withColumn(
            "priority",
            F.when(F.col("transformed_value") >= F.lit(thresholds.get("high", 1000)), F.lit(1))
            .when(F.col("transformed_value") >= F.lit(thresholds.get("medium", 500)), F.lit(2))
            .when(F.col("transformed_value") >= F.lit(thresholds.get("normal", 250)), F.lit(3))
            .when(F.col("transformed_value") >= F.lit(thresholds.get("low", 100)), F.lit(4))
            .otherwise(F.lit(5))
        )
        
        return df
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """Apply category-specific business rules."""
        # Set default category if empty
        df = df.withColumn(
            "category",
            F.when(F.col("category").isNull() | (F.col("category") == ""), F.lit("UNCATEGORIZED"))
            .otherwise(F.col("category"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply general business rules."""
        # Rule 1: Set status based on value
        df = df.withColumn(
            "status",
            F.when(F.col("value").isNull() | (F.col("value") <= 0), F.lit("INVALID"))
            .when(F.col("transformed_value") >= 750, F.lit("HIGH_VALUE"))
            .when(F.col("transformed_value") >= 300, F.lit("MEDIUM_VALUE"))
            .otherwise(F.lit("LOW_VALUE"))
        )
        
        # Rule 2: Priority override for high value items
        df = df.withColumn(
            "priority",
            F.when(F.col("transformed_value") >= 1000, F.lit(1))
            .otherwise(F.col("priority"))
        )
        
        # Rule 3: Name normalization (remove multiple spaces)
        df = df.withColumn(
            "name",
            F.regexp_replace(F.col("name"), "\\s+", " ")
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional information."""
        enrichment_config = self.config.get("enrichment", {})
        
        if enrichment_config.get("enable_premium_boost", False):
            df = df.withColumn(
                "transformed_value",
                F.when(F.col("category") == "PREMIUM", 
                       F.col("transformed_value") * F.lit(enrichment_config.get("premium_boost_factor", 1.2)))
                .otherwise(F.col("transformed_value"))
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
        self.logger.info("Starting data validation")
        errors = []
        
        # Check for null IDs
        null_ids = df.filter(F.col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null ID")
        
        # Check for null names
        null_names = df.filter(F.col("name").isNull() | (F.col("name") == "")).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null or empty name")
        
        # Check for invalid values
        invalid_values = df.filter(F.col("value") <= 0).count()
        if invalid_values > 0:
            errors.append(f"{invalid_values} records with invalid value (<= 0)")
        
        # Check for invalid priorities
        invalid_priorities = df.filter((F.col("priority") < 1) | (F.col("priority") > 5)).count()
        if invalid_priorities > 0:
            errors.append(f"{invalid_priorities} records with invalid priority (must be 1-5)")
        
        # Check for duplicate IDs
        total_count = df.count()
        unique_count = df.select("id").distinct().count()
        if total_count != unique_count:
            errors.append(f"{total_count - unique_count} duplicate IDs found")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Validation passed")
        else:
            self.logger.warning(f"Validation failed with {len(errors)} errors")
            for error in errors:
                self.logger.warning(f"  - {error}")
        
        return is_valid, errors