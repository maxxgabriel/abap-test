"""
PySpark Data Transformation Module
Applies business rules, calculations, and data enrichment
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, current_timestamp,
    current_user, expr, coalesce, round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import Dict, Any, List, Tuple
import logging


class DataTransformer:
    """Transform extracted data with business rules and enrichment"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the data transformer
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def get_transformed_schema(self) -> StructType:
        """
        Define the schema for transformed data
        
        Returns:
            StructType schema definition
        """
        return StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
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
        Main transformation method that applies all transformations
        
        Args:
            df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info(f"Starting transformation for {df.count()} records")
        
        # Step 1: Basic transformations
        df_transformed = self._apply_basic_transformations(df)
        
        # Step 2: Calculate derived values
        df_transformed = self._calculate_derived_values(df_transformed)
        
        # Step 3: Apply business rules
        df_transformed = self._apply_business_rules(df_transformed)
        
        # Step 4: Apply category-specific rules
        df_transformed = self._apply_category_rules(df_transformed)
        
        # Step 5: Enrich data
        df_transformed = self._enrich_data(df_transformed)
        
        # Step 6: Add metadata
        df_transformed = self._add_metadata(df_transformed)
        
        self.logger.info(f"Transformation complete: {df_transformed.count()} records")
        
        return df_transformed
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply basic data cleaning and formatting
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with basic transformations applied
        """
        df = df.select(
            col("id"),
            upper(trim(regexp_replace(col("name"), "\\s+", " "))).alias("name"),
            col("value"),
            col("status"),
            col("category"),
            col("source_system"),
            col("created_at"),
            col("created_by"),
            col("changed_at"),
            col("changed_by")
        )
        
        return df
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate transformed values based on business logic
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with calculated values
        """
        transformation_config = self.config['transformation']
        
        # Calculate transformed value based on category
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("value") * lit(1.5))
            .when(col("category") == "VIP", col("value") * lit(2.0))
            .when(col("category") == "STANDARD", col("value") * lit(1.2))
            .when(col("category") == "BASIC", col("value") * lit(1.0))
            .when(col("category") == "TRIAL", col("value") * lit(0.8))
            .otherwise(col("value"))
        )
        
        # Round to 2 decimal places
        df = df.withColumn("transformed_value", spark_round(col("transformed_value"), 2))
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to the data
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with business rules applied
        """
        # Rule 1: Set status based on transformed value
        df = df.withColumn(
            "status",
            when(col("value").isNull() | (col("value") == 0), lit("INVALID"))
            .when(col("transformed_value") >= 1000, lit("HIGH_VALUE"))
            .when(col("transformed_value") >= 750, lit("HIGH_VALUE"))
            .when(col("transformed_value") >= 300, lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )
        
        # Rule 2: Calculate priority
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, lit(1))
            .when(col("transformed_value") >= 750, lit(2))
            .when(col("transformed_value") >= 500, lit(3))
            .when(col("transformed_value") >= 250, lit(4))
            .otherwise(lit(5))
        )
        
        # Rule 3: Handle missing categories
        df = df.withColumn(
            "category",
            coalesce(col("category"), lit("UNCATEGORIZED"))
        )
        
        return df
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply category-specific transformation rules
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with category rules applied
        """
        # Premium category gets highest priority if value is high
        df = df.withColumn(
            "priority",
            when(
                (col("category") == "PREMIUM") & (col("transformed_value") >= 500),
                lit(1)
            ).otherwise(col("priority"))
        )
        
        # VIP category always gets priority 1 or 2
        df = df.withColumn(
            "priority",
            when(
                (col("category") == "VIP") & (col("priority") > 2),
                lit(2)
            ).otherwise(col("priority"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional information
        
        Args:
            df: Input DataFrame
            
        Returns:
            Enriched DataFrame
        """
        # Load configuration-based enrichment
        enrichment_config = self.config.get('enrichment', {})
        
        if enrichment_config.get('enable_premium_boost', False):
            premium_multiplier = enrichment_config.get('premium_multiplier', 1.2)
            
            df = df.withColumn(
                "transformed_value",
                when(
                    col("category") == "PREMIUM",
                    col("transformed_value") * lit(premium_multiplier)
                ).otherwise(col("transformed_value"))
            )
        
        return df
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add ETL metadata to the DataFrame
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with metadata columns
        """
        df = df.withColumn("etl_run_id", lit(self.run_id))
        df = df.withColumn("processed_at", current_timestamp())
        df = df.withColumn("processed_by", current_user())
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data against business rules
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        errors = []
        is_valid = True
        
        # Validation 1: Check for required fields
        null_ids = df.filter(col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null ID")
            is_valid = False
        
        null_names = df.filter(col("name").isNull()).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null name")
            is_valid = False
        
        # Validation 2: Check for positive values
        negative_values = df.filter(
            (col("value") < 0) | (col("transformed_value") < 0)
        ).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative values")
            is_valid = False
        
        # Validation 3: Check priority range
        invalid_priority = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
            is_valid = False
        
        # Validation 4: Check for duplicates
        total_count = df.count()
        distinct_count = df.select("id").distinct().count()
        if total_count != distinct_count:
            errors.append(f"{total_count - distinct_count} duplicate records found")
            is_valid = False
        
        if is_valid:
            self.logger.info("Data validation passed")
        else:
            self.logger.error(f"Data validation failed: {errors}")
        
        return is_valid, errors