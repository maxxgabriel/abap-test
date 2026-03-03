"""
PySpark ETL Transformer Module
Handles data transformation, business rules, enrichment, and validation.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, current_timestamp,
    current_user, coalesce, concat_ws
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from typing import Dict, Any, List, Tuple
import logging


class ETLTransformer:
    """
    Transformer class for ETL pipeline with business rules and validation.
    """
    
    def __init__(self, config: Dict[str, Any], run_id: str):
        """
        Initialize the transformer.
        
        Args:
            config: Configuration dictionary
            run_id: Unique identifier for this ETL run
        """
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        self.transformed_schema = self._get_transformed_schema()
    
    def _get_transformed_schema(self) -> StructType:
        """Define the schema for transformed data."""
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
            StructField("processed_by", StringType(), True)
        ])
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Main transformation orchestration.
        
        Args:
            df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        # Basic transformations
        df_transformed = self._apply_basic_transformations(df)
        
        # Calculate derived values
        df_transformed = self._calculate_derived_values(df_transformed)
        
        # Apply business rules
        df_transformed = self._apply_business_rules(df_transformed)
        
        # Apply category-specific rules
        df_transformed = self._apply_category_rules(df_transformed)
        
        # Enrich data
        df_transformed = self._enrich_data(df_transformed)
        
        record_count = df_transformed.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return df_transformed
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply basic field transformations.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with basic transformations
        """
        return (df
                .withColumn("name", upper(trim(col("name"))))
                .withColumn("name", regexp_replace(col("name"), "\\s+", " "))
                .withColumn("status", lit("TRANSFORMED"))
                .withColumn("etl_run_id", lit(self.run_id))
                .withColumn("processed_at", current_timestamp())
                .withColumn("processed_by", current_user())
                .withColumn("category", coalesce(col("category"), lit("UNCATEGORIZED"))))
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate derived and transformed values.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with calculated values
        """
        # Apply transformation multiplier based on category
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("value") * lit(1.5))
            .when(col("category") == "VIP", col("value") * lit(2.0))
            .when(col("category") == "STANDARD", col("value") * lit(1.0))
            .when(col("category") == "BASIC", col("value") * lit(0.8))
            .otherwise(col("value"))
        )
        
        # Calculate priority based on value and category
        df = df.withColumn(
            "priority",
            self._calculate_priority_udf(col("transformed_value"), col("category"))
        )
        
        return df
    
    def _calculate_priority_udf(self, value_col, category_col):
        """
        UDF for calculating priority.
        
        Args:
            value_col: Value column
            category_col: Category column
            
        Returns:
            Priority expression
        """
        return (
            when(value_col >= 1000, lit(1))  # Highest priority
            .when((value_col >= 750) | (category_col == "VIP"), lit(2))
            .when(value_col >= 500, lit(3))
            .when(value_col >= 250, lit(4))
            .otherwise(lit(5))  # Lowest priority
        )
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with business rules applied
        """
        self.logger.info("Applying business rules")
        
        # Rule 1: Set status based on transformed value
        df = df.withColumn(
            "status",
            when(col("value").isNull() | (col("value") == 0), lit("INVALID"))
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
        
        # Rule 3: Additional name normalization
        df = df.withColumn(
            "name",
            regexp_replace(trim(col("name")), "\\s+", " ")
        )
        
        return df
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply category-specific transformation rules.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with category rules applied
        """
        # Get category multiplier from config
        premium_multiplier = float(self.config.get("premium_multiplier", 1.2))
        
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", 
                 col("transformed_value") * lit(premium_multiplier))
            .otherwise(col("transformed_value"))
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
        
        # Add enrichment logic - could join with reference data
        # For now, we'll add computed fields
        
        # Create a status category description
        df = df.withColumn(
            "status_desc",
            when(col("status") == "HIGH_VALUE", lit("High Value Item"))
            .when(col("status") == "MEDIUM_VALUE", lit("Medium Value Item"))
            .when(col("status") == "LOW_VALUE", lit("Low Value Item"))
            .otherwise(lit("Unknown"))
        )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Tuple of (is_valid: bool, errors: List[str])
        """
        self.logger.info("Validating transformed data")
        errors = []
        
        # Validation 1: ID is required
        null_id_count = df.filter(col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"Found {null_id_count} records with null ID")
        
        # Validation 2: Name is required
        null_name_count = df.filter(col("name").isNull() | (col("name") == "")).count()
        if null_name_count > 0:
            errors.append(f"Found {null_name_count} records with null or empty name")
        
        # Validation 3: Value must be non-negative
        negative_value_count = df.filter(col("value") < 0).count()
        if negative_value_count > 0:
            errors.append(f"Found {negative_value_count} records with negative value")
        
        # Validation 4: Priority must be 1-5
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"Found {invalid_priority_count} records with invalid priority")
        
        # Validation 5: Check for duplicates
        total_count = df.count()
        unique_count = df.select("id").distinct().count()
        if total_count != unique_count:
            errors.append(f"Found {total_count - unique_count} duplicate IDs")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Validation passed")
        else:
            self.logger.error(f"Validation failed with {len(errors)} errors")
            for error in errors:
                self.logger.error(f"  - {error}")
        
        return is_valid, errors