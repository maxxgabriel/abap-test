"""
PySpark Data Transformation Module
Implements business rules, data enrichment, and validation
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
from typing import Dict, Any, List, Tuple
import logging


class SparkTransformer:
    """
    Handles data transformation with business rules and validation
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize transformer
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
    
    def get_transformed_schema(self) -> StructType:
        """
        Define schema for transformed data
        
        Returns:
            StructType schema definition
        """
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
        Main transformation method applying all business rules
        
        Args:
            source_df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info(f"Starting transformation for {source_df.count()} records")
        
        # Add processing metadata
        current_timestamp = F.current_timestamp()
        current_user = self.config.get("processing", {}).get("user", "etl_system")
        
        df = source_df.withColumn("etl_run_id", F.lit(self.run_id)) \
                      .withColumn("processed_at", current_timestamp) \
                      .withColumn("processed_by", F.lit(current_user))
        
        # Clean and normalize name
        df = df.withColumn("name", F.upper(F.trim(F.regexp_replace("name", "\\s+", " "))))
        
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
        
        self.logger.info(f"Transformation complete: {df.count()} records")
        
        return df
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate transformed values based on category and business logic
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with transformed_value column
        """
        # Get multiplier from config
        premium_multiplier = float(self.config.get("business_rules", {}).get("premium_multiplier", 1.5))
        standard_multiplier = float(self.config.get("business_rules", {}).get("standard_multiplier", 1.2))
        
        # Apply multipliers based on category
        df = df.withColumn(
            "transformed_value",
            F.when(F.col("category") == "PREMIUM", F.col("value") * premium_multiplier)
             .when(F.col("category") == "VIP", F.col("value") * 2.0)
             .when(F.col("category") == "STANDARD", F.col("value") * standard_multiplier)
             .otherwise(F.col("value"))
        )
        
        return df
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """
        Calculate priority based on value and category
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with priority column
        """
        df = df.withColumn(
            "priority",
            F.when(F.col("transformed_value") >= 1000, F.lit(1))  # Highest
             .when(F.col("transformed_value") >= 750, F.lit(2))
             .when(F.col("transformed_value") >= 500, F.lit(3))
             .when(F.col("transformed_value") >= 250, F.lit(4))
             .otherwise(F.lit(5))  # Lowest
        )
        
        # Override priority for VIP category
        df = df.withColumn(
            "priority",
            F.when(F.col("category") == "VIP", F.lit(1))
             .otherwise(F.col("priority"))
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
        # Ensure category is uppercase
        df = df.withColumn("category", F.upper(F.col("category")))
        
        # Set default category for nulls
        df = df.withColumn(
            "category",
            F.when(F.col("category").isNull() | (F.col("category") == ""), F.lit("UNCATEGORIZED"))
             .otherwise(F.col("category"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply core business rules and validations
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with business rules applied
        """
        # Rule 1: Set status based on value
        df = df.withColumn(
            "status",
            F.when(F.col("value").isNull() | (F.col("value") == 0), F.lit("INVALID"))
             .when(F.col("transformed_value") >= 750, F.lit("HIGH_VALUE"))
             .when(F.col("transformed_value") >= 300, F.lit("MEDIUM_VALUE"))
             .otherwise(F.lit("LOW_VALUE"))
        )
        
        # Rule 2: Override status for high transformed values
        df = df.withColumn(
            "status",
            F.when(F.col("transformed_value") >= 1000, F.lit("HIGH_VALUE"))
             .otherwise(F.col("status"))
        )
        
        # Rule 3: Additional validation - ensure no negative values
        df = df.withColumn(
            "status",
            F.when((F.col("value") < 0) | (F.col("transformed_value") < 0), F.lit("INVALID"))
             .otherwise(F.col("status"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional calculated fields
        
        Args:
            df: Input DataFrame
            
        Returns:
            Enriched DataFrame
        """
        # Add value rank within category
        window_spec = Window.partitionBy("category").orderBy(F.desc("transformed_value"))
        df = df.withColumn("value_rank", F.row_number().over(window_spec))
        
        # Add percentile information
        df = df.withColumn("value_percentile", F.percent_rank().over(window_spec))
        
        # Calculate value increase percentage
        df = df.withColumn(
            "value_increase_pct",
            F.when(F.col("value") > 0,
                   ((F.col("transformed_value") - F.col("value")) / F.col("value") * 100))
             .otherwise(F.lit(0))
        )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data against business rules
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Tuple of (is_valid: bool, error_messages: List[str])
        """
        self.logger.info("Starting data validation")
        errors = []
        
        # Rule 1: ID is required
        null_id_count = df.filter(F.col("id").isNull() | (F.col("id") == "")).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with missing ID")
        
        # Rule 2: Name is required
        null_name_count = df.filter(F.col("name").isNull() | (F.col("name") == "")).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with missing name")
        
        # Rule 3: Value must be positive
        negative_value_count = df.filter(F.col("value") < 0).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative values")
        
        # Rule 4: Priority must be 1-5
        invalid_priority_count = df.filter(
            (F.col("priority") < 1) | (F.col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        # Rule 5: Check for duplicates
        duplicate_count = df.groupBy("id").count().filter(F.col("count") > 1).count()
        if duplicate_count > 0:
            errors.append(f"{duplicate_count} duplicate IDs found")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Validation passed")
        else:
            self.logger.warning(f"Validation failed with {len(errors)} errors")
            for error in errors:
                self.logger.warning(f"  - {error}")
        
        return is_valid, errors