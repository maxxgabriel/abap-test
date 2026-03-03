"""
PySpark ETL Transformation Module
Implements value transformation logic, data validation rules, and priority/category mapping
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from typing import Dict, List, Tuple
import logging
from datetime import datetime


class ETLTransformer:
    """
    Handles data transformation, validation, and business rule application
    """
    
    def __init__(self, spark: SparkSession, run_id: str, config: Dict):
        """
        Initialize transformer with Spark session and configuration
        
        Args:
            spark: SparkSession instance
            run_id: Unique identifier for this ETL run
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = logging.getLogger(__name__)
        
    def get_output_schema(self) -> StructType:
        """Define schema for transformed data"""
        return StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("value", DecimalType(15, 2), False),
            StructField("transformed_value", DecimalType(15, 2), False),
            StructField("status", StringType(), False),
            StructField("category", StringType(), False),
            StructField("priority", IntegerType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("processed_at", TimestampType(), False),
            StructField("processed_by", StringType(), False)
        ])
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Main transformation pipeline
        
        Args:
            source_df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting data transformation")
        
        # Apply transformations in sequence
        df = source_df
        df = self._normalize_names(df)
        df = self._calculate_transformed_values(df)
        df = self._calculate_priority(df)
        df = self._apply_category_rules(df)
        df = self._apply_business_rules(df)
        df = self._add_metadata(df)
        df = self._enrich_data(df)
        
        self.logger.info(f"Transformation complete. Records: {df.count()}")
        return df
    
    def _normalize_names(self, df: DataFrame) -> DataFrame:
        """Normalize name field - uppercase and remove extra spaces"""
        return df.withColumn(
            "name",
            F.upper(F.regexp_replace(F.trim(F.col("name")), r"\s+", " "))
        )
    
    def _calculate_transformed_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate derived/transformed values based on original value and category
        Business rules:
        - PREMIUM: value * 1.5
        - VIP: value * 2.0
        - STANDARD: value * 1.2
        - BASIC: value * 1.0
        - TRIAL: value * 0.8
        - Default: value * 1.0
        """
        return df.withColumn(
            "transformed_value",
            F.when(F.col("category") == "PREMIUM", F.col("value") * 1.5)
            .when(F.col("category") == "VIP", F.col("value") * 2.0)
            .when(F.col("category") == "STANDARD", F.col("value") * 1.2)
            .when(F.col("category") == "BASIC", F.col("value") * 1.0)
            .when(F.col("category") == "TRIAL", F.col("value") * 0.8)
            .otherwise(F.col("value") * 1.0)
        )
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """
        Calculate priority based on value and category
        Priority scale: 1 (highest) to 5 (lowest)
        
        Rules:
        - value >= 1000: Priority 1
        - value >= 750: Priority 2
        - value >= 500: Priority 3
        - value >= 250: Priority 4
        - value < 250: Priority 5
        - VIP category always gets Priority 1
        - PREMIUM category gets at least Priority 2
        """
        return df.withColumn(
            "priority",
            F.when(F.col("category") == "VIP", 1)
            .when(F.col("value") >= 1000, 1)
            .when(F.col("value") >= 750, 2)
            .when(F.col("value") >= 500, 3)
            .when(F.col("value") >= 250, 4)
            .otherwise(5)
        ).withColumn(
            "priority",
            F.when((F.col("category") == "PREMIUM") & (F.col("priority") > 2), 2)
            .otherwise(F.col("priority"))
        )
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply category-specific transformation rules
        - Set default category if empty
        - Apply category-specific multipliers to transformed_value
        """
        df = df.withColumn(
            "category",
            F.when(F.col("category").isNull() | (F.col("category") == ""), "UNCATEGORIZED")
            .otherwise(F.col("category"))
        )
        
        # Additional category-based adjustment (applied after base transformation)
        premium_multiplier = self.config.get("premium_multiplier", 1.2)
        
        df = df.withColumn(
            "transformed_value",
            F.when(F.col("category") == "PREMIUM", F.col("transformed_value") * premium_multiplier)
            .otherwise(F.col("transformed_value"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to set status and perform final adjustments
        
        Status rules:
        - INVALID: if value is null or zero
        - HIGH_VALUE: transformed_value >= 750
        - MEDIUM_VALUE: transformed_value >= 300
        - LOW_VALUE: transformed_value < 300
        
        Priority override: High value items get Priority 1
        """
        df = df.withColumn(
            "status",
            F.when(F.col("value").isNull() | (F.col("value") == 0), "INVALID")
            .when(F.col("transformed_value") >= 750, "HIGH_VALUE")
            .when(F.col("transformed_value") >= 300, "MEDIUM_VALUE")
            .otherwise("LOW_VALUE")
        )
        
        # Priority override for high-value items
        df = df.withColumn(
            "priority",
            F.when(F.col("transformed_value") >= 1000, 1)
            .otherwise(F.col("priority"))
        )
        
        return df
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """Add ETL metadata fields"""
        current_time = datetime.now()
        processed_by = self.config.get("processed_by", "pyspark_etl")
        
        return df.withColumn("etl_run_id", F.lit(self.run_id)) \
                 .withColumn("processed_at", F.lit(current_time)) \
                 .withColumn("processed_by", F.lit(processed_by))
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional calculated fields or lookups
        This can be extended with external data sources
        """
        # Example enrichment: Add value tier classification
        df = df.withColumn(
            "value_tier",
            F.when(F.col("transformed_value") >= 1000, "TIER_1")
            .when(F.col("transformed_value") >= 500, "TIER_2")
            .when(F.col("transformed_value") >= 250, "TIER_3")
            .otherwise("TIER_4")
        )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data against business rules
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Rule 1: ID is required
        null_ids = df.filter(F.col("id").isNull() | (F.col("id") == "")).count()
        if null_ids > 0:
            errors.append(f"Validation failed: {null_ids} records with missing ID")
        
        # Rule 2: Name is required
        null_names = df.filter(F.col("name").isNull() | (F.col("name") == "")).count()
        if null_names > 0:
            errors.append(f"Validation failed: {null_names} records with missing name")
        
        # Rule 3: Value must be positive
        negative_values = df.filter((F.col("value") < 0) | (F.col("transformed_value") < 0)).count()
        if negative_values > 0:
            errors.append(f"Validation failed: {negative_values} records with negative values")
        
        # Rule 4: Priority must be 1-5
        invalid_priority = df.filter((F.col("priority") < 1) | (F.col("priority") > 5)).count()
        if invalid_priority > 0:
            errors.append(f"Validation failed: {invalid_priority} records with invalid priority")
        
        # Rule 5: Category must not be empty
        null_category = df.filter(F.col("category").isNull() | (F.col("category") == "")).count()
        if null_category > 0:
            errors.append(f"Validation failed: {null_category} records with missing category")
        
        # Rule 6: Status must be valid
        valid_statuses = ["INVALID", "HIGH_VALUE", "MEDIUM_VALUE", "LOW_VALUE"]
        invalid_status = df.filter(~F.col("status").isin(valid_statuses)).count()
        if invalid_status > 0:
            errors.append(f"Validation failed: {invalid_status} records with invalid status")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Data validation passed")
        else:
            self.logger.error(f"Data validation failed with {len(errors)} errors")
            for error in errors:
                self.logger.error(error)
        
        return is_valid, errors
    
    def get_validation_summary(self, df: DataFrame) -> Dict:
        """
        Get detailed validation summary statistics
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary with validation metrics
        """
        total_records = df.count()
        
        summary = {
            "total_records": total_records,
            "null_ids": df.filter(F.col("id").isNull()).count(),
            "null_names": df.filter(F.col("name").isNull()).count(),
            "null_values": df.filter(F.col("value").isNull()).count(),
            "negative_values": df.filter(F.col("value") < 0).count(),
            "invalid_priorities": df.filter((F.col("priority") < 1) | (F.col("priority") > 5)).count(),
            "null_categories": df.filter(F.col("category").isNull()).count(),
            "status_distribution": df.groupBy("status").count().collect(),
            "priority_distribution": df.groupBy("priority").count().collect(),
            "category_distribution": df.groupBy("category").count().collect(),
        }
        
        return summary


def get_category_mapping() -> Dict[str, Dict]:
    """
    Returns category mapping configuration
    Used for category validation and transformation rules
    """
    return {
        "PREMIUM": {
            "multiplier": 1.5,
            "min_priority": 2,
            "enrichment_factor": 1.2
        },
        "VIP": {
            "multiplier": 2.0,
            "min_priority": 1,
            "enrichment_factor": 1.5
        },
        "STANDARD": {
            "multiplier": 1.2,
            "min_priority": 3,
            "enrichment_factor": 1.0
        },
        "BASIC": {
            "multiplier": 1.0,
            "min_priority": 4,
            "enrichment_factor": 0.9
        },
        "TRIAL": {
            "multiplier": 0.8,
            "min_priority": 5,
            "enrichment_factor": 0.8
        }
    }


def get_priority_rules() -> Dict[str, int]:
    """
    Returns priority calculation rules
    Used for consistent priority assignment
    """
    return {
        "threshold_p1": 1000,  # Priority 1: value >= 1000
        "threshold_p2": 750,   # Priority 2: value >= 750
        "threshold_p3": 500,   # Priority 3: value >= 500
        "threshold_p4": 250,   # Priority 4: value >= 250
        "default": 5           # Priority 5: value < 250
    }