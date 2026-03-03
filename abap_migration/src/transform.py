"""
Data Transformation Module
Handles business logic, enrichment, and validation
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from typing import List, Dict, Any
import logging


class Transformer:
    """Handles data transformation and validation"""
    
    # Define transformed data schema
    TRANSFORMED_SCHEMA = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("value", DecimalType(15, 2), False),
        StructField("transformed_value", DecimalType(15, 2), True),
        StructField("status", StringType(), True),
        StructField("category", StringType(), True),
        StructField("priority", IntegerType(), True),
        StructField("etl_run_id", StringType(), True),
        StructField("processed_at", TimestampType(), True),
        StructField("processed_by", StringType(), True)
    ])
    
    def __init__(self, spark, config: Dict[str, Any], run_id: str):
        """
        Initialize Transformer
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply complete transformation pipeline
        
        Args:
            df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        # Basic transformations
        df = self._apply_basic_transformations(df)
        
        # Calculate derived values
        df = self._calculate_derived_values(df)
        
        # Calculate priority
        df = self._calculate_priority(df)
        
        # Apply business rules
        df = self._apply_business_rules(df)
        
        # Enrich data
        df = self._enrich_data(df)
        
        # Add metadata
        df = df.withColumn("etl_run_id", F.lit(self.run_id)) \
               .withColumn("processed_at", F.current_timestamp()) \
               .withColumn("processed_by", F.lit("system"))
        
        record_count = df.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return df
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic field transformations"""
        df = df.withColumn("name", F.upper(F.trim(F.col("name")))) \
               .withColumn("name", F.regexp_replace("name", r"\s+", " "))
        
        return df
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """Calculate derived values based on business logic"""
        transformation_config = self.config.get("transformation", {})
        
        # Category-based multipliers
        multipliers = transformation_config.get("category_multipliers", {
            "PREMIUM": 1.5,
            "STANDARD": 1.2,
            "BASIC": 1.0,
            "VIP": 2.0,
            "TRIAL": 0.8
        })
        
        # Create case expression for transformation
        case_expr = F.when(F.col("category") == "PREMIUM", F.col("value") * multipliers["PREMIUM"])
        for category, multiplier in multipliers.items():
            if category != "PREMIUM":
                case_expr = case_expr.when(
                    F.col("category") == category,
                    F.col("value") * multiplier
                )
        case_expr = case_expr.otherwise(F.col("value"))
        
        df = df.withColumn("transformed_value", case_expr)
        
        return df
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """Calculate priority based on value and category"""
        priority_expr = (
            F.when(F.col("transformed_value") >= 1000, 1)
            .when(F.col("transformed_value") >= 750, 2)
            .when(F.col("transformed_value") >= 500, 3)
            .when(F.col("transformed_value") >= 250, 4)
            .otherwise(5)
        )
        
        df = df.withColumn("priority", priority_expr)
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules"""
        # Rule 1: Set status based on value
        status_expr = (
            F.when(F.col("value").isNull(), "INVALID")
            .when(F.col("transformed_value") >= 750, "HIGH_VALUE")
            .when(F.col("transformed_value") >= 300, "MEDIUM_VALUE")
            .otherwise("LOW_VALUE")
        )
        
        df = df.withColumn("status", status_expr)
        
        # Rule 2: Override priority for high value items
        df = df.withColumn(
            "priority",
            F.when(F.col("transformed_value") >= 1000, 1).otherwise(F.col("priority"))
        )
        
        # Rule 3: Set default category
        df = df.withColumn(
            "category",
            F.when(F.col("category").isNull(), "UNCATEGORIZED").otherwise(F.col("category"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional information"""
        enrichment_config = self.config.get("enrichment", {})
        
        if enrichment_config.get("enabled", False):
            # Example: Add premium boost
            df = df.withColumn(
                "transformed_value",
                F.when(
                    F.col("category") == "PREMIUM",
                    F.col("transformed_value") * 1.2
                ).otherwise(F.col("transformed_value"))
            )
        
        return df
    
    def validate_data(self, df: DataFrame) -> List[str]:
        """
        Validate transformed data
        
        Args:
            df: DataFrame to validate
            
        Returns:
            List of validation error messages
        """
        self.logger.info("Starting data validation")
        errors = []
        
        # Validation 1: Required fields
        null_ids = df.filter(F.col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null ID")
        
        null_names = df.filter(F.col("name").isNull()).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null name")
        
        # Validation 2: Value constraints
        negative_values = df.filter(F.col("value") < 0).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative values")
        
        # Validation 3: Priority range
        invalid_priority = df.filter(
            (F.col("priority") < 1) | (F.col("priority") > 5)
        ).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        # Validation 4: Category presence
        null_categories = df.filter(F.col("category").isNull()).count()
        if null_categories > 0:
            errors.append(f"{null_categories} records with null category")
        
        if errors:
            self.logger.warning(f"Validation found {len(errors)} issues")
        else:
            self.logger.info("Validation passed")
        
        return errors