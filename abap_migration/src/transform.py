"""
ETL Transform Module - Data Transformation and Business Rules
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, upper, trim, when, lit, current_timestamp,
    regexp_replace, coalesce, concat_ws
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from typing import Tuple, List
import logging


class ETLTransformer:
    """Handles data transformation and business rule application"""
    
    def __init__(self, spark: SparkSession, config: dict, run_id: str):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
    
    def get_transformed_schema(self) -> StructType:
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
            StructField("processed_by", StringType(), False),
        ])
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """Main transformation method"""
        self.logger.info("Starting transformation")
        
        # Initial transformations
        df = self._apply_basic_transformations(source_df)
        
        # Calculate derived values
        df = self._calculate_derived_values(df)
        
        # Apply category-specific rules
        df = self._apply_category_rules(df)
        
        # Apply business rules
        df = self._apply_business_rules(df)
        
        # Enrich data
        df = self._enrich_data(df)
        
        record_count = df.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return df
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic data cleaning and transformations"""
        return df.select(
            col("id"),
            upper(trim(regexp_replace(col("name"), r'\s+', ' '))).alias("name"),
            col("value"),
            col("category"),
            lit(self.run_id).alias("etl_run_id"),
            current_timestamp().alias("processed_at"),
            lit(self.config['runtime']['user']).alias("processed_by")
        )
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """Calculate transformed values based on business logic"""
        return df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("value") * 1.5)
            .when(col("category") == "VIP", col("value") * 1.8)
            .when(col("category") == "STANDARD", col("value") * 1.2)
            .when(col("category") == "BASIC", col("value") * 1.0)
            .otherwise(col("value") * 1.1)
        )
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """Apply category-specific business rules"""
        return df.withColumn(
            "category",
            coalesce(col("category"), lit("UNCATEGORIZED"))
        ).withColumn(
            "priority",
            when(col("category") == "VIP", 1)
            .when(col("category") == "PREMIUM", 2)
            .when(col("category") == "STANDARD", 3)
            .when(col("category") == "BASIC", 4)
            .otherwise(5)
        )
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply comprehensive business rules"""
        df = df.withColumn(
            "status",
            when(col("value").isNull() | (col("value") == 0), "INVALID")
            .when(col("transformed_value") >= 1000, "HIGH_VALUE")
            .when(col("transformed_value") >= 750, "HIGH_VALUE")
            .when(col("transformed_value") >= 300, "MEDIUM_VALUE")
            .otherwise("LOW_VALUE")
        )
        
        # Override priority for high value items
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, 1)
            .otherwise(col("priority"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional attributes"""
        # Load enrichment config
        enrichment_rules = self.config.get('enrichment', {})
        
        if enrichment_rules.get('apply_multipliers', False):
            df = df.withColumn(
                "transformed_value",
                when(col("category") == "PREMIUM", 
                     col("transformed_value") * enrichment_rules.get('premium_multiplier', 1.0))
                .otherwise(col("transformed_value"))
            )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """Validate transformed data"""
        errors = []
        
        # Check for required fields
        null_ids = df.filter(col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with missing ID")
        
        null_names = df.filter(col("name").isNull() | (col("name") == "")).count()
        if null_names > 0:
            errors.append(f"{null_names} records with missing name")
        
        # Check for invalid values
        negative_values = df.filter(col("value") < 0).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative values")
        
        invalid_priority = df.filter((col("priority") < 1) | (col("priority") > 5)).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if not is_valid:
            self.logger.warning(f"Validation failed with {len(errors)} errors")
        else:
            self.logger.info("Validation passed successfully")
        
        return is_valid, errors
    
    def calculate_priority(self, value: float, category: str) -> int:
        """Calculate priority based on value and category"""
        if category == "VIP":
            return 1
        elif value >= 1000:
            return 1
        elif category == "PREMIUM":
            return 2
        elif category == "STANDARD":
            return 3
        elif category == "BASIC":
            return 4
        else:
            return 5