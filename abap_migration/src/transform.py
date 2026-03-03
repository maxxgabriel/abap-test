"""
Data transformation module for ETL pipeline.
Handles business rules, data enrichment, and validation.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, 
    current_timestamp, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import Tuple, List
import logging

from src.logger import ETLLogger


class ETLTransformer:
    """
    Transformer class responsible for data transformation and enrichment.
    Applies business rules and validates data quality.
    """
    
    def __init__(self, run_id: str, config: dict):
        """
        Initialize the transformer.
        
        Args:
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.run_id = run_id
        self.config = config
        self.logger = ETLLogger.get_instance()
    
    def get_transformed_schema(self) -> StructType:
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
            StructField("processed_by", StringType(), True),
        ])
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Main transformation method that applies all transformation logic.
        
        Args:
            source_df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Starting transformation"
        )
        
        try:
            # Basic transformations
            df = source_df.select(
                col("id"),
                upper(trim(regexp_replace(col("name"), "\\s+", " "))).alias("name"),
                col("value"),
                self._calculate_derived_values(col("value"), col("category")).alias("transformed_value"),
                lit("TRANSFORMED").alias("status"),
                coalesce(col("category"), lit("UNCATEGORIZED")).alias("category"),
                self._calculate_priority(col("value"), col("category")).alias("priority"),
                lit(self.run_id).alias("etl_run_id"),
                current_timestamp().alias("processed_at"),
                lit(self.config.get("processed_by", "system")).alias("processed_by")
            )
            
            # Apply business rules
            df = self.apply_business_rules(df)
            
            # Enrich data
            df = self.enrich_data(df)
            
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
    
    def _calculate_derived_values(self, value_col, category_col):
        """
        Calculate derived values based on business logic.
        
        Args:
            value_col: Value column
            category_col: Category column
            
        Returns:
            Column expression for transformed value
        """
        multipliers = self.config.get("category_multipliers", {
            "PREMIUM": 1.5,
            "VIP": 1.75,
            "STANDARD": 1.2,
            "BASIC": 1.0,
            "TRIAL": 0.8
        })
        
        transformation = value_col
        for category, multiplier in multipliers.items():
            transformation = when(
                category_col == category,
                value_col * multiplier
            ).otherwise(transformation)
        
        return transformation
    
    def _calculate_priority(self, value_col, category_col):
        """
        Calculate priority based on value and category.
        
        Args:
            value_col: Value column
            category_col: Category column
            
        Returns:
            Column expression for priority
        """
        # Priority 1 (Highest) = VIP or value >= 1000
        # Priority 2 = Premium or value >= 750
        # Priority 3 = value >= 300
        # Priority 4 = Standard
        # Priority 5 (Lowest) = Basic/Trial or value < 100
        
        return when((category_col == "VIP") | (value_col >= 1000), 1) \
            .when((category_col == "PREMIUM") | (value_col >= 750), 2) \
            .when(value_col >= 300, 3) \
            .when(category_col == "STANDARD", 4) \
            .otherwise(5)
    
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to the DataFrame.
        
        Args:
            df: DataFrame to apply rules to
            
        Returns:
            DataFrame with business rules applied
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Applying business rules"
        )
        
        # Rule 1: Set status based on transformed value
        df = df.withColumn(
            "status",
            when(col("value").isNull(), "INVALID")
            .when(col("transformed_value") >= 750, "HIGH_VALUE")
            .when(col("transformed_value") >= 300, "MEDIUM_VALUE")
            .otherwise("LOW_VALUE")
        )
        
        # Rule 2: Priority override for high value items
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, 1)
            .otherwise(col("priority"))
        )
        
        # Rule 3: Ensure category is set
        df = df.withColumn(
            "category",
            coalesce(col("category"), lit("UNCATEGORIZED"))
        )
        
        return df
    
    def enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional calculated fields.
        
        Args:
            df: DataFrame to enrich
            
        Returns:
            Enriched DataFrame
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Enriching data"
        )
        
        # Apply premium category enrichment
        premium_multiplier = self.config.get("premium_multiplier", 1.2)
        
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", 
                 col("transformed_value") * premium_multiplier)
            .otherwise(col("transformed_value"))
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
        errors = []
        
        # Validation rule 1: ID is required
        null_id_count = df.filter(col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with null ID")
        
        # Validation rule 2: Name is required
        null_name_count = df.filter(col("name").isNull()).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with null name")
        
        # Validation rule 3: Value must be positive
        negative_value_count = df.filter(col("value") < 0).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative value")
        
        # Validation rule 4: Priority must be 1-5
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        # Validation rule 5: Category must be valid
        valid_categories = self.config.get("valid_categories", [
            "PREMIUM", "VIP", "STANDARD", "BASIC", "TRIAL", "UNCATEGORIZED"
        ])
        invalid_category_count = df.filter(
            ~col("category").isin(valid_categories)
        ).count()
        if invalid_category_count > 0:
            errors.append(f"{invalid_category_count} records with invalid category")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.log_info(
                component="TRANSFORMER",
                message="Validation passed"
            )
        else:
            self.logger.log_error(
                component="TRANSFORMER",
                message="Validation failed",
                details="; ".join(errors)
            )
        
        return is_valid, errors