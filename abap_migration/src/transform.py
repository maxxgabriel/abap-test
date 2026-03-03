"""
ETL Data Transformation Module
Applies business rules, enrichment, and validation
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, current_timestamp,
    current_user, concat_ws
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import Dict, Any, List, Tuple
import logging


class ETLTransformer:
    """Handles data transformation and validation"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize transformer
        
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
        """Define transformed data schema"""
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
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply all transformations to data
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        # Basic transformations
        df_transformed = df.select(
            col("id"),
            upper(trim(regexp_replace(col("name"), "\\s+", " "))).alias("name"),
            col("value"),
            self._calculate_derived_value(col("value"), col("category")).alias("transformed_value"),
            lit("TRANSFORMED").alias("status"),
            col("category"),
            self._calculate_priority(col("value"), col("category")).alias("priority"),
            lit(self.run_id).alias("etl_run_id"),
            current_timestamp().alias("processed_at"),
            current_user().alias("processed_by")
        )
        
        # Apply business rules
        df_transformed = self.apply_business_rules(df_transformed)
        
        # Enrich data
        df_transformed = self.enrich_data(df_transformed)
        
        record_count = df_transformed.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return df_transformed
    
    def _calculate_derived_value(self, value_col, category_col):
        """Calculate derived value based on category"""
        multiplier_config = self.config.get('transformation', {}).get('category_multipliers', {})
        
        return when(category_col == "PREMIUM", value_col * multiplier_config.get('PREMIUM', 1.5)) \
            .when(category_col == "VIP", value_col * multiplier_config.get('VIP', 2.0)) \
            .when(category_col == "STANDARD", value_col * multiplier_config.get('STANDARD', 1.2)) \
            .when(category_col == "BASIC", value_col * multiplier_config.get('BASIC', 1.0)) \
            .otherwise(value_col)
    
    def _calculate_priority(self, value_col, category_col):
        """Calculate priority based on value and category"""
        return when((value_col >= 1000) | (category_col == "VIP"), lit(1)) \
            .when((value_col >= 750) | (category_col == "PREMIUM"), lit(2)) \
            .when((value_col >= 300) | (category_col == "STANDARD"), lit(3)) \
            .when(value_col >= 100, lit(4)) \
            .otherwise(lit(5))
    
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to transformed data
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            DataFrame with business rules applied
        """
        self.logger.info("Applying business rules")
        
        # Rule 1: Set status based on transformed value
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
        
        # Rule 3: Category validation
        df = df.withColumn(
            "category",
            when(col("category").isNull() | (col("category") == ""), lit("UNCATEGORIZED"))
            .otherwise(col("category"))
        )
        
        return df
    
    def enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional information
        
        Args:
            df: DataFrame to enrich
            
        Returns:
            Enriched DataFrame
        """
        self.logger.info("Enriching data")
        
        # Load enrichment config from database if configured
        if self.config.get('transformation', {}).get('enable_enrichment', True):
            # Apply enrichment based on category
            df = df.withColumn(
                "transformed_value",
                when(col("category") == "PREMIUM", col("transformed_value") * 1.2)
                .otherwise(col("transformed_value"))
            )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        self.logger.info("Validating data")
        errors = []
        
        # Validation 1: Required fields
        null_check = df.filter(
            col("id").isNull() | 
            col("name").isNull() | 
            col("value").isNull()
        )
        null_count = null_check.count()
        if null_count > 0:
            errors.append(f"{null_count} records with missing required fields")
        
        # Validation 2: Value constraints
        invalid_values = df.filter(
            (col("value") < 0) | 
            (col("transformed_value") < 0)
        )
        invalid_count = invalid_values.count()
        if invalid_count > 0:
            errors.append(f"{invalid_count} records with negative values")
        
        # Validation 3: Priority range
        invalid_priority = df.filter(
            (col("priority") < 1) | 
            (col("priority") > 5)
        )
        priority_count = invalid_priority.count()
        if priority_count > 0:
            errors.append(f"{priority_count} records with invalid priority")
        
        # Validation 4: Category validation
        empty_category = df.filter(
            col("category").isNull() | 
            (col("category") == "")
        )
        category_count = empty_category.count()
        if category_count > 0:
            errors.append(f"{category_count} records with missing category")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Data validation passed")
        else:
            self.logger.warning(f"Data validation failed with {len(errors)} issues")
            for error in errors:
                self.logger.warning(f"  - {error}")
        
        return is_valid, errors