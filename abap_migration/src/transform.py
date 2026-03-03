"""
Data transformation module for ETL pipeline.
Applies business rules, enrichment, and validation logic.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, current_timestamp,
    current_user, udf
)
from pyspark.sql.types import IntegerType, DecimalType, StringType
from typing import Dict, Any, List, Tuple
from datetime import datetime

from src.logger import ETLLogger


class DataTransformer:
    """Handles data transformation and business rules."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
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
        self.logger = ETLLogger.get_instance()
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply all transformations to source data.
        
        Args:
            source_df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.log_info(
            component='TRANSFORMER',
            message='Starting transformation'
        )
        
        # Apply transformations
        df = source_df.select(
            col("id"),
            upper(trim(regexp_replace(col("name"), "\\s+", " "))).alias("name"),
            col("value"),
            self._calculate_transformed_value(col("value"), col("category")).alias("transformed_value"),
            lit("TRANSFORMED").alias("status"),
            col("category"),
            self._calculate_priority(col("value"), col("category")).alias("priority"),
            lit(self.run_id).alias("etl_run_id"),
            current_timestamp().alias("processed_at"),
            current_user().alias("processed_by")
        )
        
        # Apply business rules
        df = self._apply_business_rules(df)
        
        # Enrich data
        df = self._enrich_data(df)
        
        record_count = df.count()
        self.logger.log_info(
            component='TRANSFORMER',
            message=f'Transformed {record_count} records'
        )
        
        return df
    
    def _calculate_transformed_value(self, value_col, category_col):
        """Calculate derived value based on category."""
        return when(category_col == "PREMIUM", value_col * 1.5) \
            .when(category_col == "STANDARD", value_col * 1.2) \
            .when(category_col == "VIP", value_col * 2.0) \
            .otherwise(value_col)
    
    def _calculate_priority(self, value_col, category_col):
        """Calculate priority based on value and category."""
        return when(value_col >= 1000, 1) \
            .when((value_col >= 500) | (category_col == "VIP"), 2) \
            .when(value_col >= 250, 3) \
            .when(value_col >= 100, 4) \
            .otherwise(5)
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with business rules applied
        """
        self.logger.log_info(
            component='TRANSFORMER',
            message='Applying business rules'
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
        
        # Rule 3: Handle uncategorized items
        df = df.withColumn(
            "category",
            when(col("category").isNull() | (col("category") == ""), "UNCATEGORIZED")
            .otherwise(col("category"))
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
        self.logger.log_info(
            component='TRANSFORMER',
            message='Enriching data'
        )
        
        # Apply premium multiplier if configured
        premium_multiplier = self.config.get('premium_multiplier', 1.2)
        
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("transformed_value") * premium_multiplier)
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []
        
        # Rule 1: ID is required
        null_id_count = df.filter(col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with missing ID")
        
        # Rule 2: Name is required
        null_name_count = df.filter(col("name").isNull() | (col("name") == "")).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with missing name")
        
        # Rule 3: Value must be non-negative
        negative_value_count = df.filter(
            (col("value") < 0) | (col("transformed_value") < 0)
        ).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative values")
        
        # Rule 4: Priority must be 1-5
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if not is_valid:
            self.logger.log_error(
                component='TRANSFORMER',
                message='Validation failed',
                details='; '.join(errors)
            )
        
        return is_valid, errors