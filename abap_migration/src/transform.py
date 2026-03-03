"""
Transform module for ETL pipeline.
Handles data transformation and business rules.
"""
from typing import Dict, Any, List, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, 
    current_timestamp, concat_ws, expr, udf
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from decimal import Decimal
import logging

logger = logging.getLogger(__name__)


class ETLTransformer:
    """Transform extracted data according to business rules."""
    
    TRANSFORMED_SCHEMA = StructType([
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
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize transformer.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply transformations to source data.
        
        Args:
            source_df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Starting transformation")
        
        try:
            # Basic transformations
            df = self._apply_basic_transforms(source_df)
            
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
            
            record_count = df.count()
            logger.info(f"Transformed {record_count} records")
            
            return df
            
        except Exception as e:
            logger.error(f"Transformation failed: {str(e)}")
            raise
    
    def _apply_basic_transforms(self, df: DataFrame) -> DataFrame:
        """Apply basic field transformations."""
        return (df
                .withColumn("name", upper(trim(col("name"))))
                .withColumn("name", regexp_replace(col("name"), "\\s+", " "))
                .withColumn("status", lit("TRANSFORMED"))
                .withColumn("etl_run_id", lit(self.run_id))
                .withColumn("processed_at", current_timestamp())
                .withColumn("processed_by", lit("ETL_SYSTEM")))
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """Calculate derived/transformed values based on business logic."""
        # Apply category-based multipliers
        multipliers = self.config['transformation']['category_multipliers']
        
        transformed_value_expr = col("value")
        for category, multiplier in multipliers.items():
            transformed_value_expr = when(
                col("category") == category,
                col("value") * lit(Decimal(str(multiplier)))
            ).otherwise(transformed_value_expr)
        
        return df.withColumn("transformed_value", transformed_value_expr.cast(DecimalType(15, 2)))
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """Calculate priority based on value and category."""
        priority_rules = self.config['transformation']['priority_rules']
        
        priority_expr = lit(5)  # Default lowest priority
        
        # Priority by value ranges
        for rule in priority_rules['by_value']:
            priority_expr = when(
                col("value") >= rule['min_value'],
                lit(rule['priority'])
            ).otherwise(priority_expr)
        
        # Override by category
        for category, priority in priority_rules['by_category'].items():
            priority_expr = when(
                col("category") == category,
                lit(priority)
            ).otherwise(priority_expr)
        
        return df.withColumn("priority", priority_expr.cast(IntegerType()))
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """Apply category-specific business rules."""
        # Premium category gets additional boost
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM",
                 col("transformed_value") * lit(Decimal("1.2")))
            .otherwise(col("transformed_value"))
        )
        
        # VIP category gets highest priority
        df = df.withColumn(
            "priority",
            when(col("category") == "VIP", lit(1))
            .otherwise(col("priority"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to transformed data."""
        # Rule 1: Set status based on transformed value
        df = df.withColumn(
            "status",
            when(col("value").isNull() | (col("value") == 0), lit("INVALID"))
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
        
        # Rule 3: Handle uncategorized items
        df = df.withColumn(
            "category",
            when(col("category").isNull() | (col("category") == ""), lit("UNCATEGORIZED"))
            .otherwise(col("category"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional computed fields."""
        # Load enrichment configuration if exists
        enrichment_enabled = self.config['transformation'].get('enable_enrichment', True)
        
        if not enrichment_enabled:
            return df
        
        # Add value categories
        df = df.withColumn(
            "value_category",
            when(col("transformed_value") >= 1000, lit("TIER_1"))
            .when(col("transformed_value") >= 500, lit("TIER_2"))
            .when(col("transformed_value") >= 100, lit("TIER_3"))
            .otherwise(lit("TIER_4"))
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
        logger.info("Starting data validation")
        errors = []
        
        # Rule 1: ID is required
        null_id_count = df.filter(col("id").isNull() | (col("id") == "")).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with missing ID")
        
        # Rule 2: Name is required
        null_name_count = df.filter(col("name").isNull() | (col("name") == "")).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with missing name")
        
        # Rule 3: Value must be non-negative
        negative_value_count = df.filter(col("value") < 0).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative value")
        
        # Rule 4: Priority must be 1-5
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            logger.info("Validation passed")
        else:
            logger.warning(f"Validation failed: {', '.join(errors)}")
        
        return is_valid, errors