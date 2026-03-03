"""
ETL Transformation Module
Handles data transformation with business rules and validation
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, current_timestamp,
    current_user, udf, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from typing import List, Tuple, Dict, Any
import logging
from src.logger import ETLLogger
from src.config import Config


class ETLTransformer:
    """Transform extracted data with business rules"""
    
    # Define transformed data schema
    TRANSFORMED_SCHEMA = StructType([
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
    
    def __init__(self, spark: SparkSession, config: Config, run_id: str):
        """
        Initialize transformer
        
        Args:
            spark: SparkSession instance
            config: Configuration object
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
        
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Main transformation method
        
        Args:
            source_df: Source data DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message=f"Starting transformation for {source_df.count()} records"
        )
        
        try:
            # Basic transformations
            df = source_df.select(
                col("id"),
                upper(trim(regexp_replace(col("name"), r"\s+", " "))).alias("name"),
                col("value"),
                col("category"),
                col("status")
            )
            
            # Calculate derived values
            df = self._calculate_derived_values(df)
            
            # Calculate priority
            df = self._calculate_priority(df)
            
            # Apply category-specific rules
            df = self._apply_category_rules(df)
            
            # Apply business rules
            df = self._apply_business_rules(df)
            
            # Add metadata
            df = df.withColumn("etl_run_id", lit(self.run_id)) \
                   .withColumn("processed_at", current_timestamp()) \
                   .withColumn("processed_by", lit(current_user()))
            
            # Enrich data
            df = self._enrich_data(df)
            
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
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate transformed values based on business logic
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with transformed_value column
        """
        # Apply category-based multipliers
        premium_multiplier = float(self.config.get("business_rules.premium_multiplier", 1.5))
        standard_multiplier = float(self.config.get("business_rules.standard_multiplier", 1.0))
        
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("value") * premium_multiplier)
            .when(col("category") == "VIP", col("value") * 1.8)
            .when(col("category") == "STANDARD", col("value") * standard_multiplier)
            .when(col("category") == "BASIC", col("value") * 0.8)
            .otherwise(col("value"))
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
            when(col("transformed_value") >= 1000, lit(1))
            .when(col("transformed_value") >= 750, lit(2))
            .when(col("transformed_value") >= 500, lit(3))
            .when(col("transformed_value") >= 250, lit(4))
            .otherwise(lit(5))
        )
        
        # Override for VIP category
        df = df.withColumn(
            "priority",
            when(col("category") == "VIP", lit(1))
            .otherwise(col("priority"))
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
        # Set default category for null values
        df = df.withColumn(
            "category",
            coalesce(col("category"), lit("UNCATEGORIZED"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to data
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with business rules applied
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Applying business rules"
        )
        
        # Rule 1: Set status based on value
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
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional information
        
        Args:
            df: Input DataFrame
            
        Returns:
            Enriched DataFrame
        """
        # Apply additional premium boost if configured
        enable_enrichment = self.config.get("business_rules.enable_enrichment", True)
        
        if enable_enrichment:
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
        errors = []
        
        # Check for required fields
        null_ids = df.filter(col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null ID")
        
        null_names = df.filter(col("name").isNull()).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null name")
        
        # Check for invalid values
        negative_values = df.filter(col("value") < 0).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative value")
        
        # Check priority range
        invalid_priority = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.log_info(
                component="TRANSFORMER",
                message="Data validation passed"
            )
        else:
            self.logger.log_error(
                component="TRANSFORMER",
                message="Data validation failed",
                details="; ".join(errors)
            )
        
        return is_valid, errors