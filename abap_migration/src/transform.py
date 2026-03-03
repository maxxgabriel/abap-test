"""
Transform module for ETL pipeline
Implements business rules, value mapping, validation, and classification logic
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, current_timestamp,
    current_user, udf, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, 
    TimestampType, IntegerType
)
from typing import Dict, List, Tuple
from datetime import datetime

from src.logger import ETLLogger


class ETLTransformer:
    """
    Data transformation class with business rules, validation,
    and classification logic
    """
    
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
    
    # Value mapping configurations
    VALUE_MULTIPLIERS = {
        "PREMIUM": 1.5,
        "STANDARD": 1.0,
        "BASIC": 0.8,
        "VIP": 2.0,
        "TRIAL": 0.5
    }
    
    # Status mapping rules
    STATUS_RULES = [
        (1000, "HIGH_VALUE"),
        (750, "HIGH_VALUE"),
        (300, "MEDIUM_VALUE"),
        (0, "LOW_VALUE")
    ]
    
    def __init__(self, spark: SparkSession, run_id: str, config: Dict = None):
        """
        Initialize transformer
        
        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
            config: Optional configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config or {}
        self.logger = ETLLogger.get_instance()
        
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Main transformation method applying all transformation logic
        
        Args:
            source_df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message=f"Starting transformation for {source_df.count()} records"
        )
        
        try:
            # Initial transformation
            df = self._initial_transform(source_df)
            
            # Apply business rules
            df = self.apply_business_rules(df)
            
            # Calculate priority
            df = self._calculate_priority(df)
            
            # Apply category-specific rules
            df = self._apply_category_rules(df)
            
            # Enrich data
            df = self.enrich_data(df)
            
            # Add metadata
            df = df.withColumn("etl_run_id", lit(self.run_id)) \
                   .withColumn("processed_at", current_timestamp()) \
                   .withColumn("processed_by", lit(current_user()))
            
            self.logger.log_info(
                component="TRANSFORMER",
                message=f"Transformed {df.count()} records"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="TRANSFORMER",
                message="Transformation failed",
                details=str(e)
            )
            raise
    
    def _initial_transform(self, df: DataFrame) -> DataFrame:
        """
        Apply initial transformations
        
        Args:
            df: Input DataFrame
            
        Returns:
            Initially transformed DataFrame
        """
        # Name normalization
        df = df.withColumn(
            "name",
            trim(upper(regexp_replace(col("name"), r"\s+", " ")))
        )
        
        # Calculate transformed value based on category
        df = self._calculate_derived_values(df)
        
        # Set initial status
        df = df.withColumn(
            "status",
            when(col("value").isNull(), lit("INVALID"))
            .otherwise(lit("TRANSFORMED"))
        )
        
        return df
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate derived values based on category multipliers
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with transformed_value column
        """
        # Build case expression for category multipliers
        case_expr = None
        for category, multiplier in self.VALUE_MULTIPLIERS.items():
            if case_expr is None:
                case_expr = when(col("category") == category, col("value") * multiplier)
            else:
                case_expr = case_expr.when(
                    col("category") == category, 
                    col("value") * multiplier
                )
        
        # Default multiplier for unknown categories
        case_expr = case_expr.otherwise(col("value"))
        
        df = df.withColumn("transformed_value", case_expr)
        
        return df
    
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules for status classification
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with business rules applied
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Applying business rules"
        )
        
        # Rule 1: Set status based on transformed value
        status_expr = None
        for threshold, status_value in self.STATUS_RULES:
            if status_expr is None:
                status_expr = when(col("transformed_value") >= threshold, lit(status_value))
            else:
                status_expr = status_expr.when(
                    col("transformed_value") >= threshold, 
                    lit(status_value)
                )
        
        df = df.withColumn(
            "status",
            when(col("value").isNull(), lit("INVALID"))
            .otherwise(status_expr.otherwise(lit("LOW_VALUE")))
        )
        
        # Rule 2: Category validation
        df = df.withColumn(
            "category",
            when(col("category").isNull() | (col("category") == ""), lit("UNCATEGORIZED"))
            .otherwise(col("category"))
        )
        
        return df
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """
        Calculate priority based on value and category
        
        Priority levels:
        1 = Highest (transformed_value >= 1000 or VIP)
        2 = High (transformed_value >= 750 or PREMIUM)
        3 = Medium (transformed_value >= 300)
        4 = Low (transformed_value >= 100)
        5 = Lowest (transformed_value < 100)
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with priority column
        """
        df = df.withColumn(
            "priority",
            when(
                (col("transformed_value") >= 1000) | (col("category") == "VIP"),
                lit(1)
            )
            .when(
                (col("transformed_value") >= 750) | (col("category") == "PREMIUM"),
                lit(2)
            )
            .when(col("transformed_value") >= 300, lit(3))
            .when(col("transformed_value") >= 100, lit(4))
            .otherwise(lit(5))
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
        # Premium category gets additional boost
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("transformed_value") * 1.2)
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional calculated fields
        
        Args:
            df: Input DataFrame
            
        Returns:
            Enriched DataFrame
        """
        # Load enrichment configuration if available
        config_multiplier = self.config.get("premium_multiplier", 1.2)
        
        # Apply enrichment based on configuration
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("transformed_value") * config_multiplier)
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Tuple of (is_valid: bool, errors: List[str])
        """
        errors = []
        
        # Validation rule 1: ID is required
        null_id_count = df.filter(col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with missing ID")
        
        # Validation rule 2: Name is required
        null_name_count = df.filter(col("name").isNull() | (col("name") == "")).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with missing name")
        
        # Validation rule 3: Value must be positive
        negative_value_count = df.filter(
            (col("value") < 0) | (col("transformed_value") < 0)
        ).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative values")
        
        # Validation rule 4: Priority must be 1-5
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if not is_valid:
            self.logger.log_warning(
                component="TRANSFORMER",
                message=f"Validation failed with {len(errors)} errors"
            )
        
        return is_valid, errors
    
    def get_transformed_schema(self) -> StructType:
        """Get the expected transformed schema"""
        return self.TRANSFORMED_SCHEMA