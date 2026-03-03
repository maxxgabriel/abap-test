"""
PySpark ETL Transformation Module
Applies business rules, data enrichment, and validation
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, upper, trim, when, lit, current_timestamp, 
    regexp_replace, coalesce, round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import List, Tuple
from src.logger import ETLLogger
from src.config import ETLConfig


class ETLTransformer:
    """Handles data transformation and business rules application"""
    
    def __init__(self, spark: SparkSession, config: ETLConfig, run_id: str):
        """
        Initialize transformer
        
        Args:
            spark: Active SparkSession
            config: ETL configuration object
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
    
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
            StructField("processed_by", StringType(), False)
        ])
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply all transformations to source data
        
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
            df = self._apply_initial_transformations(source_df)
            
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
            
            # Add metadata
            df = df.withColumn("etl_run_id", lit(self.run_id)) \
                   .withColumn("processed_at", current_timestamp()) \
                   .withColumn("processed_by", lit(self.config.get("user", "system")))
            
            record_count = df.count()
            self.logger.log_info(
                component="TRANSFORMER",
                message=f"Transformed {record_count} records successfully"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="TRANSFORMER",
                message="Transformation failed",
                details=str(e)
            )
            raise
    
    def _apply_initial_transformations(self, df: DataFrame) -> DataFrame:
        """Apply initial data cleaning and normalization"""
        return df.select(
            col("id"),
            upper(trim(regexp_replace(col("name"), "\\s+", " "))).alias("name"),
            col("value"),
            col("status"),
            coalesce(col("category"), lit("UNCATEGORIZED")).alias("category"),
            col("source_system"),
            col("created_at"),
            col("created_by"),
            col("changed_at"),
            col("changed_by")
        )
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """Calculate transformed values based on business logic"""
        rules = self.config.get_transformation_rules()
        
        # Base calculation with multipliers by category
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("value") * lit(rules.get("premium_multiplier", 1.5)))
            .when(col("category") == "VIP", col("value") * lit(rules.get("vip_multiplier", 2.0)))
            .when(col("category") == "STANDARD", col("value") * lit(rules.get("standard_multiplier", 1.0)))
            .otherwise(col("value") * lit(rules.get("default_multiplier", 1.0)))
        )
        
        # Round to 2 decimal places
        df = df.withColumn("transformed_value", spark_round(col("transformed_value"), 2))
        
        return df
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """Calculate priority based on value and category"""
        return df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, lit(1))
            .when((col("transformed_value") >= 750) & (col("category") == "PREMIUM"), lit(1))
            .when(col("transformed_value") >= 750, lit(2))
            .when(col("transformed_value") >= 300, lit(3))
            .when(col("transformed_value") >= 100, lit(4))
            .otherwise(lit(5))
        )
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """Apply category-specific business rules"""
        # Set initial status to TRANSFORMED
        df = df.withColumn("status", lit("TRANSFORMED"))
        
        # Apply category-specific adjustments
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("transformed_value") * 1.2)
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply comprehensive business rules"""
        # Rule 1: Set status based on value
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
        
        # Rule 3: Ensure category is set
        df = df.withColumn(
            "category",
            when(col("category").isNull() | (col("category") == ""), lit("UNCATEGORIZED"))
            .otherwise(col("category"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional information"""
        enrichment_config = self.config.get("enrichment", {})
        
        if enrichment_config.get("enabled", True):
            # Add enrichment logic here
            # Example: join with reference data, add calculated fields, etc.
            pass
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data against business rules
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        errors = []
        
        self.logger.log_info(
            component="TRANSFORMER",
            message="Starting data validation"
        )
        
        try:
            # Rule 1: ID is required
            null_ids = df.filter(col("id").isNull() | (col("id") == "")).count()
            if null_ids > 0:
                errors.append(f"{null_ids} records with missing ID")
            
            # Rule 2: Name is required
            null_names = df.filter(col("name").isNull() | (col("name") == "")).count()
            if null_names > 0:
                errors.append(f"{null_names} records with missing name")
            
            # Rule 3: Value must be positive
            negative_values = df.filter((col("value") < 0) | (col("transformed_value") < 0)).count()
            if negative_values > 0:
                errors.append(f"{negative_values} records with negative values")
            
            # Rule 4: Priority must be 1-5
            invalid_priority = df.filter((col("priority") < 1) | (col("priority") > 5)).count()
            if invalid_priority > 0:
                errors.append(f"{invalid_priority} records with invalid priority")
            
            # Rule 5: Category must be valid
            invalid_category = df.filter(col("category").isNull() | (col("category") == "")).count()
            if invalid_category > 0:
                errors.append(f"{invalid_category} records with invalid category")
            
            is_valid = len(errors) == 0
            
            if is_valid:
                self.logger.log_info(
                    component="TRANSFORMER",
                    message="Validation passed successfully"
                )
            else:
                self.logger.log_error(
                    component="TRANSFORMER",
                    message=f"Validation failed with {len(errors)} errors",
                    details="; ".join(errors)
                )
            
            return is_valid, errors
            
        except Exception as e:
            error_msg = f"Validation error: {str(e)}"
            errors.append(error_msg)
            self.logger.log_error(
                component="TRANSFORMER",
                message="Validation process failed",
                details=str(e)
            )
            return False, errors