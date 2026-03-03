"""
ETL Transformer Module
Handles data transformation, validation, and business rule application.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, current_timestamp,
    current_user, lit, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from typing import Dict, List, Tuple
import logging


class ETLTransformer:
    """Transform and validate data with business rules"""
    
    def __init__(self, spark: SparkSession, run_id: str, config: Dict):
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = logging.getLogger(__name__)
        
    def get_transformed_schema(self) -> StructType:
        """Define transformed data schema"""
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
        Apply transformations to source data
        
        Args:
            source_df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        # Basic transformations
        df = source_df.select(
            col("id"),
            # Clean and normalize name
            upper(trim(regexp_replace(col("name"), "\\s+", " "))).alias("name"),
            col("value"),
            # Calculate transformed value
            self._calculate_derived_values(col("value"), col("category")).alias("transformed_value"),
            lit("TRANSFORMED").alias("status"),
            coalesce(col("category"), lit("UNCATEGORIZED")).alias("category"),
            # Calculate priority
            self._calculate_priority(col("value"), col("category")).alias("priority"),
            lit(self.run_id).alias("etl_run_id"),
            current_timestamp().alias("processed_at"),
            current_user().alias("processed_by")
        )
        
        # Apply business rules
        df = self._apply_business_rules(df)
        
        # Apply category-specific rules
        df = self._apply_category_rules(df)
        
        # Enrich data
        df = self._enrich_data(df)
        
        record_count = df.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return df
    
    def _calculate_derived_values(self, value_col, category_col):
        """Calculate transformed value based on category and rules"""
        premium_multiplier = float(self.config.get("premium_multiplier", 1.5))
        
        return when(category_col == "PREMIUM", value_col * premium_multiplier) \
            .when(category_col == "VIP", value_col * 1.8) \
            .when(category_col == "STANDARD", value_col * 1.2) \
            .when(category_col == "BASIC", value_col * 1.0) \
            .when(category_col == "TRIAL", value_col * 0.8) \
            .otherwise(value_col)
    
    def _calculate_priority(self, value_col, category_col):
        """
        Calculate priority based on value and category
        Priority: 1 (highest) to 5 (lowest)
        """
        return when(value_col >= 1000, lit(1)) \
            .when((value_col >= 750) | (category_col == "VIP"), lit(2)) \
            .when((value_col >= 500) | (category_col == "PREMIUM"), lit(3)) \
            .when(value_col >= 250, lit(4)) \
            .otherwise(lit(5))
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to transformed data"""
        self.logger.info("Applying business rules")
        
        # Rule 1: Set status based on transformed value
        df = df.withColumn(
            "status",
            when(col("value").isNull(), lit("INVALID"))
            .when(col("transformed_value") >= 750, lit("HIGH_VALUE"))
            .when(col("transformed_value") >= 300, lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )
        
        # Rule 2: Priority override for high-value items
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
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """Apply category-specific transformation rules"""
        self.logger.info("Applying category-specific rules")
        
        # Premium category adjustments
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("transformed_value") * 1.1)
            .otherwise(col("transformed_value"))
        )
        
        # VIP category priority boost
        df = df.withColumn(
            "priority",
            when((col("category") == "VIP") & (col("priority") > 1), col("priority") - 1)
            .otherwise(col("priority"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional calculated fields"""
        self.logger.info("Enriching data")
        
        # Add value tier
        df = df.withColumn(
            "value_tier",
            when(col("transformed_value") >= 1000, lit("TIER_1"))
            .when(col("transformed_value") >= 500, lit("TIER_2"))
            .when(col("transformed_value") >= 250, lit("TIER_3"))
            .otherwise(lit("TIER_4"))
        )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        self.logger.info("Validating data")
        errors = []
        
        # Validation 1: Required fields must not be null
        null_ids = df.filter(col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null ID")
        
        null_names = df.filter(col("name").isNull()).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null name")
        
        # Validation 2: Values must be positive
        negative_values = df.filter(col("value") < 0).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative value")
        
        # Validation 3: Priority must be 1-5
        invalid_priority = df.filter((col("priority") < 1) | (col("priority") > 5)).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        # Validation 4: Transformed value should not be null
        null_transformed = df.filter(col("transformed_value").isNull()).count()
        if null_transformed > 0:
            errors.append(f"{null_transformed} records with null transformed_value")
        
        # Validation 5: Category should not be empty
        invalid_category = df.filter(
            (col("category").isNull()) | (col("category") == "")
        ).count()
        if invalid_category > 0:
            errors.append(f"{invalid_category} records with invalid category")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Data validation passed")
        else:
            self.logger.error(f"Data validation failed: {errors}")
        
        return is_valid, errors
    
    def apply_value_mapping(self, df: DataFrame, mapping_config: Dict[str, Dict]) -> DataFrame:
        """
        Apply value mapping transformations
        
        Args:
            df: Input DataFrame
            mapping_config: Dictionary of column -> value mappings
            
        Returns:
            DataFrame with mapped values
        """
        self.logger.info("Applying value mappings")
        
        for column, mappings in mapping_config.items():
            if column in df.columns:
                # Create mapping expression
                mapping_expr = col(column)
                for old_val, new_val in mappings.items():
                    mapping_expr = when(col(column) == old_val, lit(new_val)) \
                        .otherwise(mapping_expr)
                
                df = df.withColumn(column, mapping_expr)
        
        return df