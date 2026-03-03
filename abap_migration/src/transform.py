"""
PySpark ETL Transformer Module
Migrated from zcl_etl_transformer.abap
Converts to_upper()/condense() to upper()/trim()
Converts CASE statements to when().otherwise()
Converts LOOP AT assignments to withColumn() chains
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from typing import List, Tuple
import logging
from datetime import datetime


class ETLTransformer:
    """Transform extracted data with business rules."""
    
    def __init__(self, run_id: str, spark: SparkSession = None):
        """
        Initialize ETL Transformer.
        
        Args:
            run_id: Unique run identifier
            spark: SparkSession instance
        """
        self.run_id = run_id
        self.spark = spark or SparkSession.builder.getOrCreate()
        self.logger = logging.getLogger(__name__)
        
    def get_transformed_schema(self) -> StructType:
        """Define schema for transformed data."""
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
        Main transformation method - applies all transformations.
        
        Migration notes:
        - ABAP to_upper() + condense() -> upper() + trim()
        - ABAP LOOP AT -> withColumn() chains
        - ABAP CASE -> when().otherwise()
        
        Args:
            source_df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        current_timestamp = F.current_timestamp()
        current_user = F.lit("pyspark_user")  # Could be from config
        
        # Apply transformations using withColumn chains (replaces LOOP AT pattern)
        transformed_df = source_df \
            .withColumn("id", F.col("id")) \
            .withColumn("name", F.upper(F.trim(F.col("name")))) \
            .withColumn("value", F.col("value")) \
            .withColumn("transformed_value", 
                       self._calculate_derived_values(F.col("value"), F.col("category"))) \
            .withColumn("status", F.lit("TRANSFORMED")) \
            .withColumn("category", F.col("category")) \
            .withColumn("priority", 
                       self._calculate_priority(F.col("value"), F.col("category"))) \
            .withColumn("etl_run_id", F.lit(self.run_id)) \
            .withColumn("processed_at", current_timestamp) \
            .withColumn("processed_by", current_user)
        
        # Apply category-specific rules
        transformed_df = self._apply_category_rules(transformed_df)
        
        # Apply business rules
        transformed_df = self._apply_business_rules(transformed_df)
        
        # Enrich data
        transformed_df = self._enrich_data(transformed_df)
        
        record_count = transformed_df.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return transformed_df
    
    def _calculate_derived_values(self, value_col, category_col):
        """
        Calculate derived/transformed values.
        
        Migrated from ABAP calculate_derived_values method.
        
        Args:
            value_col: Value column
            category_col: Category column
            
        Returns:
            Column expression for transformed value
        """
        # ABAP CASE converted to when().otherwise() chain
        return F.when(category_col == "PREMIUM", value_col * 1.5) \
                .when(category_col == "VIP", value_col * 2.0) \
                .when(category_col == "STANDARD", value_col * 1.2) \
                .when(category_col == "BASIC", value_col * 1.0) \
                .otherwise(value_col * 1.1)
    
    def _calculate_priority(self, value_col, category_col):
        """
        Calculate priority based on value and category.
        
        Migrated from ABAP calculate_priority method.
        Uses when().otherwise() instead of CASE statements.
        
        Args:
            value_col: Value column
            category_col: Category column
            
        Returns:
            Column expression for priority
        """
        # Complex priority logic using nested when/otherwise
        return F.when(value_col >= 1000, 1) \
                .when((value_col >= 750) & (category_col == "PREMIUM"), 1) \
                .when(value_col >= 500, 2) \
                .when(value_col >= 250, 3) \
                .when(value_col >= 100, 4) \
                .otherwise(5)
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply category-specific transformation rules.
        
        Migrated from ABAP apply_category_rules method.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with category rules applied
        """
        # Apply premium category boost
        df = df.withColumn(
            "transformed_value",
            F.when(F.col("category") == "PREMIUM", 
                   F.col("transformed_value") * 1.2)
            .otherwise(F.col("transformed_value"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business transformation rules.
        
        Migrated from ABAP apply_business_rules method.
        Uses withColumn chains instead of LOOP AT ASSIGNING.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with business rules applied
        """
        self.logger.info("Applying business rules")
        
        # Rule 1: Set status based on value (ABAP CASE -> when/otherwise)
        df = df.withColumn(
            "status",
            F.when(F.col("value").isNull(), "INVALID")
            .when(F.col("transformed_value") >= 750, "HIGH_VALUE")
            .when(F.col("transformed_value") >= 300, "MEDIUM_VALUE")
            .otherwise("LOW_VALUE")
        )
        
        # Rule 2: Priority override for high value items
        df = df.withColumn(
            "priority",
            F.when(F.col("transformed_value") >= 1000, 1)
            .otherwise(F.col("priority"))
        )
        
        # Rule 3: Name normalization (ABAP CONDENSE -> trim + regexp_replace)
        df = df.withColumn(
            "name",
            F.trim(F.regexp_replace(F.col("name"), "\\s+", " "))
        )
        
        # Rule 4: Category validation
        df = df.withColumn(
            "category",
            F.when(F.col("category").isNull() | (F.col("category") == ""), 
                   "UNCATEGORIZED")
            .otherwise(F.col("category"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional calculated fields.
        
        Migrated from ABAP enrich_data method.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Enriched DataFrame
        """
        self.logger.info("Enriching data")
        
        # Load enrichment configuration (placeholder - would load from config table)
        config_multiplier = 1.2
        
        # Apply premium enrichment
        df = df.withColumn(
            "transformed_value",
            F.when(F.col("category") == "PREMIUM",
                   F.col("transformed_value") * config_multiplier)
            .otherwise(F.col("transformed_value"))
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
        
        # Validation rule 1: ID is required
        null_id_count = df.filter(F.col("id").isNull() | (F.col("id") == "")).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with missing ID")
        
        # Validation rule 2: Name is required
        null_name_count = df.filter(F.col("name").isNull() | (F.col("name") == "")).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with missing name")
        
        # Validation rule 3: Value must be positive
        invalid_value_count = df.filter(F.col("value") < 0).count()
        if invalid_value_count > 0:
            errors.append(f"{invalid_value_count} records with negative values")
        
        # Validation rule 4: Priority must be 1-5
        invalid_priority_count = df.filter(
            (F.col("priority") < 1) | (F.col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Data validation passed")
        else:
            self.logger.error(f"Data validation failed: {errors}")
        
        return is_valid, errors