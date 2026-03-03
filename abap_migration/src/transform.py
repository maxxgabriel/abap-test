"""
PySpark Data Transformation Module

Migrated from ABAP zcl_etl_transformer class.
Converts ABAP to_upper()/condense() to upper()/trim(), 
CASE statements to when().otherwise(), and LOOP AT to withColumn() chains.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, 
    current_timestamp, current_user
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from typing import List, Tuple

from src.logger import ETLLogger
from src.config import ConfigManager


class DataTransformer:
    """Transform extracted data applying business rules and enrichment."""
    
    # Define transformed data schema
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
    
    def __init__(self, run_id: str):
        """
        Initialize transformer with run ID.
        
        Args:
            run_id: Unique identifier for this ETL run
        """
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
        self.config = ConfigManager()
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Main transformation method applying all transformations.
        
        Migrates ABAP LOOP AT pattern to DataFrame.withColumn() chains.
        Converts to_upper(condense()) to upper(trim()).
        
        Args:
            source_df: Source data DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Starting transformation"
        )
        
        # Chain transformations using withColumn() pattern
        # Replaces ABAP: LOOP AT it_source_data INTO DATA(ls_source)
        transformed_df = source_df \
            .withColumn("id", col("id")) \
            .withColumn("name", upper(trim(regexp_replace(col("name"), "\\s+", " ")))) \
            .withColumn("value", col("value")) \
            .withColumn("transformed_value", 
                       self._calculate_derived_values(col("value"), col("category"))) \
            .withColumn("status", lit("TRANSFORMED")) \
            .withColumn("category", col("category")) \
            .withColumn("priority", 
                       self._calculate_priority(col("value"), col("category"))) \
            .withColumn("etl_run_id", lit(self.run_id)) \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("processed_by", lit(current_user()))
        
        # Apply category-specific rules
        transformed_df = self._apply_category_rules(transformed_df)
        
        # Apply business rules
        transformed_df = self._apply_business_rules(transformed_df)
        
        # Enrich data
        transformed_df = self._enrich_data(transformed_df)
        
        record_count = transformed_df.count()
        self.logger.log_info(
            component="TRANSFORMER",
            message=f"Transformed {record_count} records"
        )
        
        return transformed_df
    
    def _calculate_derived_values(self, value_col, category_col):
        """
        Calculate derived values based on category.
        
        Migrates ABAP CASE statement to when().otherwise() chain.
        
        Args:
            value_col: Column expression for value
            category_col: Column expression for category
            
        Returns:
            Column expression with calculated value
        """
        premium_multiplier = float(self.config.get("premium_multiplier", "1.5"))
        
        # CASE category migration to when().otherwise()
        return when(category_col == "PREMIUM", value_col * premium_multiplier) \
            .when(category_col == "VIP", value_col * 2.0) \
            .when(category_col == "STANDARD", value_col * 1.2) \
            .when(category_col == "BASIC", value_col * 1.0) \
            .when(category_col == "TRIAL", value_col * 0.8) \
            .otherwise(value_col)
    
    def _calculate_priority(self, value_col, category_col):
        """
        Calculate priority based on value and category.
        
        Uses when().otherwise() for CASE logic.
        
        Args:
            value_col: Column expression for value
            category_col: Column expression for category
            
        Returns:
            Column expression with priority (1-5)
        """
        return when(value_col >= 1000, 1) \
            .when((value_col >= 750) & (category_col == "PREMIUM"), 1) \
            .when(value_col >= 500, 2) \
            .when(value_col >= 300, 3) \
            .when(value_col >= 100, 4) \
            .otherwise(5)
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply category-specific transformation rules.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with category rules applied
        """
        # Apply premium category bonus
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", 
                 col("transformed_value") * 1.2)
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to transformed data.
        
        Migrates ABAP LOOP AT with IF-ELSEIF-ELSE to when().otherwise().
        Replaces ABAP condense() and REPLACE ALL with trim() and regexp_replace().
        
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
        # Migrates ABAP IF-ELSEIF-ELSE to when().otherwise()
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
        
        # Rule 3: Name normalization
        # Migrates ABAP: REPLACE ALL OCCURRENCES OF '  ' IN <fs_data>-name WITH ' '
        # and CONDENSE <fs_data>-name
        df = df.withColumn(
            "name",
            trim(regexp_replace(col("name"), "\\s+", " "))
        )
        
        # Rule 4: Category validation
        df = df.withColumn(
            "category",
            when(col("category").isNull() | (col("category") == ""), 
                 "UNCATEGORIZED")
            .otherwise(col("category"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional calculated fields.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Enriched DataFrame
        """
        # Load enrichment configuration
        enable_reconciliation = self.config.get("enable_reconciliation", "true")
        
        # Apply enrichment based on category
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", 
                 col("transformed_value") * 1.2)
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
        
        # Validation rule 1: ID is required
        null_id_count = df.filter(col("id").isNull() | (col("id") == "")).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with missing ID")
        
        # Validation rule 2: Name is required
        null_name_count = df.filter(col("name").isNull() | (col("name") == "")).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with missing name")
        
        # Validation rule 3: Value must be non-negative
        negative_value_count = df.filter(col("value") < 0).count()
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