"""
PySpark ETL Transformer Module
Migrated from ABAP zcl_etl_transformer
Converts ABAP functions to PySpark operations:
- to_upper() + condense() → upper() + trim()
- CASE statements → when().otherwise()
- LOOP AT assignments → withColumn() chains
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, 
    current_timestamp, lit, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import List, Tuple
from datetime import datetime
from src.logger import ETLLogger


class ETLTransformer:
    """Transform extracted data with business rules"""
    
    def __init__(self, spark: SparkSession, config: dict, run_id: str):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
    
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
            StructField("processed_by", StringType(), True)
        ])
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Main transformation method
        Converts ABAP LOOP AT assignments to DataFrame withColumn() chains
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Starting transformation"
        )
        
        # Convert ABAP to_upper(condense(name)) to upper(trim(name))
        # ABAP: name = to_upper( condense( ls_source-name ) )
        df = df.withColumn(
            "name",
            upper(trim(regexp_replace(col("name"), "\\s+", " ")))
        )
        
        # Calculate derived values
        df = df.withColumn(
            "transformed_value",
            self._calculate_derived_values(col("value"), col("category"))
        )
        
        # Calculate priority
        df = df.withColumn(
            "priority",
            self._calculate_priority(col("value"), col("category"))
        )
        
        # Add ETL metadata columns (replacing ABAP LOOP AT VALUE assignments)
        df = df.withColumn("etl_run_id", lit(self.run_id)) \
               .withColumn("processed_at", current_timestamp()) \
               .withColumn("processed_by", lit("pyspark_etl")) \
               .withColumn("status", lit("TRANSFORMED"))
        
        # Apply category-specific rules
        df = self._apply_category_rules(df)
        
        # Apply business rules
        df = self._apply_business_rules(df)
        
        # Enrich data
        df = self._enrich_data(df)
        
        record_count = df.count()
        self.logger.log_info(
            component="TRANSFORMER",
            message=f"Transformed {record_count} records"
        )
        
        return df
    
    def _calculate_derived_values(self, value_col, category_col):
        """
        Calculate transformed value based on category
        Converts ABAP CASE to when().otherwise()
        """
        # ABAP CASE statement converted to when().otherwise()
        # CASE iv_category
        #   WHEN 'PREMIUM' -> value * 1.5
        #   WHEN 'VIP' -> value * 2.0
        #   WHEN OTHERS -> value * 1.1
        return when(category_col == "PREMIUM", value_col * 1.5) \
               .when(category_col == "VIP", value_col * 2.0) \
               .when(category_col == "STANDARD", value_col * 1.2) \
               .otherwise(value_col * 1.1)
    
    def _calculate_priority(self, value_col, category_col):
        """
        Calculate priority based on value and category
        Converts ABAP IF-ELSEIF to when().otherwise() chain
        """
        # ABAP IF-ELSEIF converted to when().otherwise()
        # IF value >= 1000 THEN priority = 1
        # ELSEIF value >= 500 THEN priority = 2
        # ELSE priority = 3
        return when(value_col >= 1000, lit(1)) \
               .when(value_col >= 500, lit(2)) \
               .when(value_col >= 250, lit(3)) \
               .when(value_col >= 100, lit(4)) \
               .otherwise(lit(5))
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply category-specific transformations
        ABAP apply_category_rules converted to withColumn chains
        """
        # Different rules per category using when().otherwise()
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("transformed_value") * 1.2)
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to transformed data
        Converts ABAP LOOP AT with IF statements to when().otherwise()
        
        ABAP code:
        LOOP AT ct_data ASSIGNING FIELD-SYMBOL(<fs_data>).
          IF <fs_data>-value IS INITIAL.
            <fs_data>-status = 'INVALID'.
          ELSEIF <fs_data>-transformed_value >= 750.
            <fs_data>-status = 'HIGH_VALUE'.
          ...
        ENDLOOP.
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Applying business rules"
        )
        
        # Rule 1: Set status based on value (CASE to when().otherwise())
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
        
        # Rule 3: Name normalization (ABAP REPLACE + CONDENSE)
        # ABAP: REPLACE ALL OCCURRENCES OF '  ' IN name WITH ' '
        df = df.withColumn(
            "name",
            trim(regexp_replace(col("name"), "\\s+", " "))
        )
        
        # Rule 4: Category validation
        df = df.withColumn(
            "category",
            when(col("category").isNull() | (col("category") == ""), lit("UNCATEGORIZED"))
            .otherwise(col("category"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional calculations
        ABAP LOOP AT with enrichment logic
        """
        # Load enrichment config (simulated)
        premium_multiplier = self.config.get('business_rules', {}).get('premium_multiplier', 1.2)
        
        # Apply enrichment based on category
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("transformed_value") * premium_multiplier)
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data
        Returns (is_valid, error_messages)
        """
        validation_errors = []
        is_valid = True
        
        # Rule 1: ID is required
        null_ids = df.filter(col("id").isNull()).count()
        if null_ids > 0:
            validation_errors.append(f"{null_ids} records with missing ID")
            is_valid = False
        
        # Rule 2: Name is required
        null_names = df.filter(col("name").isNull() | (col("name") == "")).count()
        if null_names > 0:
            validation_errors.append(f"{null_names} records with missing name")
            is_valid = False
        
        # Rule 3: Value must be positive
        negative_values = df.filter(col("value") < 0).count()
        if negative_values > 0:
            validation_errors.append(f"{negative_values} records with negative values")
            is_valid = False
        
        # Rule 4: Priority must be 1-5
        invalid_priority = df.filter((col("priority") < 1) | (col("priority") > 5)).count()
        if invalid_priority > 0:
            validation_errors.append(f"{invalid_priority} records with invalid priority")
            is_valid = False
        
        return is_valid, validation_errors