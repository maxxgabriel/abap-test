"""
ETL Data Transformation Module
Applies business rules, enrichment, and validation to extracted data
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, current_timestamp,
    current_user, udf, monotonically_increasing_id
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import List, Tuple
import logging


class ETLTransformer:
    """
    Handles data transformation, business rule application, and validation
    """
    
    def __init__(self, spark: SparkSession, run_id: str):
        """
        Initialize transformer with Spark session
        
        Args:
            spark: Active SparkSession
            run_id: Unique run identifier
        """
        self.spark = spark
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
    
    def get_transformed_schema(self) -> StructType:
        """
        Define schema for transformed data
        
        Returns:
            StructType schema definition
        """
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
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Main transformation pipeline
        
        Args:
            source_df: Source DataFrame from extraction
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting data transformation")
        
        # Initial transformations
        df = source_df.select(
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
        df = self.apply_business_rules(df)
        
        # Enrich data
        df = self.enrich_data(df)
        
        record_count = df.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return df
    
    def _calculate_derived_value(self, value_col, category_col):
        """
        Calculate derived/transformed value based on business rules
        
        Args:
            value_col: Value column
            category_col: Category column
            
        Returns:
            Column expression for transformed value
        """
        return when(category_col == "PREMIUM", value_col * 1.5) \
            .when(category_col == "VIP", value_col * 2.0) \
            .when(category_col == "STANDARD", value_col * 1.2) \
            .when(category_col == "BASIC", value_col * 1.0) \
            .when(category_col == "TRIAL", value_col * 0.8) \
            .otherwise(value_col)
    
    def _calculate_priority(self, value_col, category_col):
        """
        Calculate priority based on value and category
        
        Args:
            value_col: Value column
            category_col: Category column
            
        Returns:
            Column expression for priority (1-5)
        """
        return when(value_col >= 1000, 1) \
            .when((value_col >= 750) | (category_col == "PREMIUM"), 2) \
            .when((value_col >= 500) | (category_col == "VIP"), 2) \
            .when(value_col >= 300, 3) \
            .when(value_col >= 100, 4) \
            .otherwise(5)
    
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to transformed data
        
        Args:
            df: DataFrame to apply rules to
            
        Returns:
            DataFrame with business rules applied
        """
        self.logger.info("Applying business rules")
        
        # Rule 1: Set status based on value
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
        
        # Rule 3: Category validation
        df = df.withColumn(
            "category",
            when(col("category").isNull() | (trim(col("category")) == ""), "UNCATEGORIZED")
            .otherwise(col("category"))
        )
        
        # Rule 4: Ensure name is properly formatted
        df = df.withColumn(
            "name",
            regexp_replace(trim(col("name")), "\\s+", " ")
        )
        
        return df
    
    def enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional calculated fields
        
        Args:
            df: DataFrame to enrich
            
        Returns:
            Enriched DataFrame
        """
        self.logger.info("Enriching data")
        
        # Load enrichment configuration
        config_multiplier = float(
            self.spark.conf.get("spark.etl.premium.multiplier", "1.2")
        )
        
        # Apply premium multiplier if configured
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("transformed_value") * config_multiplier)
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data against business rules
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        self.logger.info("Validating transformed data")
        
        errors = []
        
        # Rule 1: ID is required
        null_id_count = df.filter(col("id").isNull() | (trim(col("id")) == "")).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with missing ID")
        
        # Rule 2: Name is required
        null_name_count = df.filter(col("name").isNull() | (trim(col("name")) == "")).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with missing name")
        
        # Rule 3: Value must be positive
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
        
        # Rule 5: Category must not be empty
        null_category_count = df.filter(
            col("category").isNull() | (trim(col("category")) == "")
        ).count()
        if null_category_count > 0:
            errors.append(f"{null_category_count} records with missing category")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Validation passed")
        else:
            self.logger.error(f"Validation failed with {len(errors)} errors")
            for error in errors:
                self.logger.error(f"  - {error}")
        
        return is_valid, errors