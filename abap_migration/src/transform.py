"""
PySpark Data Transformer
Converted from ABAP class zcl_etl_transformer
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
from typing import List, Tuple
import logging


class DataTransformer:
    """
    Transforms extracted data using PySpark DataFrame operations.
    Applies business rules, enrichment, and validation.
    """
    
    def __init__(self, spark: SparkSession, run_id: str):
        """
        Initialize the transformer.
        
        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
        """
        self.spark = spark
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def get_transformed_schema(self) -> StructType:
        """
        Define the schema for transformed data.
        
        Returns:
            StructType schema definition
        """
        return StructType([
            StructField("id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("value", DecimalType(15, 2), nullable=True),
            StructField("transformed_value", DecimalType(15, 2), nullable=True),
            StructField("status", StringType(), nullable=True),
            StructField("category", StringType(), nullable=True),
            StructField("priority", IntegerType(), nullable=True),
            StructField("etl_run_id", StringType(), nullable=True),
            StructField("processed_at", TimestampType(), nullable=True),
            StructField("processed_by", StringType(), nullable=True)
        ])
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Main transformation method.
        Applies all transformation steps in sequence.
        
        Args:
            source_df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        timestamp = datetime.now()
        
        # Apply transformations using DataFrame operations
        df = source_df.select(
            F.col("id"),
            F.upper(F.trim(F.col("name"))).alias("name"),
            F.col("value"),
            self._calculate_derived_values(F.col("value"), F.col("category")).alias("transformed_value"),
            F.lit("TRANSFORMED").alias("status"),
            F.col("category"),
            self._calculate_priority(F.col("value"), F.col("category")).alias("priority"),
            F.lit(self.run_id).alias("etl_run_id"),
            F.lit(timestamp).alias("processed_at"),
            F.lit("spark_user").alias("processed_by")
        )
        
        # Apply category-specific rules
        df = self._apply_category_rules(df)
        
        # Apply business rules
        df = self._apply_business_rules(df)
        
        # Enrich data
        df = self._enrich_data(df)
        
        record_count = df.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return df
    
    def _calculate_derived_values(self, value_col, category_col):
        """
        Calculate derived/transformed values based on business logic.
        Uses PySpark when() for conditional logic.
        
        Args:
            value_col: Value column
            category_col: Category column
            
        Returns:
            Column expression with transformed value
        """
        return F.when(category_col == "PREMIUM", value_col * 1.5) \
            .when(category_col == "STANDARD", value_col * 1.2) \
            .when(category_col == "VIP", value_col * 1.8) \
            .otherwise(value_col)
    
    def _calculate_priority(self, value_col, category_col):
        """
        Calculate priority based on value and category.
        
        Args:
            value_col: Value column
            category_col: Category column
            
        Returns:
            Column expression with priority (1-5)
        """
        return F.when(value_col >= 1000, 1) \
            .when((value_col >= 750) & (category_col == "PREMIUM"), 2) \
            .when(value_col >= 500, 2) \
            .when(value_col >= 250, 3) \
            .when(value_col >= 100, 4) \
            .otherwise(5)
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply category-specific transformation rules.
        
        Args:
            df: DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        # Add category-specific transformations
        df = df.withColumn(
            "transformed_value",
            F.when(F.col("category") == "PREMIUM", F.col("transformed_value") * 1.2)
            .otherwise(F.col("transformed_value"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to the data.
        Replaces ABAP LOOP with DataFrame transformations.
        
        Args:
            df: DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Applying business rules")
        
        # Rule 1: Set status based on transformed value
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
        
        # Rule 3: Name normalization (remove multiple spaces)
        df = df.withColumn(
            "name",
            F.regexp_replace(F.trim(F.col("name")), "\\s+", " ")
        )
        
        # Rule 4: Category validation
        df = df.withColumn(
            "category",
            F.when(F.col("category").isNull() | (F.col("category") == ""), "UNCATEGORIZED")
            .otherwise(F.col("category"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional calculated fields or lookups.
        
        Args:
            df: DataFrame to enrich
            
        Returns:
            Enriched DataFrame
        """
        self.logger.info("Enriching data")
        
        # Load configuration for enrichment (if needed)
        # config_df = self.spark.read.format("delta").load("path/to/config")
        
        # Example enrichment: Add percentile rank
        window_spec = Window.orderBy("transformed_value")
        df = df.withColumn("value_percentile", F.percent_rank().over(window_spec))
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        Returns validation status and list of errors.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, error_list)
        """
        errors = []
        
        # Validation rule 1: ID is required
        null_ids = df.filter(F.col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with missing ID")
        
        # Validation rule 2: Name is required
        null_names = df.filter(F.col("name").isNull() | (F.col("name") == "")).count()
        if null_names > 0:
            errors.append(f"{null_names} records with missing name")
        
        # Validation rule 3: Value must be positive
        negative_values = df.filter(F.col("value") < 0).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative values")
        
        # Validation rule 4: Priority must be 1-5
        invalid_priority = df.filter(
            (F.col("priority") < 1) | (F.col("priority") > 5)
        ).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Data validation passed")
        else:
            self.logger.warning(f"Data validation failed with {len(errors)} errors")
            
        return is_valid, errors