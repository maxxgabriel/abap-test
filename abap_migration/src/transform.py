"""
PySpark Data Transformer
Replaces ABAP zcl_etl_transformer with DataFrame transformations
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, current_timestamp,
    udf, lit, coalesce, concat_ws, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, 
    TimestampType, IntegerType
)
from typing import List, Tuple, Dict, Any
import logging


class DataTransformer:
    """
    PySpark transformer applying business rules and data enrichment.
    Replaces ABAP transformation logic with DataFrame operations.
    """
    
    def __init__(self, spark: SparkSession, run_id: str):
        """
        Initialize transformer.
        
        Args:
            spark: SparkSession instance
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.run_id = run_id
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
        Main transformation method applying all business rules.
        Replaces ABAP transform_data method with DataFrame operations.
        
        Args:
            source_df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        # Step 1: Basic transformations (name normalization, etc.)
        df = self._apply_basic_transformations(source_df)
        
        # Step 2: Calculate derived values
        df = self._calculate_derived_values(df)
        
        # Step 3: Calculate priority
        df = self._calculate_priority(df)
        
        # Step 4: Add metadata
        df = self._add_metadata(df)
        
        # Step 5: Apply business rules
        df = self.apply_business_rules(df)
        
        # Step 6: Enrich data
        df = self.enrich_data(df)
        
        record_count = df.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return df
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply basic transformations like name normalization.
        Replaces ABAP string operations with Spark functions.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with basic transformations applied
        """
        return df.withColumn(
            "name",
            upper(trim(regexp_replace(col("name"), "\\s+", " ")))
        )
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate transformed value based on category.
        Replaces ABAP calculate_derived_values method.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with transformed_value column
        """
        # Premium category gets 1.5x multiplier
        # Standard gets 1.2x
        # Others get 1.0x
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("value") * 1.5)
            .when(col("category") == "STANDARD", col("value") * 1.2)
            .when(col("category") == "VIP", col("value") * 2.0)
            .otherwise(col("value"))
        )
        
        return df
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """
        Calculate priority based on value and category.
        Replaces ABAP calculate_priority method.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with priority column
        """
        # Priority rules:
        # 1 (highest) - VIP or value >= 1000
        # 2 - Premium or value >= 750
        # 3 - Standard or value >= 500
        # 4 - value >= 250
        # 5 (lowest) - everything else
        df = df.withColumn(
            "priority",
            when((col("category") == "VIP") | (col("transformed_value") >= 1000), 1)
            .when((col("category") == "PREMIUM") | (col("transformed_value") >= 750), 2)
            .when((col("category") == "STANDARD") | (col("transformed_value") >= 500), 3)
            .when(col("transformed_value") >= 250, 4)
            .otherwise(5)
        )
        
        return df
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add ETL metadata columns.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with metadata columns
        """
        return df \
            .withColumn("etl_run_id", lit(self.run_id)) \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("processed_by", lit("spark_etl"))
    
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to data.
        Replaces ABAP apply_business_rules method.
        
        Args:
            df: Input DataFrame
            
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
        
        # Rule 2: Priority override for very high value items
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, 1)
            .otherwise(col("priority"))
        )
        
        # Rule 3: Category validation and default
        df = df.withColumn(
            "category",
            when(col("category").isNull() | (col("category") == ""), "UNCATEGORIZED")
            .otherwise(col("category"))
        )
        
        return df
    
    def enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional information from config.
        Replaces ABAP enrich_data method.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Enriched DataFrame
        """
        self.logger.info("Enriching data")
        
        # Load enrichment configuration (could be from Delta table, config file, etc.)
        # For now, applying sample enrichment logic
        
        # Additional enrichment: Premium category gets extra boost
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("transformed_value") * 1.2)
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data quality.
        Replaces ABAP validate_data method.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid: bool, validation_errors: List[str])
        """
        self.logger.info("Validating transformed data")
        
        validation_errors = []
        
        # Validation rule 1: ID is required
        null_id_count = df.filter(col("id").isNull()).count()
        if null_id_count > 0:
            validation_errors.append(f"{null_id_count} records with null ID")
        
        # Validation rule 2: Name is required
        null_name_count = df.filter(col("name").isNull()).count()
        if null_name_count > 0:
            validation_errors.append(f"{null_name_count} records with null name")
        
        # Validation rule 3: Value must be non-negative
        negative_value_count = df.filter(col("value") < 0).count()
        if negative_value_count > 0:
            validation_errors.append(f"{negative_value_count} records with negative value")
        
        # Validation rule 4: Priority must be 1-5
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            validation_errors.append(f"{invalid_priority_count} records with invalid priority")
        
        # Validation rule 5: transformed_value must be >= value
        invalid_transform_count = df.filter(
            col("transformed_value") < col("value")
        ).count()
        if invalid_transform_count > 0:
            validation_errors.append(f"{invalid_transform_count} records with invalid transformation")
        
        is_valid = len(validation_errors) == 0
        
        if is_valid:
            self.logger.info("Data validation passed")
        else:
            self.logger.error(f"Data validation failed: {validation_errors}")
        
        return is_valid, validation_errors
    
    def apply_category_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply category-specific transformation rules.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with category rules applied
        """
        # Category-specific rules can be added here
        # For example, special handling for VIP category
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "VIP", col("transformed_value") * 1.1)
            .otherwise(col("transformed_value"))
        )
        
        return df