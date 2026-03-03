"""ETL Data Transformation Module"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, 
    current_timestamp, current_user, concat_ws
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import List, Tuple
import logging


class ETLTransformer:
    """Transform and validate data"""
    
    def __init__(self, spark: SparkSession, run_id: str):
        self.spark = spark
        self.run_id = run_id
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
        """Main transformation method"""
        self.logger.info("Starting transformation")
        
        # Apply transformations
        df = source_df.select(
            col("id"),
            upper(trim(col("name"))).alias("name"),
            col("value"),
            self._calculate_transformed_value(col("value"), col("category")).alias("transformed_value"),
            lit("TRANSFORMED").alias("status"),
            col("category"),
            self._calculate_priority(col("value"), col("category")).alias("priority"),
            lit(self.run_id).alias("etl_run_id"),
            current_timestamp().alias("processed_at"),
            current_user().alias("processed_by")
        )
        
        # Clean up names
        df = df.withColumn("name", regexp_replace(col("name"), r"\s+", " "))
        
        # Apply business rules
        df = self._apply_business_rules(df)
        
        # Enrich data
        df = self._enrich_data(df)
        
        self.logger.info(f"Transformed {df.count()} records")
        
        return df
    
    def _calculate_transformed_value(self, value_col, category_col):
        """Calculate transformed value based on category"""
        return when(category_col == "PREMIUM", value_col * 1.5) \
            .when(category_col == "VIP", value_col * 2.0) \
            .when(category_col == "STANDARD", value_col * 1.2) \
            .otherwise(value_col)
    
    def _calculate_priority(self, value_col, category_col):
        """Calculate priority based on value and category"""
        return when(value_col >= 1000, 1) \
            .when((value_col >= 750) | (category_col == "VIP"), 2) \
            .when(value_col >= 500, 3) \
            .when(value_col >= 250, 4) \
            .otherwise(5)
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to data"""
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
        
        # Rule 3: Handle null categories
        df = df.withColumn(
            "category",
            when(col("category").isNull(), "UNCATEGORIZED")
            .otherwise(col("category"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional information"""
        self.logger.info("Enriching data")
        
        # Apply premium multiplier to premium category
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("transformed_value") * 1.2)
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """Validate transformed data"""
        errors = []
        
        # Rule 1: ID is required
        null_ids = df.filter(col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null ID")
        
        # Rule 2: Name is required
        null_names = df.filter(col("name").isNull()).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null name")
        
        # Rule 3: Value must be positive
        negative_values = df.filter(col("value") < 0).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative values")
        
        # Rule 4: Priority must be 1-5
        invalid_priority = df.filter((col("priority") < 1) | (col("priority") > 5)).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Data validation passed")
        else:
            self.logger.warning(f"Data validation failed: {errors}")
        
        return is_valid, errors