"""
Data transformation module for PySpark ETL pipeline.
Applies business rules, enrichment, and validation.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, current_timestamp,
    current_user, udf, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import List, Tuple
import logging


class ETLTransformer:
    """Handles data transformation and business rule application."""
    
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
        
    @staticmethod
    def get_transformed_schema() -> StructType:
        """Define transformed data schema."""
        return StructType([
            StructField("id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("value", DecimalType(15, 2), nullable=False),
            StructField("transformed_value", DecimalType(15, 2), nullable=False),
            StructField("status", StringType(), nullable=False),
            StructField("category", StringType(), nullable=False),
            StructField("priority", IntegerType(), nullable=False),
            StructField("etl_run_id", StringType(), nullable=False),
            StructField("processed_at", TimestampType(), nullable=False),
            StructField("processed_by", StringType(), nullable=False),
        ])
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply all transformations to source data.
        
        Args:
            source_df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        # Apply basic transformations
        df = self._apply_basic_transformations(source_df)
        
        # Apply business rules
        df = self._apply_business_rules(df)
        
        # Enrich data
        df = self._enrich_data(df)
        
        self.logger.info(f"Transformed {df.count()} records")
        
        return df
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply basic field transformations.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Applying basic transformations")
        
        df = df.select(
            col("id"),
            upper(trim(regexp_replace(col("name"), r'\s+', ' '))).alias("name"),
            col("value"),
            self._calculate_transformed_value(col("value"), col("category")).alias("transformed_value"),
            lit("TRANSFORMED").alias("status"),
            coalesce(col("category"), lit("UNCATEGORIZED")).alias("category"),
            self._calculate_priority(col("value"), col("category")).alias("priority"),
            lit(self.run_id).alias("etl_run_id"),
            current_timestamp().alias("processed_at"),
            current_user().alias("processed_by")
        )
        
        return df
    
    def _calculate_transformed_value(self, value_col, category_col):
        """Calculate transformed value based on category."""
        return when(category_col == "PREMIUM", value_col * 1.5) \
            .when(category_col == "VIP", value_col * 2.0) \
            .when(category_col == "STANDARD", value_col * 1.2) \
            .otherwise(value_col)
    
    def _calculate_priority(self, value_col, category_col):
        """Calculate priority based on value and category."""
        return when(value_col >= 1000, lit(1)) \
            .when((value_col >= 750) | (category_col == "VIP"), lit(2)) \
            .when(value_col >= 500, lit(3)) \
            .when(value_col >= 300, lit(4)) \
            .otherwise(lit(5))
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to transformed data.
        
        Args:
            df: DataFrame to apply rules to
            
        Returns:
            DataFrame with business rules applied
        """
        self.logger.info("Applying business rules")
        
        # Rule 1: Set status based on transformed value
        df = df.withColumn(
            "status",
            when(col("value").isNull(), lit("INVALID"))
            .when(col("transformed_value") >= 750, lit("HIGH_VALUE"))
            .when(col("transformed_value") >= 300, lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )
        
        # Rule 2: Override priority for high-value items
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, lit(1))
            .otherwise(col("priority"))
        )
        
        # Rule 3: Ensure category is not null
        df = df.withColumn(
            "category",
            when(col("category").isNull() | (col("category") == ""), lit("UNCATEGORIZED"))
            .otherwise(col("category"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional information.
        
        Args:
            df: DataFrame to enrich
            
        Returns:
            Enriched DataFrame
        """
        self.logger.info("Enriching data")
        
        # Load configuration for enrichment
        config_df = self.spark.read \
            .format("jdbc") \
            .option("url", "${db.url}") \
            .option("dbtable", "${db.config_table}") \
            .option("user", "${db.user}") \
            .option("password", "${db.password}") \
            .load() \
            .filter("is_active = true")
        
        # Apply premium multiplier if configured
        premium_config = config_df.filter("config_key = 'PREMIUM_MULTIPLIER'")
        if premium_config.count() > 0:
            multiplier = float(premium_config.first()["config_value"])
            df = df.withColumn(
                "transformed_value",
                when(col("category") == "PREMIUM", col("transformed_value") * multiplier)
                .otherwise(col("transformed_value"))
            )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        self.logger.info("Validating data")
        
        errors = []
        
        # Validation 1: Check for null IDs
        null_id_count = df.filter(col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with null ID")
        
        # Validation 2: Check for null names
        null_name_count = df.filter(col("name").isNull()).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with null name")
        
        # Validation 3: Check for invalid values
        invalid_value_count = df.filter(
            (col("value") < 0) | (col("transformed_value") < 0)
        ).count()
        if invalid_value_count > 0:
            errors.append(f"{invalid_value_count} records with negative values")
        
        # Validation 4: Check for invalid priorities
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        # Validation 5: Check for duplicate IDs
        duplicate_count = df.groupBy("id").count().filter(col("count") > 1).count()
        if duplicate_count > 0:
            errors.append(f"{duplicate_count} duplicate IDs found")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Validation passed")
        else:
            self.logger.warning(f"Validation failed with {len(errors)} errors")
        
        return is_valid, errors