"""
ETL Transformer Module
Handles data transformation and business logic
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, upper, trim, when, lit, current_timestamp,
    regexp_replace, coalesce, round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import List, Tuple
import logging


class ETLTransformer:
    """Transformer class for applying business rules and enrichment"""
    
    def __init__(self, spark: SparkSession, run_id: str, config: dict):
        """
        Initialize transformer
        
        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def get_transformed_schema(self) -> StructType:
        """Define transformed data schema"""
        return StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("value", DecimalType(15, 2), True),
            StructField("transformed_value", DecimalType(15, 2), True),
            StructField("status", StringType(), True),
            StructField("category", StringType(), True),
            StructField("priority", IntegerType(), True),
            StructField("etl_run_id", StringType(), False),
            StructField("processed_at", TimestampType(), False),
            StructField("processed_by", StringType(), True)
        ])
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply all transformations to source data
        
        Args:
            source_df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        # Basic transformations
        df = source_df.select(
            col("id"),
            upper(trim(regexp_replace(col("name"), r'\s+', ' '))).alias("name"),
            col("value"),
            col("category"),
            col("status")
        )
        
        # Calculate derived values
        df = self._calculate_derived_values(df)
        
        # Apply business rules
        df = self._apply_business_rules(df)
        
        # Enrich data
        df = self._enrich_data(df)
        
        # Add metadata
        df = df.withColumn("etl_run_id", lit(self.run_id)) \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("processed_by", lit(self.config.get("user", "system")))
        
        record_count = df.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return df
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate derived values based on source data
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with derived values
        """
        # Get category multipliers from config
        premium_multiplier = float(self.config.get("premium_multiplier", 1.5))
        standard_multiplier = float(self.config.get("standard_multiplier", 1.2))
        
        # Calculate transformed value based on category
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("value") * premium_multiplier)
            .when(col("category") == "VIP", col("value") * 1.8)
            .when(col("category") == "STANDARD", col("value") * standard_multiplier)
            .otherwise(col("value"))
        )
        
        # Round to 2 decimal places
        df = df.withColumn("transformed_value", spark_round(col("transformed_value"), 2))
        
        # Calculate priority
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, 1)
            .when(col("transformed_value") >= 750, 2)
            .when(col("transformed_value") >= 500, 3)
            .when(col("transformed_value") >= 250, 4)
            .otherwise(5)
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to data
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with business rules applied
        """
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
            coalesce(col("category"), lit("UNCATEGORIZED"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional information
        
        Args:
            df: Input DataFrame
            
        Returns:
            Enriched DataFrame
        """
        # Load enrichment configuration if available
        enrichment_enabled = self.config.get("enable_enrichment", True)
        
        if enrichment_enabled:
            # Apply category-specific enrichment
            df = df.withColumn(
                "transformed_value",
                when((col("category") == "PREMIUM") & (col("transformed_value") < 1000),
                     col("transformed_value") * 1.2)
                .otherwise(col("transformed_value"))
            )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        validation_errors = []
        
        # Validation 1: Check for null IDs
        null_id_count = df.filter(col("id").isNull()).count()
        if null_id_count > 0:
            validation_errors.append(f"{null_id_count} records with null ID")
        
        # Validation 2: Check for null names
        null_name_count = df.filter(col("name").isNull()).count()
        if null_name_count > 0:
            validation_errors.append(f"{null_name_count} records with null name")
        
        # Validation 3: Check for negative values
        negative_value_count = df.filter(col("value") < 0).count()
        if negative_value_count > 0:
            validation_errors.append(f"{negative_value_count} records with negative value")
        
        # Validation 4: Check priority range
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            validation_errors.append(f"{invalid_priority_count} records with invalid priority")
        
        # Validation 5: Check for duplicates
        total_count = df.count()
        distinct_count = df.select("id").distinct().count()
        if total_count != distinct_count:
            validation_errors.append(f"{total_count - distinct_count} duplicate IDs found")
        
        is_valid = len(validation_errors) == 0
        
        if is_valid:
            self.logger.info("Data validation passed")
        else:
            self.logger.warning(f"Data validation failed: {validation_errors}")
        
        return is_valid, validation_errors