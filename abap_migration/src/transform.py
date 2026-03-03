"""
PySpark ETL Transformer Module
Applies business rules, data enrichment, and validation using DataFrame operations
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, current_timestamp,
    current_user, lit, coalesce, concat_ws
)
from typing import Dict, Any, Tuple, List
import logging


class ETLTransformer:
    """Handles data transformation with business rules"""
    
    # Define schema for transformed data
    TRANSFORMED_SCHEMA = StructType([
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
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize transformer
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Main transformation method - applies all transformations
        
        Args:
            source_df: Source DataFrame from extractor
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        # Step 1: Basic transformations and derived values
        df = self._apply_basic_transformations(source_df)
        
        # Step 2: Calculate derived values
        df = self._calculate_derived_values(df)
        
        # Step 3: Calculate priority
        df = self._calculate_priority(df)
        
        # Step 4: Apply category-specific rules
        df = self._apply_category_rules(df)
        
        # Step 5: Apply business rules
        df = self._apply_business_rules(df)
        
        # Step 6: Enrich data
        df = self._enrich_data(df)
        
        # Step 7: Add ETL metadata
        df = self._add_etl_metadata(df)
        
        record_count = df.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return df
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic string transformations without loops"""
        return df.select(
            col("id"),
            # Name normalization: uppercase, trim, remove extra spaces
            upper(trim(regexp_replace(col("name"), "\\s+", " "))).alias("name"),
            col("value"),
            col("status"),
            coalesce(col("category"), lit("UNCATEGORIZED")).alias("category"),
            col("source_system"),
            col("created_at"),
            col("created_by"),
            col("changed_at"),
            col("changed_by")
        )
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """Calculate transformed values based on category using conditional logic"""
        # Different transformation logic per category
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("value") * lit(1.5))
            .when(col("category") == "VIP", col("value") * lit(2.0))
            .when(col("category") == "STANDARD", col("value") * lit(1.2))
            .when(col("category") == "BASIC", col("value") * lit(1.0))
            .when(col("category") == "TRIAL", col("value") * lit(0.8))
            .otherwise(col("value"))
        )
        
        return df
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """Calculate priority based on value and category"""
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, lit(1))  # Highest priority
            .when(col("transformed_value") >= 750, lit(2))
            .when(col("transformed_value") >= 500, lit(3))
            .when(col("transformed_value") >= 250, lit(4))
            .otherwise(lit(5))  # Lowest priority
        )
        
        # Priority boost for premium categories
        df = df.withColumn(
            "priority",
            when(
                (col("category").isin(["PREMIUM", "VIP"])) & (col("priority") > 2),
                col("priority") - 1
            ).otherwise(col("priority"))
        )
        
        return df
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """Apply category-specific business rules"""
        # VIP category gets minimum 20% boost
        df = df.withColumn(
            "transformed_value",
            when(
                (col("category") == "VIP") & (col("transformed_value") < col("value") * 1.2),
                col("value") * 1.2
            ).otherwise(col("transformed_value"))
        )
        
        # Trial category capped at 100
        df = df.withColumn(
            "transformed_value",
            when(
                (col("category") == "TRIAL") & (col("transformed_value") > 100),
                lit(100)
            ).otherwise(col("transformed_value"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules for status determination"""
        self.logger.info("Applying business rules")
        
        # Rule 1: Set status based on value
        df = df.withColumn(
            "status",
            when(col("value").isNull() | (col("value") == 0), lit("INVALID"))
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
        
        # Rule 3: Additional name cleaning
        df = df.withColumn(
            "name",
            trim(regexp_replace(col("name"), "\\s+", " "))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional calculated fields"""
        self.logger.info("Enriching data")
        
        # Load enrichment configuration if available
        config_multiplier = self.config.get('premium_multiplier', 1.2)
        
        # Apply premium enrichment from config
        df = df.withColumn(
            "transformed_value",
            when(
                col("category") == "PREMIUM",
                col("transformed_value") * lit(config_multiplier)
            ).otherwise(col("transformed_value"))
        )
        
        return df
    
    def _add_etl_metadata(self, df: DataFrame) -> DataFrame:
        """Add ETL processing metadata"""
        return df.withColumn("etl_run_id", lit(self.run_id)) \
                 .withColumn("processed_at", current_timestamp()) \
                 .withColumn("processed_by", lit(self.config.get('user', 'system')))
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        self.logger.info("Validating data")
        errors = []
        
        # Rule 1: ID is required
        null_id_count = df.filter(col("id").isNull() | (col("id") == "")).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with missing ID")
        
        # Rule 2: Name is required
        null_name_count = df.filter(col("name").isNull() | (col("name") == "")).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with missing name")
        
        # Rule 3: Value must be positive
        invalid_value_count = df.filter((col("value") < 0) | (col("transformed_value") < 0)).count()
        if invalid_value_count > 0:
            errors.append(f"{invalid_value_count} records with negative values")
        
        # Rule 4: Priority must be 1-5
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        # Rule 5: Category must not be empty
        invalid_category_count = df.filter(
            col("category").isNull() | (col("category") == "")
        ).count()
        if invalid_category_count > 0:
            errors.append(f"{invalid_category_count} records with missing category")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Validation passed")
        else:
            self.logger.error(f"Validation failed with {len(errors)} errors")
            for error in errors:
                self.logger.error(f"  - {error}")
        
        return is_valid, errors