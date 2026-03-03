"""
PySpark ETL Transformation Module
Applies business rules, data enrichment, and validation logic.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Dict, Any, Tuple, List
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class ETLTransformer:
    """Transform extracted data with business rules and enrichment."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the transformer.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.current_user = config.get('user', 'etl_system')
        
    def get_transformed_schema(self) -> StructType:
        """Define the schema for transformed data."""
        return StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), False),
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
        Apply all transformation steps to source data.
        
        Args:
            source_df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Starting transformation")
        
        current_timestamp = F.current_timestamp()
        
        # Initial transformations
        df = source_df.select(
            F.col("id"),
            F.upper(F.trim(F.col("name"))).alias("name"),
            F.col("value"),
            self._calculate_derived_values(
                F.col("value"), 
                F.col("category")
            ).alias("transformed_value"),
            F.lit("TRANSFORMED").alias("status"),
            F.col("category"),
            self._calculate_priority(
                F.col("value"),
                F.col("category")
            ).alias("priority"),
            F.lit(self.run_id).alias("etl_run_id"),
            current_timestamp.alias("processed_at"),
            F.lit(self.current_user).alias("processed_by")
        )
        
        # Apply category-specific rules
        df = self._apply_category_rules(df)
        
        # Apply business rules
        df = self._apply_business_rules(df)
        
        # Enrich data
        df = self._enrich_data(df)
        
        record_count = df.count()
        logger.info(f"Transformed {record_count} records")
        
        return df
    
    def _calculate_derived_values(self, value_col, category_col):
        """
        Calculate derived/transformed values based on business logic.
        
        Args:
            value_col: Column containing original value
            category_col: Column containing category
            
        Returns:
            Column expression for transformed value
        """
        return F.when(category_col == "PREMIUM", value_col * 1.5) \
                .when(category_col == "VIP", value_col * 2.0) \
                .when(category_col == "STANDARD", value_col * 1.2) \
                .otherwise(value_col)
    
    def _calculate_priority(self, value_col, category_col):
        """
        Calculate priority based on value and category.
        
        Args:
            value_col: Column containing value
            category_col: Column containing category
            
        Returns:
            Column expression for priority (1-5)
        """
        return F.when(
            (value_col >= 1000) | (category_col == "VIP"), 1
        ).when(
            (value_col >= 750) | (category_col == "PREMIUM"), 2
        ).when(
            (value_col >= 500) | (category_col == "STANDARD"), 3
        ).when(
            value_col >= 250, 4
        ).otherwise(5)
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply category-specific transformation rules.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with category rules applied
        """
        # Handle null categories
        df = df.withColumn(
            "category",
            F.when(
                F.col("category").isNull() | (F.col("category") == ""),
                F.lit("UNCATEGORIZED")
            ).otherwise(F.col("category"))
        )
        
        # Premium category adjustment
        df = df.withColumn(
            "transformed_value",
            F.when(
                F.col("category") == "PREMIUM",
                F.col("transformed_value") * 1.2
            ).otherwise(F.col("transformed_value"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to transformed data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with business rules applied
        """
        logger.info("Applying business rules")
        
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
        
        # Rule 3: Name normalization - remove multiple spaces
        df = df.withColumn(
            "name",
            F.regexp_replace(F.trim(F.col("name")), "\\s+", " ")
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional calculated fields and lookups.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Enriched DataFrame
        """
        logger.info("Enriching data")
        
        # Load enrichment configuration if available
        config_multipliers = self.config.get('category_multipliers', {})
        
        # Apply configuration-based enrichment
        for category, multiplier in config_multipliers.items():
            df = df.withColumn(
                "transformed_value",
                F.when(
                    F.col("category") == category,
                    F.col("transformed_value") * multiplier
                ).otherwise(F.col("transformed_value"))
            )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data against business rules.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        logger.info("Validating transformed data")
        errors = []
        
        # Validation 1: ID is required
        null_id_count = df.filter(F.col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with missing ID")
        
        # Validation 2: Name is required
        null_name_count = df.filter(
            F.col("name").isNull() | (F.col("name") == "")
        ).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with missing name")
        
        # Validation 3: Value must be positive
        negative_value_count = df.filter(
            F.col("value") < 0
        ).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative values")
        
        # Validation 4: Priority must be 1-5
        invalid_priority_count = df.filter(
            (F.col("priority") < 1) | (F.col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        # Validation 5: Check for duplicates
        duplicate_count = df.groupBy("id").count().filter(F.col("count") > 1).count()
        if duplicate_count > 0:
            errors.append(f"{duplicate_count} duplicate IDs found")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            logger.info("Data validation passed")
        else:
            logger.warning(f"Data validation failed with {len(errors)} issues")
            for error in errors:
                logger.warning(f"  - {error}")
        
        return is_valid, errors


def create_transformer(config: Dict[str, Any], run_id: str) -> ETLTransformer:
    """
    Factory function to create an ETLTransformer instance.
    
    Args:
        config: Configuration dictionary
        run_id: Unique run identifier
        
    Returns:
        Configured ETLTransformer instance
    """
    spark = SparkSession.builder.getOrCreate()
    return ETLTransformer(spark, config, run_id)