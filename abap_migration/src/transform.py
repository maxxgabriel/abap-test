"""
ETL Transformer Module - Handles data transformation and business rules
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, trim, when, lit, current_timestamp, 
    regexp_replace, coalesce, round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import Dict, Any, List, Tuple
import logging


class ETLTransformer:
    """Transforms extracted data with business rules and enrichment"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the transformer
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
    
    def get_transformed_schema(self) -> StructType:
        """Define the transformed data schema"""
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
            StructField("processed_by", StringType(), False)
        ])
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply all transformations to the data
        
        Args:
            df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting data transformation")
        
        # Basic transformations
        df_transformed = df.select(
            col("id"),
            upper(trim(regexp_replace(col("name"), "\\s+", " "))).alias("name"),
            col("value"),
            self._calculate_transformed_value(col("value"), col("category")).alias("transformed_value"),
            lit("TRANSFORMED").alias("status"),
            col("category"),
            self._calculate_priority(col("value"), col("category")).alias("priority"),
            lit(self.run_id).alias("etl_run_id"),
            current_timestamp().alias("processed_at"),
            lit(self.config['runtime'].get('user', 'system')).alias("processed_by")
        )
        
        # Apply business rules
        df_transformed = self._apply_business_rules(df_transformed)
        
        # Enrich data
        df_transformed = self._enrich_data(df_transformed)
        
        record_count = df_transformed.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return df_transformed
    
    def _calculate_transformed_value(self, value_col, category_col):
        """Calculate transformed value based on category"""
        rules = self.config['transformation']['value_rules']
        
        return when(category_col == "PREMIUM", value_col * rules['premium_multiplier']) \
            .when(category_col == "VIP", value_col * rules['vip_multiplier']) \
            .when(category_col == "STANDARD", value_col * rules['standard_multiplier']) \
            .otherwise(value_col)
    
    def _calculate_priority(self, value_col, category_col):
        """Calculate priority based on value and category"""
        thresholds = self.config['transformation']['priority_thresholds']
        
        return when(value_col >= thresholds['high'], 1) \
            .when(value_col >= thresholds['medium'], 2) \
            .when(value_col >= thresholds['low'], 3) \
            .when(category_col == "VIP", 1) \
            .otherwise(4)
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business transformation rules"""
        self.logger.info("Applying business rules")
        
        rules = self.config['transformation']['business_rules']
        
        # Rule 1: Set status based on transformed value
        df = df.withColumn(
            "status",
            when(col("value").isNull(), "INVALID")
            .when(col("transformed_value") >= rules['high_value_threshold'], "HIGH_VALUE")
            .when(col("transformed_value") >= rules['medium_value_threshold'], "MEDIUM_VALUE")
            .otherwise("LOW_VALUE")
        )
        
        # Rule 2: Priority override for high value items
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= rules['priority_override_threshold'], 1)
            .otherwise(col("priority"))
        )
        
        # Rule 3: Category validation
        df = df.withColumn(
            "category",
            when(col("category").isNull() | (col("category") == ""), "UNCATEGORIZED")
            .otherwise(col("category"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional information"""
        self.logger.info("Enriching data")
        
        # Apply category-specific enrichment
        enrichment_config = self.config['transformation'].get('enrichment', {})
        
        if enrichment_config.get('apply_premium_bonus', False):
            df = df.withColumn(
                "transformed_value",
                when(col("category") == "PREMIUM", 
                     col("transformed_value") * enrichment_config.get('premium_bonus', 1.2))
                .otherwise(col("transformed_value"))
            )
        
        # Round transformed values
        df = df.withColumn(
            "transformed_value",
            spark_round(col("transformed_value"), 2)
        )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        self.logger.info("Validating transformed data")
        
        errors = []
        
        # Rule 1: ID is required
        null_ids = df.filter(col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null ID")
        
        # Rule 2: Name is required
        null_names = df.filter(col("name").isNull() | (col("name") == "")).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null/empty name")
        
        # Rule 3: Value must be non-negative
        negative_values = df.filter(col("value") < 0).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative values")
        
        # Rule 4: Priority must be 1-5
        invalid_priority = df.filter((col("priority") < 1) | (col("priority") > 5)).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        # Rule 5: Category must not be null
        null_category = df.filter(col("category").isNull()).count()
        if null_category > 0:
            errors.append(f"{null_category} records with null category")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Data validation passed")
        else:
            self.logger.warning(f"Data validation failed with {len(errors)} errors")
            for error in errors:
                self.logger.warning(f"  - {error}")
        
        return is_valid, errors