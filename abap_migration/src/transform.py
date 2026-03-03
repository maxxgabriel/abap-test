"""
PySpark ETL Transformation Module
Handles data transformation, business rules, and enrichment.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, upper, trim, when, lit, current_timestamp, 
    regexp_replace, coalesce, concat_ws
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import Dict, Any, List, Tuple
import logging

from src.logger import ETLLogger


class ETLTransformer:
    """Transforms data with business rules and enrichment."""
    
    TRANSFORMED_SCHEMA = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("value", DecimalType(15, 2), False),
        StructField("transformed_value", DecimalType(15, 2), False),
        StructField("status", StringType(), False),
        StructField("category", StringType(), False),
        StructField("priority", IntegerType(), False),
        StructField("etl_run_id", StringType(), False),
        StructField("processed_at", TimestampType(), False),
        StructField("processed_by", StringType(), False),
    ])
    
    def __init__(self, spark: SparkSession, run_id: str, config: Dict[str, Any]):
        """
        Initialize ETL Transformer.
        
        Args:
            spark: SparkSession instance
            run_id: Unique identifier for this ETL run
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = ETLLogger.get_instance()
        
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Transform source data with all business rules.
        
        Args:
            source_df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        self.logger.log_info(
            component='TRANSFORMER',
            message='Starting transformation',
            run_id=self.run_id
        )
        
        try:
            # Base transformation
            transformed_df = source_df.select(
                col("id"),
                upper(trim(col("name"))).alias("name"),
                col("value"),
                self._calculate_transformed_value(col("value"), col("category")).alias("transformed_value"),
                lit("TRANSFORMED").alias("status"),
                coalesce(col("category"), lit("UNCATEGORIZED")).alias("category"),
                self._calculate_priority(col("value"), col("category")).alias("priority"),
                lit(self.run_id).alias("etl_run_id"),
                current_timestamp().alias("processed_at"),
                lit(self.config.get('user', 'system')).alias("processed_by")
            )
            
            # Apply business rules
            transformed_df = self._apply_business_rules(transformed_df)
            
            # Enrich data
            transformed_df = self._enrich_data(transformed_df)
            
            # Clean up data
            transformed_df = self._clean_data(transformed_df)
            
            record_count = transformed_df.count()
            
            self.logger.log_info(
                component='TRANSFORMER',
                message=f'Transformed {record_count} records',
                run_id=self.run_id,
                details={'record_count': record_count}
            )
            
            return transformed_df
            
        except Exception as e:
            self.logger.log_error(
                component='TRANSFORMER',
                message='Transformation failed',
                run_id=self.run_id,
                details={'error': str(e)}
            )
            raise
    
    def _calculate_transformed_value(self, value_col, category_col):
        """Calculate transformed value based on category."""
        multipliers = self.config.get('transformation', {}).get('multipliers', {})
        
        return when(category_col == "PREMIUM", value_col * multipliers.get('premium', 1.5)) \
            .when(category_col == "VIP", value_col * multipliers.get('vip', 2.0)) \
            .when(category_col == "STANDARD", value_col * multipliers.get('standard', 1.0)) \
            .otherwise(value_col * multipliers.get('default', 1.0))
    
    def _calculate_priority(self, value_col, category_col):
        """Calculate priority based on value and category."""
        return when(value_col >= 1000, 1) \
            .when(value_col >= 750, 2) \
            .when(value_col >= 500, 3) \
            .when(value_col >= 300, 4) \
            .otherwise(5)
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to transformed data."""
        self.logger.log_info(
            component='TRANSFORMER',
            message='Applying business rules',
            run_id=self.run_id
        )
        
        # Rule 1: Set status based on transformed value
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
        
        # Rule 3: Name normalization
        df = df.withColumn(
            "name",
            regexp_replace(trim(col("name")), "\\s+", " ")
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional attributes."""
        # Load enrichment configuration
        enrichment_config = self.config.get('transformation', {}).get('enrichment', {})
        
        if enrichment_config.get('enabled', False):
            # Add premium calculation
            df = df.withColumn(
                "transformed_value",
                when(col("category") == "PREMIUM", col("transformed_value") * 1.2)
                .otherwise(col("transformed_value"))
            )
        
        return df
    
    def _clean_data(self, df: DataFrame) -> DataFrame:
        """Clean and standardize data."""
        # Remove duplicate spaces
        df = df.withColumn("name", regexp_replace(col("name"), "\\s+", " "))
        
        # Ensure category is not null
        df = df.withColumn(
            "category",
            coalesce(col("category"), lit("UNCATEGORIZED"))
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
        errors = []
        
        # Check for null IDs
        null_id_count = df.filter(col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with null ID")
        
        # Check for null names
        null_name_count = df.filter(col("name").isNull()).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with null name")
        
        # Check for negative values
        negative_value_count = df.filter(col("value") < 0).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative values")
        
        # Check for invalid priorities
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if not is_valid:
            self.logger.log_warning(
                component='TRANSFORMER',
                message=f'Validation found {len(errors)} issues',
                run_id=self.run_id,
                details={'errors': errors}
            )
        
        return is_valid, errors