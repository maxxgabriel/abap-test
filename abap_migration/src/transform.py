"""
Data Transformation Module
Applies business rules, enrichment, and validation using PySpark DataFrame operations.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, upper, trim, regexp_replace, lit, 
    current_timestamp, concat_ws, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from typing import Dict, Any, Tuple, List
import logging

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transform data with business rules and validation."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the DataTransformer.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.transform_config = config.get('transformation', {})
        
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply all transformations to source data.
        
        Args:
            source_df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Starting transformation")
        
        try:
            # Step 1: Clean and normalize data
            df = self._clean_data(source_df)
            
            # Step 2: Calculate derived values
            df = self._calculate_derived_values(df)
            
            # Step 3: Calculate priority
            df = self._calculate_priority(df)
            
            # Step 4: Apply business rules
            df = self._apply_business_rules(df)
            
            # Step 5: Apply category-specific rules
            df = self._apply_category_rules(df)
            
            # Step 6: Enrich data
            df = self._enrich_data(df)
            
            # Step 7: Add metadata
            df = self._add_metadata(df)
            
            record_count = df.count()
            logger.info(f"Transformed {record_count} records")
            
            return df
            
        except Exception as e:
            logger.error(f"Transformation failed: {str(e)}")
            raise
    
    def _clean_data(self, df: DataFrame) -> DataFrame:
        """
        Clean and normalize data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        # Normalize name: uppercase, trim, remove multiple spaces
        df = df.withColumn(
            "name",
            upper(trim(regexp_replace(col("name"), "\\s+", " ")))
        )
        
        # Handle null categories
        df = df.withColumn(
            "category",
            coalesce(col("category"), lit("UNCATEGORIZED"))
        )
        
        return df
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate transformed values based on category and value.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with transformed_value column
        """
        rules = self.transform_config.get('rules', {})
        multipliers = rules.get('category_multipliers', {})
        
        # Build when-otherwise chain for category multipliers
        transform_expr = col("value")
        for category, multiplier in multipliers.items():
            transform_expr = when(
                col("category") == category,
                col("value") * lit(multiplier)
            ).otherwise(transform_expr)
        
        df = df.withColumn("transformed_value", transform_expr)
        
        return df
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """
        Calculate priority based on transformed value.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with priority column
        """
        rules = self.transform_config.get('rules', {})
        thresholds = rules.get('priority_thresholds', {})
        
        high_threshold = thresholds.get('high', 1000)
        medium_threshold = thresholds.get('medium', 750)
        low_threshold = thresholds.get('low', 300)
        
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= high_threshold, lit(1))
            .when(col("transformed_value") >= medium_threshold, lit(2))
            .when(col("transformed_value") >= low_threshold, lit(3))
            .when(col("transformed_value") >= 100, lit(4))
            .otherwise(lit(5))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to determine status.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with updated status
        """
        rules = self.transform_config.get('rules', {})
        status_mapping = rules.get('status_mapping', {})
        
        high_value = status_mapping.get('high_value', 750)
        medium_value = status_mapping.get('medium_value', 300)
        
        # Rule 1: Set status based on transformed value
        df = df.withColumn(
            "status",
            when(col("value").isNull(), lit("INVALID"))
            .when(col("transformed_value") >= high_value, lit("HIGH_VALUE"))
            .when(col("transformed_value") >= medium_value, lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )
        
        # Rule 2: Priority override for high value items
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, lit(1))
            .otherwise(col("priority"))
        )
        
        return df
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply category-specific transformation rules.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with category-specific transformations
        """
        # Premium category gets additional boost
        df = df.withColumn(
            "transformed_value",
            when(
                col("category") == "PREMIUM",
                col("transformed_value") * lit(1.2)
            ).otherwise(col("transformed_value"))
        )
        
        # VIP category always gets priority 1
        df = df.withColumn(
            "priority",
            when(col("category") == "VIP", lit(1))
            .otherwise(col("priority"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional calculated fields.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Enriched DataFrame
        """
        # Add value category indicator
        df = df.withColumn(
            "value_category",
            when(col("transformed_value") >= 1000, lit("VERY_HIGH"))
            .when(col("transformed_value") >= 750, lit("HIGH"))
            .when(col("transformed_value") >= 300, lit("MEDIUM"))
            .when(col("transformed_value") >= 100, lit("LOW"))
            .otherwise(lit("VERY_LOW"))
        )
        
        # Add priority label
        df = df.withColumn(
            "priority_label",
            when(col("priority") == 1, lit("CRITICAL"))
            .when(col("priority") == 2, lit("HIGH"))
            .when(col("priority") == 3, lit("MEDIUM"))
            .when(col("priority") == 4, lit("LOW"))
            .otherwise(lit("NORMAL"))
        )
        
        return df
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add ETL metadata columns.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with metadata
        """
        df = df.withColumn("etl_run_id", lit(self.run_id)) \
               .withColumn("processed_at", current_timestamp()) \
               .withColumn("processed_by", lit("pyspark_etl"))
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        logger.info("Starting data validation")
        errors = []
        
        # Check 1: No null IDs
        null_id_count = df.filter(col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with null ID")
        
        # Check 2: No null names
        null_name_count = df.filter(col("name").isNull()).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with null name")
        
        # Check 3: Values must be positive
        negative_value_count = df.filter(col("value") < 0).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative value")
        
        # Check 4: Priority must be 1-5
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        # Check 5: Category must not be empty
        empty_category_count = df.filter(
            col("category").isNull() | (col("category") == "")
        ).count()
        if empty_category_count > 0:
            errors.append(f"{empty_category_count} records with empty category")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            logger.info("Data validation passed")
        else:
            logger.error(f"Data validation failed with {len(errors)} errors")
            for error in errors:
                logger.error(f"  - {error}")
        
        return is_valid, errors