"""
ETL Transformation Module
Implements value transformation logic, validation rules, and priority/category mapping
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, current_timestamp,
    udf, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, 
    IntegerType, TimestampType, BooleanType
)
from typing import Dict, List, Tuple, Optional
from decimal import Decimal
import logging
from datetime import datetime


class TransformationError(Exception):
    """Custom exception for transformation errors"""
    pass


class ETLTransformer:
    """
    Main transformer class implementing business rules and data transformations
    """
    
    def __init__(self, spark: SparkSession, run_id: str, config: Dict):
        """
        Initialize transformer with Spark session and configuration
        
        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
            config: Configuration dictionary with transformation parameters
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Category multipliers from config
        self.category_multipliers = config.get('transformation', {}).get('category_multipliers', {
            'PREMIUM': 1.5,
            'VIP': 1.8,
            'STANDARD': 1.0,
            'BASIC': 0.9,
            'TRIAL': 0.8
        })
        
        # Priority thresholds
        self.priority_thresholds = config.get('transformation', {}).get('priority_thresholds', {
            'priority_1': 1000,  # Highest priority
            'priority_2': 750,
            'priority_3': 500,
            'priority_4': 250,
            'priority_5': 0      # Lowest priority
        })
        
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply complete transformation pipeline to source data
        
        Args:
            source_df: Source DataFrame with raw data
            
        Returns:
            Transformed DataFrame ready for loading
        """
        self.logger.info(f"Starting transformation for run_id: {self.run_id}")
        
        try:
            # Step 1: Apply basic transformations
            transformed_df = self._apply_basic_transformations(source_df)
            
            # Step 2: Calculate derived values
            transformed_df = self._calculate_derived_values(transformed_df)
            
            # Step 3: Apply business rules
            transformed_df = self._apply_business_rules(transformed_df)
            
            # Step 4: Calculate priority
            transformed_df = self._calculate_priority(transformed_df)
            
            # Step 5: Apply category-specific rules
            transformed_df = self._apply_category_rules(transformed_df)
            
            # Step 6: Enrich data
            transformed_df = self._enrich_data(transformed_df)
            
            # Step 7: Add metadata
            transformed_df = self._add_metadata(transformed_df)
            
            record_count = transformed_df.count()
            self.logger.info(f"Transformation complete. Records transformed: {record_count}")
            
            return transformed_df
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}")
            raise TransformationError(f"Data transformation failed: {str(e)}") from e
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply basic data cleaning and normalization
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with basic transformations applied
        """
        self.logger.info("Applying basic transformations")
        
        # Name normalization: uppercase, trim, remove extra spaces
        df = df.withColumn(
            "name",
            upper(trim(regexp_replace(col("name"), r"\s+", " ")))
        )
        
        # Ensure category is not null
        df = df.withColumn(
            "category",
            coalesce(upper(trim(col("category"))), lit("UNCATEGORIZED"))
        )
        
        # Ensure status is uppercase
        df = df.withColumn(
            "status",
            upper(coalesce(col("status"), lit("UNKNOWN")))
        )
        
        return df
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate transformed values based on category multipliers
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with calculated transformed_value column
        """
        self.logger.info("Calculating derived values")
        
        # Create multiplier mapping expression
        multiplier_expr = when(col("category") == "PREMIUM", lit(self.category_multipliers['PREMIUM']))
        for category, multiplier in self.category_multipliers.items():
            if category != "PREMIUM":
                multiplier_expr = multiplier_expr.when(
                    col("category") == category, 
                    lit(multiplier)
                )
        multiplier_expr = multiplier_expr.otherwise(lit(1.0))
        
        # Calculate transformed value
        df = df.withColumn(
            "transformed_value",
            (col("value") * multiplier_expr).cast(DecimalType(15, 2))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules for status assignment
        
        Business Rules:
        1. Set status based on transformed value ranges
        2. Override for invalid data
        3. High-value item identification
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with business rules applied
        """
        self.logger.info("Applying business rules")
        
        # Rule 1: Set status based on value
        df = df.withColumn(
            "status",
            when(col("value").isNull() | (col("value") == 0), lit("INVALID"))
            .when(col("transformed_value") >= 750, lit("HIGH_VALUE"))
            .when(col("transformed_value") >= 300, lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )
        
        return df
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """
        Calculate priority based on transformed value and category
        
        Priority Levels:
        1 = Highest (value >= 1000 or VIP category)
        2 = High (value >= 750)
        3 = Medium (value >= 500)
        4 = Low (value >= 250)
        5 = Lowest (value < 250)
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with priority column
        """
        self.logger.info("Calculating priority")
        
        # Priority calculation with VIP override
        priority_expr = (
            when((col("transformed_value") >= self.priority_thresholds['priority_1']) |
                 (col("category") == "VIP"), lit(1))
            .when(col("transformed_value") >= self.priority_thresholds['priority_2'], lit(2))
            .when(col("transformed_value") >= self.priority_thresholds['priority_3'], lit(3))
            .when(col("transformed_value") >= self.priority_thresholds['priority_4'], lit(4))
            .otherwise(lit(5))
        )
        
        df = df.withColumn("priority", priority_expr.cast(IntegerType()))
        
        return df
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply category-specific transformation rules
        
        Category Rules:
        - PREMIUM: Already has 1.5x multiplier
        - VIP: Force priority to 1
        - TRIAL: Apply discount
        - UNCATEGORIZED: Flag for review
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with category rules applied
        """
        self.logger.info("Applying category-specific rules")
        
        # VIP category: override priority
        df = df.withColumn(
            "priority",
            when(col("category") == "VIP", lit(1))
            .otherwise(col("priority"))
        )
        
        # UNCATEGORIZED: set flag
        df = df.withColumn(
            "needs_review",
            when(col("category") == "UNCATEGORIZED", lit(True))
            .otherwise(lit(False))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional calculated fields
        
        Args:
            df: Input DataFrame
            
        Returns:
            Enriched DataFrame
        """
        self.logger.info("Enriching data")
        
        # Calculate value tier
        df = df.withColumn(
            "value_tier",
            when(col("transformed_value") >= 1000, lit("TIER_1"))
            .when(col("transformed_value") >= 500, lit("TIER_2"))
            .when(col("transformed_value") >= 250, lit("TIER_3"))
            .otherwise(lit("TIER_4"))
        )
        
        # Calculate discount percentage (if applicable)
        df = df.withColumn(
            "discount_pct",
            when(col("category") == "TRIAL", lit(0.20))
            .when(col("category") == "BASIC", lit(0.10))
            .otherwise(lit(0.00))
        )
        
        return df
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add ETL metadata columns
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with metadata columns
        """
        self.logger.info("Adding metadata")
        
        df = df.withColumn("etl_run_id", lit(self.run_id))
        df = df.withColumn("processed_at", current_timestamp())
        df = df.withColumn("processed_by", lit("ETL_SYSTEM"))
        
        return df


class DataValidator:
    """
    Data validation class implementing comprehensive validation rules
    """
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize validator
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Validation thresholds
        self.min_value = config.get('validation', {}).get('min_value', 0)
        self.max_value = config.get('validation', {}).get('max_value', 999999.99)
        self.max_name_length = config.get('validation', {}).get('max_name_length', 100)
        
        # Valid categories
        self.valid_categories = config.get('validation', {}).get('valid_categories', [
            'PREMIUM', 'VIP', 'STANDARD', 'BASIC', 'TRIAL', 'UNCATEGORIZED'
        ])
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[Dict]]:
        """
        Perform comprehensive data validation
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid: bool, validation_errors: List[Dict])
        """
        self.logger.info("Starting data validation")
        
        validation_errors = []
        
        # Validation Rule 1: ID is required
        null_id_count = df.filter(col("id").isNull() | (col("id") == "")).count()
        if null_id_count > 0:
            validation_errors.append({
                'rule': 'ID_REQUIRED',
                'failed_count': null_id_count,
                'message': f'{null_id_count} records have missing ID'
            })
        
        # Validation Rule 2: Name is required and within length limit
        null_name_count = df.filter(col("name").isNull() | (col("name") == "")).count()
        if null_name_count > 0:
            validation_errors.append({
                'rule': 'NAME_REQUIRED',
                'failed_count': null_name_count,
                'message': f'{null_name_count} records have missing name'
            })
        
        long_name_count = df.filter(
            col("name").isNotNull() & (col("name") != "") & 
            (col("name").cast(StringType()).length() > self.max_name_length)
        ).count()
        if long_name_count > 0:
            validation_errors.append({
                'rule': 'NAME_LENGTH',
                'failed_count': long_name_count,
                'message': f'{long_name_count} records have name exceeding {self.max_name_length} characters'
            })
        
        # Validation Rule 3: Value must be positive and within range
        invalid_value_count = df.filter(
            col("value").isNull() | 
            (col("value") < self.min_value) | 
            (col("value") > self.max_value)
        ).count()
        if invalid_value_count > 0:
            validation_errors.append({
                'rule': 'VALUE_RANGE',
                'failed_count': invalid_value_count,
                'message': f'{invalid_value_count} records have invalid value (must be between {self.min_value} and {self.max_value})'
            })
        
        # Validation Rule 4: Transformed value must be positive
        invalid_transformed_count = df.filter(
            col("transformed_value").isNull() | 
            (col("transformed_value") < 0)
        ).count()
        if invalid_transformed_count > 0:
            validation_errors.append({
                'rule': 'TRANSFORMED_VALUE_POSITIVE',
                'failed_count': invalid_transformed_count,
                'message': f'{invalid_transformed_count} records have invalid transformed value'
            })
        
        # Validation Rule 5: Priority must be 1-5
        invalid_priority_count = df.filter(
            col("priority").isNull() | 
            (col("priority") < 1) | 
            (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            validation_errors.append({
                'rule': 'PRIORITY_RANGE',
                'failed_count': invalid_priority_count,
                'message': f'{invalid_priority_count} records have invalid priority (must be 1-5)'
            })
        
        # Validation Rule 6: Category must be valid
        if self.valid_categories:
            invalid_category_count = df.filter(
                ~col("category").isin(self.valid_categories)
            ).count()
            if invalid_category_count > 0:
                validation_errors.append({
                    'rule': 'CATEGORY_VALID',
                    'failed_count': invalid_category_count,
                    'message': f'{invalid_category_count} records have invalid category'
                })
        
        # Validation Rule 7: Check for duplicates
        total_records = df.count()
        unique_records = df.select("id").distinct().count()
        duplicate_count = total_records - unique_records
        if duplicate_count > 0:
            validation_errors.append({
                'rule': 'DUPLICATE_IDS',
                'failed_count': duplicate_count,
                'message': f'{duplicate_count} duplicate IDs found'
            })
        
        # Validation Rule 8: Status consistency check
        inconsistent_status_count = df.filter(
            ((col("transformed_value") >= 750) & (col("status") != "HIGH_VALUE")) |
            ((col("transformed_value") >= 300) & (col("transformed_value") < 750) & 
             (col("status") != "MEDIUM_VALUE")) |
            ((col("transformed_value") < 300) & (col("status") != "LOW_VALUE"))
        ).count()
        if inconsistent_status_count > 0:
            validation_errors.append({
                'rule': 'STATUS_CONSISTENCY',
                'failed_count': inconsistent_status_count,
                'message': f'{inconsistent_status_count} records have inconsistent status values'
            })
        
        is_valid = len(validation_errors) == 0
        
        if is_valid:
            self.logger.info("All validation checks passed")
        else:
            self.logger.warning(f"Validation failed with {len(validation_errors)} errors")
            for error in validation_errors:
                self.logger.warning(f"  - {error['rule']}: {error['message']}")
        
        return is_valid, validation_errors
    
    def get_validation_summary(self, df: DataFrame) -> Dict:
        """
        Get validation summary statistics
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary with validation summary
        """
        total_records = df.count()
        
        summary = {
            'total_records': total_records,
            'null_ids': df.filter(col("id").isNull()).count(),
            'null_names': df.filter(col("name").isNull()).count(),
            'null_values': df.filter(col("value").isNull()).count(),
            'negative_values': df.filter(col("value") < 0).count(),
            'invalid_priorities': df.filter(
                (col("priority") < 1) | (col("priority") > 5)
            ).count(),
            'unique_ids': df.select("id").distinct().count(),
            'unique_categories': df.select("category").distinct().count()
        }
        
        summary['duplicate_ids'] = total_records - summary['unique_ids']
        summary['completeness_pct'] = (
            (total_records - summary['null_ids'] - summary['null_names']) / 
            total_records * 100 if total_records > 0 else 0
        )
        
        return summary


def get_transformed_schema() -> StructType:
    """
    Get the schema for transformed data
    
    Returns:
        StructType defining the schema
    """
    return StructType([
        StructField("id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("value", DecimalType(15, 2), nullable=False),
        StructField("transformed_value", DecimalType(15, 2), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("priority", IntegerType(), nullable=False),
        StructField("value_tier", StringType(), nullable=True),
        StructField("discount_pct", DecimalType(5, 2), nullable=True),
        StructField("needs_review", BooleanType(), nullable=True),
        StructField("etl_run_id", StringType(), nullable=False),
        StructField("processed_at", TimestampType(), nullable=False),
        StructField("processed_by", StringType(), nullable=False)
    ])