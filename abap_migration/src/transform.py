"""
PySpark ETL Data Transformer Module
Transforms data using built-in functions without loops.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import Dict, Any, Tuple, List
from datetime import datetime
from src.logger import ETLLogger


class DataTransformer:
    """Handles data transformation using PySpark DataFrame API."""
    
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
        self.logger = ETLLogger.get_instance()
        self.transform_config = config['transformation']
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply all transformations to source data.
        
        Args:
            source_df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        self.logger.log_info(
            component='TRANSFORMER',
            message=f"Starting transformation for {source_df.count()} records"
        )
        
        try:
            # Add processing metadata
            df = self._add_metadata(source_df)
            
            # Clean and normalize names
            df = self._clean_names(df)
            
            # Calculate derived values
            df = self._calculate_derived_values(df)
            
            # Calculate priority
            df = self._calculate_priority(df)
            
            # Apply business rules
            df = self._apply_business_rules(df)
            
            # Apply category-specific rules
            df = self._apply_category_rules(df)
            
            # Enrich data
            df = self._enrich_data(df)
            
            # Select final columns
            df = self._select_final_columns(df)
            
            record_count = df.count()
            self.logger.log_info(
                component='TRANSFORMER',
                message=f"Transformed {record_count} records"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component='TRANSFORMER',
                message='Transformation failed',
                details=str(e)
            )
            raise
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """Add ETL processing metadata."""
        current_timestamp = F.current_timestamp()
        current_user = F.lit(self.config.get('etl_user', 'ETL_SYSTEM'))
        
        return df.withColumn('etl_run_id', F.lit(self.run_id)) \
                 .withColumn('processed_at', current_timestamp) \
                 .withColumn('processed_by', current_user)
    
    def _clean_names(self, df: DataFrame) -> DataFrame:
        """Clean and normalize name field using PySpark functions."""
        return df.withColumn(
            'name',
            F.trim(
                F.regexp_replace(
                    F.upper(F.col('name')),
                    r'\s+',
                    ' '
                )
            )
        )
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """Calculate transformed values based on category and value."""
        # Get multipliers from config
        multipliers = self.transform_config['category_multipliers']
        
        # Create conditional expression for each category
        transform_expr = F.when(
            F.col('category') == 'PREMIUM',
            F.col('value') * multipliers['PREMIUM']
        )
        
        for category, multiplier in multipliers.items():
            if category != 'PREMIUM':
                transform_expr = transform_expr.when(
                    F.col('category') == category,
                    F.col('value') * multiplier
                )
        
        # Default multiplier for unknown categories
        transform_expr = transform_expr.otherwise(F.col('value') * 1.0)
        
        return df.withColumn('transformed_value', transform_expr)
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """Calculate priority based on value and category."""
        priority_thresholds = self.transform_config['priority_thresholds']
        
        # Priority calculation using conditional expressions
        priority_expr = (
            F.when(F.col('transformed_value') >= priority_thresholds['critical'], 1)
            .when(F.col('transformed_value') >= priority_thresholds['high'], 2)
            .when(F.col('transformed_value') >= priority_thresholds['medium'], 3)
            .when(F.col('transformed_value') >= priority_thresholds['low'], 4)
            .otherwise(5)
        )
        
        # Override for VIP category
        priority_expr = F.when(
            F.col('category') == 'VIP',
            1
        ).otherwise(priority_expr)
        
        return df.withColumn('priority', priority_expr)
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to set status and adjust values."""
        status_thresholds = self.transform_config['status_thresholds']
        
        # Rule 1: Set status based on transformed value
        status_expr = (
            F.when(F.col('value').isNull(), 'INVALID')
            .when(F.col('transformed_value') >= status_thresholds['high'], 'HIGH_VALUE')
            .when(F.col('transformed_value') >= status_thresholds['medium'], 'MEDIUM_VALUE')
            .otherwise('LOW_VALUE')
        )
        
        df = df.withColumn('status', status_expr)
        
        # Rule 2: Priority override for very high values
        df = df.withColumn(
            'priority',
            F.when(F.col('transformed_value') >= 1000, 1)
            .otherwise(F.col('priority'))
        )
        
        # Rule 3: Set default category for empty values
        df = df.withColumn(
            'category',
            F.when(F.col('category').isNull() | (F.col('category') == ''), 'UNCATEGORIZED')
            .otherwise(F.col('category'))
        )
        
        return df
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """Apply category-specific business rules."""
        # Premium category rules
        df = df.withColumn(
            'transformed_value',
            F.when(
                F.col('category') == 'PREMIUM',
                F.col('transformed_value') * 1.2
            ).otherwise(F.col('transformed_value'))
        )
        
        # VIP category rules
        df = df.withColumn(
            'status',
            F.when(
                F.col('category') == 'VIP',
                F.concat(F.lit('VIP_'), F.col('status'))
            ).otherwise(F.col('status'))
        )
        
        # Trial category rules - cap at threshold
        trial_cap = self.transform_config.get('trial_cap', 100)
        df = df.withColumn(
            'transformed_value',
            F.when(
                F.col('category') == 'TRIAL',
                F.least(F.col('transformed_value'), F.lit(trial_cap))
            ).otherwise(F.col('transformed_value'))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional calculated fields."""
        # Add value tier
        df = df.withColumn(
            'value_tier',
            F.when(F.col('transformed_value') >= 750, 'TIER_1')
            .when(F.col('transformed_value') >= 300, 'TIER_2')
            .when(F.col('transformed_value') >= 100, 'TIER_3')
            .otherwise('TIER_4')
        )
        
        # Add quality score (0-100)
        df = df.withColumn(
            'quality_score',
            F.least(
                F.round((F.col('transformed_value') / 10), 0),
                F.lit(100)
            )
        )
        
        # Add risk flag
        df = df.withColumn(
            'risk_flag',
            F.when(
                (F.col('transformed_value') < 50) | (F.col('status') == 'INVALID'),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
        
        return df
    
    def _select_final_columns(self, df: DataFrame) -> DataFrame:
        """Select and order final output columns."""
        return df.select(
            'id',
            'name',
            'value',
            'transformed_value',
            'status',
            'category',
            'priority',
            'value_tier',
            'quality_score',
            'risk_flag',
            'etl_run_id',
            'processed_at',
            'processed_by'
        )
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        self.logger.log_info(
            component='TRANSFORMER',
            message='Starting data validation'
        )
        
        errors = []
        
        # Check for null IDs
        null_ids = df.filter(F.col('id').isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null ID")
        
        # Check for null names
        null_names = df.filter(F.col('name').isNull() | (F.col('name') == '')).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null or empty name")
        
        # Check for negative values
        negative_values = df.filter(
            (F.col('value') < 0) | (F.col('transformed_value') < 0)
        ).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative values")
        
        # Check for invalid priority
        invalid_priority = df.filter(
            (F.col('priority') < 1) | (F.col('priority') > 5)
        ).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        # Check for duplicate IDs
        total_records = df.count()
        unique_ids = df.select('id').distinct().count()
        if total_records != unique_ids:
            errors.append(f"{total_records - unique_ids} duplicate IDs found")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.log_info(
                component='TRANSFORMER',
                message='Validation passed'
            )
        else:
            self.logger.log_error(
                component='TRANSFORMER',
                message='Validation failed',
                details='; '.join(errors)
            )
        
        return is_valid, errors