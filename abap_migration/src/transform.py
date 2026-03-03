"""
PySpark Data Transformation Module
Applies business rules, enrichment, and validation to extracted data
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
from typing import Dict, Any, List, Tuple
import logging


class DataTransformer:
    """
    Handles data transformation with business rules and enrichment
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        """
        Initialize the DataTransformer
        
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
        """
        Define the schema for transformed data
        
        Returns:
            StructType schema definition
        """
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
        Main transformation method
        
        Args:
            source_df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        try:
            # Initialize with current timestamp
            current_timestamp = F.current_timestamp()
            
            # Base transformations
            df = source_df.select(
                F.col("id"),
                F.upper(F.trim(F.col("name"))).alias("name"),
                F.col("value"),
                F.col("category"),
                F.lit(self.run_id).alias("etl_run_id"),
                current_timestamp.alias("processed_at"),
                F.lit(self.config.get('user', 'system')).alias("processed_by")
            )
            
            # Calculate derived values
            df = self._calculate_derived_values(df)
            
            # Calculate priority
            df = self._calculate_priority(df)
            
            # Apply category-specific rules
            df = self._apply_category_rules(df)
            
            # Apply business rules
            df = self._apply_business_rules(df)
            
            # Enrich data
            df = self._enrich_data(df)
            
            # Clean up names
            df = df.withColumn(
                "name",
                F.regexp_replace(F.col("name"), "\\s+", " ")
            )
            
            record_count = df.count()
            self.logger.info(f"Transformed {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}")
            raise
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate derived values based on category and value
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with transformed_value column
        """
        category_multipliers = self.config.get('category_multipliers', {
            'PREMIUM': 1.5,
            'STANDARD': 1.2,
            'BASIC': 1.0,
            'VIP': 2.0,
            'TRIAL': 0.5
        })
        
        # Create case when expression for multipliers
        multiplier_expr = F.when(F.col("category") == "PREMIUM", F.lit(category_multipliers.get('PREMIUM', 1.5)))
        for category, multiplier in category_multipliers.items():
            if category != 'PREMIUM':
                multiplier_expr = multiplier_expr.when(
                    F.col("category") == category,
                    F.lit(multiplier)
                )
        multiplier_expr = multiplier_expr.otherwise(F.lit(1.0))
        
        df = df.withColumn(
            "transformed_value",
            F.round(F.col("value") * multiplier_expr, 2)
        )
        
        return df
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """
        Calculate priority based on value and category
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with priority column
        """
        priority_expr = (
            F.when(F.col("transformed_value") >= 1000, F.lit(1))
            .when(F.col("transformed_value") >= 750, F.lit(2))
            .when(F.col("transformed_value") >= 300, F.lit(3))
            .when(F.col("transformed_value") >= 100, F.lit(4))
            .otherwise(F.lit(5))
        )
        
        # Override for VIP category
        priority_expr = F.when(
            F.col("category") == "VIP",
            F.lit(1)
        ).otherwise(priority_expr)
        
        df = df.withColumn("priority", priority_expr)
        
        return df
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply category-specific transformation rules
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with category rules applied
        """
        # Set default category for null values
        df = df.withColumn(
            "category",
            F.when(F.col("category").isNull(), F.lit("UNCATEGORIZED"))
            .otherwise(F.col("category"))
        )
        
        # Premium category gets additional boost
        df = df.withColumn(
            "transformed_value",
            F.when(
                F.col("category") == "PREMIUM",
                F.round(F.col("transformed_value") * 1.2, 2)
            ).otherwise(F.col("transformed_value"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to the data
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with business rules applied
        """
        # Rule 1: Set status based on transformed value
        status_expr = (
            F.when(F.col("value").isNull(), F.lit("INVALID"))
            .when(F.col("transformed_value") >= 750, F.lit("HIGH_VALUE"))
            .when(F.col("transformed_value") >= 300, F.lit("MEDIUM_VALUE"))
            .otherwise(F.lit("LOW_VALUE"))
        )
        
        df = df.withColumn("status", status_expr)
        
        # Rule 2: Priority override for high value items
        df = df.withColumn(
            "priority",
            F.when(F.col("transformed_value") >= 1000, F.lit(1))
            .otherwise(F.col("priority"))
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
        # Add value rank within category
        window_spec = Window.partitionBy("category").orderBy(F.desc("transformed_value"))
        df = df.withColumn("value_rank", F.rank().over(window_spec))
        
        # Add percentile within category
        df = df.withColumn(
            "value_percentile",
            F.percent_rank().over(window_spec)
        )
        
        # Drop temporary columns if not needed in final output
        if not self.config.get('keep_enrichment_columns', False):
            df = df.drop("value_rank", "value_percentile")
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []
        
        # Validation 1: Check for null IDs
        null_id_count = df.filter(F.col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with null IDs")
        
        # Validation 2: Check for null names
        null_name_count = df.filter(F.col("name").isNull()).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with null names")
        
        # Validation 3: Check for negative values
        negative_value_count = df.filter(
            (F.col("value") < 0) | (F.col("transformed_value") < 0)
        ).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative values")
        
        # Validation 4: Check priority range
        invalid_priority_count = df.filter(
            (F.col("priority") < 1) | (F.col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        # Validation 5: Check for empty categories
        empty_category_count = df.filter(
            F.col("category").isNull() | (F.trim(F.col("category")) == "")
        ).count()
        if empty_category_count > 0:
            errors.append(f"{empty_category_count} records with empty categories")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Data validation passed")
        else:
            self.logger.error(f"Data validation failed with {len(errors)} error(s)")
            for error in errors:
                self.logger.error(f"  - {error}")
        
        return is_valid, errors
    
    def apply_data_quality_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply data quality rules and mark invalid records
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with quality flags
        """
        df = df.withColumn(
            "quality_flag",
            F.when(
                (F.col("id").isNull()) |
                (F.col("name").isNull()) |
                (F.col("value") < 0) |
                (F.col("priority") < 1) |
                (F.col("priority") > 5),
                F.lit("INVALID")
            ).otherwise(F.lit("VALID"))
        )
        
        return df