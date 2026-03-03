"""
ETL Transformation Module
Implements business rules, data enrichment, and validation logic.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, current_timestamp,
    udf, expr, coalesce
)
from typing import List, Tuple, Dict, Any
from decimal import Decimal

from src.logger import ETLLogger


class ETLTransformer:
    """Handles data transformation, business rules, and validation."""
    
    # Define transformed data schema
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
    
    def __init__(
        self,
        spark: SparkSession,
        run_id: str,
        config: Dict[str, Any] = None
    ):
        """
        Initialize the ETL Transformer.
        
        Args:
            spark: SparkSession instance
            run_id: Unique identifier for this ETL run
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config or {}
        self.logger = ETLLogger.get_instance()
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Transform source data applying all rules.
        
        Args:
            source_df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Starting transformation"
        )
        
        try:
            # Basic transformations
            df = source_df.select(
                col("id"),
                upper(trim(regexp_replace(col("name"), r"\s+", " "))).alias("name"),
                col("value"),
                self._calculate_derived_value(col("value"), col("category")).alias("transformed_value"),
                lit("TRANSFORMED").alias("status"),
                col("category"),
                self._calculate_priority(col("value"), col("category")).alias("priority"),
                lit(self.run_id).alias("etl_run_id"),
                current_timestamp().alias("processed_at"),
                lit(self.config.get("processed_by", "etl_system")).alias("processed_by")
            )
            
            # Apply category-specific rules
            df = self._apply_category_rules(df)
            
            # Apply business rules
            df = self._apply_business_rules(df)
            
            # Enrich data
            df = self._enrich_data(df)
            
            record_count = df.count()
            self.logger.log_info(
                component="TRANSFORMER",
                message=f"Transformed {record_count} records"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="TRANSFORMER",
                message="Transformation failed",
                details=str(e)
            )
            raise
    
    def _calculate_derived_value(self, value_col, category_col):
        """Calculate derived value based on category."""
        return when(category_col == "PREMIUM", value_col * 1.5) \
            .when(category_col == "VIP", value_col * 1.8) \
            .when(category_col == "STANDARD", value_col * 1.2) \
            .when(category_col == "BASIC", value_col * 1.0) \
            .otherwise(value_col * 1.1)
    
    def _calculate_priority(self, value_col, category_col):
        """Calculate priority based on value and category."""
        return when((value_col >= 1000) | (category_col == "VIP"), 1) \
            .when((value_col >= 750) | (category_col == "PREMIUM"), 2) \
            .when((value_col >= 500) | (category_col == "STANDARD"), 3) \
            .when(value_col >= 300, 4) \
            .otherwise(5)
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """Apply category-specific transformation rules."""
        return df.withColumn(
            "category",
            coalesce(col("category"), lit("UNCATEGORIZED"))
        )
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to transformed data.
        
        Args:
            df: DataFrame to apply rules to
            
        Returns:
            DataFrame with business rules applied
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Applying business rules"
        )
        
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
        
        # Rule 3: Name normalization (already done in main transform)
        
        # Rule 4: Category validation
        df = df.withColumn(
            "category",
            when(col("category").isNull(), "UNCATEGORIZED")
            .otherwise(col("category"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional calculated fields.
        
        Args:
            df: DataFrame to enrich
            
        Returns:
            Enriched DataFrame
        """
        # Load enrichment configuration
        premium_multiplier = self.config.get("premium_multiplier", 1.2)
        
        # Apply enrichment based on category
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("transformed_value") * premium_multiplier)
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def validate_data(
        self,
        df: DataFrame
    ) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        validation_errors = []
        is_valid = True
        
        # Validation rule 1: ID is required
        null_id_count = df.filter(col("id").isNull() | (col("id") == "")).count()
        if null_id_count > 0:
            validation_errors.append(f"{null_id_count} records with missing ID")
            is_valid = False
        
        # Validation rule 2: Name is required
        null_name_count = df.filter(col("name").isNull() | (col("name") == "")).count()
        if null_name_count > 0:
            validation_errors.append(f"{null_name_count} records with missing name")
            is_valid = False
        
        # Validation rule 3: Value must be positive
        negative_value_count = df.filter(
            (col("value") < 0) | (col("transformed_value") < 0)
        ).count()
        if negative_value_count > 0:
            validation_errors.append(f"{negative_value_count} records with negative values")
            is_valid = False
        
        # Validation rule 4: Priority must be 1-5
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            validation_errors.append(f"{invalid_priority_count} records with invalid priority")
            is_valid = False
        
        # Validation rule 5: Category is required
        null_category_count = df.filter(col("category").isNull() | (col("category") == "")).count()
        if null_category_count > 0:
            validation_errors.append(f"{null_category_count} records with missing category")
            is_valid = False
        
        if not is_valid:
            self.logger.log_warning(
                component="TRANSFORMER",
                message=f"Validation failed with {len(validation_errors)} errors"
            )
        
        return is_valid, validation_errors