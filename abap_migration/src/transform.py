"""
Data transformation module for ETL pipeline.
Applies business rules, enrichment, and validation logic.
"""

from typing import Dict, List, Tuple, Any
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, upper, trim, when, regexp_replace, 
    current_timestamp, lit, round as spark_round, udf
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    TimestampType, IntegerType
)
from src.logger import ETLLogger


class DataTransformer:
    """
    Transforms extracted data by applying business rules and enrichment.
    """
    
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
        StructField("processed_by", StringType(), False)
    ])
    
    def __init__(
        self,
        spark: SparkSession,
        config: Dict[str, Any],
        run_id: str
    ):
        """
        Initialize transformer.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
        self.business_rules = config.get("business_rules", {})
        
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply all transformations to the input DataFrame.
        
        Args:
            df: Input DataFrame from extraction
            
        Returns:
            Transformed DataFrame
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message=f"Starting transformation on {df.count()} records"
        )
        
        try:
            # Basic transformations
            df_transformed = self._apply_basic_transformations(df)
            
            # Calculate derived values
            df_transformed = self._calculate_derived_values(df_transformed)
            
            # Apply business rules
            df_transformed = self._apply_business_rules(df_transformed)
            
            # Enrich data
            df_transformed = self._enrich_data(df_transformed)
            
            # Add metadata
            df_transformed = self._add_metadata(df_transformed)
            
            record_count = df_transformed.count()
            self.logger.log_info(
                component="TRANSFORMER",
                message=f"Transformed {record_count} records"
            )
            
            return df_transformed
            
        except Exception as e:
            self.logger.log_error(
                component="TRANSFORMER",
                message="Transformation failed",
                details=str(e)
            )
            raise
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic data cleansing transformations."""
        df = df.withColumn("name", upper(trim(col("name"))))
        df = df.withColumn("name", regexp_replace(col("name"), "\\s+", " "))
        df = df.withColumn("category", 
            when(col("category").isNull(), lit("UNCATEGORIZED"))
            .otherwise(col("category"))
        )
        return df
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """Calculate transformed values based on business logic."""
        # Get multipliers from config
        premium_multiplier = self.business_rules.get("premium_multiplier", 1.5)
        vip_multiplier = self.business_rules.get("vip_multiplier", 2.0)
        
        df = df.withColumn("transformed_value",
            when(col("category") == "PREMIUM", col("value") * premium_multiplier)
            .when(col("category") == "VIP", col("value") * vip_multiplier)
            .when(col("category") == "STANDARD", col("value") * 1.2)
            .otherwise(col("value"))
        )
        
        # Calculate priority
        df = df.withColumn("priority",
            when(col("transformed_value") >= 1000, lit(1))
            .when(col("transformed_value") >= 750, lit(2))
            .when(col("transformed_value") >= 500, lit(3))
            .when(col("transformed_value") >= 250, lit(4))
            .otherwise(lit(5))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to set status and other derived fields."""
        # Rule 1: Set status based on value
        df = df.withColumn("status",
            when(col("value").isNull(), lit("INVALID"))
            .when(col("transformed_value") >= 750, lit("HIGH_VALUE"))
            .when(col("transformed_value") >= 300, lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )
        
        # Rule 2: Priority override for high value items
        df = df.withColumn("priority",
            when(col("transformed_value") >= 1000, lit(1))
            .otherwise(col("priority"))
        )
        
        # Rule 3: Round transformed values
        df = df.withColumn("transformed_value", 
            spark_round(col("transformed_value"), 2)
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional information."""
        # Load enrichment configuration if available
        enable_enrichment = self.config.get("enable_enrichment", True)
        
        if not enable_enrichment:
            return df
        
        # Example: Apply category-specific enrichment
        df = df.withColumn("transformed_value",
            when(col("category") == "PREMIUM", col("transformed_value") * 1.1)
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """Add ETL metadata columns."""
        import getpass
        
        df = df.withColumn("etl_run_id", lit(self.run_id))
        df = df.withColumn("processed_at", current_timestamp())
        df = df.withColumn("processed_by", lit(getpass.getuser()))
        
        # Select only required columns in the correct order
        df = df.select(
            "id", "name", "value", "transformed_value",
            "status", "category", "priority",
            "etl_run_id", "processed_at", "processed_by"
        )
        
        return df
    
    def validate_data(
        self,
        df: DataFrame
    ) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        errors = []
        
        # Validation 1: Required fields
        null_count = df.filter(
            col("id").isNull() | 
            col("name").isNull() | 
            col("value").isNull()
        ).count()
        
        if null_count > 0:
            errors.append(f"{null_count} records with missing required fields")
        
        # Validation 2: Value ranges
        invalid_values = df.filter(
            (col("value") < 0) | 
            (col("transformed_value") < 0)
        ).count()
        
        if invalid_values > 0:
            errors.append(f"{invalid_values} records with negative values")
        
        # Validation 3: Priority range
        invalid_priority = df.filter(
            (col("priority") < 1) | 
            (col("priority") > 5)
        ).count()
        
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        # Validation 4: Category validation
        invalid_category = df.filter(
            col("category").isNull() | 
            (col("category") == "")
        ).count()
        
        if invalid_category > 0:
            errors.append(f"{invalid_category} records with invalid category")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.log_info(
                component="TRANSFORMER",
                message="Data validation passed"
            )
        else:
            self.logger.log_warning(
                component="TRANSFORMER",
                message=f"Data validation found {len(errors)} issues",
                details="; ".join(errors)
            )
        
        return is_valid, errors