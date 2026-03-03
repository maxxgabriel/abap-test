"""
Data transformation module for ETL pipeline.
Applies business rules, enrichment, and validation to extracted data.
"""

from datetime import datetime
from typing import List, Dict, Any, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from decimal import Decimal

from src.logger import ETLLogger


class DataTransformer:
    """Transforms extracted data according to business rules."""
    
    def __init__(self, spark: SparkSession, run_id: str):
        """
        Initialize the data transformer.
        
        Args:
            spark: SparkSession instance
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
        
    def get_transformed_schema(self) -> StructType:
        """
        Define the schema for transformed data.
        
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
            StructField("etl_run_id", StringType(), True),
            StructField("processed_at", TimestampType(), True),
            StructField("processed_by", StringType(), True),
        ])
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Transform source data applying all business rules.
        
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
            # Initial transformations
            df = source_df.select(
                F.col("id"),
                F.upper(F.trim(F.col("name"))).alias("name"),
                F.col("value"),
                F.col("value").alias("transformed_value"),  # Will be recalculated
                F.lit("TRANSFORMED").alias("status"),
                F.col("category"),
                F.lit(3).alias("priority"),  # Default priority
                F.lit(self.run_id).alias("etl_run_id"),
                F.current_timestamp().alias("processed_at"),
                F.lit("spark_etl").alias("processed_by")
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
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate derived values based on business logic.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with calculated values
        """
        # Apply multiplier based on category
        df = df.withColumn(
            "transformed_value",
            F.when(F.col("category") == "PREMIUM", F.col("value") * 1.5)
            .when(F.col("category") == "VIP", F.col("value") * 2.0)
            .when(F.col("category") == "STANDARD", F.col("value") * 1.2)
            .otherwise(F.col("value"))
        )
        
        return df
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """
        Calculate priority based on value and category.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with calculated priority
        """
        df = df.withColumn(
            "priority",
            F.when(F.col("transformed_value") >= 1000, 1)
            .when(F.col("transformed_value") >= 750, 2)
            .when(F.col("transformed_value") >= 300, 3)
            .when(F.col("transformed_value") >= 100, 4)
            .otherwise(5)
        )
        
        return df
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply category-specific business rules.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with category rules applied
        """
        # Handle uncategorized items
        df = df.withColumn(
            "category",
            F.when(F.col("category").isNull(), "UNCATEGORIZED")
            .otherwise(F.col("category"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply core business rules to the data.
        
        Args:
            df: Input DataFrame
            
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
        
        # Rule 3: Name normalization - remove extra spaces
        df = df.withColumn(
            "name",
            F.regexp_replace(F.col("name"), "\\s+", " ")
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional information from configuration.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Enriched DataFrame
        """
        try:
            # Load configuration
            config_df = self._load_enrichment_config()
            
            if config_df is not None:
                # Example: Apply premium multiplier from config
                premium_multiplier = self._get_config_value(
                    config_df, 
                    "PREMIUM_MULTIPLIER", 
                    1.5
                )
                
                df = df.withColumn(
                    "transformed_value",
                    F.when(
                        F.col("category") == "PREMIUM",
                        F.col("value") * premium_multiplier
                    ).otherwise(F.col("transformed_value"))
                )
            
            return df
            
        except Exception as e:
            self.logger.log_warning(
                component="TRANSFORMER",
                message=f"Enrichment partially failed: {str(e)}"
            )
            return df
    
    def _load_enrichment_config(self) -> DataFrame:
        """Load enrichment configuration from database."""
        try:
            config_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.spark.conf.get("spark.etl.source.url")) \
                .option("dbtable", "etl_config") \
                .option("user", self.spark.conf.get("spark.etl.source.user")) \
                .option("password", self.spark.conf.get("spark.etl.source.password")) \
                .load() \
                .filter("is_active = 'X'")
            
            return config_df
        except:
            return None
    
    def _get_config_value(self, config_df: DataFrame, key: str, default: Any) -> Any:
        """Get configuration value by key."""
        try:
            rows = config_df.filter(f"config_key = '{key}'").collect()
            if rows:
                return float(rows[0]["config_value"])
            return default
        except:
            return default
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        errors = []
        
        # Validation rule 1: ID is required
        null_id_count = df.filter(F.col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with missing ID")
        
        # Validation rule 2: Name is required
        null_name_count = df.filter(F.col("name").isNull()).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with missing name")
        
        # Validation rule 3: Value must be positive
        negative_value_count = df.filter(
            (F.col("value") < 0) | (F.col("transformed_value") < 0)
        ).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative values")
        
        # Validation rule 4: Priority must be 1-5
        invalid_priority_count = df.filter(
            (F.col("priority") < 1) | (F.col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if not is_valid:
            self.logger.log_warning(
                component="TRANSFORMER",
                message=f"Validation found {len(errors)} issues"
            )
        
        return is_valid, errors