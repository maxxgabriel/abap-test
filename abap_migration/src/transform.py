"""
PySpark Data Transformer

Replaces ABAP zcl_etl_transformer class with PySpark transformations.
Applies business rules, enrichment, and validation using DataFrame operations.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, 
    current_timestamp, udf, concat_ws
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    TimestampType, IntegerType
)
from typing import List, Tuple, Dict, Any
import logging

from src.logger import ETLLogger
from src.config_manager import ConfigManager


class ETLTransformer:
    """
    Transform extracted data applying business rules and enrichment.
    """
    
    # Define schema for transformed data
    TRANSFORMED_SCHEMA = StructType([
        StructField("id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("value", DecimalType(15, 2), nullable=True),
        StructField("transformed_value", DecimalType(15, 2), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("category", StringType(), nullable=True),
        StructField("priority", IntegerType(), nullable=True),
        StructField("etl_run_id", StringType(), nullable=False),
        StructField("processed_at", TimestampType(), nullable=False),
        StructField("processed_by", StringType(), nullable=False),
    ])
    
    def __init__(
        self,
        spark: SparkSession,
        config: ConfigManager,
        run_id: str
    ):
        """
        Initialize the ETL Transformer.
        
        Args:
            spark: Active SparkSession
            config: Configuration manager instance
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger.get_logger(__name__)
        
        self.logger.info(f"ETL Transformer initialized - Run ID: {run_id}")
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Main transformation method - applies all transformations.
        
        Args:
            source_df: Source data DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting data transformation")
        
        try:
            # Step 1: Basic transformations
            df = self._apply_basic_transformations(source_df)
            
            # Step 2: Calculate derived values
            df = self._calculate_derived_values(df)
            
            # Step 3: Calculate priority
            df = self._calculate_priority(df)
            
            # Step 4: Apply category rules
            df = self._apply_category_rules(df)
            
            # Step 5: Apply business rules
            df = self._apply_business_rules(df)
            
            # Step 6: Enrich data
            df = self._enrich_data(df)
            
            # Step 7: Add metadata
            df = self._add_metadata(df)
            
            record_count = df.count()
            self.logger.info(f"Transformed {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}", exc_info=True)
            raise
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply basic data cleansing transformations.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with basic transformations
        """
        self.logger.info("Applying basic transformations")
        
        return df.select(
            col("id"),
            # Name normalization: upper case, trim, remove extra spaces
            upper(trim(regexp_replace(col("name"), "\\s+", " "))).alias("name"),
            col("value"),
            col("status"),
            col("category"),
            col("source_system"),
            col("created_at"),
            col("created_by"),
            col("changed_at"),
            col("changed_by")
        )
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate transformed values based on category and value.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with transformed_value column
        """
        self.logger.info("Calculating derived values")
        
        # Apply category-based multipliers
        premium_multiplier = float(
            self.config.get("processing.premium_multiplier", 1.5)
        )
        
        return df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("value") * premium_multiplier)
            .when(col("category") == "VIP", col("value") * 2.0)
            .when(col("category") == "STANDARD", col("value") * 1.2)
            .when(col("category") == "BASIC", col("value") * 1.0)
            .when(col("category") == "TRIAL", col("value") * 0.8)
            .otherwise(col("value"))
        )
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """
        Calculate priority based on value and category.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with priority column
        """
        self.logger.info("Calculating priority")
        
        return df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, lit(1))
            .when(col("transformed_value") >= 750, lit(2))
            .when(col("transformed_value") >= 500, lit(3))
            .when(col("transformed_value") >= 250, lit(4))
            .otherwise(lit(5))
        )
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply category-specific business rules.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with category rules applied
        """
        self.logger.info("Applying category-specific rules")
        
        # Handle uncategorized items
        df = df.withColumn(
            "category",
            when(col("category").isNull() | (col("category") == ""), 
                 lit("UNCATEGORIZED"))
            .otherwise(col("category"))
        )
        
        # Apply premium category special handling
        df = df.withColumn(
            "status",
            when(
                (col("category") == "PREMIUM") & (col("transformed_value") >= 1000),
                lit("HIGH_VALUE_PREMIUM")
            )
            .otherwise(col("status"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to determine status and perform validations.
        
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
        
        # Rule 2: Priority override for high value items
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, lit(1))
            .otherwise(col("priority"))
        )
        
        # Rule 3: Additional name cleaning
        df = df.withColumn(
            "name",
            regexp_replace(col("name"), "[^A-Z0-9 ]", "")
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
        self.logger.info("Enriching data")
        
        # Load enrichment configuration if exists
        try:
            config_df = self._load_enrichment_config()
            if config_df:
                df = df.join(
                    config_df,
                    on="category",
                    how="left"
                )
        except Exception as e:
            self.logger.warning(f"Could not load enrichment config: {str(e)}")
        
        # Add calculated enrichment fields
        df = df.withColumn(
            "value_tier",
            when(col("transformed_value") >= 1000, lit("TIER_1"))
            .when(col("transformed_value") >= 500, lit("TIER_2"))
            .when(col("transformed_value") >= 100, lit("TIER_3"))
            .otherwise(lit("TIER_4"))
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
        self.logger.info("Adding metadata")
        
        return df.withColumn("etl_run_id", lit(self.run_id)) \
                 .withColumn("processed_at", current_timestamp()) \
                 .withColumn("processed_by", lit("pyspark_etl"))
    
    def validate_data(
        self, 
        df: DataFrame
    ) -> Tuple[bool, List[str]]:
        """
        Validate transformed data against business rules.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        self.logger.info("Validating transformed data")
        
        validation_errors = []
        
        # Validation 1: ID is required
        null_id_count = df.filter(col("id").isNull() | (col("id") == "")).count()
        if null_id_count > 0:
            validation_errors.append(f"{null_id_count} records with null/empty ID")
        
        # Validation 2: Name is required
        null_name_count = df.filter(col("name").isNull() | (col("name") == "")).count()
        if null_name_count > 0:
            validation_errors.append(
                f"{null_name_count} records with null/empty name"
            )
        
        # Validation 3: Value must be non-negative
        negative_value_count = df.filter(col("value") < 0).count()
        if negative_value_count > 0:
            validation_errors.append(
                f"{negative_value_count} records with negative value"
            )
        
        # Validation 4: Priority must be 1-5
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            validation_errors.append(
                f"{invalid_priority_count} records with invalid priority"
            )
        
        # Validation 5: Category must not be null
        null_category_count = df.filter(col("category").isNull()).count()
        if null_category_count > 0:
            validation_errors.append(
                f"{null_category_count} records with null category"
            )
        
        is_valid = len(validation_errors) == 0
        
        if is_valid:
            self.logger.info("Data validation passed")
        else:
            self.logger.error(
                f"Data validation failed with {len(validation_errors)} errors"
            )
            for error in validation_errors:
                self.logger.error(f"  - {error}")
        
        return is_valid, validation_errors
    
    def _load_enrichment_config(self) -> DataFrame:
        """
        Load enrichment configuration from Delta Lake.
        
        Returns:
            DataFrame with enrichment config, or None
        """
        try:
            config_path = "/data/config/etl_config"
            if self.spark._jsparkSession.catalog().tableExists(config_path):
                return (
                    self.spark.read
                    .format("delta")
                    .load(config_path)
                    .filter(col("is_active") == True)
                    .filter(col("config_type") == "ENRICHMENT")
                )
        except Exception:
            return None
    
    def get_transformation_summary(self, df: DataFrame) -> Dict[str, Any]:
        """
        Get summary statistics of transformed data.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Dictionary with transformation summary
        """
        return {
            "run_id": self.run_id,
            "total_records": df.count(),
            "status_distribution": (
                df.groupBy("status")
                .count()
                .collect()
            ),
            "category_distribution": (
                df.groupBy("category")
                .count()
                .collect()
            ),
            "priority_distribution": (
                df.groupBy("priority")
                .count()
                .collect()
            ),
            "avg_value": df.agg({"value": "avg"}).collect()[0][0],
            "avg_transformed_value": df.agg({"transformed_value": "avg"}).collect()[0][0],
        }