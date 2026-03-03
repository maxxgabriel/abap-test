"""
ETL Data Transformation Module
Applies business rules, validations, and enrichment to extracted data
"""
import logging
from typing import List, Dict, Tuple
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, 
    TimestampType, IntegerType
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class ETLTransformer:
    """Handles data transformation and business rules"""
    
    OUTPUT_SCHEMA = StructType([
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
    
    def __init__(self, spark: SparkSession, config: Dict, run_id: str):
        """
        Initialize transformer
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply complete transformation pipeline
        
        Args:
            df: Input DataFrame from extraction
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        try:
            # Apply transformations step by step
            df = self._calculate_derived_values(df)
            df = self._calculate_priority(df)
            df = self._apply_business_rules(df)
            df = self._enrich_data(df)
            df = self._add_metadata(df)
            
            record_count = df.count()
            self.logger.info(f"Transformed {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}")
            raise RuntimeError(f"Data transformation failed: {str(e)}") from e
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate transformed values based on business logic
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with transformed_value column
        """
        # Get multipliers from config
        multipliers = self.config.get('transformation', {}).get('multipliers', {})
        
        # Default transformation: value * 1.5
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
        Calculate priority based on value and category
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with priority column
        """
        df = df.withColumn(
            "priority",
            F.when(F.col("transformed_value") >= 1000, 1)
             .when(F.col("transformed_value") >= 750, 2)
             .when(F.col("transformed_value") >= 500, 3)
             .when(F.col("transformed_value") >= 300, 4)
             .otherwise(5)
        )
        
        # Priority override for VIP category
        df = df.withColumn(
            "priority",
            F.when(F.col("category") == "VIP", 1)
             .otherwise(F.col("priority"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to data
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with rules applied
        """
        self.logger.info("Applying business rules")
        
        # Rule 1: Set status based on transformed value
        df = df.withColumn(
            "status",
            F.when(F.col("value").isNull(), "INVALID")
             .when(F.col("transformed_value") >= 750, "HIGH_VALUE")
             .when(F.col("transformed_value") >= 300, "MEDIUM_VALUE")
             .otherwise("LOW_VALUE")
        )
        
        # Rule 2: Name normalization
        df = df.withColumn(
            "name",
            F.upper(F.trim(F.regexp_replace(F.col("name"), "\\s+", " ")))
        )
        
        # Rule 3: Category validation
        df = df.withColumn(
            "category",
            F.when(F.col("category").isNull(), "UNCATEGORIZED")
             .otherwise(F.col("category"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional information
        
        Args:
            df: Input DataFrame
            
        Returns:
            Enriched DataFrame
        """
        self.logger.info("Enriching data")
        
        # Add enrichment from configuration
        enrichment_config = self.config.get('transformation', {}).get('enrichment', {})
        
        if enrichment_config.get('add_percentile_rank', False):
            # Add percentile rank by category
            window = Window.partitionBy("category").orderBy("transformed_value")
            df = df.withColumn(
                "value_percentile",
                F.percent_rank().over(window)
            )
        
        # Apply premium multiplier from config
        premium_multiplier = enrichment_config.get('premium_multiplier', 1.0)
        if premium_multiplier != 1.0:
            df = df.withColumn(
                "transformed_value",
                F.when(F.col("category") == "PREMIUM",
                       F.col("transformed_value") * premium_multiplier)
                 .otherwise(F.col("transformed_value"))
            )
        
        return df
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add ETL metadata to records
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with metadata
        """
        return df.withColumn("etl_run_id", F.lit(self.run_id)) \
                 .withColumn("processed_at", F.current_timestamp()) \
                 .withColumn("processed_by", F.lit("pyspark_etl"))
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        self.logger.info("Validating transformed data")
        
        errors = []
        
        # Validation 1: Check for required fields
        null_ids = df.filter(F.col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null ID")
        
        null_names = df.filter(F.col("name").isNull()).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null name")
        
        # Validation 2: Check value constraints
        negative_values = df.filter(
            (F.col("value") < 0) | (F.col("transformed_value") < 0)
        ).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative values")
        
        # Validation 3: Check priority range
        invalid_priority = df.filter(
            (F.col("priority") < 1) | (F.col("priority") > 5)
        ).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        # Validation 4: Check for duplicates
        duplicate_count = df.groupBy("id").count().filter(F.col("count") > 1).count()
        if duplicate_count > 0:
            errors.append(f"{duplicate_count} duplicate IDs found")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Validation passed")
        else:
            self.logger.warning(f"Validation failed with {len(errors)} errors")
            for error in errors:
                self.logger.warning(f"  - {error}")
        
        return is_valid, errors