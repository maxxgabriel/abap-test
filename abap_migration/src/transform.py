"""
PySpark Data Transformation Module
Applies business rules, enrichment, and validation logic
"""
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from pyspark.sql import functions as F
from typing import List, Tuple, Dict, Any

from src.logger import ETLLogger
from src.config import ETLConfig


class ETLTransformer:
    """
    Transform extracted data with business rules and validation
    """
    
    def __init__(self, config: ETLConfig, run_id: str):
        """
        Initialize transformer
        
        Args:
            config: ETL configuration object
            run_id: Unique identifier for this ETL run
        """
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger.get_logger("TRANSFORMER")
    
    def get_transformed_schema(self) -> StructType:
        """
        Define transformed data schema
        
        Returns:
            StructType schema definition
        """
        return StructType([
            StructField("id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("value", DecimalType(15, 2), nullable=True),
            StructField("transformed_value", DecimalType(15, 2), nullable=True),
            StructField("status", StringType(), nullable=True),
            StructField("category", StringType(), nullable=True),
            StructField("priority", IntegerType(), nullable=True),
            StructField("etl_run_id", StringType(), nullable=False),
            StructField("processed_at", TimestampType(), nullable=False),
            StructField("processed_by", StringType(), nullable=False)
        ])
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Main transformation method
        
        Args:
            df: Input DataFrame from extraction
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        try:
            # Step 1: Basic transformations
            df = self._apply_basic_transformations(df)
            
            # Step 2: Calculate derived values
            df = self._calculate_derived_values(df)
            
            # Step 3: Apply business rules
            df = self._apply_business_rules(df)
            
            # Step 4: Apply category-specific rules
            df = self._apply_category_rules(df)
            
            # Step 5: Enrich data
            df = self._enrich_data(df)
            
            # Step 6: Add processing metadata
            df = self._add_processing_metadata(df)
            
            record_count = df.count()
            self.logger.info(f"Transformed {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}", exc_info=True)
            raise
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply basic data transformations
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with basic transformations
        """
        # Normalize name: uppercase and trim
        df = df.withColumn("name", F.upper(F.trim(F.col("name"))))
        
        # Remove multiple spaces
        df = df.withColumn("name", F.regexp_replace(F.col("name"), "\\s+", " "))
        
        # Set default status if null
        df = df.withColumn(
            "status",
            F.when(F.col("status").isNull(), F.lit("ACTIVE"))
            .otherwise(F.col("status"))
        )
        
        # Set default category if null
        df = df.withColumn(
            "category",
            F.when(F.col("category").isNull(), F.lit("UNCATEGORIZED"))
            .otherwise(F.col("category"))
        )
        
        return df
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate derived and transformed values
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with calculated values
        """
        # Get transformation multipliers from config
        transform_config = self.config.get_transform_config()
        
        # Calculate transformed value based on category
        df = df.withColumn(
            "transformed_value",
            F.when(F.col("category") == "PREMIUM", F.col("value") * 1.5)
            .when(F.col("category") == "VIP", F.col("value") * 2.0)
            .when(F.col("category") == "STANDARD", F.col("value") * 1.2)
            .when(F.col("category") == "BASIC", F.col("value") * 1.0)
            .when(F.col("category") == "TRIAL", F.col("value") * 0.5)
            .otherwise(F.col("value"))
        )
        
        # Calculate priority based on transformed value
        df = df.withColumn(
            "priority",
            F.when(F.col("transformed_value") >= 1000, F.lit(1))
            .when(F.col("transformed_value") >= 750, F.lit(2))
            .when(F.col("transformed_value") >= 500, F.lit(3))
            .when(F.col("transformed_value") >= 250, F.lit(4))
            .otherwise(F.lit(5))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules and validation logic
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with business rules applied
        """
        # Rule 1: Set status based on value
        df = df.withColumn(
            "status",
            F.when(F.col("value").isNull(), F.lit("INVALID"))
            .when(F.col("transformed_value") >= 750, F.lit("HIGH_VALUE"))
            .when(F.col("transformed_value") >= 300, F.lit("MEDIUM_VALUE"))
            .when(F.col("transformed_value") > 0, F.lit("LOW_VALUE"))
            .otherwise(F.lit("INVALID"))
        )
        
        # Rule 2: Priority override for high value items
        df = df.withColumn(
            "priority",
            F.when(F.col("transformed_value") >= 1000, F.lit(1))
            .otherwise(F.col("priority"))
        )
        
        # Rule 3: Mark empty names as invalid
        df = df.withColumn(
            "status",
            F.when(
                (F.col("name").isNull()) | (F.trim(F.col("name")) == ""),
                F.lit("INVALID")
            ).otherwise(F.col("status"))
        )
        
        return df
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply category-specific transformation rules
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with category rules applied
        """
        # Premium category gets additional value boost
        df = df.withColumn(
            "transformed_value",
            F.when(
                F.col("category") == "PREMIUM",
                F.col("transformed_value") * 1.2
            ).otherwise(F.col("transformed_value"))
        )
        
        # VIP category always gets priority 1
        df = df.withColumn(
            "priority",
            F.when(F.col("category") == "VIP", F.lit(1))
            .otherwise(F.col("priority"))
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
        # Add value tier classification
        df = df.withColumn(
            "value_tier",
            F.when(F.col("transformed_value") >= 1000, F.lit("TIER_1"))
            .when(F.col("transformed_value") >= 500, F.lit("TIER_2"))
            .when(F.col("transformed_value") >= 250, F.lit("TIER_3"))
            .otherwise(F.lit("TIER_4"))
        )
        
        # Add data quality flag
        df = df.withColumn(
            "quality_flag",
            F.when(
                (F.col("status") == "INVALID") | 
                (F.col("value").isNull()) |
                (F.col("name").isNull()),
                F.lit("POOR")
            ).otherwise(F.lit("GOOD"))
        )
        
        return df
    
    def _add_processing_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add ETL processing metadata
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with metadata
        """
        import os
        
        df = df.withColumn("etl_run_id", F.lit(self.run_id)) \
            .withColumn("processed_at", F.current_timestamp()) \
            .withColumn("processed_by", F.lit(os.environ.get("USER", "spark_user")))
        
        return df
    
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
        
        # Validation 1: Check for null IDs
        null_id_count = df.filter(F.col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with null ID")
        
        # Validation 2: Check for null names
        null_name_count = df.filter(
            F.col("name").isNull() | (F.trim(F.col("name")) == "")
        ).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with null/empty name")
        
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
        
        # Validation 5: Check for duplicate IDs
        duplicate_count = df.groupBy("id").count().filter(F.col("count") > 1).count()
        if duplicate_count > 0:
            errors.append(f"{duplicate_count} duplicate IDs found")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Data validation passed")
        else:
            self.logger.error(f"Data validation failed: {'; '.join(errors)}")
        
        return is_valid, errors
    
    def get_validation_summary(self, df: DataFrame) -> Dict[str, Any]:
        """
        Generate detailed validation summary
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Dictionary with validation metrics
        """
        total_records = df.count()
        
        return {
            "total_records": total_records,
            "null_ids": df.filter(F.col("id").isNull()).count(),
            "null_names": df.filter(F.col("name").isNull()).count(),
            "invalid_status": df.filter(F.col("status") == "INVALID").count(),
            "null_values": df.filter(F.col("value").isNull()).count(),
            "negative_values": df.filter(F.col("value") < 0).count(),
            "duplicate_ids": df.groupBy("id").count().filter(F.col("count") > 1).count(),
            "invalid_priority": df.filter(
                (F.col("priority") < 1) | (F.col("priority") > 5)
            ).count()
        }