"""
ETL Transformer Module
Transforms extracted data using business rules
Migrated from zcl_etl_transformer.abap
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, 
    current_timestamp, current_user
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import List, Tuple
import logging


class ETLTransformer:
    """Transforms data using business rules and enrichment"""
    
    def __init__(self, spark: SparkSession, run_id: str):
        """
        Initialize transformer
        
        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
        """
        self.spark = spark
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
    
    def get_transformed_schema(self) -> StructType:
        """Define schema for transformed data"""
        return StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("value", DecimalType(15, 2), True),
            StructField("transformed_value", DecimalType(15, 2), True),
            StructField("status", StringType(), True),
            StructField("category", StringType(), True),
            StructField("priority", IntegerType(), True),
            StructField("etl_run_id", StringType(), True),
            StructField("processed_at", TimestampType(), True),
            StructField("processed_by", StringType(), True)
        ])
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Main transformation method
        
        Args:
            df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        # Apply column transformations using withColumn chains
        df = df.withColumn("name", upper(trim(col("name")))) \
            .withColumn("transformed_value", self._calculate_derived_value(col("value"), col("category"))) \
            .withColumn("status", lit("TRANSFORMED")) \
            .withColumn("priority", self._calculate_priority(col("value"), col("category"))) \
            .withColumn("etl_run_id", lit(self.run_id)) \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("processed_by", current_user())
        
        # Apply category-specific rules
        df = self._apply_category_rules(df)
        
        # Apply business rules
        df = self._apply_business_rules(df)
        
        # Enrich data
        df = self._enrich_data(df)
        
        # Clean up name field - remove multiple spaces
        df = df.withColumn("name", regexp_replace(col("name"), "\\s+", " "))
        
        record_count = df.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return df
    
    def _calculate_derived_value(self, value_col, category_col):
        """
        Calculate transformed value based on category
        Uses when().otherwise() for CASE logic
        """
        return when(category_col == lit("PREMIUM"), value_col * lit(1.5)) \
            .when(category_col == lit("VIP"), value_col * lit(2.0)) \
            .when(category_col == lit("STANDARD"), value_col * lit(1.2)) \
            .when(category_col == lit("BASIC"), value_col * lit(1.0)) \
            .otherwise(value_col * lit(1.1))
    
    def _calculate_priority(self, value_col, category_col):
        """
        Calculate priority based on value and category
        Uses when().otherwise() for CASE logic
        """
        return when(category_col == lit("VIP"), lit(1)) \
            .when((category_col == lit("PREMIUM")) & (value_col >= lit(1000)), lit(1)) \
            .when(value_col >= lit(1000), lit(2)) \
            .when(value_col >= lit(500), lit(3)) \
            .when(value_col >= lit(200), lit(4)) \
            .otherwise(lit(5))
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """Apply category-specific transformation rules"""
        # Premium category gets additional boost
        df = df.withColumn(
            "transformed_value",
            when(col("category") == lit("PREMIUM"), col("transformed_value") * lit(1.2))
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules using withColumn chains
        Migrated from LOOP AT assignments to DataFrame operations
        """
        self.logger.info("Applying business rules")
        
        # Rule 1: Set status based on value using when().otherwise()
        df = df.withColumn(
            "status",
            when(col("value").isNull(), lit("INVALID"))
            .when(col("transformed_value") >= lit(750), lit("HIGH_VALUE"))
            .when(col("transformed_value") >= lit(300), lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )
        
        # Rule 2: Priority override for high value items
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= lit(1000), lit(1))
            .otherwise(col("priority"))
        )
        
        # Rule 3: Name normalization - already applied upper() and trim()
        # Additional cleanup handled in main transform
        
        # Rule 4: Category validation
        df = df.withColumn(
            "category",
            when(col("category").isNull() | (col("category") == lit("")), lit("UNCATEGORIZED"))
            .otherwise(col("category"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional calculated fields"""
        self.logger.info("Enriching data")
        
        # Load enrichment configuration (simplified)
        config_df = self._load_enrichment_config()
        
        # Apply enrichment - example: boost premium category
        df = df.withColumn(
            "transformed_value",
            when(col("category") == lit("PREMIUM"), col("transformed_value") * lit(1.2))
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def _load_enrichment_config(self) -> DataFrame:
        """Load enrichment configuration from database"""
        try:
            return self.spark.read \
                .format("jdbc") \
                .option("url", self._get_jdbc_url()) \
                .option("dbtable", "zetl_config") \
                .option("driver", "org.postgresql.Driver") \
                .load() \
                .filter(col("is_active") == lit("X"))
        except Exception as e:
            self.logger.warning(f"Could not load config: {str(e)}")
            return self.spark.createDataFrame([], StructType([]))
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []
        
        # Validation rule 1: ID is required
        null_ids = df.filter(col("id").isNull() | (col("id") == lit(""))).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with missing ID")
        
        # Validation rule 2: Name is required
        null_names = df.filter(col("name").isNull() | (col("name") == lit(""))).count()
        if null_names > 0:
            errors.append(f"{null_names} records with missing name")
        
        # Validation rule 3: Value must be positive
        invalid_values = df.filter(col("value") < lit(0)).count()
        if invalid_values > 0:
            errors.append(f"{invalid_values} records with negative values")
        
        # Validation rule 4: Priority must be 1-5
        invalid_priority = df.filter((col("priority") < lit(1)) | (col("priority") > lit(5))).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Validation passed")
        else:
            self.logger.error(f"Validation failed with {len(errors)} errors")
        
        return is_valid, errors
    
    def _get_jdbc_url(self) -> str:
        """Get JDBC connection URL"""
        return "jdbc:postgresql://localhost:5432/etl_db"