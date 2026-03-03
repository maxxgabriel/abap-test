"""
ETL Data Transformation Module
Handles data transformation, business rules, and data enrichment.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, upper, trim, when, lit, current_timestamp,
    regexp_replace, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from typing import Tuple, List
from decimal import Decimal
from src.logger import ETLLogger


class ETLTransformer:
    """
    Transforms extracted data according to business rules.
    """
    
    def __init__(self, spark: SparkSession, run_id: str):
        """
        Initialize the transformer.
        
        Args:
            spark: SparkSession instance
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
    
    @staticmethod
    def get_transformed_schema() -> StructType:
        """
        Define the schema for transformed data.
        
        Returns:
            StructType: Schema for transformed data
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
            StructField("processed_by", StringType(), nullable=True)
        ])
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Transform source data.
        
        Args:
            source_df: Source DataFrame
            
        Returns:
            DataFrame: Transformed data
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Starting transformation"
        )
        
        try:
            # Basic transformations
            transformed_df = source_df.select(
                col("id"),
                upper(trim(col("name"))).alias("name"),
                col("value"),
                self._calculate_derived_values(col("value"), col("category")).alias("transformed_value"),
                lit("TRANSFORMED").alias("status"),
                col("category"),
                self._calculate_priority(col("value"), col("category")).alias("priority"),
                lit(self.run_id).alias("etl_run_id"),
                current_timestamp().alias("processed_at"),
                lit(self.spark.conf.get("spark.etl.user", "system")).alias("processed_by")
            )
            
            # Apply category-specific rules
            transformed_df = self._apply_category_rules(transformed_df)
            
            # Apply business rules
            transformed_df = self._apply_business_rules(transformed_df)
            
            # Enrich data
            transformed_df = self._enrich_data(transformed_df)
            
            record_count = transformed_df.count()
            self.logger.log_info(
                component="TRANSFORMER",
                message=f"Transformed {record_count} records"
            )
            
            return transformed_df
            
        except Exception as e:
            self.logger.log_error(
                component="TRANSFORMER",
                message="Transformation failed",
                details=str(e)
            )
            raise
    
    def _calculate_derived_values(self, value_col, category_col):
        """
        Calculate derived values based on category.
        
        Args:
            value_col: Value column
            category_col: Category column
            
        Returns:
            Column: Calculated transformed value
        """
        return when(category_col == "PREMIUM", value_col * 1.5) \
            .when(category_col == "VIP", value_col * 2.0) \
            .when(category_col == "STANDARD", value_col * 1.2) \
            .otherwise(value_col)
    
    def _calculate_priority(self, value_col, category_col):
        """
        Calculate priority based on value and category.
        
        Args:
            value_col: Value column
            category_col: Category column
            
        Returns:
            Column: Calculated priority (1-5)
        """
        return when(value_col >= 1000, lit(1)) \
            .when((value_col >= 750) | (category_col == "VIP"), lit(2)) \
            .when(value_col >= 500, lit(3)) \
            .when(value_col >= 250, lit(4)) \
            .otherwise(lit(5))
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to transformed data.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            DataFrame: Data with business rules applied
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Applying business rules"
        )
        
        # Rule 1: Set status based on transformed value
        df = df.withColumn(
            "status",
            when(col("value").isNull(), lit("INVALID"))
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
        
        # Rule 3: Name normalization - remove multiple spaces
        df = df.withColumn(
            "name",
            trim(regexp_replace(col("name"), "\\s+", " "))
        )
        
        # Rule 4: Category validation
        df = df.withColumn(
            "category",
            when(col("category").isNull() | (col("category") == ""), lit("UNCATEGORIZED"))
            .otherwise(col("category"))
        )
        
        return df
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply category-specific rules.
        
        Args:
            df: DataFrame
            
        Returns:
            DataFrame: Data with category rules applied
        """
        # Apply premium multiplier
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("transformed_value") * 1.2)
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional information.
        
        Args:
            df: DataFrame
            
        Returns:
            DataFrame: Enriched data
        """
        try:
            # Load enrichment configuration from database
            jdbc_url = self.spark.conf.get("spark.etl.source.jdbc.url")
            jdbc_properties = {
                "user": self.spark.conf.get("spark.etl.source.jdbc.user"),
                "password": self.spark.conf.get("spark.etl.source.jdbc.password"),
                "driver": self.spark.conf.get("spark.etl.source.jdbc.driver")
            }
            
            config_df = self.spark.read.jdbc(
                url=jdbc_url,
                table="(SELECT * FROM etl_config WHERE is_active = 'X') as config",
                properties=jdbc_properties
            )
            
            # Apply enrichment logic based on config
            # For now, keep the transformations already applied
            
            return df
            
        except Exception as e:
            self.logger.log_warning(
                component="TRANSFORMER",
                message="Enrichment step had issues, continuing without enrichment",
                details=str(e)
            )
            return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Tuple[bool, List[str]]: (is_valid, list of error messages)
        """
        errors = []
        is_valid = True
        
        # Validation rule 1: ID is required
        null_id_count = df.filter(col("id").isNull() | (col("id") == "")).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with missing ID")
            is_valid = False
        
        # Validation rule 2: Name is required
        null_name_count = df.filter(col("name").isNull() | (col("name") == "")).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with missing name")
            is_valid = False
        
        # Validation rule 3: Value must be positive
        negative_value_count = df.filter(col("value") < 0).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative values")
            is_valid = False
        
        # Validation rule 4: Priority must be 1-5
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
            is_valid = False
        
        # Validation rule 5: Category must not be empty
        null_category_count = df.filter(
            col("category").isNull() | (col("category") == "")
        ).count()
        if null_category_count > 0:
            errors.append(f"{null_category_count} records with missing category")
            is_valid = False
        
        if not is_valid:
            self.logger.log_warning(
                component="TRANSFORMER",
                message=f"Validation failed with {len(errors)} issues",
                details="; ".join(errors)
            )
        
        return is_valid, errors