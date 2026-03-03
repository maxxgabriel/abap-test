"""
ETL Transformer module.
Handles data transformation and business rules.
"""

from datetime import datetime
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, current_timestamp,
    current_user, concat_ws
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType

from src.utils.logger import ETLLogger
from src.utils.config import ConfigManager


class ETLTransformer:
    """
    Data transformation class.
    Converts ABAP zcl_etl_transformer class to PySpark.
    """
    
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
        StructField("processed_by", StringType(), True)
    ])
    
    def __init__(
        self,
        spark: SparkSession,
        config: ConfigManager,
        run_id: str
    ):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
        
    def transform_data(self, df_source: DataFrame) -> DataFrame:
        """
        Main transformation method.
        Replaces transform_data ABAP method.
        """
        self.logger.log_info("TRANSFORMER", "Starting transformation")
        
        # Base transformations
        df = df_source.withColumn(
            "name",
            trim(upper(regexp_replace(col("name"), "\\s+", " ")))
        ).withColumn(
            "transformed_value",
            self._calculate_derived_value(col("value"), col("category"))
        ).withColumn(
            "priority",
            self._calculate_priority(col("value"), col("category"))
        ).withColumn(
            "status",
            lit("TRANSFORMED")
        ).withColumn(
            "etl_run_id",
            lit(self.run_id)
        ).withColumn(
            "processed_at",
            current_timestamp()
        ).withColumn(
            "processed_by",
            current_user()
        )
        
        # Apply business rules
        df = self._apply_business_rules(df)
        
        # Enrich data
        df = self._enrich_data(df)
        
        record_count = df.count()
        self.logger.log_info(
            "TRANSFORMER",
            f"Transformed {record_count} records"
        )
        
        return df
    
    def _calculate_derived_value(self, value_col, category_col):
        """Calculate transformed value based on category."""
        premium_multiplier = float(
            self.config.get("business_rules.premium_multiplier", 1.5)
        )
        
        return when(
            category_col == "PREMIUM",
            value_col * premium_multiplier
        ).when(
            category_col == "VIP",
            value_col * 1.8
        ).when(
            category_col == "STANDARD",
            value_col * 1.2
        ).otherwise(value_col)
    
    def _calculate_priority(self, value_col, category_col):
        """Calculate priority based on value and category."""
        return when(
            value_col >= 1000, 1
        ).when(
            value_col >= 750, 2
        ).when(
            value_col >= 300, 3
        ).when(
            value_col >= 100, 4
        ).otherwise(5)
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to transformed data."""
        self.logger.log_info("TRANSFORMER", "Applying business rules")
        
        df = df.withColumn(
            "status",
            when(
                col("value").isNull() | (col("value") == 0),
                lit("INVALID")
            ).when(
                col("transformed_value") >= 750,
                lit("HIGH_VALUE")
            ).when(
                col("transformed_value") >= 300,
                lit("MEDIUM_VALUE")
            ).otherwise(lit("LOW_VALUE"))
        ).withColumn(
            "priority",
            when(
                col("transformed_value") >= 1000,
                lit(1)
            ).otherwise(col("priority"))
        ).withColumn(
            "category",
            when(
                col("category").isNull() | (col("category") == ""),
                lit("UNCATEGORIZED")
            ).otherwise(col("category"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional information."""
        # Load enrichment config
        config_df = self._load_config_data()
        
        if config_df is not None:
            # Join with config for enrichment
            df = df.join(
                config_df.select("config_key", "config_value"),
                how="left",
                on=[]  # Cross join for global configs
            )
        
        return df
    
    def _load_config_data(self) -> DataFrame:
        """Load configuration data for enrichment."""
        try:
            db_config = self.config.get_database_config("config")
            
            return (self.spark.read
                    .format("jdbc")
                    .option("url", db_config["url"])
                    .option("dbtable", "etl_config")
                    .load()
                    .filter("is_active = true"))
        except Exception as e:
            self.logger.log_warning(
                "TRANSFORMER",
                f"Could not load config data: {str(e)}"
            )
            return None
    
    def validate_data(self, df: DataFrame) -> Dict[str, Any]:
        """
        Validate transformed data.
        Replaces validate_data ABAP method.
        """
        errors = []
        
        # Check for null IDs
        null_id_count = df.filter(col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with null ID")
        
        # Check for null names
        null_name_count = df.filter(col("name").isNull()).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with null name")
        
        # Check for negative values
        negative_value_count = df.filter(col("value") < 0).count()
        if negative_value_count > 0:
            errors.append(f"{negative_value_count} records with negative value")
        
        # Check priority range
        invalid_priority_count = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"{invalid_priority_count} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.log_info("TRANSFORMER", "Validation passed")
        else:
            self.logger.log_error(
                "TRANSFORMER",
                f"Validation failed: {len(errors)} issues found"
            )
        
        return {
            "is_valid": is_valid,
            "error_count": len(errors),
            "errors": errors
        }