"""
ETL Transformer Module
Handles data transformation and business rules
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, upper, trim, when, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

from src.utils.logger import ETLLogger


class ETLTransformer:
    """Transforms data according to business rules"""
    
    def __init__(self, spark: SparkSession, config: dict, run_id: str):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger(run_id=run_id)
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply transformations to data
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Starting transformation"
        )
        
        # Apply transformations
        transformed_df = df \
            .withColumn("name", upper(trim(col("name")))) \
            .withColumn("transformed_value", self._calculate_derived_value(col("value"), col("category"))) \
            .withColumn("status", lit("TRANSFORMED")) \
            .withColumn("priority", self._calculate_priority(col("value"), col("category"))) \
            .withColumn("etl_run_id", lit(self.run_id)) \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("processed_by", lit("ETL_SYSTEM"))
        
        # Apply business rules
        transformed_df = self._apply_business_rules(transformed_df)
        
        # Apply enrichment
        transformed_df = self._enrich_data(transformed_df)
        
        count = transformed_df.count()
        self.logger.log_info(
            component="TRANSFORMER",
            message=f"Transformed {count} records"
        )
        
        return transformed_df
    
    def _calculate_derived_value(self, value_col, category_col):
        """Calculate derived value based on category"""
        premium_multiplier = float(self.config["business_rules"].get("premium_multiplier", 1.5))
        
        return when(category_col == "PREMIUM", value_col * premium_multiplier) \
            .when(category_col == "VIP", value_col * 1.8) \
            .otherwise(value_col)
    
    def _calculate_priority(self, value_col, category_col):
        """Calculate priority based on value and category"""
        return when(value_col >= 1000, 1) \
            .when(value_col >= 750, 2) \
            .when(value_col >= 300, 3) \
            .when(value_col >= 100, 4) \
            .otherwise(5)
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules"""
        self.logger.log_info(
            component="TRANSFORMER",
            message="Applying business rules"
        )
        
        # Rule 1: Set status based on transformed value
        df = df.withColumn(
            "status",
            when(col("value").isNull(), "INVALID")
            .when(col("transformed_value") >= 750, "HIGH_VALUE")
            .when(col("transformed_value") >= 300, "MEDIUM_VALUE")
            .otherwise("LOW_VALUE")
        )
        
        # Rule 2: Priority override for high value
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, 1)
            .otherwise(col("priority"))
        )
        
        # Rule 3: Category validation
        df = df.withColumn(
            "category",
            when(col("category").isNull(), "UNCATEGORIZED")
            .otherwise(col("category"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional fields"""
        self.logger.log_info(
            component="TRANSFORMER",
            message="Enriching data"
        )
        
        # Add enrichment logic
        # Example: Add calculated fields, lookups, etc.
        
        return df