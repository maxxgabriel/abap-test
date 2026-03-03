"""
ETL Transformer - Handles data transformation and validation
"""
from typing import Tuple, List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, upper, trim, when, current_timestamp, lit
from pyspark.sql.types import IntegerType

from src.logger import ETLLogger


class ETLTransformer:
    """Transforms and validates data"""
    
    def __init__(
        self,
        spark: SparkSession,
        run_id: str,
        logger: Optional[ETLLogger] = None
    ):
        self.spark = spark
        self.run_id = run_id
        self.logger = logger or ETLLogger()
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """Apply transformations to data"""
        self.logger.log_info(
            component="TRANSFORMER",
            message="Starting transformation"
        )
        
        # Apply transformations
        transformed_df = df.select(
            col("id"),
            upper(trim(col("name"))).alias("name"),
            col("value"),
            self._calculate_transformed_value(col("value"), col("category")).alias("transformed_value"),
            lit("TRANSFORMED").alias("status"),
            col("category"),
            self._calculate_priority(col("value"), col("category")).alias("priority"),
            lit(self.run_id).alias("etl_run_id"),
            current_timestamp().alias("processed_at"),
            lit("system").alias("processed_by")
        )
        
        # Apply business rules
        transformed_df = self._apply_business_rules(transformed_df)
        
        count = transformed_df.count()
        self.logger.log_info(
            component="TRANSFORMER",
            message=f"Transformed {count} records"
        )
        
        return transformed_df
    
    def _calculate_transformed_value(self, value_col, category_col):
        """Calculate transformed value based on category"""
        return when(category_col == "PREMIUM", value_col * 1.5) \
            .when(category_col == "STANDARD", value_col * 1.2) \
            .when(category_col == "VIP", value_col * 2.0) \
            .otherwise(value_col)
    
    def _calculate_priority(self, value_col, category_col):
        """Calculate priority based on value and category"""
        return when(value_col >= 1000, 1) \
            .when(value_col >= 500, 2) \
            .when(value_col >= 300, 3) \
            .when(value_col >= 100, 4) \
            .otherwise(5).cast(IntegerType())
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules"""
        return df.withColumn(
            "status",
            when(col("value").isNull(), "INVALID")
            .when(col("transformed_value") >= 750, "HIGH_VALUE")
            .when(col("transformed_value") >= 300, "MEDIUM_VALUE")
            .otherwise("LOW_VALUE")
        ).withColumn(
            "priority",
            when(col("transformed_value") >= 1000, 1)
            .otherwise(col("priority"))
        )
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """Validate transformed data"""
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
            errors.append(f"{negative_value_count} records with negative values")
        
        is_valid = len(errors) == 0
        
        return is_valid, errors