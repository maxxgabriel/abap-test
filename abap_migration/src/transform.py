"""
ETL Data Transformation Module
Handles data transformations and business rules
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from datetime import datetime
from typing import Tuple, List
from src.logger import ETLLogger


class ETLTransformer:
    """Data transformation component"""
    
    def __init__(self, spark, config: dict, run_id: str):
        """
        Initialize transformer
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = ETLLogger(run_id)
    
    def get_transformed_schema(self) -> StructType:
        """Define transformed data schema"""
        return StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("value", DecimalType(15, 2), True),
            StructField("transformed_value", DecimalType(15, 2), True),
            StructField("status", StringType(), True),
            StructField("category", StringType(), True),
            StructField("priority", IntegerType(), True),
            StructField("etl_run_id", StringType(), False),
            StructField("processed_at", TimestampType(), False),
            StructField("processed_by", StringType(), False)
        ])
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply all transformations to source data
        
        Args:
            source_df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Starting transformation"
        )
        
        try:
            # Basic transformations
            df = source_df.select(
                F.col("id"),
                F.upper(F.trim(F.col("name"))).alias("name"),
                F.col("value"),
                self._calculate_transformed_value(F.col("value"), F.col("category")).alias("transformed_value"),
                F.lit("TRANSFORMED").alias("status"),
                F.col("category"),
                self._calculate_priority(F.col("value"), F.col("category")).alias("priority"),
                F.lit(self.run_id).alias("etl_run_id"),
                F.current_timestamp().alias("processed_at"),
                F.lit(self.config.get('user', 'system')).alias("processed_by")
            )
            
            # Apply business rules
            df = self._apply_business_rules(df)
            
            # Enrich data
            df = self._enrich_data(df)
            
            count = df.count()
            self.logger.log_info(
                component="TRANSFORMER",
                message=f"Transformed {count} records"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="TRANSFORMER",
                message="Transformation failed",
                details=str(e)
            )
            raise
    
    def _calculate_transformed_value(self, value_col, category_col):
        """Calculate transformed value based on category"""
        return F.when(category_col == "PREMIUM", value_col * 1.5) \
            .when(category_col == "VIP", value_col * 2.0) \
            .when(category_col == "STANDARD", value_col * 1.2) \
            .otherwise(value_col)
    
    def _calculate_priority(self, value_col, category_col):
        """Calculate priority based on value and category"""
        return F.when(value_col >= 1000, 1) \
            .when(value_col >= 750, 2) \
            .when(value_col >= 500, 3) \
            .when(value_col >= 250, 4) \
            .otherwise(5)
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to data"""
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
        
        # Rule 3: Category validation
        df = df.withColumn(
            "category",
            F.when(F.col("category").isNull(), "UNCATEGORIZED")
            .otherwise(F.col("category"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional information"""
        # Load configuration for enrichment
        config_table = self.config.get('tables', {}).get('config')
        
        if config_table:
            try:
                config_df = self.spark.table(config_table) \
                    .filter("is_active = true AND config_type = 'BUSINESS_RULE'")
                
                # Apply enrichment based on configuration
                # Example: Premium multiplier
                premium_config = config_df.filter("config_key = 'PREMIUM_MULTIPLIER'").first()
                if premium_config:
                    multiplier = float(premium_config['config_value'])
                    df = df.withColumn(
                        "transformed_value",
                        F.when(F.col("category") == "PREMIUM", F.col("value") * multiplier)
                        .otherwise(F.col("transformed_value"))
                    )
            except Exception as e:
                self.logger.log_warning(
                    component="TRANSFORMER",
                    message="Enrichment configuration not available",
                    details=str(e)
                )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []
        
        # Check for null required fields
        null_ids = df.filter(F.col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null ID")
        
        null_names = df.filter(F.col("name").isNull()).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null name")
        
        # Check for invalid values
        negative_values = df.filter(
            (F.col("value") < 0) | (F.col("transformed_value") < 0)
        ).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative values")
        
        # Check priority range
        invalid_priority = df.filter(
            (F.col("priority") < 1) | (F.col("priority") > 5)
        ).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if not is_valid:
            self.logger.log_warning(
                component="TRANSFORMER",
                message=f"Validation found {len(errors)} issues"
            )
        
        return is_valid, errors