"""
PySpark Data Transformer
Converts ABAP transformation logic to PySpark DataFrame operations
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from pyspark.sql.window import Window
from typing import List, Tuple, Optional
from datetime import datetime
from src.logger import ETLLogger


class DataTransformer:
    """Transforms data using PySpark DataFrame API"""
    
    def __init__(self, spark: SparkSession, run_id: str):
        """
        Initialize transformer
        
        Args:
            spark: Active SparkSession
            run_id: Unique run identifier
        """
        self.spark = spark
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
        self.current_timestamp = datetime.now()
    
    @staticmethod
    def get_transformed_schema() -> StructType:
        """Define schema for transformed data"""
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
            StructField("processed_by", StringType(), False),
        ])
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Main transformation method
        
        Args:
            source_df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Starting transformation"
        )
        
        # Basic transformations
        df = source_df.select(
            F.col("id"),
            F.upper(F.trim(F.col("name"))).alias("name"),
            F.col("value"),
            F.col("status"),
            F.col("category")
        )
        
        # Calculate derived values
        df = df.withColumn(
            "transformed_value",
            self._calculate_derived_value_udf(F.col("value"), F.col("category"))
        )
        
        # Calculate priority
        df = df.withColumn(
            "priority",
            self._calculate_priority_udf(F.col("value"), F.col("category"))
        )
        
        # Add ETL metadata
        df = df.withColumn("etl_run_id", F.lit(self.run_id)) \
               .withColumn("processed_at", F.lit(self.current_timestamp)) \
               .withColumn("processed_by", F.lit("pyspark_etl"))
        
        # Set initial status
        df = df.withColumn("status", F.lit("TRANSFORMED"))
        
        # Apply business rules
        df = self.apply_business_rules(df)
        
        # Enrich data
        df = self.enrich_data(df)
        
        record_count = df.count()
        self.logger.log_info(
            component="TRANSFORMER",
            message=f"Transformed {record_count} records"
        )
        
        return df
    
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules to data
        
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
        
        # Rule 3: Name normalization - remove multiple spaces
        df = df.withColumn(
            "name",
            F.regexp_replace(F.trim(F.col("name")), r'\s+', ' ')
        )
        
        # Rule 4: Category validation
        df = df.withColumn(
            "category",
            F.when(F.col("category").isNull(), "UNCATEGORIZED")
             .when(F.col("category") == "", "UNCATEGORIZED")
             .otherwise(F.col("category"))
        )
        
        return df
    
    def enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional information
        
        Args:
            df: Input DataFrame
            
        Returns:
            Enriched DataFrame
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Enriching data"
        )
        
        # Load configuration
        config = self._load_enrichment_config()
        
        # Apply premium multiplier from config
        premium_multiplier = config.get("PREMIUM_MULTIPLIER", 1.2)
        
        df = df.withColumn(
            "transformed_value",
            F.when(F.col("category") == "PREMIUM", 
                   F.col("transformed_value") * premium_multiplier)
             .otherwise(F.col("transformed_value"))
        )
        
        # Add ranking within category
        window_spec = Window.partitionBy("category").orderBy(F.col("transformed_value").desc())
        df = df.withColumn("category_rank", F.row_number().over(window_spec))
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Validation rule 1: ID is required
        null_ids = df.filter(F.col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null ID")
        
        # Validation rule 2: Name is required
        null_names = df.filter(F.col("name").isNull()).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null name")
        
        # Validation rule 3: Value must be positive
        negative_values = df.filter(F.col("value") < 0).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative value")
        
        # Validation rule 4: Priority must be 1-5
        invalid_priority = df.filter((F.col("priority") < 1) | (F.col("priority") > 5)).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        # Validation rule 5: Check for duplicates
        total_count = df.count()
        unique_count = df.select("id").distinct().count()
        if total_count != unique_count:
            errors.append(f"{total_count - unique_count} duplicate IDs found")
        
        is_valid = len(errors) == 0
        
        if not is_valid:
            self.logger.log_warning(
                component="TRANSFORMER",
                message=f"Validation failed with {len(errors)} errors"
            )
        
        return is_valid, errors
    
    def _calculate_derived_value_udf(self, value_col, category_col):
        """
        Calculate derived value based on category
        Returns column expression
        """
        return F.when(category_col == "PREMIUM", value_col * 1.5) \
                .when(category_col == "VIP", value_col * 2.0) \
                .when(category_col == "STANDARD", value_col * 1.2) \
                .otherwise(value_col)
    
    def _calculate_priority_udf(self, value_col, category_col):
        """
        Calculate priority based on value and category
        Returns column expression
        """
        return F.when(value_col >= 1000, 1) \
                .when((value_col >= 500) & (category_col == "VIP"), 1) \
                .when(value_col >= 500, 2) \
                .when(value_col >= 300, 3) \
                .when(value_col >= 100, 4) \
                .otherwise(5)
    
    def _load_enrichment_config(self) -> dict:
        """
        Load enrichment configuration from config table
        
        Returns:
            Dictionary of config key-value pairs
        """
        try:
            config_table = self.spark.conf.get("spark.etl.config_table", "etl_config")
            
            config_df = self.spark.read \
                .format("delta") \
                .load(config_table) \
                .filter(F.col("is_active") == True) \
                .select("config_key", "config_value")
            
            config_dict = {row["config_key"]: row["config_value"] for row in config_df.collect()}
            
            # Convert numeric values
            for key in ["PREMIUM_MULTIPLIER", "BATCH_SIZE", "MAX_RETRIES"]:
                if key in config_dict:
                    try:
                        config_dict[key] = float(config_dict[key])
                    except ValueError:
                        pass
            
            return config_dict
            
        except Exception as e:
            self.logger.log_warning(
                component="TRANSFORMER",
                message=f"Could not load config: {str(e)}"
            )
            return {}