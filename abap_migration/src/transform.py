"""Data transformation module for ETL pipeline."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, current_timestamp,
    current_user, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import Tuple, List
import logging


class ETLTransformer:
    """Handles data transformation and business rules."""
    
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
        StructField("processed_by", StringType(), False),
    ])
    
    def __init__(self, spark: SparkSession, run_id: str):
        """
        Initialize transformer.
        
        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
        """
        self.spark = spark
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply transformations to source data.
        
        Args:
            df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        # Basic transformations
        transformed_df = df.select(
            col("id"),
            upper(trim(regexp_replace(col("name"), "\\s+", " "))).alias("name"),
            col("value"),
            self._calculate_derived_value(col("value"), col("category")).alias("transformed_value"),
            lit("TRANSFORMED").alias("status"),
            coalesce(col("category"), lit("UNCATEGORIZED")).alias("category"),
            self._calculate_priority(col("value"), col("category")).alias("priority"),
            lit(self.run_id).alias("etl_run_id"),
            current_timestamp().alias("processed_at"),
            current_user().alias("processed_by")
        )
        
        # Apply business rules
        transformed_df = self._apply_business_rules(transformed_df)
        
        # Enrich data
        transformed_df = self._enrich_data(transformed_df)
        
        record_count = transformed_df.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return transformed_df
    
    def _calculate_derived_value(self, value_col, category_col):
        """Calculate transformed value based on category."""
        return when(category_col == "PREMIUM", value_col * 1.5) \
            .when(category_col == "VIP", value_col * 1.8) \
            .when(category_col == "STANDARD", value_col * 1.2) \
            .otherwise(value_col)
    
    def _calculate_priority(self, value_col, category_col):
        """Calculate priority based on value and category."""
        return when(value_col >= 1000, lit(1)) \
            .when(value_col >= 750, lit(2)) \
            .when((value_col >= 500) | (category_col == "PREMIUM"), lit(2)) \
            .when(value_col >= 300, lit(3)) \
            .when(value_col >= 100, lit(4)) \
            .otherwise(lit(5))
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to data."""
        self.logger.info("Applying business rules")
        
        # Rule 1: Set status based on transformed value
        df = df.withColumn(
            "status",
            when(col("value").isNull(), lit("INVALID"))
            .when(col("transformed_value") >= 750, lit("HIGH_VALUE"))
            .when(col("transformed_value") >= 300, lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )
        
        # Rule 2: Override priority for high-value items
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, lit(1))
            .otherwise(col("priority"))
        )
        
        # Rule 3: Ensure category is set
        df = df.withColumn(
            "category",
            when(col("category").isNull(), lit("UNCATEGORIZED"))
            .otherwise(col("category"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional information."""
        self.logger.info("Enriching data")
        
        # Load enrichment configuration
        config_df = self._load_config()
        
        # Apply category-specific enrichment
        premium_multiplier = config_df.filter(
            col("config_key") == "PREMIUM_MULTIPLIER"
        ).select("config_value").first()
        
        if premium_multiplier:
            multiplier = float(premium_multiplier["config_value"])
            df = df.withColumn(
                "transformed_value",
                when(col("category") == "PREMIUM", col("transformed_value") * multiplier)
                .otherwise(col("transformed_value"))
            )
        
        return df
    
    def _load_config(self) -> DataFrame:
        """Load configuration from database."""
        try:
            config_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.spark.conf.get("spark.jdbc.url")) \
                .option("dbtable", "(SELECT * FROM zetl_config WHERE is_active = 'X') AS config") \
                .option("user", self.spark.conf.get("spark.jdbc.user")) \
                .option("password", self.spark.conf.get("spark.jdbc.password")) \
                .option("driver", self.spark.conf.get("spark.jdbc.driver")) \
                .load()
            return config_df
        except Exception as e:
            self.logger.warning(f"Could not load config: {e}")
            return self.spark.createDataFrame([], StructType([
                StructField("config_key", StringType()),
                StructField("config_value", StringType())
            ]))
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        self.logger.info("Validating data")
        errors = []
        
        # Rule 1: ID is required
        null_ids = df.filter(col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null ID")
        
        # Rule 2: Name is required
        null_names = df.filter(col("name").isNull()).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null name")
        
        # Rule 3: Value must be positive
        negative_values = df.filter(col("value") < 0).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative value")
        
        # Rule 4: Priority must be 1-5
        invalid_priority = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        # Rule 5: Category is required
        null_category = df.filter(col("category").isNull()).count()
        if null_category > 0:
            errors.append(f"{null_category} records with null category")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Validation passed")
        else:
            self.logger.error(f"Validation failed: {errors}")
        
        return is_valid, errors