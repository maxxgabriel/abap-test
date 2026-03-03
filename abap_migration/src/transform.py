"""
ETL Transformation Module for PySpark
Handles data transformation, validation, and business rule application
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from typing import Dict, List, Tuple
from datetime import datetime
import yaml
import logging


class TransformationConfig:
    """Configuration management for transformations"""
    
    def __init__(self, config_path: str = "config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.validation_rules = self.config.get('validation_rules', {})
        self.business_rules = self.config.get('business_rules', {})
        self.category_mappings = self.config.get('category_mappings', {})
        self.priority_rules = self.config.get('priority_rules', {})


class DataTransformer:
    """Main transformation class for ETL pipeline"""
    
    def __init__(self, spark: SparkSession, run_id: str, config_path: str = "config.yaml"):
        self.spark = spark
        self.run_id = run_id
        self.config = TransformationConfig(config_path)
        self.logger = logging.getLogger(__name__)
        self._setup_logging()
    
    def _setup_logging(self):
        """Configure logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def get_output_schema(self) -> StructType:
        """Define transformed data schema"""
        return StructType([
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
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Main transformation method
        
        Args:
            source_df: Source DataFrame from extraction
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting data transformation")
        
        # Apply basic transformations
        df = self._apply_basic_transformations(source_df)
        
        # Calculate derived values
        df = self._calculate_derived_values(df)
        
        # Apply category-specific rules
        df = self._apply_category_rules(df)
        
        # Calculate priority
        df = self._calculate_priority(df)
        
        # Apply business rules
        df = self._apply_business_rules(df)
        
        # Enrich data
        df = self._enrich_data(df)
        
        # Add metadata
        df = self._add_metadata(df)
        
        self.logger.info(f"Transformation complete: {df.count()} records")
        
        return df
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic field transformations"""
        return df.select(
            F.col("id"),
            F.upper(F.trim(F.col("name"))).alias("name"),
            F.col("value"),
            F.col("status"),
            F.col("category"),
            F.col("source_system"),
            F.col("created_at"),
            F.col("created_by")
        )
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """
        Calculate transformed values based on category and value
        
        Business logic:
        - PREMIUM: value * 1.5
        - VIP: value * 1.8
        - STANDARD: value * 1.2
        - BASIC: value * 1.0
        - TRIAL: value * 0.9
        - Default: value * 1.0
        """
        category_multipliers = self.config.category_mappings.get('value_multipliers', {})
        
        return df.withColumn(
            "transformed_value",
            F.when(F.col("category") == "PREMIUM", F.col("value") * F.lit(category_multipliers.get('PREMIUM', 1.5)))
            .when(F.col("category") == "VIP", F.col("value") * F.lit(category_multipliers.get('VIP', 1.8)))
            .when(F.col("category") == "STANDARD", F.col("value") * F.lit(category_multipliers.get('STANDARD', 1.2)))
            .when(F.col("category") == "BASIC", F.col("value") * F.lit(category_multipliers.get('BASIC', 1.0)))
            .when(F.col("category") == "TRIAL", F.col("value") * F.lit(category_multipliers.get('TRIAL', 0.9)))
            .otherwise(F.col("value"))
        )
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """Apply category-specific transformation rules"""
        # Normalize category values
        category_map = self.config.category_mappings.get('normalization', {})
        
        df = df.withColumn(
            "category",
            F.when(F.col("category").isNull(), F.lit("UNCATEGORIZED"))
            .otherwise(F.upper(F.trim(F.col("category"))))
        )
        
        # Apply premium bonus for high-value items
        premium_threshold = self.config.business_rules.get('premium_threshold', 500)
        
        df = df.withColumn(
            "transformed_value",
            F.when(
                (F.col("transformed_value") >= premium_threshold) & (F.col("category") == "PREMIUM"),
                F.col("transformed_value") * 1.2
            ).otherwise(F.col("transformed_value"))
        )
        
        return df
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """
        Calculate priority based on transformed value and category
        
        Priority Rules:
        - Priority 1 (Highest): transformed_value >= 1000 OR category = VIP
        - Priority 2 (High): transformed_value >= 750
        - Priority 3 (Medium): transformed_value >= 300
        - Priority 4 (Low): transformed_value >= 100
        - Priority 5 (Lowest): transformed_value < 100
        """
        priority_rules = self.config.priority_rules
        
        return df.withColumn(
            "priority",
            F.when(
                (F.col("transformed_value") >= F.lit(priority_rules.get('priority_1_threshold', 1000))) |
                (F.col("category") == "VIP"),
                F.lit(1)
            )
            .when(F.col("transformed_value") >= F.lit(priority_rules.get('priority_2_threshold', 750)), F.lit(2))
            .when(F.col("transformed_value") >= F.lit(priority_rules.get('priority_3_threshold', 300)), F.lit(3))
            .when(F.col("transformed_value") >= F.lit(priority_rules.get('priority_4_threshold', 100)), F.lit(4))
            .otherwise(F.lit(5))
        )
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules for status determination
        
        Rules:
        1. Invalid if value is null or zero
        2. HIGH_VALUE if transformed_value >= 750
        3. MEDIUM_VALUE if transformed_value >= 300
        4. LOW_VALUE otherwise
        5. Priority override: transformed_value >= 1000 -> priority = 1
        """
        self.logger.info("Applying business rules")
        
        # Rule 1: Set status based on value
        df = df.withColumn(
            "status",
            F.when(F.col("value").isNull() | (F.col("value") == 0), F.lit("INVALID"))
            .when(F.col("transformed_value") >= F.lit(750), F.lit("HIGH_VALUE"))
            .when(F.col("transformed_value") >= F.lit(300), F.lit("MEDIUM_VALUE"))
            .otherwise(F.lit("LOW_VALUE"))
        )
        
        # Rule 2: Priority override for very high value items
        df = df.withColumn(
            "priority",
            F.when(F.col("transformed_value") >= F.lit(1000), F.lit(1))
            .otherwise(F.col("priority"))
        )
        
        # Rule 3: Name normalization - remove extra spaces
        df = df.withColumn(
            "name",
            F.regexp_replace(F.trim(F.col("name")), "\\s+", " ")
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional calculated fields"""
        # Add enrichment based on configuration
        enrichment_config = self.config.business_rules.get('enrichment', {})
        
        # Add premium bonus if configured
        if enrichment_config.get('apply_premium_bonus', False):
            df = df.withColumn(
                "transformed_value",
                F.when(
                    F.col("category") == "PREMIUM",
                    F.col("transformed_value") * F.lit(enrichment_config.get('premium_bonus_rate', 1.2))
                ).otherwise(F.col("transformed_value"))
            )
        
        return df
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """Add ETL metadata to records"""
        import socket
        
        return df.withColumn("etl_run_id", F.lit(self.run_id)) \
                 .withColumn("processed_at", F.current_timestamp()) \
                 .withColumn("processed_by", F.lit(socket.gethostname()))
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data against business rules
        
        Returns:
            Tuple of (is_valid, validation_errors)
        """
        self.logger.info("Starting data validation")
        errors = []
        
        validation_rules = self.config.validation_rules
        
        # Rule 1: ID is required
        null_id_count = df.filter(F.col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"Validation failed: {null_id_count} records with null ID")
        
        # Rule 2: Name is required
        null_name_count = df.filter(F.col("name").isNull() | (F.trim(F.col("name")) == "")).count()
        if null_name_count > 0:
            errors.append(f"Validation failed: {null_name_count} records with null/empty name")
        
        # Rule 3: Value must be positive
        min_value = validation_rules.get('min_value', 0)
        negative_value_count = df.filter(
            (F.col("value") < F.lit(min_value)) | 
            (F.col("transformed_value") < F.lit(min_value))
        ).count()
        if negative_value_count > 0:
            errors.append(f"Validation failed: {negative_value_count} records with value < {min_value}")
        
        # Rule 4: Priority must be between 1 and 5
        invalid_priority_count = df.filter(
            (F.col("priority") < 1) | (F.col("priority") > 5)
        ).count()
        if invalid_priority_count > 0:
            errors.append(f"Validation failed: {invalid_priority_count} records with invalid priority")
        
        # Rule 5: Category must not be empty
        empty_category_count = df.filter(
            F.col("category").isNull() | (F.trim(F.col("category")) == "")
        ).count()
        if empty_category_count > 0:
            errors.append(f"Validation failed: {empty_category_count} records with empty category")
        
        # Rule 6: Transformed value should be >= original value (except for TRIAL)
        invalid_transform_count = df.filter(
            (F.col("transformed_value") < F.col("value")) & 
            (F.col("category") != "TRIAL")
        ).count()
        if invalid_transform_count > 0:
            errors.append(f"Validation failed: {invalid_transform_count} records with invalid transformation")
        
        # Rule 7: Status must be valid
        valid_statuses = validation_rules.get('valid_statuses', 
            ['INVALID', 'HIGH_VALUE', 'MEDIUM_VALUE', 'LOW_VALUE', 'ACTIVE', 'TRANSFORMED'])
        invalid_status_count = df.filter(~F.col("status").isin(valid_statuses)).count()
        if invalid_status_count > 0:
            errors.append(f"Validation failed: {invalid_status_count} records with invalid status")
        
        # Rule 8: Max value threshold
        max_value = validation_rules.get('max_value', 1000000)
        excessive_value_count = df.filter(F.col("transformed_value") > F.lit(max_value)).count()
        if excessive_value_count > 0:
            errors.append(f"Validation failed: {excessive_value_count} records exceed max value {max_value}")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Validation passed successfully")
        else:
            self.logger.error(f"Validation failed with {len(errors)} errors")
            for error in errors:
                self.logger.error(error)
        
        return is_valid, errors


class ValidationRules:
    """Centralized validation rules"""
    
    @staticmethod
    def validate_required_fields(df: DataFrame, required_fields: List[str]) -> List[str]:
        """Validate required fields are not null"""
        errors = []
        for field in required_fields:
            null_count = df.filter(F.col(field).isNull()).count()
            if null_count > 0:
                errors.append(f"Field '{field}' has {null_count} null values")
        return errors
    
    @staticmethod
    def validate_value_range(df: DataFrame, field: str, min_val: float, max_val: float) -> List[str]:
        """Validate numeric field is within range"""
        errors = []
        out_of_range = df.filter(
            (F.col(field) < F.lit(min_val)) | (F.col(field) > F.lit(max_val))
        ).count()
        if out_of_range > 0:
            errors.append(f"Field '{field}' has {out_of_range} values outside range [{min_val}, {max_val}]")
        return errors
    
    @staticmethod
    def validate_enum_values(df: DataFrame, field: str, valid_values: List[str]) -> List[str]:
        """Validate field contains only valid enum values"""
        errors = []
        invalid_count = df.filter(~F.col(field).isin(valid_values)).count()
        if invalid_count > 0:
            errors.append(f"Field '{field}' has {invalid_count} invalid values. Valid values: {valid_values}")
        return errors


def create_transformer(spark: SparkSession, run_id: str, config_path: str = "config.yaml") -> DataTransformer:
    """Factory function to create transformer instance"""
    return DataTransformer(spark, run_id, config_path)