===FILE: src/quality.py===
"""
Data Quality Validation Module for PySpark ETL Pipeline

Provides comprehensive data quality checks including:
- Completeness validation
- Uniqueness checks
- Value validity rules
- Consistency validation
- Data profiling statistics
- Record count thresholds
- Anomaly detection
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, min as spark_min, max as spark_max,
    avg, stddev, when, isnan, isnull, lit, sum as spark_sum
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import logging


class QualityCheckResult:
    """Represents result of a single quality check"""
    
    def __init__(self, check_name: str, check_type: str, passed: bool, 
                 failed_count: int = 0, message: str = ""):
        self.check_name = check_name
        self.check_type = check_type
        self.passed = passed
        self.failed_count = failed_count
        self.message = message
        self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict:
        return {
            "check_name": self.check_name,
            "check_type": self.check_type,
            "passed": self.passed,
            "failed_count": self.failed_count,
            "message": self.message,
            "timestamp": self.timestamp.isoformat()
        }


class DataProfile:
    """Data profiling statistics container"""
    
    def __init__(self):
        self.total_records = 0
        self.null_count = 0
        self.duplicate_count = 0
        self.min_value = 0.0
        self.max_value = 0.0
        self.avg_value = 0.0
        self.std_deviation = 0.0
        self.unique_categories = 0
        self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict:
        return {
            "total_records": self.total_records,
            "null_count": self.null_count,
            "duplicate_count": self.duplicate_count,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "avg_value": self.avg_value,
            "std_deviation": self.std_deviation,
            "unique_categories": self.unique_categories,
            "timestamp": self.timestamp.isoformat()
        }


class DataQualityValidator:
    """Main data quality validation engine"""
    
    def __init__(self, spark: SparkSession, run_id: str, config: Dict):
        """
        Initialize validator
        
        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
            config: Configuration dictionary with thresholds and rules
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Load quality thresholds from config
        self.thresholds = config.get("quality_thresholds", {})
        self.null_threshold = self.thresholds.get("max_null_percent", 5.0)
        self.duplicate_threshold = self.thresholds.get("max_duplicate_percent", 1.0)
        self.record_count_min = self.thresholds.get("min_record_count", 1)
        self.record_count_max = self.thresholds.get("max_record_count", 10000000)
        
        self.logger.info(f"DataQualityValidator initialized for run_id: {run_id}")
    
    def perform_quality_checks(self, df: DataFrame) -> List[QualityCheckResult]:
        """
        Execute all data quality checks
        
        Args:
            df: Input DataFrame to validate
            
        Returns:
            List of QualityCheckResult objects
        """
        self.logger.info("Starting comprehensive data quality checks")
        
        results = []
        
        # Execute all checks
        results.append(self.check_completeness(df))
        results.append(self.check_uniqueness(df))
        results.append(self.check_validity(df))
        results.append(self.check_consistency(df))
        results.append(self.check_record_count(df))
        
        # Summary statistics
        passed = sum(1 for r in results if r.passed)
        failed = len(results) - passed
        
        self.logger.info(f"Quality checks complete: {passed} passed, {failed} failed")
        
        return results
    
    def check_completeness(self, df: DataFrame) -> QualityCheckResult:
        """
        Validate data completeness (no null/empty values in critical fields)
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheckResult
        """
        self.logger.info("Executing completeness check")
        
        required_fields = self.config.get("required_fields", 
                                         ["id", "name", "value"])
        
        total_count = df.count()
        
        # Check for nulls in required fields
        null_condition = None
        for field in required_fields:
            if field in df.columns:
                field_condition = col(field).isNull() | (col(field) == "")
                null_condition = field_condition if null_condition is None else null_condition | field_condition
        
        if null_condition is None:
            return QualityCheckResult(
                check_name="Completeness Check",
                check_type="COMPLETENESS",
                passed=True,
                failed_count=0,
                message="No required fields defined or found"
            )
        
        null_count = df.filter(null_condition).count()
        null_percent = (null_count / total_count * 100) if total_count > 0 else 0
        
        passed = null_percent <= self.null_threshold
        
        message = (f"Null percentage: {null_percent:.2f}% "
                  f"(threshold: {self.null_threshold}%)")
        
        return QualityCheckResult(
            check_name="Completeness Check",
            check_type="COMPLETENESS",
            passed=passed,
            failed_count=null_count,
            message=message
        )
    
    def check_uniqueness(self, df: DataFrame) -> QualityCheckResult:
        """
        Validate uniqueness of primary key fields
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheckResult
        """
        self.logger.info("Executing uniqueness check")
        
        id_column = self.config.get("id_column", "id")
        
        if id_column not in df.columns:
            return QualityCheckResult(
                check_name="Uniqueness Check",
                check_type="UNIQUENESS",
                passed=True,
                failed_count=0,
                message=f"ID column '{id_column}' not found in DataFrame"
            )
        
        total_count = df.count()
        unique_count = df.select(id_column).distinct().count()
        
        duplicate_count = total_count - unique_count
        duplicate_percent = (duplicate_count / total_count * 100) if total_count > 0 else 0
        
        passed = duplicate_percent <= self.duplicate_threshold
        
        message = (f"Duplicate percentage: {duplicate_percent:.2f}% "
                  f"(threshold: {self.duplicate_threshold}%)")
        
        return QualityCheckResult(
            check_name="Uniqueness Check",
            check_type="UNIQUENESS",
            passed=passed,
            failed_count=duplicate_count,
            message=message
        )
    
    def check_validity(self, df: DataFrame) -> QualityCheckResult:
        """
        Validate value ranges and data types
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheckResult
        """
        self.logger.info("Executing validity check")
        
        validation_rules = self.config.get("validation_rules", {})
        
        invalid_condition = lit(False)
        
        # Check numeric value ranges
        if "value" in df.columns:
            min_value = validation_rules.get("min_value", 0)
            max_value = validation_rules.get("max_value", float('inf'))
            invalid_condition = invalid_condition | (col("value") < min_value) | (col("value") > max_value)
        
        if "transformed_value" in df.columns:
            invalid_condition = invalid_condition | (col("transformed_value") < 0)
        
        # Check priority range
        if "priority" in df.columns:
            invalid_condition = invalid_condition | (col("priority") < 1) | (col("priority") > 5)
        
        # Check category not empty
        if "category" in df.columns:
            invalid_condition = invalid_condition | col("category").isNull() | (col("category") == "")
        
        invalid_count = df.filter(invalid_condition).count()
        total_count = df.count()
        invalid_percent = (invalid_count / total_count * 100) if total_count > 0 else 0
        
        max_invalid_percent = validation_rules.get("max_invalid_percent", 5.0)
        passed = invalid_percent <= max_invalid_percent
        
        message = (f"Invalid records: {invalid_count} ({invalid_percent:.2f}%) "
                  f"(threshold: {max_invalid_percent}%)")
        
        return QualityCheckResult(
            check_name="Validity Check",
            check_type="VALIDITY",
            passed=passed,
            failed_count=invalid_count,
            message=message
        )
    
    def check_consistency(self, df: DataFrame) -> QualityCheckResult:
        """
        Validate data consistency and business rules
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheckResult
        """
        self.logger.info("Executing consistency check")
        
        inconsistency_condition = lit(False)
        
        # Check transformed_value is consistent with value
        if "value" in df.columns and "transformed_value" in df.columns:
            # Transformed value should be >= original value (based on business rules)
            inconsistency_condition = inconsistency_condition | (col("transformed_value") < col("value") * 0.5)
        
        # Check status consistency with values
        if "status" in df.columns and "transformed_value" in df.columns:
            # High value status should have high transformed values
            inconsistency_condition = inconsistency_condition | (
                (col("status") == "HIGH_VALUE") & (col("transformed_value") < 750)
            )
        
        inconsistent_count = df.filter(inconsistency_condition).count()
        total_count = df.count()
        inconsistent_percent = (inconsistent_count / total_count * 100) if total_count > 0 else 0
        
        max_inconsistent_percent = self.config.get("consistency_threshold", 2.0)
        passed = inconsistent_percent <= max_inconsistent_percent
        
        message = (f"Inconsistent records: {inconsistent_count} ({inconsistent_percent:.2f}%) "
                  f"(threshold: {max_inconsistent_percent}%)")
        
        return QualityCheckResult(
            check_name="Consistency Check",
            check_type="CONSISTENCY",
            passed=passed,
            failed_count=inconsistent_count,
            message=message
        )
    
    def check_record_count(self, df: DataFrame) -> QualityCheckResult:
        """
        Validate record count is within expected thresholds
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheckResult
        """
        self.logger.info("Executing record count check")
        
        record_count = df.count()
        
        passed = self.record_count_min <= record_count <= self.record_count_max
        
        if passed:
            message = f"Record count {record_count} within threshold [{self.record_count_min}, {self.record_count_max}]"
        else:
            message = f"Record count {record_count} outside threshold [{self.record_count_min}, {self.record_count_max}]"
        
        return QualityCheckResult(
            check_name="Record Count Check",
            check_type="RECORD_COUNT",
            passed=passed,
            failed_count=0 if passed else 1,
            message=message
        )
    
    def profile_data(self, df: DataFrame) -> DataProfile:
        """
        Generate comprehensive data profiling statistics
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataProfile object with statistics
        """
        self.logger.info("Generating data profile statistics")
        
        profile = DataProfile()
        
        # Basic counts
        profile.total_records = df.count()
        
        # Null count across all columns
        null_counts = []
        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            null_counts.append(null_count)
        profile.null_count = sum(null_counts)
        
        # Duplicate count
        id_column = self.config.get("id_column", "id")
        if id_column in df.columns:
            unique_count = df.select(id_column).distinct().count()
            profile.duplicate_count = profile.total_records - unique_count
        
        # Numeric statistics
        if "value" in df.columns:
            stats = df.select(
                spark_min("value").alias("min"),
                spark_max("value").alias("max"),
                avg("value").alias("avg"),
                stddev("value").alias("stddev")
            ).first()
            
            profile.min_value = float(stats["min"]) if stats["min"] else 0.0
            profile.max_value = float(stats["max"]) if stats["max"] else 0.0
            profile.avg_value = float(stats["avg"]) if stats["avg"] else 0.0
            profile.std_deviation = float(stats["stddev"]) if stats["stddev"] else 0.0
        
        # Category statistics
        if "category" in df.columns:
            profile.unique_categories = df.select("category").distinct().count()
        
        self.logger.info(f"Data profiling complete: {profile.total_records} total records")
        
        return profile
    
    def detect_anomalies(self, df: DataFrame) -> List[str]:
        """
        Detect anomalies in the data using statistical methods
        
        Args:
            df: Input DataFrame
            
        Returns:
            List of anomaly descriptions
        """
        self.logger.info("Detecting data anomalies")
        
        anomalies = []
        
        # Statistical outlier detection for numeric columns
        if "value" in df.columns:
            stats = df.select(
                avg("value").alias("mean"),
                stddev("value").alias("stddev")
            ).first()
            
            mean_val = float(stats["mean"]) if stats["mean"] else 0.0
            std_val = float(stats["stddev"]) if stats["stddev"] else 0.0
            
            # Detect outliers (> 3 standard deviations)
            if std_val > 0:
                outlier_count = df.filter(
                    (col("value") < mean_val - 3 * std_val) |
                    (col("value") > mean_val + 3 * std_val)
                ).count()
                
                if outlier_count > 0:
                    anomalies.append(
                        f"Found {outlier_count} statistical outliers in 'value' column "
                        f"(mean: {mean_val:.2f}, stddev: {std_val:.2f})"
                    )
        
        # Check for unexpected null patterns
        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            null_percent = (null_count / df.count() * 100) if df.count() > 0 else 0
            
            if null_percent > 50:
                anomalies.append(
                    f"Column '{column}' has unusually high null percentage: {null_percent:.2f}%"
                )
        
        # Check for skewed category distributions
        if "category" in df.columns:
            category_counts = df.groupBy("category").count()
            total = df.count()
            
            skewed_categories = category_counts.filter(
                (col("count") / total) > 0.8
            ).collect()
            
            if skewed_categories:
                for row in skewed_categories:
                    pct = (row["count"] / total * 100)
                    anomalies.append(
                        f"Category '{row['category']}' is highly skewed: {pct:.2f}% of records"
                    )
        
        self.logger.info(f"Anomaly detection complete: {len(anomalies)} anomalies found")
        
        return anomalies
    
    def generate_quality_report(self, df: DataFrame) -> Dict:
        """
        Generate comprehensive quality report
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary containing full quality report
        """
        self.logger.info("Generating comprehensive quality report")
        
        checks = self.perform_quality_checks(df)
        profile = self.profile_data(df)
        anomalies = self.detect_anomalies(df)
        
        report = {
            "run_id": self.run_id,
            "timestamp": datetime.now().isoformat(),
            "quality_checks": [check.to_dict() for check in checks],
            "data_profile": profile.to_dict(),
            "anomalies": anomalies,
            "overall_status": all(check.passed for check in checks),
            "checks_passed": sum(1 for check in checks if check.passed),
            "checks_failed": sum(1 for check in checks if not check.passed)
        }
        
        self.logger.info(
            f"Quality report generated: "
            f"{report['checks_passed']}/{len(checks)} checks passed"
        )
        
        return report


def validate_data_quality(spark: SparkSession, df: DataFrame, 
                          run_id: str, config: Dict) -> Tuple[bool, Dict]:
    """
    Convenience function to validate data quality
    
    Args:
        spark: SparkSession instance
        df: DataFrame to validate
        run_id: Run identifier
        config: Configuration dictionary
        
    Returns:
        Tuple of (is_valid, quality_report)
    """
    validator = DataQualityValidator(spark, run_id, config)
    report = validator.generate_quality_report(df)
    
    is_valid = report["overall_status"]
    
    return is_valid, report


===FILE: src/extract.py===
"""
Data extraction module for PySpark ETL pipeline
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from datetime import datetime
import logging
from typing import Dict, Optional


class DataExtractor:
    """Extract data from various sources"""
    
    def __init__(self, spark: SparkSession, run_id: str, config: Dict):
        """
        Initialize extractor
        
        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        self.source_type = config.get("source_type", "parquet")
        self.source_path = config.get("source_path", "data/source")
        
        self.logger.info(f"DataExtractor initialized for run_id: {run_id}")
    
    def get_source_schema(self) -> StructType:
        """Define source data schema"""
        return StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("value", DoubleType(), False),
            StructField("status", StringType(), True),
            StructField("category", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", StringType(), True),
            StructField("changed_at", TimestampType(), True),
            StructField("changed_by", StringType(), True)
        ])
    
    def extract_data(self, filter_condition: Optional[str] = None, 
                     max_records: int = 0) -> DataFrame:
        """
        Extract data from source
        
        Args:
            filter_condition: Optional SQL filter condition
            max_records: Maximum records to extract (0 for unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.info(f"Starting extraction - Source type: {self.source_type}")
        
        df = self._read_from_source()
        
        if filter_condition:
            df = df.filter(filter_condition)
            self.logger.info(f"Applied filter: {filter_condition}")
        
        if max_records > 0:
            df = df.limit(max_records)
            self.logger.info(f"Limited to {max_records} records")
        
        record_count = df.count()
        self.logger.info(f"Extracted {record_count} records")
        
        return df
    
    def _read_from_source(self) -> DataFrame:
        """Read data from configured source"""
        if self.source_type == "parquet":
            return self.spark.read.schema(self.get_source_schema()).parquet(self.source_path)
        elif self.source_type == "csv":
            return self.spark.read.schema(self.get_source_schema()).option("header", "true").csv(self.source_path)
        elif self.source_type == "jdbc":
            return self._extract_from_database()
        else:
            raise ValueError(f"Unsupported source type: {self.source_type}")
    
    def _extract_from_database(self) -> DataFrame:
        """Extract from JDBC database"""
        jdbc_config = self.config.get("jdbc", {})
        
        return self.spark.read.format("jdbc") \
            .option("url", jdbc_config.get("url")) \
            .option("dbtable", jdbc_config.get("table")) \
            .option("user", jdbc_config.get("user")) \
            .option("password", jdbc_config.get("password")) \
            .option("driver", jdbc_config.get("driver", "org.postgresql.Driver")) \
            .load()


===FILE: src/transform.py===
"""
Data transformation module for PySpark ETL pipeline
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, upper, trim, when, lit, current_timestamp, 
    expr, round as spark_round, regexp_replace
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
import logging
from typing import Dict, List, Tuple


class DataTransformer:
    """Transform and enrich data"""
    
    def __init__(self, spark: SparkSession, run_id: str, config: Dict):
        """
        Initialize transformer
        
        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Load business rules from config
        self.premium_multiplier = float(config.get("business_rules", {}).get("premium_multiplier", 1.5))
        self.standard_multiplier = float(config.get("business_rules", {}).get("standard_multiplier", 1.2))
        
        self.logger.info(f"DataTransformer initialized for run_id: {run_id}")
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply all transformations to the data
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting data transformation")
        
        # Step 1: Basic transformations
        df_transformed = self._apply_basic_transformations(df)
        
        # Step 2: Calculate derived values
        df_transformed = self._calculate_derived_values(df_transformed)
        
        # Step 3: Apply business rules
        df_transformed = self._apply_business_rules(df_transformed)
        
        # Step 4: Enrich data
        df_transformed = self._enrich_data(df_transformed)
        
        # Step 5: Add ETL metadata
        df_transformed = self._add_etl_metadata(df_transformed)
        
        record_count = df_transformed.count()
        self.logger.info(f"Transformation complete: {record_count} records")
        
        return df_transformed
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic data cleansing transformations"""
        self.logger.info("Applying basic transformations")
        
        return df.withColumn("name", upper(trim(col("name")))) \
                 .withColumn("name", regexp_replace(col("name"), "\\s+", " ")) \
                 .withColumn("status", upper(trim(col("status")))) \
                 .withColumn("category", when(col("category").isNull(), lit("UNCATEGORIZED"))
                            .otherwise(upper(trim(col("category")))))
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """Calculate transformed values based on category"""
        self.logger.info("Calculating derived values")
        
        return df.withColumn("transformed_value",
            when(col("category") == "PREMIUM", col("value") * self.premium_multiplier)
            .when(col("category") == "STANDARD", col("value") * self.standard_multiplier)
            .when(col("category") == "VIP", col("value") * 2.0)
            .otherwise(col("value"))
        ).withColumn("transformed_value", spark_round(col("transformed_value"), 2))
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules and set statuses"""
        self.logger.info("Applying business rules")
        
        # Rule 1: Set status based on transformed value
        df = df.withColumn("status",
            when(col("value").isNull(), lit("INVALID"))
            .when(col("transformed_value") >= 750, lit("HIGH_VALUE"))
            .when(col("transformed_value") >= 300, lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )
        
        # Rule 2: Calculate priority
        df = df.withColumn("priority",
            when(col("transformed_value") >= 1000, lit(1))
            .when(col("transformed_value") >= 750, lit(2))
            .when(col("transformed_value") >= 300, lit(3))
            .when(col("transformed_value") >= 100, lit(4))
            .otherwise(lit(5))
        )
        
        # Rule 3: Override priority for high value items
        df = df.withColumn("priority",
            when(col("transformed_value") >= 1000, lit(1))
            .otherwise(col("priority"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional information"""
        self.logger.info("Enriching data")
        
        # Add enrichment based on category
        df = df.withColumn("transformed_value",
            when(col("category") == "PREMIUM", col("transformed_value") * 1.2)
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def _add_etl_metadata(self, df: DataFrame) -> DataFrame:
        """Add ETL metadata columns"""
        return df.withColumn("etl_run_id", lit(self.run_id)) \
                 .withColumn("processed_at", current_timestamp()) \
                 .withColumn("processed_by", lit("PYSPARK_ETL"))
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        self.logger.info("Validating transformed data")
        
        errors = []
        
        # Check for required fields
        required_fields = ["id", "name", "value", "transformed_value"]
        for field in required_fields:
            if field not in df.columns:
                errors.append(f"Missing required field: {field}")
        
        if errors:
            return False, errors
        
        # Check for null values in critical fields
        null_count = df.filter(
            col("id").isNull() | 
            col("name").isNull() | 
            col("value").isNull()
        ).count()
        
        if null_count > 0:
            errors.append(f"{null_count} records with null critical fields")
        
        # Check for invalid values
        invalid_count = df.filter(
            (col("value") < 0) | 
            (col("transformed_value") < 0) |
            (col("priority") < 1) | 
            (col("priority") > 5)
        ).count()
        
        if invalid_count > 0:
            errors.append(f"{invalid_count} records with invalid values")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.info("Data validation passed")
        else:
            self.logger.warning(f"Data validation failed: {len(errors)} errors")
        
        return is_valid, errors


===FILE: src/load.py===
"""
Data loading module for PySpark ETL pipeline
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
import logging
from typing import Dict


class LoadResult:
    """Container for load operation results"""
    
    def __init__(self):
        self.success_count = 0
        self.error_count = 0
        self.total_count = 0
        self.errors = []
    
    def to_dict(self) -> Dict:
        return {
            "success_count": self.success_count,
            "error_count": self.error_count,
            "total_count": self.total_count,
            "errors": self.errors
        }


class DataLoader:
    """Load data to target destination"""
    
    def __init__(self, spark: SparkSession, run_id: str, config: Dict):
        """
        Initialize loader
        
        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        self.target_type = config.get("target_type", "parquet")
        self.target_path = config.get("target_path", "data/target")
        self.batch_size = config.get("batch_size", 1000