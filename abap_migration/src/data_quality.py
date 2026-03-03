"""
ETL Data Quality Module

Migrated from ABAP zcl_etl_data_quality
Performs comprehensive data quality checks and profiling.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, countDistinct, avg, stddev, min as spark_min, max as spark_max
from dataclasses import dataclass
from typing import List
import math

from src.logger import ETLLogger


@dataclass
class QualityCheck:
    """Data quality check result"""
    check_name: str
    check_type: str
    passed: bool
    failed_count: int
    message: str


@dataclass
class DataProfile:
    """Data profile statistics"""
    total_records: int
    null_count: int
    duplicate_count: int
    min_value: float
    max_value: float
    avg_value: float
    std_deviation: float
    unique_categories: int


class ETLDataQuality:
    """
    Data quality checker and profiler
    """

    def __init__(self, spark: SparkSession, run_id: str, config: dict):
        """
        Initialize data quality checker

        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = ETLLogger.get_instance()

    def perform_quality_checks(self, df: DataFrame) -> List[QualityCheck]:
        """
        Perform all quality checks

        Args:
            df: DataFrame to check

        Returns:
            List of QualityCheck results
        """
        self.logger.log_info(
            component="DATA_QUALITY",
            message="Starting data quality checks"
        )

        checks = []
        checks.append(self._check_completeness(df))
        checks.append(self._check_uniqueness(df))
        checks.append(self._check_validity(df))
        checks.append(self._check_consistency(df))

        passed = sum(1 for c in checks if c.passed)
        failed = len(checks) - passed

        self.logger.log_info(
            component="DATA_QUALITY",
            message=f"Quality checks complete: {passed} passed, {failed} failed"
        )

        return checks

    def _check_completeness(self, df: DataFrame) -> QualityCheck:
        """Check for missing/null values in critical fields"""
        null_count = df.filter(
            col("id").isNull() |
            col("name").isNull() |
            (col("name") == "") |
            col("value").isNull()
        ).count()

        passed = null_count == 0
        message = "All required fields are complete" if passed else f"{null_count} records with incomplete data"

        return QualityCheck(
            check_name="Completeness Check",
            check_type="COMPLETENESS",
            passed=passed,
            failed_count=null_count,
            message=message
        )

    def _check_uniqueness(self, df: DataFrame) -> QualityCheck:
        """Check for duplicate IDs"""
        total_count = df.count()
        unique_count = df.select("id").distinct().count()
        duplicate_count = total_count - unique_count

        passed = duplicate_count == 0
        message = "All IDs are unique" if passed else f"{duplicate_count} duplicate IDs found"

        return QualityCheck(
            check_name="Uniqueness Check",
            check_type="UNIQUENESS",
            passed=passed,
            failed_count=duplicate_count,
            message=message
        )

    def _check_validity(self, df: DataFrame) -> QualityCheck:
        """Check for invalid values"""
        invalid_count = df.filter(
            (col("value") < 0) |
            (col("transformed_value") < 0) |
            (col("priority") < 1) |
            (col("priority") > 5) |
            col("category").isNull() |
            (col("category") == "")
        ).count()

        passed = invalid_count == 0
        message = "All values are valid" if passed else f"{invalid_count} records with invalid values"

        return QualityCheck(
            check_name="Validity Check",
            check_type="VALIDITY",
            passed=passed,
            failed_count=invalid_count,
            message=message
        )

    def _check_consistency(self, df: DataFrame) -> QualityCheck:
        """Check for data inconsistencies"""
        # Check if transformed_value is consistent with value
        inconsistent_count = df.filter(
            (col("transformed_value") < col("value") * 0.5) |
            (col("transformed_value") > col("value") * 3)
        ).count()

        passed = inconsistent_count == 0
        message = "All values are consistent" if passed else f"{inconsistent_count} records with inconsistent values"

        return QualityCheck(
            check_name="Consistency Check",
            check_type="CONSISTENCY",
            passed=passed,
            failed_count=inconsistent_count,
            message=message
        )

    def profile_data(self, df: DataFrame) -> DataProfile:
        """
        Generate data profile with statistics

        Args:
            df: DataFrame to profile

        Returns:
            DataProfile with statistics
        """
        stats = df.agg(
            count("*").alias("total"),
            count(col("id")).alias("non_null"),
            countDistinct("id").alias("unique_ids"),
            spark_min("value").alias("min_val"),
            spark_max("value").alias("max_val"),
            avg("value").alias("avg_val"),
            stddev("value").alias("std_val"),
            countDistinct("category").alias("unique_cats")
        ).first()

        total = stats["total"]
        null_count = total - stats["non_null"]
        duplicate_count = total - stats["unique_ids"]

        return DataProfile(
            total_records=total,
            null_count=null_count,
            duplicate_count=duplicate_count,
            min_value=float(stats["min_val"]) if stats["min_val"] else 0.0,
            max_value=float(stats["max_val"]) if stats["max_val"] else 0.0,
            avg_value=float(stats["avg_val"]) if stats["avg_val"] else 0.0,
            std_deviation=float(stats["std_val"]) if stats["std_val"] else 0.0,
            unique_categories=stats["unique_cats"]
        )

    def detect_anomalies(self, df: DataFrame) -> List[str]:
        """
        Detect anomalies in data

        Args:
            df: DataFrame to check

        Returns:
            List of anomaly descriptions
        """
        anomalies = []

        # Calculate statistics
        profile = self.profile_data(df)

        # Check for outliers (values > 3 std deviations from mean)
        if profile.std_deviation > 0:
            upper_bound = profile.avg_value + (3 * profile.std_deviation)
            lower_bound = profile.avg_value - (3 * profile.std_deviation)

            outlier_count = df.filter(
                (col("value") > upper_bound) |
                (col("value") < lower_bound)
            ).count()

            if outlier_count > 0:
                anomalies.append(f"{outlier_count} statistical outliers detected")

        # Check for unexpected categories
        valid_categories = self.config.get("valid_categories", ["PREMIUM", "STANDARD", "BASIC", "VIP", "TRIAL"])
        invalid_cat_count = df.filter(~col("category").isin(valid_categories)).count()

        if invalid_cat_count > 0:
            anomalies.append(f"{invalid_cat_count} records with unexpected categories")

        return anomalies