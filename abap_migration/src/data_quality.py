"""
ETL Data Quality Management Module
Performs comprehensive data quality checks and profiling
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Dict, Tuple
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class QualityCheck:
    """Quality check result"""
    check_name: str
    check_type: str
    passed: bool
    failed_count: int
    message: str


@dataclass
class DataProfile:
    """Data profiling result"""
    total_records: int
    null_count: int
    duplicate_count: int
    min_value: float
    max_value: float
    avg_value: float
    std_deviation: float
    unique_categories: int


class DataQualityManager:
    """Manage data quality checks and profiling"""
    
    def __init__(self, spark: SparkSession, run_id: str, config: dict):
        self.spark = spark
        self.run_id = run_id
        self.config = config
        
    def perform_quality_checks(self, df: DataFrame) -> List[QualityCheck]:
        """Perform all quality checks on data"""
        logger.info("Starting data quality checks...")
        
        checks = []
        checks.append(self.check_completeness(df))
        checks.append(self.check_uniqueness(df))
        checks.append(self.check_validity(df))
        checks.append(self.check_consistency(df))
        
        passed = sum(1 for c in checks if c.passed)
        failed = len(checks) - passed
        
        logger.info(f"Quality checks complete: {passed} passed, {failed} failed")
        
        return checks
        
    def check_completeness(self, df: DataFrame) -> QualityCheck:
        """Check for null/empty values in critical fields"""
        logger.info("Running completeness check...")
        
        critical_fields = ['id', 'name', 'value']
        
        null_condition = None
        for field in critical_fields:
            field_null = F.col(field).isNull() | (F.col(field) == '')
            null_condition = field_null if null_condition is None else null_condition | field_null
        
        null_count = df.filter(null_condition).count()
        
        passed = null_count == 0
        message = "All required fields are complete" if passed else f"{null_count} records with incomplete data"
        
        return QualityCheck(
            check_name="Completeness Check",
            check_type="COMPLETENESS",
            passed=passed,
            failed_count=null_count,
            message=message
        )
        
    def check_uniqueness(self, df: DataFrame) -> QualityCheck:
        """Check for duplicate IDs"""
        logger.info("Running uniqueness check...")
        
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
        
    def check_validity(self, df: DataFrame) -> QualityCheck:
        """Check for invalid values"""
        logger.info("Running validity check...")
        
        invalid_condition = (
            (F.col("value") < 0) |
            (F.col("transformed_value") < 0) |
            (F.col("priority") < 1) |
            (F.col("priority") > 5) |
            (F.col("category").isNull())
        )
        
        invalid_count = df.filter(invalid_condition).count()
        
        passed = invalid_count == 0
        message = "All values are valid" if passed else f"{invalid_count} records with invalid values"
        
        return QualityCheck(
            check_name="Validity Check",
            check_type="VALIDITY",
            passed=passed,
            failed_count=invalid_count,
            message=message
        )
        
    def check_consistency(self, df: DataFrame) -> QualityCheck:
        """Check for data consistency issues"""
        logger.info("Running consistency check...")
        
        # Check if transformed_value is consistent with value
        inconsistent_condition = (
            F.col("transformed_value") < F.col("value") * 0.5
        ) | (
            F.col("transformed_value") > F.col("value") * 3.0
        )
        
        inconsistent_count = df.filter(inconsistent_condition).count()
        
        passed = inconsistent_count == 0
        message = "Data is consistent" if passed else f"{inconsistent_count} records with consistency issues"
        
        return QualityCheck(
            check_name="Consistency Check",
            check_type="CONSISTENCY",
            passed=passed,
            failed_count=inconsistent_count,
            message=message
        )
        
    def profile_data(self, df: DataFrame) -> DataProfile:
        """Generate data profile statistics"""
        logger.info("Generating data profile...")
        
        # Calculate statistics
        stats = df.agg(
            F.count("*").alias("total_records"),
            F.sum(F.when(F.col("value").isNull(), 1).otherwise(0)).alias("null_count"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
            F.avg("value").alias("avg_value"),
            F.stddev("value").alias("std_deviation"),
            F.countDistinct("category").alias("unique_categories")
        ).collect()[0]
        
        # Check duplicates
        total_count = df.count()
        unique_count = df.select("id").distinct().count()
        duplicate_count = total_count - unique_count
        
        profile = DataProfile(
            total_records=stats["total_records"],
            null_count=stats["null_count"],
            duplicate_count=duplicate_count,
            min_value=float(stats["min_value"]) if stats["min_value"] else 0.0,
            max_value=float(stats["max_value"]) if stats["max_value"] else 0.0,
            avg_value=float(stats["avg_value"]) if stats["avg_value"] else 0.0,
            std_deviation=float(stats["std_deviation"]) if stats["std_deviation"] else 0.0,
            unique_categories=stats["unique_categories"]
        )
        
        logger.info(f"Data profile: {profile.total_records} records, {profile.duplicate_count} duplicates")
        
        return profile
        
    def detect_anomalies(self, df: DataFrame) -> List[str]:
        """Detect anomalies in data"""
        logger.info("Detecting anomalies...")
        
        anomalies = []
        
        # Calculate statistics for anomaly detection
        stats = df.agg(
            F.avg("value").alias("mean"),
            F.stddev("value").alias("std")
        ).collect()[0]
        
        mean = float(stats["mean"])
        std = float(stats["std"])
        
        # Detect outliers (values > 3 standard deviations from mean)
        threshold = 3.0
        outlier_condition = (
            (F.col("value") > mean + threshold * std) |
            (F.col("value") < mean - threshold * std)
        )
        
        outlier_count = df.filter(outlier_condition).count()
        
        if outlier_count > 0:
            anomalies.append(f"Found {outlier_count} outliers (>3σ from mean)")
        
        # Check for suspicious patterns
        category_counts = df.groupBy("category").count().collect()
        min_count = min(row["count"] for row in category_counts)
        max_count = max(row["count"] for row in category_counts)
        
        if max_count > min_count * 10:
            anomalies.append(f"Imbalanced categories: max={max_count}, min={min_count}")
        
        logger.info(f"Detected {len(anomalies)} anomalies")
        
        return anomalies
        
    def generate_quality_report(self, df: DataFrame) -> Dict:
        """Generate comprehensive quality report"""
        logger.info("Generating quality report...")
        
        checks = self.perform_quality_checks(df)
        profile = self.profile_data(df)
        anomalies = self.detect_anomalies(df)
        
        report = {
            'run_id': self.run_id,
            'checks': [
                {
                    'name': c.check_name,
                    'type': c.check_type,
                    'passed': c.passed,
                    'failed_count': c.failed_count,
                    'message': c.message
                }
                for c in checks
            ],
            'profile': {
                'total_records': profile.total_records,
                'null_count': profile.null_count,
                'duplicate_count': profile.duplicate_count,
                'min_value': profile.min_value,
                'max_value': profile.max_value,
                'avg_value': profile.avg_value,
                'std_deviation': profile.std_deviation,
                'unique_categories': profile.unique_categories
            },
            'anomalies': anomalies
        }
        
        logger.info("Quality report generated successfully")
        
        return report