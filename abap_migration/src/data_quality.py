"""
Data quality module for ETL pipeline.
Performs comprehensive quality checks and data profiling.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct, avg, stddev, min as _min, max as _max
from typing import List, Dict, Any
from dataclasses import dataclass

from src.logger import ETLLogger


@dataclass
class QualityCheck:
    """Container for quality check results."""
    check_name: str
    check_type: str
    passed: bool
    failed_count: int
    message: str


@dataclass
class DataProfile:
    """Container for data profiling results."""
    total_records: int
    null_count: int
    duplicate_count: int
    min_value: float
    max_value: float
    avg_value: float
    std_deviation: float
    unique_categories: int


class DataQuality:
    """Handles data quality checks and profiling."""
    
    def __init__(self, run_id: str):
        """
        Initialize data quality checker.
        
        Args:
            run_id: Unique run identifier
        """
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
    
    def perform_quality_checks(self, df: DataFrame) -> List[QualityCheck]:
        """
        Perform all quality checks on data.
        
        Args:
            df: DataFrame to check
            
        Returns:
            List of quality check results
        """
        self.logger.log_info(
            component='DATA_QUALITY',
            message='Starting data quality checks'
        )
        
        checks = [
            self._check_completeness(df),
            self._check_uniqueness(df),
            self._check_validity(df),
            self._check_consistency(df)
        ]
        
        passed_count = sum(1 for check in checks if check.passed)
        failed_count = len(checks) - passed_count
        
        self.logger.log_info(
            component='DATA_QUALITY',
            message=f'Quality checks complete: {passed_count} passed, {failed_count} failed'
        )
        
        return checks
    
    def _check_completeness(self, df: DataFrame) -> QualityCheck:
        """Check for null/empty values in critical fields."""
        null_count = df.filter(
            col("id").isNull() |
            col("name").isNull() |
            (col("name") == "") |
            col("value").isNull()
        ).count()
        
        passed = null_count == 0
        message = "All required fields are complete" if passed else \
                  f"{null_count} records with incomplete data"
        
        return QualityCheck(
            check_name="Completeness Check",
            check_type="COMPLETENESS",
            passed=passed,
            failed_count=null_count,
            message=message
        )
    
    def _check_uniqueness(self, df: DataFrame) -> QualityCheck:
        """Check for duplicate IDs."""
        total_count = df.count()
        unique_count = df.select("id").distinct().count()
        duplicate_count = total_count - unique_count
        
        passed = duplicate_count == 0
        message = "All IDs are unique" if passed else \
                  f"{duplicate_count} duplicate IDs found"
        
        return QualityCheck(
            check_name="Uniqueness Check",
            check_type="UNIQUENESS",
            passed=passed,
            failed_count=duplicate_count,
            message=message
        )
    
    def _check_validity(self, df: DataFrame) -> QualityCheck:
        """Check for invalid values."""
        invalid_count = df.filter(
            (col("value") < 0) |
            (col("transformed_value") < 0) |
            (col("priority") < 1) |
            (col("priority") > 5) |
            col("category").isNull() |
            (col("category") == "")
        ).count()
        
        passed = invalid_count == 0
        message = "All values are valid" if passed else \
                  f"{invalid_count} records with invalid values"
        
        return QualityCheck(
            check_name="Validity Check",
            check_type="VALIDITY",
            passed=passed,
            failed_count=invalid_count,
            message=message
        )
    
    def _check_consistency(self, df: DataFrame) -> QualityCheck:
        """Check for data consistency issues."""
        inconsistent_count = df.filter(
            col("transformed_value") < col("value")
        ).count()
        
        passed = inconsistent_count == 0
        message = "Data is consistent" if passed else \
                  f"{inconsistent_count} records with inconsistencies"
        
        return QualityCheck(
            check_name="Consistency Check",
            check_type="CONSISTENCY",
            passed=passed,
            failed_count=inconsistent_count,
            message=message
        )
    
    def profile_data(self, df: DataFrame) -> DataProfile:
        """
        Generate data profile statistics.
        
        Args:
            df: DataFrame to profile
            
        Returns:
            DataProfile with statistics
        """
        stats = df.agg(
            count("*").alias("total"),
            count(col("value").isNull()).alias("nulls"),
            _min("value").alias("min_val"),
            _max("value").alias("max_val"),
            avg("value").alias("avg_val"),
            stddev("value").alias("std_val"),
            countDistinct("category").alias("unique_cat")
        ).first()
        
        total_count = stats["total"]
        unique_count = df.select("id").distinct().count()
        duplicate_count = total_count - unique_count
        
        return DataProfile(
            total_records=total_count,
            null_count=stats["nulls"],
            duplicate_count=duplicate_count,
            min_value=stats["min_val"] or 0.0,
            max_value=stats["max_val"] or 0.0,
            avg_value=stats["avg_val"] or 0.0,
            std_deviation=stats["std_val"] or 0.0,
            unique_categories=stats["unique_cat"]
        )
    
    def detect_anomalies(self, df: DataFrame) -> List[str]:
        """
        Detect anomalies in data.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            List of anomaly descriptions
        """
        anomalies = []
        
        # Check for extreme values (beyond 3 standard deviations)
        stats = df.agg(
            avg("value").alias("mean"),
            stddev("value").alias("std")
        ).first()
        
        mean_val = stats["mean"] or 0
        std_val = stats["std"] or 1
        
        extreme_count = df.filter(
            (col("value") > mean_val + 3 * std_val) |
            (col("value") < mean_val - 3 * std_val)
        ).count()
        
        if extreme_count > 0:
            anomalies.append(f"Found {extreme_count} extreme values (>3 std deviations)")
        
        return anomalies