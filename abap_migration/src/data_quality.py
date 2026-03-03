"""
Data quality module providing comprehensive quality checks.
"""

from typing import List, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct, avg, stddev, min as spark_min, max as spark_max
import logging

logger = logging.getLogger(__name__)


class QualityCheck:
    """Represents a single quality check result."""
    
    def __init__(self, name: str, check_type: str, passed: bool, 
                 failed_count: int, message: str):
        self.name = name
        self.check_type = check_type
        self.passed = passed
        self.failed_count = failed_count
        self.message = message


class DataProfile:
    """Statistical profile of dataset."""
    
    def __init__(self):
        self.total_records = 0
        self.null_count = 0
        self.duplicate_count = 0
        self.min_value = 0.0
        self.max_value = 0.0
        self.avg_value = 0.0
        self.std_deviation = 0.0
        self.unique_categories = 0


class DataQualityChecker:
    """Performs comprehensive data quality checks."""
    
    def __init__(self, run_id: str, config: dict):
        """
        Initialize quality checker.
        
        Args:
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.run_id = run_id
        self.config = config
        logger.info(f"Data quality checker initialized - Run ID: {run_id}")
    
    def perform_quality_checks(self, df: DataFrame) -> List[QualityCheck]:
        """
        Perform all quality checks.
        
        Args:
            df: DataFrame to check
            
        Returns:
            List of QualityCheck results
        """
        logger.info("Starting data quality checks")
        
        checks = [
            self._check_completeness(df),
            self._check_uniqueness(df),
            self._check_validity(df),
            self._check_consistency(df)
        ]
        
        passed = sum(1 for c in checks if c.passed)
        failed = len(checks) - passed
        
        logger.info(f"Quality checks complete: {passed} passed, {failed} failed")
        
        return checks
    
    def _check_completeness(self, df: DataFrame) -> QualityCheck:
        """Check for null/empty values in critical fields."""
        logger.info("Running completeness check")
        
        null_count = df.filter(
            col("id").isNull() | 
            col("name").isNull() | 
            (col("name") == "") |
            col("value").isNull()
        ).count()
        
        passed = null_count == 0
        message = ("All required fields are complete" if passed 
                  else f"{null_count} records with incomplete data")
        
        return QualityCheck(
            name="Completeness Check",
            check_type="COMPLETENESS",
            passed=passed,
            failed_count=null_count,
            message=message
        )
    
    def _check_uniqueness(self, df: DataFrame) -> QualityCheck:
        """Check for duplicate IDs."""
        logger.info("Running uniqueness check")
        
        total_count = df.count()
        unique_count = df.select("id").distinct().count()
        duplicate_count = total_count - unique_count
        
        passed = duplicate_count == 0
        message = ("All IDs are unique" if passed 
                  else f"{duplicate_count} duplicate IDs found")
        
        return QualityCheck(
            name="Uniqueness Check",
            check_type="UNIQUENESS",
            passed=passed,
            failed_count=duplicate_count,
            message=message
        )
    
    def _check_validity(self, df: DataFrame) -> QualityCheck:
        """Check for invalid values."""
        logger.info("Running validity check")
        
        invalid_count = df.filter(
            (col("value") < 0) | 
            (col("transformed_value") < 0) |
            (col("priority") < 1) | 
            (col("priority") > 5) |
            col("category").isNull() |
            (col("category") == "")
        ).count()
        
        passed = invalid_count == 0
        message = ("All values are valid" if passed 
                  else f"{invalid_count} records with invalid values")
        
        return QualityCheck(
            name="Validity Check",
            check_type="VALIDITY",
            passed=passed,
            failed_count=invalid_count,
            message=message
        )
    
    def _check_consistency(self, df: DataFrame) -> QualityCheck:
        """Check data consistency rules."""
        logger.info("Running consistency check")
        
        # Check if transformed_value is derived correctly from value
        inconsistent_count = df.filter(
            (col("transformed_value") < col("value") * 0.5) |
            (col("transformed_value") > col("value") * 3.0)
        ).count()
        
        passed = inconsistent_count == 0
        message = ("Data is consistent" if passed 
                  else f"{inconsistent_count} records with inconsistent values")
        
        return QualityCheck(
            name="Consistency Check",
            check_type="CONSISTENCY",
            passed=passed,
            failed_count=inconsistent_count,
            message=message
        )
    
    def profile_data(self, df: DataFrame) -> DataProfile:
        """
        Generate statistical profile of data.
        
        Args:
            df: DataFrame to profile
            
        Returns:
            DataProfile with statistics
        """
        logger.info("Profiling data")
        
        profile = DataProfile()
        
        # Basic counts
        profile.total_records = df.count()
        profile.null_count = df.filter(col("value").isNull()).count()
        
        # Duplicate count
        total = df.count()
        unique = df.select("id").distinct().count()
        profile.duplicate_count = total - unique
        
        # Value statistics
        stats = df.agg(
            spark_min("transformed_value").alias("min_val"),
            spark_max("transformed_value").alias("max_val"),
            avg("transformed_value").alias("avg_val"),
            stddev("transformed_value").alias("std_val")
        ).first()
        
        if stats:
            profile.min_value = float(stats.min_val or 0)
            profile.max_value = float(stats.max_val or 0)
            profile.avg_value = float(stats.avg_val or 0)
            profile.std_deviation = float(stats.std_val or 0)
        
        # Category count
        profile.unique_categories = df.select("category").distinct().count()
        
        logger.info(f"Profile complete - {profile.total_records} records analyzed")
        
        return profile
    
    def detect_anomalies(self, df: DataFrame) -> List[str]:
        """
        Detect anomalies in data.
        
        Args:
            df: DataFrame to check
            
        Returns:
            List of anomaly descriptions
        """
        logger.info("Detecting anomalies")
        
        anomalies = []
        
        # Get statistics
        stats = df.agg(
            avg("transformed_value").alias("avg"),
            stddev("transformed_value").alias("std")
        ).first()
        
        if stats and stats.avg and stats.std:
            avg_val = float(stats.avg)
            std_val = float(stats.std)
            
            # Outliers beyond 3 standard deviations
            outlier_threshold = avg_val + (3 * std_val)
            outlier_count = df.filter(
                col("transformed_value") > outlier_threshold
            ).count()
            
            if outlier_count > 0:
                anomalies.append(
                    f"{outlier_count} outliers detected "
                    f"(> {outlier_threshold:.2f})"
                )
        
        # Check for unusual category distribution
        category_counts = df.groupBy("category").count().collect()
        total = df.count()
        
        for row in category_counts:
            percentage = (row['count'] / total) * 100
            if percentage > 80:
                anomalies.append(
                    f"Category '{row.category}' represents {percentage:.1f}% of data"
                )
        
        logger.info(f"Anomaly detection complete - {len(anomalies)} anomalies found")
        
        return anomalies