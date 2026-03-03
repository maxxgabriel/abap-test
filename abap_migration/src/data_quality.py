"""Data quality management module."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, stddev, avg, min as spark_min, max as spark_max
from typing import List, Dict
import logging


class DataQuality:
    """Perform data quality checks and profiling."""
    
    def __init__(self, spark, config: dict, run_id: str):
        """Initialize data quality manager.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_id: Unique run identifier
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
    
    def perform_quality_checks(self, df: DataFrame) -> List[Dict]:
        """Run all quality checks.
        
        Args:
            df: DataFrame to check
            
        Returns:
            List of check results
        """
        self.logger.info("Starting data quality checks")
        
        checks = [
            self.check_completeness(df),
            self.check_uniqueness(df),
            self.check_validity(df),
            self.check_consistency(df)
        ]
        
        passed = sum(1 for c in checks if c["passed"])
        failed = len(checks) - passed
        
        self.logger.info(
            f"Quality checks complete: {passed} passed, {failed} failed"
        )
        
        return checks
    
    def check_completeness(self, df: DataFrame) -> Dict:
        """Check for missing required values.
        
        Args:
            df: DataFrame to check
            
        Returns:
            Check result dictionary
        """
        null_count = df.filter(
            col("id").isNull() | col("name").isNull() | col("value").isNull()
        ).count()
        
        passed = null_count == 0
        
        return {
            "check_name": "Completeness Check",
            "check_type": "COMPLETENESS",
            "passed": passed,
            "failed_count": null_count,
            "message": (
                "All required fields are complete"
                if passed
                else f"{null_count} records with incomplete data"
            )
        }
    
    def check_uniqueness(self, df: DataFrame) -> Dict:
        """Check for duplicate IDs.
        
        Args:
            df: DataFrame to check
            
        Returns:
            Check result dictionary
        """
        total_count = df.count()
        unique_count = df.select("id").distinct().count()
        duplicate_count = total_count - unique_count
        
        passed = duplicate_count == 0
        
        return {
            "check_name": "Uniqueness Check",
            "check_type": "UNIQUENESS",
            "passed": passed,
            "failed_count": duplicate_count,
            "message": (
                "All IDs are unique"
                if passed
                else f"{duplicate_count} duplicate IDs found"
            )
        }
    
    def check_validity(self, df: DataFrame) -> Dict:
        """Check for invalid values.
        
        Args:
            df: DataFrame to check
            
        Returns:
            Check result dictionary
        """
        invalid_count = df.filter(
            (col("value") < 0) |
            (col("transformed_value") < 0) |
            (col("priority") < 1) |
            (col("priority") > 5) |
            col("category").isNull()
        ).count()
        
        passed = invalid_count == 0
        
        return {
            "check_name": "Validity Check",
            "check_type": "VALIDITY",
            "passed": passed,
            "failed_count": invalid_count,
            "message": (
                "All values are valid"
                if passed
                else f"{invalid_count} records with invalid values"
            )
        }
    
    def check_consistency(self, df: DataFrame) -> Dict:
        """Check for data consistency issues.
        
        Args:
            df: DataFrame to check
            
        Returns:
            Check result dictionary
        """
        # Check if transformed_value >= value (with multipliers applied)
        inconsistent_count = df.filter(
            (col("category") == "PREMIUM") &
            (col("transformed_value") < col("value"))
        ).count()
        
        passed = inconsistent_count == 0
        
        return {
            "check_name": "Consistency Check",
            "check_type": "CONSISTENCY",
            "passed": passed,
            "failed_count": inconsistent_count,
            "message": (
                "Data is consistent"
                if passed
                else f"{inconsistent_count} records with consistency issues"
            )
        }
    
    def profile_data(self, df: DataFrame) -> Dict:
        """Generate data profile statistics.
        
        Args:
            df: DataFrame to profile
            
        Returns:
            Profile statistics dictionary
        """
        total_records = df.count()
        
        # Aggregate statistics
        stats = df.agg(
            count(when(col("value").isNull(), 1)).alias("null_count"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value"),
            avg("value").alias("avg_value"),
            stddev("value").alias("std_deviation")
        ).first()
        
        # Count unique categories
        unique_categories = df.select("category").distinct().count()
        
        # Calculate duplicates
        duplicate_count = total_records - df.select("id").distinct().count()
        
        profile = {
            "total_records": total_records,
            "null_count": stats["null_count"],
            "duplicate_count": duplicate_count,
            "min_value": float(stats["min_value"]) if stats["min_value"] else 0.0,
            "max_value": float(stats["max_value"]) if stats["max_value"] else 0.0,
            "avg_value": float(stats["avg_value"]) if stats["avg_value"] else 0.0,
            "std_deviation": float(stats["std_deviation"]) if stats["std_deviation"] else 0.0,
            "unique_categories": unique_categories
        }
        
        self.logger.info(f"Data profile: {profile}")
        
        return profile
    
    def detect_anomalies(self, df: DataFrame) -> List[str]:
        """Detect anomalies in data.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            List of anomaly descriptions
        """
        anomalies = []
        
        # Calculate mean and stddev
        stats = df.agg(
            avg("value").alias("mean"),
            stddev("value").alias("stddev")
        ).first()
        
        mean_val = stats["mean"]
        stddev_val = stats["stddev"]
        
        if mean_val and stddev_val:
            # Find outliers (> 3 standard deviations)
            outlier_count = df.filter(
                (col("value") > mean_val + 3 * stddev_val) |
                (col("value") < mean_val - 3 * stddev_val)
            ).count()
            
            if outlier_count > 0:
                anomalies.append(
                    f"{outlier_count} statistical outliers detected"
                )
        
        # Check for unusual category distributions
        category_counts = df.groupBy("category").count().collect()
        total_count = df.count()
        
        for row in category_counts:
            pct = (row["count"] / total_count) * 100
            if pct < 1:  # Less than 1% of data
                anomalies.append(
                    f"Category '{row['category']}' has unusually low "
                    f"representation ({pct:.2f}%)"
                )
        
        if anomalies:
            self.logger.warning(f"Detected {len(anomalies)} anomalies")
        
        return anomalies