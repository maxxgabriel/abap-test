"""ETL Data Quality Module"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, mean, stddev, min as spark_min, max as spark_max
from typing import List, Dict, Any
import logging


class ETLDataQuality:
    """Perform data quality checks and profiling"""
    
    def __init__(self, spark: SparkSession, run_id: str):
        self.spark = spark
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
    
    def perform_quality_checks(self, df: DataFrame) -> List[Dict[str, Any]]:
        """Run all quality checks"""
        self.logger.info("Starting data quality checks")
        
        checks = [
            self.check_completeness(df),
            self.check_uniqueness(df),
            self.check_validity(df),
            self.check_consistency(df)
        ]
        
        passed = sum(1 for check in checks if check["passed"])
        failed = len(checks) - passed
        
        self.logger.info(f"Quality checks complete: {passed} passed, {failed} failed")
        
        return checks
    
    def check_completeness(self, df: DataFrame) -> Dict[str, Any]:
        """Check for null/empty values in critical fields"""
        null_count = df.filter(
            col("id").isNull() | 
            col("name").isNull() | 
            col("value").isNull()
        ).count()
        
        return {
            "check_name": "Completeness Check",
            "check_type": "COMPLETENESS",
            "passed": null_count == 0,
            "failed_count": null_count,
            "message": "All required fields are complete" if null_count == 0 else f"{null_count} records with incomplete data"
        }
    
    def check_uniqueness(self, df: DataFrame) -> Dict[str, Any]:
        """Check for duplicate IDs"""
        total_count = df.count()
        unique_count = df.select("id").distinct().count()
        duplicate_count = total_count - unique_count
        
        return {
            "check_name": "Uniqueness Check",
            "check_type": "UNIQUENESS",
            "passed": duplicate_count == 0,
            "failed_count": duplicate_count,
            "message": "All IDs are unique" if duplicate_count == 0 else f"{duplicate_count} duplicate IDs found"
        }
    
    def check_validity(self, df: DataFrame) -> Dict[str, Any]:
        """Check for invalid values"""
        invalid_count = df.filter(
            (col("value") < 0) | 
            (col("transformed_value") < 0) |
            (col("priority") < 1) | 
            (col("priority") > 5) |
            col("category").isNull()
        ).count()
        
        return {
            "check_name": "Validity Check",
            "check_type": "VALIDITY",
            "passed": invalid_count == 0,
            "failed_count": invalid_count,
            "message": "All values are valid" if invalid_count == 0 else f"{invalid_count} records with invalid values"
        }
    
    def check_consistency(self, df: DataFrame) -> Dict[str, Any]:
        """Check for data consistency"""
        # Check if transformed_value >= value (should be true for most categories)
        inconsistent_count = df.filter(
            (col("transformed_value") < col("value")) & 
            (col("category") != "BASIC")
        ).count()
        
        return {
            "check_name": "Consistency Check",
            "check_type": "CONSISTENCY",
            "passed": inconsistent_count == 0,
            "failed_count": inconsistent_count,
            "message": "Data is consistent" if inconsistent_count == 0 else f"{inconsistent_count} inconsistent records"
        }
    
    def profile_data(self, df: DataFrame) -> Dict[str, Any]:
        """Generate data profile statistics"""
        stats = df.select(
            count("*").alias("total_records"),
            count(when(col("id").isNull(), 1)).alias("null_count"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value"),
            mean("value").alias("avg_value"),
            stddev("value").alias("std_deviation")
        ).collect()[0]
        
        unique_categories = df.select("category").distinct().count()
        duplicate_count = df.count() - df.dropDuplicates(["id"]).count()
        
        profile = {
            "total_records": stats["total_records"],
            "null_count": stats["null_count"],
            "duplicate_count": duplicate_count,
            "min_value": float(stats["min_value"]) if stats["min_value"] else 0,
            "max_value": float(stats["max_value"]) if stats["max_value"] else 0,
            "avg_value": float(stats["avg_value"]) if stats["avg_value"] else 0,
            "std_deviation": float(stats["std_deviation"]) if stats["std_deviation"] else 0,
            "unique_categories": unique_categories
        }
        
        self.logger.info(f"Data profile: {profile}")
        
        return profile
    
    def detect_anomalies(self, df: DataFrame) -> List[str]:
        """Detect anomalies in data"""
        anomalies = []
        
        # Calculate statistics for anomaly detection
        stats = df.select(
            mean("value").alias("mean"),
            stddev("value").alias("stddev")
        ).collect()[0]
        
        mean_val = float(stats["mean"]) if stats["mean"] else 0
        std_val = float(stats["stddev"]) if stats["stddev"] else 0
        
        # Detect outliers (values beyond 3 standard deviations)
        if std_val > 0:
            outlier_count = df.filter(
                (col("value") < (mean_val - 3 * std_val)) |
                (col("value") > (mean_val + 3 * std_val))
            ).count()
            
            if outlier_count > 0:
                anomalies.append(f"{outlier_count} outlier values detected")
        
        # Detect unusual categories
        category_counts = df.groupBy("category").count()
        total = df.count()
        
        rare_categories = category_counts.filter(col("count") < (total * 0.01)).count()
        if rare_categories > 0:
            anomalies.append(f"{rare_categories} rare categories detected")
        
        return anomalies