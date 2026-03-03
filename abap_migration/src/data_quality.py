"""
Data Quality Module - Data profiling and quality checks
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, countDistinct, avg, stddev, min as spark_min, max as spark_max
from typing import List, Dict, Any
import logging


class DataQualityChecker:
    """Performs data quality checks and profiling"""
    
    def __init__(self, spark: SparkSession, config: dict, run_id: str):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
    
    def perform_quality_checks(self, df: DataFrame) -> List[Dict[str, Any]]:
        """Perform all quality checks"""
        self.logger.info("Starting data quality checks")
        
        checks = [
            self._check_completeness(df),
            self._check_uniqueness(df),
            self._check_validity(df),
            self._check_consistency(df)
        ]
        
        passed = sum(1 for check in checks if check['passed'])
        failed = len(checks) - passed
        
        self.logger.info(f"Quality checks complete: {passed} passed, {failed} failed")
        
        return checks
    
    def _check_completeness(self, df: DataFrame) -> Dict[str, Any]:
        """Check for null/empty values in critical fields"""
        null_count = df.filter(
            col("id").isNull() | 
            col("name").isNull() | 
            (col("name") == "") |
            col("value").isNull()
        ).count()
        
        return {
            'check_name': 'Completeness Check',
            'check_type': 'COMPLETENESS',
            'passed': null_count == 0,
            'failed_count': null_count,
            'message': f"{null_count} records with incomplete data" if null_count > 0 
                      else "All required fields are complete"
        }
    
    def _check_uniqueness(self, df: DataFrame) -> Dict[str, Any]:
        """Check for duplicate IDs"""
        total_count = df.count()
        unique_count = df.select("id").distinct().count()
        duplicate_count = total_count - unique_count
        
        return {
            'check_name': 'Uniqueness Check',
            'check_type': 'UNIQUENESS',
            'passed': duplicate_count == 0,
            'failed_count': duplicate_count,
            'message': f"{duplicate_count} duplicate IDs found" if duplicate_count > 0
                      else "All IDs are unique"
        }
    
    def _check_validity(self, df: DataFrame) -> Dict[str, Any]:
        """Check for invalid values"""
        invalid_count = df.filter(
            (col("value") < 0) |
            (col("transformed_value") < 0) |
            (col("priority") < 1) |
            (col("priority") > 5) |
            col("category").isNull() |
            (col("category") == "")
        ).count()
        
        return {
            'check_name': 'Validity Check',
            'check_type': 'VALIDITY',
            'passed': invalid_count == 0,
            'failed_count': invalid_count,
            'message': f"{invalid_count} records with invalid values" if invalid_count > 0
                      else "All values are valid"
        }
    
    def _check_consistency(self, df: DataFrame) -> Dict[str, Any]:
        """Check for data consistency"""
        inconsistent_count = df.filter(
            col("transformed_value") < col("value")
        ).count()
        
        return {
            'check_name': 'Consistency Check',
            'check_type': 'CONSISTENCY',
            'passed': inconsistent_count == 0,
            'failed_count': inconsistent_count,
            'message': f"{inconsistent_count} records with inconsistent values" if inconsistent_count > 0
                      else "Data is consistent"
        }
    
    def profile_data(self, df: DataFrame) -> Dict[str, Any]:
        """Generate data profile statistics"""
        stats = df.agg(
            count("*").alias("total_records"),
            count(col("id")).alias("non_null_ids"),
            countDistinct("id").alias("unique_ids"),
            countDistinct("category").alias("unique_categories"),
            avg("value").alias("avg_value"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value"),
            stddev("value").alias("std_deviation")
        ).collect()[0]
        
        profile = {
            'total_records': stats['total_records'],
            'null_count': stats['total_records'] - stats['non_null_ids'],
            'duplicate_count': stats['total_records'] - stats['unique_ids'],
            'unique_categories': stats['unique_categories'],
            'min_value': float(stats['min_value']) if stats['min_value'] else 0,
            'max_value': float(stats['max_value']) if stats['max_value'] else 0,
            'avg_value': float(stats['avg_value']) if stats['avg_value'] else 0,
            'std_deviation': float(stats['std_deviation']) if stats['std_deviation'] else 0
        }
        
        self.logger.info(f"Data profile: {profile}")
        
        return profile