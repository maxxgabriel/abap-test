"""
Data Quality Module for ETL Pipeline
Provides profiling, validation, and quality check capabilities.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class QualityCheck:
    """Represents a single quality check result"""
    
    def __init__(self, check_name: str, check_type: str, passed: bool, 
                 failed_count: int, message: str):
        self.check_name = check_name
        self.check_type = check_type
        self.passed = passed
        self.failed_count = failed_count
        self.message = message
    
    def to_dict(self) -> Dict:
        return {
            'check_name': self.check_name,
            'check_type': self.check_type,
            'passed': self.passed,
            'failed_count': self.failed_count,
            'message': self.message
        }


class DataProfile:
    """Represents data profiling statistics"""
    
    def __init__(self, total_records: int, null_count: int, duplicate_count: int,
                 min_value: float, max_value: float, avg_value: float,
                 std_deviation: float, unique_categories: int):
        self.total_records = total_records
        self.null_count = null_count
        self.duplicate_count = duplicate_count
        self.min_value = min_value
        self.max_value = max_value
        self.avg_value = avg_value
        self.std_deviation = std_deviation
        self.unique_categories = unique_categories
    
    def to_dict(self) -> Dict:
        return {
            'total_records': self.total_records,
            'null_count': self.null_count,
            'duplicate_count': self.duplicate_count,
            'min_value': self.min_value,
            'max_value': self.max_value,
            'avg_value': self.avg_value,
            'std_deviation': self.std_deviation,
            'unique_categories': self.unique_categories
        }


class DataQualityChecker:
    """
    Performs comprehensive data quality checks and profiling.
    """
    
    def __init__(self, spark: SparkSession, run_id: str):
        self.spark = spark
        self.run_id = run_id
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def perform_quality_checks(self, df: DataFrame) -> List[QualityCheck]:
        """
        Execute all quality checks on the DataFrame.
        
        Args:
            df: Input DataFrame to validate
            
        Returns:
            List of QualityCheck results
        """
        self.logger.info(f"Starting data quality checks for run {self.run_id}")
        
        checks = []
        
        # Run individual checks
        checks.append(self.check_completeness(df))
        checks.append(self.check_uniqueness(df))
        checks.append(self.check_validity(df))
        checks.append(self.check_consistency(df))
        
        # Count results
        passed = sum(1 for c in checks if c.passed)
        failed = sum(1 for c in checks if not c.passed)
        
        self.logger.info(f"Quality checks complete: {passed} passed, {failed} failed")
        
        return checks
    
    def check_completeness(self, df: DataFrame) -> QualityCheck:
        """
        Check for null/empty values in critical fields.
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Running completeness check")
        
        # Count records with null values in critical fields
        null_condition = (
            F.col('id').isNull() | 
            F.col('name').isNull() | 
            F.col('value').isNull() |
            (F.col('id') == '') |
            (F.col('name') == '')
        )
        
        null_count = df.filter(null_condition).count()
        
        passed = null_count == 0
        message = 'All required fields are complete' if passed else f'{null_count} records with incomplete data'
        
        return QualityCheck(
            check_name='Completeness Check',
            check_type='COMPLETENESS',
            passed=passed,
            failed_count=null_count,
            message=message
        )
    
    def check_uniqueness(self, df: DataFrame) -> QualityCheck:
        """
        Check for duplicate IDs.
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Running uniqueness check")
        
        total_count = df.count()
        unique_count = df.select('id').distinct().count()
        duplicate_count = total_count - unique_count
        
        passed = duplicate_count == 0
        message = 'All IDs are unique' if passed else f'{duplicate_count} duplicate IDs found'
        
        return QualityCheck(
            check_name='Uniqueness Check',
            check_type='UNIQUENESS',
            passed=passed,
            failed_count=duplicate_count,
            message=message
        )
    
    def check_validity(self, df: DataFrame) -> QualityCheck:
        """
        Check for invalid values based on business rules.
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Running validity check")
        
        # Check for invalid values
        invalid_condition = (
            (F.col('value') < 0) |
            (F.col('transformed_value') < 0) |
            (F.col('priority') < 1) |
            (F.col('priority') > 5) |
            F.col('category').isNull() |
            (F.col('category') == '')
        )
        
        invalid_count = df.filter(invalid_condition).count()
        
        passed = invalid_count == 0
        message = 'All values are valid' if passed else f'{invalid_count} records with invalid values'
        
        return QualityCheck(
            check_name='Validity Check',
            check_type='VALIDITY',
            passed=passed,
            failed_count=invalid_count,
            message=message
        )
    
    def check_consistency(self, df: DataFrame) -> QualityCheck:
        """
        Check for data consistency issues.
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Running consistency check")
        
        # Check for consistency issues
        # Example: transformed_value should be >= value
        inconsistent_condition = (
            (F.col('transformed_value') < F.col('value') * 0.5) |
            (F.col('status').isNull()) |
            (F.col('processed_at').isNull())
        )
        
        inconsistent_count = df.filter(inconsistent_condition).count()
        
        passed = inconsistent_count == 0
        message = 'All data is consistent' if passed else f'{inconsistent_count} records with consistency issues'
        
        return QualityCheck(
            check_name='Consistency Check',
            check_type='CONSISTENCY',
            passed=passed,
            failed_count=inconsistent_count,
            message=message
        )
    
    def profile_data(self, df: DataFrame) -> DataProfile:
        """
        Generate comprehensive data profiling statistics.
        
        Args:
            df: Input DataFrame to profile
            
        Returns:
            DataProfile with statistics
        """
        self.logger.info("Generating data profile")
        
        # Calculate statistics
        total_records = df.count()
        
        # Null count across all critical fields
        null_count = df.filter(
            F.col('id').isNull() | 
            F.col('name').isNull() | 
            F.col('value').isNull()
        ).count()
        
        # Duplicate count
        unique_ids = df.select('id').distinct().count()
        duplicate_count = total_records - unique_ids
        
        # Value statistics
        stats = df.select(
            F.min('value').alias('min_value'),
            F.max('value').alias('max_value'),
            F.avg('value').alias('avg_value'),
            F.stddev('value').alias('std_deviation')
        ).first()
        
        # Unique categories
        unique_categories = df.select('category').distinct().count()
        
        profile = DataProfile(
            total_records=total_records,
            null_count=null_count,
            duplicate_count=duplicate_count,
            min_value=float(stats['min_value']) if stats['min_value'] else 0.0,
            max_value=float(stats['max_value']) if stats['max_value'] else 0.0,
            avg_value=float(stats['avg_value']) if stats['avg_value'] else 0.0,
            std_deviation=float(stats['std_deviation']) if stats['std_deviation'] else 0.0,
            unique_categories=unique_categories
        )
        
        self.logger.info(f"Data profile generated: {total_records} records analyzed")
        return profile
    
    def detect_anomalies(self, df: DataFrame) -> List[str]:
        """
        Detect anomalies in the data using statistical methods.
        
        Args:
            df: Input DataFrame
            
        Returns:
            List of anomaly descriptions
        """
        self.logger.info("Detecting anomalies")
        
        anomalies = []
        
        # Calculate value statistics for outlier detection
        stats = df.select(
            F.avg('value').alias('mean'),
            F.stddev('value').alias('std')
        ).first()
        
        if stats['mean'] and stats['std']:
            mean = float(stats['mean'])
            std = float(stats['std'])
            
            # Detect outliers (values beyond 3 standard deviations)
            outlier_condition = (
                (F.col('value') > mean + 3 * std) |
                (F.col('value') < mean - 3 * std)
            )
            
            outlier_count = df.filter(outlier_condition).count()
            
            if outlier_count > 0:
                anomalies.append(f'{outlier_count} outlier values detected (beyond 3 std deviations)')
        
        # Check for unusual category distribution
        category_counts = df.groupBy('category').count().collect()
        if category_counts:
            total = df.count()
            for row in category_counts:
                percentage = (row['count'] / total) * 100
                if percentage > 80:
                    anomalies.append(
                        f"Category '{row['category']}' dominates with {percentage:.1f}% of records"
                    )
        
        # Check for suspicious patterns in IDs
        id_pattern_check = df.filter(
            F.col('id').rlike(r'^(.)\1+$')  # All same character
        ).count()
        
        if id_pattern_check > 0:
            anomalies.append(f'{id_pattern_check} records with suspicious ID patterns')
        
        # Check for duplicate names with different IDs
        duplicate_names = df.groupBy('name').agg(
            F.countDistinct('id').alias('id_count')
        ).filter(F.col('id_count') > 1).count()
        
        if duplicate_names > 0:
            anomalies.append(f'{duplicate_names} names associated with multiple IDs')
        
        self.logger.info(f"Anomaly detection complete: {len(anomalies)} anomalies found")
        return anomalies
    
    def validate_record_counts(self, source_count: int, transformed_count: int, 
                              loaded_count: int) -> Tuple[bool, str]:
        """
        Validate that record counts match across pipeline stages.
        
        Args:
            source_count: Number of records extracted
            transformed_count: Number of records transformed
            loaded_count: Number of records loaded
            
        Returns:
            Tuple of (validation passed, message)
        """
        self.logger.info(
            f"Validating record counts - Source: {source_count}, "
            f"Transformed: {transformed_count}, Loaded: {loaded_count}"
        )
        
        # Allow for some records to be filtered out, but not too many
        acceptable_loss_rate = 0.05  # 5%
        
        # Check extraction to transformation
        if transformed_count > source_count:
            return False, f"Transformed count ({transformed_count}) exceeds source count ({source_count})"
        
        loss_rate = (source_count - transformed_count) / source_count if source_count > 0 else 0
        if loss_rate > acceptable_loss_rate:
            return False, f"Excessive record loss in transformation: {loss_rate*100:.1f}%"
        
        # Check transformation to load
        if loaded_count > transformed_count:
            return False, f"Loaded count ({loaded_count}) exceeds transformed count ({transformed_count})"
        
        loss_rate = (transformed_count - loaded_count) / transformed_count if transformed_count > 0 else 0
        if loss_rate > acceptable_loss_rate:
            return False, f"Excessive record loss in loading: {loss_rate*100:.1f}%"
        
        # All checks passed
        message = f"Record counts validated successfully - Loss rate: {loss_rate*100:.2f}%"
        self.logger.info(message)
        return True, message
    
    def generate_quality_report(self, df: DataFrame, 
                               source_count: int = 0,
                               transformed_count: int = 0,
                               loaded_count: int = 0) -> Dict:
        """
        Generate a comprehensive data quality report.
        
        Args:
            df: DataFrame to analyze
            source_count: Optional source record count
            transformed_count: Optional transformed record count
            loaded_count: Optional loaded record count
            
        Returns:
            Dictionary containing complete quality report
        """
        self.logger.info("Generating comprehensive quality report")
        
        # Perform all checks
        quality_checks = self.perform_quality_checks(df)
        profile = self.profile_data(df)
        anomalies = self.detect_anomalies(df)
        
        # Validate counts if provided
        count_validation = None
        if source_count > 0 and transformed_count > 0 and loaded_count > 0:
            passed, message = self.validate_record_counts(
                source_count, transformed_count, loaded_count
            )
            count_validation = {
                'passed': passed,
                'message': message,
                'source_count': source_count,
                'transformed_count': transformed_count,
                'loaded_count': loaded_count
            }
        
        # Calculate overall quality score
        total_checks = len(quality_checks)
        passed_checks = sum(1 for c in quality_checks if c.passed)
        quality_score = (passed_checks / total_checks * 100) if total_checks > 0 else 0
        
        report = {
            'run_id': self.run_id,
            'timestamp': datetime.now().isoformat(),
            'quality_score': quality_score,
            'checks': [check.to_dict() for check in quality_checks],
            'profile': profile.to_dict(),
            'anomalies': anomalies,
            'count_validation': count_validation,
            'summary': {
                'total_checks': total_checks,
                'passed_checks': passed_checks,
                'failed_checks': total_checks - passed_checks,
                'total_anomalies': len(anomalies)
            }
        }
        
        self.logger.info(f"Quality report generated - Score: {quality_score:.1f}%")
        return report


def create_quality_checker(spark: SparkSession, run_id: str) -> DataQualityChecker:
    """
    Factory function to create a DataQualityChecker instance.
    
    Args:
        spark: SparkSession instance
        run_id: Unique identifier for the ETL run
        
    Returns:
        Configured DataQualityChecker instance
    """
    return DataQualityChecker(spark, run_id)