"""
Data Quality Module for ETL Pipeline
Implements quality checks, profiling, and validation
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, countDistinct, avg, stddev, min as spark_min, 
    max as spark_max, sum as spark_sum, when, isnan, isnull
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, asdict
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@dataclass
class QualityCheck:
    """Data class for quality check results"""
    check_name: str
    check_type: str
    passed: bool
    failed_count: int
    message: str
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class DataProfile:
    """Data class for data profiling statistics"""
    total_records: int
    null_count: int
    duplicate_count: int
    min_value: float
    max_value: float
    avg_value: float
    std_deviation: float
    unique_categories: int
    completeness_rate: float
    uniqueness_rate: float


class DataQualityChecker:
    """
    Performs comprehensive data quality checks including:
    - Completeness checks
    - Uniqueness checks
    - Validity checks
    - Consistency checks
    - Data profiling
    - Anomaly detection
    """
    
    def __init__(self, spark: SparkSession, run_id: str):
        """
        Initialize Data Quality Checker
        
        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
        """
        self.spark = spark
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
    def perform_quality_checks(self, df: DataFrame) -> List[QualityCheck]:
        """
        Perform all quality checks on the dataset
        
        Args:
            df: Input DataFrame to check
            
        Returns:
            List of QualityCheck results
        """
        self.logger.info(f"Starting data quality checks for run_id: {self.run_id}")
        
        checks = []
        
        # Run all quality checks
        checks.append(self.check_completeness(df))
        checks.append(self.check_uniqueness(df))
        checks.append(self.check_validity(df))
        checks.append(self.check_consistency(df))
        
        # Count results
        passed = sum(1 for check in checks if check.passed)
        failed = sum(1 for check in checks if not check.passed)
        
        self.logger.info(f"Quality checks complete: {passed} passed, {failed} failed")
        
        return checks
    
    def check_completeness(self, df: DataFrame) -> QualityCheck:
        """
        Check for completeness (null/empty values in critical fields)
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Running completeness check")
        
        # Critical fields that must not be null
        critical_fields = ['id', 'name', 'value']
        
        # Count records with null values in critical fields
        null_condition = None
        for field in critical_fields:
            if field in df.columns:
                field_condition = col(field).isNull() | (col(field) == '')
                null_condition = field_condition if null_condition is None else null_condition | field_condition
        
        if null_condition is None:
            return QualityCheck(
                check_name='Completeness Check',
                check_type='COMPLETENESS',
                passed=True,
                failed_count=0,
                message='No critical fields found to check'
            )
        
        null_count = df.filter(null_condition).count()
        total_count = df.count()
        
        passed = null_count == 0
        message = (
            'All required fields are complete' if passed 
            else f'{null_count} records with incomplete data ({null_count/total_count*100:.2f}%)'
        )
        
        return QualityCheck(
            check_name='Completeness Check',
            check_type='COMPLETENESS',
            passed=passed,
            failed_count=null_count,
            message=message
        )
    
    def check_uniqueness(self, df: DataFrame) -> QualityCheck:
        """
        Check for uniqueness (duplicate IDs)
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Running uniqueness check")
        
        if 'id' not in df.columns:
            return QualityCheck(
                check_name='Uniqueness Check',
                check_type='UNIQUENESS',
                passed=True,
                failed_count=0,
                message='No ID field to check'
            )
        
        total_count = df.count()
        unique_count = df.select('id').distinct().count()
        duplicate_count = total_count - unique_count
        
        passed = duplicate_count == 0
        message = (
            'All IDs are unique' if passed 
            else f'{duplicate_count} duplicate IDs found ({duplicate_count/total_count*100:.2f}%)'
        )
        
        return QualityCheck(
            check_name='Uniqueness Check',
            check_type='UNIQUENESS',
            passed=passed,
            failed_count=duplicate_count,
            message=message
        )
    
    def check_validity(self, df: DataFrame) -> QualityCheck:
        """
        Check for validity (values within acceptable ranges)
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Running validity check")
        
        invalid_condition = None
        
        # Check value fields are positive
        if 'value' in df.columns:
            invalid_condition = col('value') < 0
        
        if 'transformed_value' in df.columns:
            value_check = col('transformed_value') < 0
            invalid_condition = value_check if invalid_condition is None else invalid_condition | value_check
        
        # Check priority is within range (1-5)
        if 'priority' in df.columns:
            priority_check = (col('priority') < 1) | (col('priority') > 5)
            invalid_condition = priority_check if invalid_condition is None else invalid_condition | priority_check
        
        # Check category is not empty
        if 'category' in df.columns:
            category_check = col('category').isNull() | (col('category') == '')
            invalid_condition = category_check if invalid_condition is None else invalid_condition | category_check
        
        if invalid_condition is None:
            return QualityCheck(
                check_name='Validity Check',
                check_type='VALIDITY',
                passed=True,
                failed_count=0,
                message='No validity rules configured'
            )
        
        invalid_count = df.filter(invalid_condition).count()
        total_count = df.count()
        
        passed = invalid_count == 0
        message = (
            'All values are valid' if passed 
            else f'{invalid_count} records with invalid values ({invalid_count/total_count*100:.2f}%)'
        )
        
        return QualityCheck(
            check_name='Validity Check',
            check_type='VALIDITY',
            passed=passed,
            failed_count=invalid_count,
            message=message
        )
    
    def check_consistency(self, df: DataFrame) -> QualityCheck:
        """
        Check for consistency (logical relationships between fields)
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Running consistency check")
        
        inconsistent_condition = None
        
        # Check transformed_value is not less than original value for standard categories
        if 'value' in df.columns and 'transformed_value' in df.columns and 'category' in df.columns:
            inconsistent_condition = (
                (col('category') == 'STANDARD') & 
                (col('transformed_value') < col('value'))
            )
        
        # Check status consistency with value
        if 'status' in df.columns and 'transformed_value' in df.columns:
            status_check = (
                (col('status') == 'HIGH_VALUE') & (col('transformed_value') < 750)
            ) | (
                (col('status') == 'LOW_VALUE') & (col('transformed_value') >= 300)
            )
            inconsistent_condition = (
                status_check if inconsistent_condition is None 
                else inconsistent_condition | status_check
            )
        
        if inconsistent_condition is None:
            return QualityCheck(
                check_name='Consistency Check',
                check_type='CONSISTENCY',
                passed=True,
                failed_count=0,
                message='No consistency rules configured'
            )
        
        inconsistent_count = df.filter(inconsistent_condition).count()
        total_count = df.count()
        
        passed = inconsistent_count == 0
        message = (
            'All records are consistent' if passed 
            else f'{inconsistent_count} inconsistent records found ({inconsistent_count/total_count*100:.2f}%)'
        )
        
        return QualityCheck(
            check_name='Consistency Check',
            check_type='CONSISTENCY',
            passed=passed,
            failed_count=inconsistent_count,
            message=message
        )
    
    def profile_data(self, df: DataFrame) -> DataProfile:
        """
        Generate comprehensive data profiling statistics
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataProfile with statistics
        """
        self.logger.info("Generating data profile")
        
        total_records = df.count()
        
        # Calculate null counts across all critical fields
        null_count = 0
        if 'id' in df.columns:
            null_count += df.filter(col('id').isNull()).count()
        if 'name' in df.columns:
            null_count += df.filter(col('name').isNull()).count()
        if 'value' in df.columns:
            null_count += df.filter(col('value').isNull()).count()
        
        # Calculate duplicates
        duplicate_count = 0
        if 'id' in df.columns:
            duplicate_count = total_records - df.select('id').distinct().count()
        
        # Calculate value statistics
        value_stats = None
        if 'transformed_value' in df.columns:
            value_stats = df.agg(
                spark_min('transformed_value').alias('min_val'),
                spark_max('transformed_value').alias('max_val'),
                avg('transformed_value').alias('avg_val'),
                stddev('transformed_value').alias('std_dev')
            ).collect()[0]
        elif 'value' in df.columns:
            value_stats = df.agg(
                spark_min('value').alias('min_val'),
                spark_max('value').alias('max_val'),
                avg('value').alias('avg_val'),
                stddev('value').alias('std_dev')
            ).collect()[0]
        
        # Calculate unique categories
        unique_categories = 0
        if 'category' in df.columns:
            unique_categories = df.select('category').distinct().count()
        
        # Calculate rates
        completeness_rate = ((total_records * 3 - null_count) / (total_records * 3) * 100) if total_records > 0 else 0
        uniqueness_rate = ((total_records - duplicate_count) / total_records * 100) if total_records > 0 else 0
        
        profile = DataProfile(
            total_records=total_records,
            null_count=null_count,
            duplicate_count=duplicate_count,
            min_value=float(value_stats['min_val']) if value_stats else 0.0,
            max_value=float(value_stats['max_val']) if value_stats else 0.0,
            avg_value=float(value_stats['avg_val']) if value_stats else 0.0,
            std_deviation=float(value_stats['std_dev']) if value_stats and value_stats['std_dev'] else 0.0,
            unique_categories=unique_categories,
            completeness_rate=completeness_rate,
            uniqueness_rate=uniqueness_rate
        )
        
        self.logger.info(f"Data profile generated: {total_records} records, "
                        f"{completeness_rate:.2f}% complete, {uniqueness_rate:.2f}% unique")
        
        return profile
    
    def detect_anomalies(self, df: DataFrame, threshold_std_dev: float = 3.0) -> List[str]:
        """
        Detect anomalies using statistical methods
        
        Args:
            df: Input DataFrame
            threshold_std_dev: Number of standard deviations to consider anomaly
            
        Returns:
            List of anomaly descriptions
        """
        self.logger.info(f"Detecting anomalies (threshold: {threshold_std_dev} std dev)")
        
        anomalies = []
        
        # Detect value outliers
        if 'transformed_value' in df.columns:
            stats = df.agg(
                avg('transformed_value').alias('mean'),
                stddev('transformed_value').alias('stddev')
            ).collect()[0]
            
            mean_val = float(stats['mean']) if stats['mean'] else 0
            std_val = float(stats['stddev']) if stats['stddev'] else 0
            
            if std_val > 0:
                lower_bound = mean_val - (threshold_std_dev * std_val)
                upper_bound = mean_val + (threshold_std_dev * std_val)
                
                outlier_count = df.filter(
                    (col('transformed_value') < lower_bound) | 
                    (col('transformed_value') > upper_bound)
                ).count()
                
                if outlier_count > 0:
                    anomalies.append(
                        f"Found {outlier_count} value outliers "
                        f"(outside range {lower_bound:.2f} - {upper_bound:.2f})"
                    )
        
        # Detect unusual category distributions
        if 'category' in df.columns:
            total_count = df.count()
            category_counts = df.groupBy('category').count().collect()
            
            for row in category_counts:
                percentage = (row['count'] / total_count) * 100
                if percentage > 50:
                    anomalies.append(
                        f"Category '{row['category']}' has unusually high representation: {percentage:.2f}%"
                    )
                elif percentage < 1:
                    anomalies.append(
                        f"Category '{row['category']}' has unusually low representation: {percentage:.2f}%"
                    )
        
        # Detect suspicious duplicate patterns
        if 'name' in df.columns:
            name_dup_count = df.count() - df.select('name').distinct().count()
            if name_dup_count > df.count() * 0.1:  # More than 10% duplicates
                anomalies.append(
                    f"Suspicious duplicate name pattern: {name_dup_count} duplicate names "
                    f"({name_dup_count/df.count()*100:.2f}%)"
                )
        
        self.logger.info(f"Anomaly detection complete: {len(anomalies)} anomalies found")
        
        return anomalies
    
    def validate_record_counts(
        self, 
        source_count: int, 
        transformed_count: int, 
        loaded_count: int,
        tolerance: float = 0.05
    ) -> Tuple[bool, str]:
        """
        Validate record counts across pipeline stages
        
        Args:
            source_count: Records extracted
            transformed_count: Records transformed
            loaded_count: Records loaded
            tolerance: Acceptable loss percentage (default 5%)
            
        Returns:
            Tuple of (is_valid, message)
        """
        self.logger.info(f"Validating record counts: "
                        f"source={source_count}, transformed={transformed_count}, loaded={loaded_count}")
        
        # Check if counts are within tolerance
        extract_to_transform_loss = source_count - transformed_count
        transform_to_load_loss = transformed_count - loaded_count
        total_loss = source_count - loaded_count
        
        max_acceptable_loss = int(source_count * tolerance)
        
        if total_loss > max_acceptable_loss:
            loss_percentage = (total_loss / source_count * 100) if source_count > 0 else 0
            message = (
                f"Record count validation FAILED: Lost {total_loss} records ({loss_percentage:.2f}%). "
                f"Extract→Transform: {extract_to_transform_loss}, Transform→Load: {transform_to_load_loss}. "
                f"Tolerance: {tolerance*100}%"
            )
            self.logger.error(message)
            return False, message
        
        loss_percentage = (total_loss / source_count * 100) if source_count > 0 else 0
        message = (
            f"Record count validation PASSED: {loaded_count}/{source_count} records loaded "
            f"({100-loss_percentage:.2f}% success rate). "
            f"Total loss: {total_loss} records ({loss_percentage:.2f}%)"
        )
        self.logger.info(message)
        return True, message
    
    def generate_quality_report(
        self, 
        checks: List[QualityCheck],
        profile: DataProfile,
        anomalies: List[str]
    ) -> Dict:
        """
        Generate comprehensive quality report
        
        Args:
            checks: List of quality check results
            profile: Data profile statistics
            anomalies: List of detected anomalies
            
        Returns:
            Dictionary containing full quality report
        """
        report = {
            'run_id': self.run_id,
            'timestamp': datetime.now().isoformat(),
            'quality_checks': [asdict(check) for check in checks],
            'data_profile': asdict(profile),
            'anomalies': anomalies,
            'summary': {
                'total_checks': len(checks),
                'passed_checks': sum(1 for check in checks if check.passed),
                'failed_checks': sum(1 for check in checks if not check.passed),
                'total_anomalies': len(anomalies),
                'overall_quality_score': self._calculate_quality_score(checks, profile)
            }
        }
        
        return report
    
    def _calculate_quality_score(self, checks: List[QualityCheck], profile: DataProfile) -> float:
        """
        Calculate overall quality score (0-100)
        
        Args:
            checks: List of quality check results
            profile: Data profile
            
        Returns:
            Quality score between 0 and 100
        """
        if not checks:
            return 0.0
        
        # Base score from passed checks
        check_score = (sum(1 for check in checks if check.passed) / len(checks)) * 100
        
        # Adjust for completeness and uniqueness rates
        completeness_score = profile.completeness_rate
        uniqueness_score = profile.uniqueness_rate
        
        # Weighted average
        overall_score = (
            check_score * 0.5 +
            completeness_score * 0.25 +
            uniqueness_score * 0.25
        )
        
        return round(overall_score, 2)