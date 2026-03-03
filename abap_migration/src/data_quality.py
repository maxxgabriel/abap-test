"""
Data Quality Validation Module for ETL Pipeline

This module provides comprehensive data quality checks including:
- Validation rule execution
- Data profiling and statistics generation
- Record count validation against thresholds
- Anomaly detection
- Completeness, uniqueness, validity, and consistency checks
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import logging


@dataclass
class QualityCheck:
    """Represents a single quality check result"""
    check_name: str
    check_type: str
    passed: bool
    failed_count: int
    message: str
    details: Dict = field(default_factory=dict)


@dataclass
class DataProfile:
    """Represents data profiling statistics"""
    total_records: int
    null_count: int
    duplicate_count: int
    min_value: float
    max_value: float
    avg_value: float
    std_deviation: float
    unique_categories: int
    field_completeness: Dict[str, float] = field(default_factory=dict)
    
    
@dataclass
class ValidationResult:
    """Complete validation result with all checks and profiles"""
    run_id: str
    timestamp: datetime
    checks: List[QualityCheck]
    profile: DataProfile
    anomalies: List[str]
    overall_passed: bool
    error_rate: float


class DataQualityValidator:
    """Main data quality validation engine"""
    
    def __init__(self, spark: SparkSession, config: Dict, run_id: str):
        """
        Initialize the validator
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary with quality thresholds
            run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
        # Load quality thresholds from config
        self.thresholds = config.get('quality_thresholds', {})
        
    def perform_quality_checks(self, df: DataFrame) -> List[QualityCheck]:
        """
        Execute all configured quality checks on the dataset
        
        Args:
            df: DataFrame to validate
            
        Returns:
            List of QualityCheck results
        """
        self.logger.info(f"Starting data quality checks for run_id: {self.run_id}")
        
        checks = []
        
        # Execute all quality check methods
        checks.append(self.check_completeness(df))
        checks.append(self.check_uniqueness(df))
        checks.append(self.check_validity(df))
        checks.append(self.check_consistency(df))
        checks.append(self.check_record_count(df))
        checks.append(self.check_value_ranges(df))
        
        # Count passed/failed
        passed = sum(1 for check in checks if check.passed)
        failed = sum(1 for check in checks if not check.passed)
        
        self.logger.info(f"Quality checks complete: {passed} passed, {failed} failed")
        
        return checks
    
    def check_completeness(self, df: DataFrame) -> QualityCheck:
        """
        Check for null/empty values in critical fields
        
        Args:
            df: DataFrame to check
            
        Returns:
            QualityCheck result
        """
        required_fields = self.config.get('required_fields', ['id', 'name', 'value'])
        null_threshold = self.thresholds.get('max_null_percentage', 5.0)
        
        total_records = df.count()
        null_counts = {}
        total_nulls = 0
        
        for field in required_fields:
            if field in df.columns:
                null_count = df.filter(F.col(field).isNull() | (F.col(field) == '')).count()
                null_counts[field] = null_count
                total_nulls += null_count
        
        null_percentage = (total_nulls / (total_records * len(required_fields))) * 100 if total_records > 0 else 0
        passed = null_percentage <= null_threshold
        
        return QualityCheck(
            check_name='Completeness Check',
            check_type='COMPLETENESS',
            passed=passed,
            failed_count=total_nulls,
            message=f"Null percentage: {null_percentage:.2f}% (threshold: {null_threshold}%)",
            details={'null_counts_by_field': null_counts, 'total_records': total_records}
        )
    
    def check_uniqueness(self, df: DataFrame) -> QualityCheck:
        """
        Check for duplicate records based on primary key
        
        Args:
            df: DataFrame to check
            
        Returns:
            QualityCheck result
        """
        primary_key = self.config.get('primary_key', 'id')
        max_duplicate_percentage = self.thresholds.get('max_duplicate_percentage', 0.0)
        
        total_records = df.count()
        unique_records = df.select(primary_key).distinct().count()
        duplicate_count = total_records - unique_records
        
        duplicate_percentage = (duplicate_count / total_records) * 100 if total_records > 0 else 0
        passed = duplicate_percentage <= max_duplicate_percentage
        
        return QualityCheck(
            check_name='Uniqueness Check',
            check_type='UNIQUENESS',
            passed=passed,
            failed_count=duplicate_count,
            message=f"Duplicate records: {duplicate_count} ({duplicate_percentage:.2f}%)",
            details={'total_records': total_records, 'unique_records': unique_records}
        )
    
    def check_validity(self, df: DataFrame) -> QualityCheck:
        """
        Check for invalid values based on business rules
        
        Args:
            df: DataFrame to check
            
        Returns:
            QualityCheck result
        """
        invalid_count = 0
        validation_rules = []
        
        # Rule 1: Values must be positive
        if 'value' in df.columns:
            negative_values = df.filter(F.col('value') < 0).count()
            invalid_count += negative_values
            validation_rules.append(f"Negative values: {negative_values}")
        
        if 'transformed_value' in df.columns:
            negative_transformed = df.filter(F.col('transformed_value') < 0).count()
            invalid_count += negative_transformed
            validation_rules.append(f"Negative transformed values: {negative_transformed}")
        
        # Rule 2: Priority must be 1-5
        if 'priority' in df.columns:
            invalid_priority = df.filter(
                (F.col('priority') < 1) | (F.col('priority') > 5)
            ).count()
            invalid_count += invalid_priority
            validation_rules.append(f"Invalid priority: {invalid_priority}")
        
        # Rule 3: Category must not be empty
        if 'category' in df.columns:
            empty_category = df.filter(
                F.col('category').isNull() | (F.col('category') == '')
            ).count()
            invalid_count += empty_category
            validation_rules.append(f"Empty categories: {empty_category}")
        
        # Rule 4: Status must be valid
        if 'status' in df.columns:
            valid_statuses = self.config.get('valid_statuses', [
                'ACTIVE', 'INACTIVE', 'TRANSFORMED', 'HIGH_VALUE', 'MEDIUM_VALUE', 'LOW_VALUE'
            ])
            invalid_status = df.filter(~F.col('status').isin(valid_statuses)).count()
            invalid_count += invalid_status
            validation_rules.append(f"Invalid status: {invalid_status}")
        
        max_invalid_percentage = self.thresholds.get('max_invalid_percentage', 1.0)
        total_records = df.count()
        invalid_percentage = (invalid_count / total_records) * 100 if total_records > 0 else 0
        passed = invalid_percentage <= max_invalid_percentage
        
        return QualityCheck(
            check_name='Validity Check',
            check_type='VALIDITY',
            passed=passed,
            failed_count=invalid_count,
            message=f"Invalid records: {invalid_count} ({invalid_percentage:.2f}%)",
            details={'validation_rules': validation_rules}
        )
    
    def check_consistency(self, df: DataFrame) -> QualityCheck:
        """
        Check for data consistency issues
        
        Args:
            df: DataFrame to check
            
        Returns:
            QualityCheck result
        """
        inconsistent_count = 0
        consistency_issues = []
        
        # Rule 1: Transformed value should be >= original value (in most cases)
        if 'value' in df.columns and 'transformed_value' in df.columns:
            # Allow some tolerance for business rules that reduce values
            min_ratio = self.config.get('min_transformation_ratio', 0.8)
            inconsistent_transform = df.filter(
                (F.col('transformed_value') / F.col('value')) < min_ratio
            ).count()
            if inconsistent_transform > 0:
                inconsistent_count += inconsistent_transform
                consistency_issues.append(f"Inconsistent transformations: {inconsistent_transform}")
        
        # Rule 2: High value items should have high priority
        if 'transformed_value' in df.columns and 'priority' in df.columns:
            high_value_low_priority = df.filter(
                (F.col('transformed_value') >= 1000) & (F.col('priority') > 2)
            ).count()
            if high_value_low_priority > 0:
                inconsistent_count += high_value_low_priority
                consistency_issues.append(f"High value items with low priority: {high_value_low_priority}")
        
        # Rule 3: Status should match value ranges
        if 'status' in df.columns and 'transformed_value' in df.columns:
            status_value_mismatch = df.filter(
                ((F.col('status') == 'HIGH_VALUE') & (F.col('transformed_value') < 750)) |
                ((F.col('status') == 'LOW_VALUE') & (F.col('transformed_value') >= 300))
            ).count()
            if status_value_mismatch > 0:
                inconsistent_count += status_value_mismatch
                consistency_issues.append(f"Status-value mismatches: {status_value_mismatch}")
        
        max_inconsistent_percentage = self.thresholds.get('max_inconsistent_percentage', 5.0)
        total_records = df.count()
        inconsistent_percentage = (inconsistent_count / total_records) * 100 if total_records > 0 else 0
        passed = inconsistent_percentage <= max_inconsistent_percentage
        
        return QualityCheck(
            check_name='Consistency Check',
            check_type='CONSISTENCY',
            passed=passed,
            failed_count=inconsistent_count,
            message=f"Inconsistent records: {inconsistent_count} ({inconsistent_percentage:.2f}%)",
            details={'consistency_issues': consistency_issues}
        )
    
    def check_record_count(self, df: DataFrame) -> QualityCheck:
        """
        Validate record count against expected thresholds
        
        Args:
            df: DataFrame to check
            
        Returns:
            QualityCheck result
        """
        actual_count = df.count()
        
        min_expected = self.thresholds.get('min_record_count', 0)
        max_expected = self.thresholds.get('max_record_count', float('inf'))
        
        passed = min_expected <= actual_count <= max_expected
        
        if actual_count < min_expected:
            message = f"Record count {actual_count} below minimum threshold {min_expected}"
        elif actual_count > max_expected:
            message = f"Record count {actual_count} exceeds maximum threshold {max_expected}"
        else:
            message = f"Record count {actual_count} within expected range [{min_expected}, {max_expected}]"
        
        return QualityCheck(
            check_name='Record Count Validation',
            check_type='RECORD_COUNT',
            passed=passed,
            failed_count=0 if passed else 1,
            message=message,
            details={
                'actual_count': actual_count,
                'min_expected': min_expected,
                'max_expected': max_expected
            }
        )
    
    def check_value_ranges(self, df: DataFrame) -> QualityCheck:
        """
        Check if numeric values are within expected ranges
        
        Args:
            df: DataFrame to check
            
        Returns:
            QualityCheck result
        """
        out_of_range_count = 0
        range_issues = []
        
        # Check value ranges
        if 'value' in df.columns:
            value_min = self.config.get('value_min', 0)
            value_max = self.config.get('value_max', 10000)
            out_of_range_values = df.filter(
                (F.col('value') < value_min) | (F.col('value') > value_max)
            ).count()
            if out_of_range_values > 0:
                out_of_range_count += out_of_range_values
                range_issues.append(f"Values out of range [{value_min}, {value_max}]: {out_of_range_values}")
        
        max_out_of_range_percentage = self.thresholds.get('max_out_of_range_percentage', 1.0)
        total_records = df.count()
        out_of_range_percentage = (out_of_range_count / total_records) * 100 if total_records > 0 else 0
        passed = out_of_range_percentage <= max_out_of_range_percentage
        
        return QualityCheck(
            check_name='Value Range Check',
            check_type='VALUE_RANGE',
            passed=passed,
            failed_count=out_of_range_count,
            message=f"Out of range values: {out_of_range_count} ({out_of_range_percentage:.2f}%)",
            details={'range_issues': range_issues}
        )
    
    def profile_data(self, df: DataFrame) -> DataProfile:
        """
        Generate comprehensive data profiling statistics
        
        Args:
            df: DataFrame to profile
            
        Returns:
            DataProfile with statistics
        """
        self.logger.info("Generating data profile statistics")
        
        total_records = df.count()
        
        # Calculate null counts
        null_count = 0
        field_completeness = {}
        for col_name in df.columns:
            col_null_count = df.filter(F.col(col_name).isNull()).count()
            null_count += col_null_count
            completeness = ((total_records - col_null_count) / total_records * 100) if total_records > 0 else 0
            field_completeness[col_name] = completeness
        
        # Calculate duplicate count
        primary_key = self.config.get('primary_key', 'id')
        unique_count = df.select(primary_key).distinct().count()
        duplicate_count = total_records - unique_count
        
        # Calculate numeric statistics
        if 'value' in df.columns:
            value_stats = df.select(
                F.min('value').alias('min_value'),
                F.max('value').alias('max_value'),
                F.avg('value').alias('avg_value'),
                F.stddev('value').alias('std_deviation')
            ).collect()[0]
            
            min_value = float(value_stats['min_value']) if value_stats['min_value'] else 0.0
            max_value = float(value_stats['max_value']) if value_stats['max_value'] else 0.0
            avg_value = float(value_stats['avg_value']) if value_stats['avg_value'] else 0.0
            std_deviation = float(value_stats['std_deviation']) if value_stats['std_deviation'] else 0.0
        else:
            min_value = max_value = avg_value = std_deviation = 0.0
        
        # Count unique categories
        unique_categories = 0
        if 'category' in df.columns:
            unique_categories = df.select('category').distinct().count()
        
        return DataProfile(
            total_records=total_records,
            null_count=null_count,
            duplicate_count=duplicate_count,
            min_value=min_value,
            max_value=max_value,
            avg_value=avg_value,
            std_deviation=std_deviation,
            unique_categories=unique_categories,
            field_completeness=field_completeness
        )
    
    def detect_anomalies(self, df: DataFrame) -> List[str]:
        """
        Detect statistical anomalies in the data
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            List of anomaly descriptions
        """
        self.logger.info("Detecting data anomalies")
        anomalies = []
        
        # Detect outliers using IQR method for numeric columns
        if 'value' in df.columns:
            quantiles = df.approxQuantile('value', [0.25, 0.75], 0.01)
            if len(quantiles) == 2:
                q1, q3 = quantiles
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                
                outlier_count = df.filter(
                    (F.col('value') < lower_bound) | (F.col('value') > upper_bound)
                ).count()
                
                if outlier_count > 0:
                    total_records = df.count()
                    outlier_percentage = (outlier_count / total_records) * 100
                    anomalies.append(
                        f"Value outliers detected: {outlier_count} records ({outlier_percentage:.2f}%) "
                        f"outside range [{lower_bound:.2f}, {upper_bound:.2f}]"
                    )
        
        # Detect unusual category distributions
        if 'category' in df.columns:
            category_dist = df.groupBy('category').count().collect()
            total = df.count()
            
            for row in category_dist:
                percentage = (row['count'] / total) * 100
                if percentage < 1:  # Less than 1% of records
                    anomalies.append(
                        f"Rare category '{row['category']}': only {row['count']} records ({percentage:.2f}%)"
                    )
        
        # Detect sudden changes in record volumes (requires historical data)
        if self.config.get('enable_volume_anomaly_detection', False):
            # This would compare against historical averages
            # Implementation depends on having access to historical run data
            pass
        
        return anomalies
    
    def validate_and_report(self, df: DataFrame) -> ValidationResult:
        """
        Execute complete validation suite and generate comprehensive report
        
        Args:
            df: DataFrame to validate
            
        Returns:
            ValidationResult with all checks, profile, and anomalies
        """
        timestamp = datetime.now()
        
        # Execute all quality checks
        checks = self.perform_quality_checks(df)
        
        # Generate data profile
        profile = self.profile_data(df)
        
        # Detect anomalies
        anomalies = self.detect_anomalies(df)
        
        # Calculate overall pass/fail
        failed_checks = [c for c in checks if not c.passed]
        overall_passed = len(failed_checks) == 0
        
        total_records = profile.total_records
        total_failed = sum(c.failed_count for c in checks)
        error_rate = (total_failed / total_records * 100) if total_records > 0 else 0
        
        result = ValidationResult(
            run_id=self.run_id,
            timestamp=timestamp,
            checks=checks,
            profile=profile,
            anomalies=anomalies,
            overall_passed=overall_passed,
            error_rate=error_rate
        )
        
        # Log summary
        self.logger.info(f"Validation complete - Overall: {'PASSED' if overall_passed else 'FAILED'}")
        self.logger.info(f"Error rate: {error_rate:.2f}%")
        self.logger.info(f"Failed checks: {len(failed_checks)}/{len(checks)}")
        self.logger.info(f"Anomalies detected: {len(anomalies)}")
        
        return result
    
    def generate_quality_report(self, result: ValidationResult) -> str:
        """
        Generate human-readable quality report
        
        Args:
            result: ValidationResult to report on
            
        Returns:
            Formatted report string
        """
        report_lines = [
            "=" * 80,
            f"DATA QUALITY VALIDATION REPORT",
            f"Run ID: {result.run_id}",
            f"Timestamp: {result.timestamp}",
            "=" * 80,
            "",
            f"OVERALL STATUS: {'✓ PASSED' if result.overall_passed else '✗ FAILED'}",
            f"Error Rate: {result.error_rate:.2f}%",
            "",
            "=" * 80,
            "QUALITY CHECKS",
            "=" * 80,
        ]
        
        for check in result.checks:
            status = "✓ PASS" if check.passed else "✗ FAIL"
            report_lines.append(f"\n{check.check_name} [{check.check_type}]: {status}")
            report_lines.append(f"  {check.message}")
            if check.failed_count > 0:
                report_lines.append(f"  Failed records: {check.failed_count}")
        
        report_lines.extend([
            "",
            "=" * 80,
            "DATA PROFILE",
            "=" * 80,
            f"Total Records: {result.profile.total_records:,}",
            f"Null Values: {result.profile.null_count:,}",
            f"Duplicate Records: {result.profile.duplicate_count:,}",
            f"Unique Categories: {result.profile.unique_categories}",
            "",
            f"Value Statistics:",
            f"  Min: {result.profile.min_value:.2f}",
            f"  Max: {result.profile.max_value:.2f}",
            f"  Average: {result.profile.avg_value:.2f}",
            f"  Std Dev: {result.profile.std_deviation:.2f}",
            "",
            "Field Completeness:",
        ])
        
        for field, completeness in result.profile.field_completeness.items():
            report_lines.append(f"  {field}: {completeness:.2f}%")
        
        if result.anomalies:
            report_lines.extend([
                "",
                "=" * 80,
                "ANOMALIES DETECTED",
                "=" * 80,
            ])
            for anomaly in result.anomalies:
                report_lines.append(f"⚠ {anomaly}")
        
        report_lines.append("=" * 80)
        
        return "\n".join(report_lines)


def create_quality_validator(spark: SparkSession, config: Dict, run_id: str) -> DataQualityValidator:
    """
    Factory function to create a configured DataQualityValidator
    
    Args:
        spark: Active SparkSession
        config: Configuration dictionary
        run_id: Unique run identifier
        
    Returns:
        Configured DataQualityValidator instance
    """
    return DataQualityValidator(spark, config, run_id)