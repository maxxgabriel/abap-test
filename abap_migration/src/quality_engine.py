"""
Data Quality Validation Module
Executes validation rules, generates profiling statistics, and validates record counts.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, field
from datetime import datetime
import logging
import yaml


@dataclass
class QualityCheck:
    """Represents a single quality check result"""
    check_name: str
    check_type: str
    passed: bool
    failed_count: int
    message: str
    details: Optional[Dict] = None


@dataclass
class DataProfile:
    """Data profiling statistics"""
    total_records: int
    null_count: int
    duplicate_count: int
    min_value: float
    max_value: float
    avg_value: float
    std_deviation: float
    unique_categories: int
    profile_time: datetime = field(default_factory=datetime.now)


@dataclass
class ValidationResult:
    """Complete validation result"""
    run_id: str
    checks: List[QualityCheck]
    profile: DataProfile
    anomalies: List[str]
    overall_passed: bool
    validation_time: datetime = field(default_factory=datetime.now)


class DataQualityEngine:
    """
    Engine for data quality validation and profiling.
    Executes configurable validation rules and generates statistics.
    """
    
    def __init__(self, spark: SparkSession, config: Dict, run_id: str):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        
        # Load quality rules from config
        self.quality_rules = config.get('quality_rules', {})
        self.thresholds = config.get('thresholds', {})
        
    def perform_quality_checks(self, df: DataFrame) -> ValidationResult:
        """
        Execute all quality checks on the DataFrame
        
        Args:
            df: Input DataFrame to validate
            
        Returns:
            ValidationResult with all checks and statistics
        """
        self.logger.info(f"Starting quality checks for run {self.run_id}")
        
        checks = []
        
        # Execute all validation rules
        checks.append(self.check_completeness(df))
        checks.append(self.check_uniqueness(df))
        checks.append(self.check_validity(df))
        checks.append(self.check_consistency(df))
        checks.append(self.check_record_count(df))
        checks.append(self.check_data_types(df))
        checks.append(self.check_value_ranges(df))
        
        # Generate data profile
        profile = self.profile_data(df)
        
        # Detect anomalies
        anomalies = self.detect_anomalies(df, profile)
        
        # Determine overall result
        overall_passed = all(check.passed for check in checks)
        
        result = ValidationResult(
            run_id=self.run_id,
            checks=checks,
            profile=profile,
            anomalies=anomalies,
            overall_passed=overall_passed
        )
        
        self._log_validation_summary(result)
        
        return result
    
    def check_completeness(self, df: DataFrame) -> QualityCheck:
        """
        Check for null/empty values in critical fields
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Executing completeness check")
        
        required_fields = self.quality_rules.get('required_fields', [])
        null_threshold = self.thresholds.get('null_threshold_percent', 5)
        
        total_records = df.count()
        null_counts = {}
        failed_count = 0
        
        for field in required_fields:
            if field in df.columns:
                null_count = df.filter(F.col(field).isNull() | (F.col(field) == '')).count()
                null_percent = (null_count / total_records * 100) if total_records > 0 else 0
                null_counts[field] = {
                    'count': null_count,
                    'percent': round(null_percent, 2)
                }
                
                if null_percent > null_threshold:
                    failed_count += 1
        
        passed = failed_count == 0
        message = (f"All required fields complete" if passed 
                  else f"{failed_count} fields exceed null threshold ({null_threshold}%)")
        
        return QualityCheck(
            check_name="Completeness Check",
            check_type="COMPLETENESS",
            passed=passed,
            failed_count=failed_count,
            message=message,
            details={'null_counts': null_counts, 'threshold': null_threshold}
        )
    
    def check_uniqueness(self, df: DataFrame) -> QualityCheck:
        """
        Check for duplicate records based on key fields
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Executing uniqueness check")
        
        key_fields = self.quality_rules.get('unique_keys', ['id'])
        max_duplicates = self.thresholds.get('max_duplicate_percent', 0)
        
        total_records = df.count()
        
        # Count distinct records
        distinct_count = df.select(key_fields).distinct().count()
        duplicate_count = total_records - distinct_count
        duplicate_percent = (duplicate_count / total_records * 100) if total_records > 0 else 0
        
        passed = duplicate_percent <= max_duplicates
        message = (f"All records unique" if duplicate_count == 0
                  else f"Found {duplicate_count} duplicates ({duplicate_percent:.2f}%)")
        
        return QualityCheck(
            check_name="Uniqueness Check",
            check_type="UNIQUENESS",
            passed=passed,
            failed_count=duplicate_count,
            message=message,
            details={
                'total_records': total_records,
                'distinct_records': distinct_count,
                'duplicate_count': duplicate_count,
                'duplicate_percent': round(duplicate_percent, 2)
            }
        )
    
    def check_validity(self, df: DataFrame) -> QualityCheck:
        """
        Check for invalid values based on business rules
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Executing validity check")
        
        validity_rules = self.quality_rules.get('validity_rules', {})
        invalid_count = 0
        invalid_details = {}
        
        # Check numeric ranges
        if 'value' in df.columns:
            min_value = validity_rules.get('min_value', 0)
            max_value = validity_rules.get('max_value', float('inf'))
            
            invalid_values = df.filter(
                (F.col('value') < min_value) | (F.col('value') > max_value)
            ).count()
            
            if invalid_values > 0:
                invalid_count += invalid_values
                invalid_details['value_range'] = invalid_values
        
        # Check priority range
        if 'priority' in df.columns:
            invalid_priority = df.filter(
                (F.col('priority') < 1) | (F.col('priority') > 5)
            ).count()
            
            if invalid_priority > 0:
                invalid_count += invalid_priority
                invalid_details['priority_range'] = invalid_priority
        
        # Check category values
        if 'category' in df.columns:
            valid_categories = validity_rules.get('valid_categories', [])
            if valid_categories:
                invalid_category = df.filter(
                    ~F.col('category').isin(valid_categories)
                ).count()
                
                if invalid_category > 0:
                    invalid_count += invalid_category
                    invalid_details['invalid_category'] = invalid_category
        
        passed = invalid_count == 0
        message = (f"All values valid" if passed 
                  else f"Found {invalid_count} invalid values")
        
        return QualityCheck(
            check_name="Validity Check",
            check_type="VALIDITY",
            passed=passed,
            failed_count=invalid_count,
            message=message,
            details=invalid_details
        )
    
    def check_consistency(self, df: DataFrame) -> QualityCheck:
        """
        Check for data consistency across related fields
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Executing consistency check")
        
        inconsistent_count = 0
        inconsistency_details = {}
        
        # Check transformed_value consistency
        if 'value' in df.columns and 'transformed_value' in df.columns:
            # Transformed value should be greater than or equal to original
            inconsistent_transforms = df.filter(
                F.col('transformed_value') < F.col('value') * 0.5
            ).count()
            
            if inconsistent_transforms > 0:
                inconsistent_count += inconsistent_transforms
                inconsistency_details['transform_logic'] = inconsistent_transforms
        
        # Check status consistency with value
        if 'status' in df.columns and 'transformed_value' in df.columns:
            # HIGH_VALUE status should have high transformed values
            inconsistent_status = df.filter(
                (F.col('status') == 'HIGH_VALUE') & (F.col('transformed_value') < 750)
            ).count()
            
            if inconsistent_status > 0:
                inconsistent_count += inconsistent_status
                inconsistency_details['status_value_mismatch'] = inconsistent_status
        
        # Check timestamp consistency
        if 'processed_at' in df.columns:
            future_timestamps = df.filter(
                F.col('processed_at') > F.current_timestamp()
            ).count()
            
            if future_timestamps > 0:
                inconsistent_count += future_timestamps
                inconsistency_details['future_timestamps'] = future_timestamps
        
        passed = inconsistent_count == 0
        message = (f"Data is consistent" if passed 
                  else f"Found {inconsistent_count} inconsistencies")
        
        return QualityCheck(
            check_name="Consistency Check",
            check_type="CONSISTENCY",
            passed=passed,
            failed_count=inconsistent_count,
            message=message,
            details=inconsistency_details
        )
    
    def check_record_count(self, df: DataFrame) -> QualityCheck:
        """
        Validate record count against expected thresholds
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Executing record count check")
        
        actual_count = df.count()
        expected_min = self.thresholds.get('min_record_count', 0)
        expected_max = self.thresholds.get('max_record_count', float('inf'))
        
        passed = expected_min <= actual_count <= expected_max
        
        if actual_count < expected_min:
            message = f"Record count {actual_count} below minimum threshold {expected_min}"
            failed_count = expected_min - actual_count
        elif actual_count > expected_max:
            message = f"Record count {actual_count} above maximum threshold {expected_max}"
            failed_count = actual_count - expected_max
        else:
            message = f"Record count {actual_count} within expected range"
            failed_count = 0
        
        return QualityCheck(
            check_name="Record Count Check",
            check_type="RECORD_COUNT",
            passed=passed,
            failed_count=failed_count,
            message=message,
            details={
                'actual_count': actual_count,
                'expected_min': expected_min,
                'expected_max': expected_max
            }
        )
    
    def check_data_types(self, df: DataFrame) -> QualityCheck:
        """
        Validate data types match expected schema
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Executing data type check")
        
        expected_types = self.quality_rules.get('expected_types', {})
        type_mismatches = []
        
        for field, expected_type in expected_types.items():
            if field in df.columns:
                actual_type = dict(df.dtypes)[field]
                if actual_type != expected_type:
                    type_mismatches.append({
                        'field': field,
                        'expected': expected_type,
                        'actual': actual_type
                    })
        
        passed = len(type_mismatches) == 0
        message = (f"All data types correct" if passed 
                  else f"Found {len(type_mismatches)} type mismatches")
        
        return QualityCheck(
            check_name="Data Type Check",
            check_type="DATA_TYPE",
            passed=passed,
            failed_count=len(type_mismatches),
            message=message,
            details={'mismatches': type_mismatches}
        )
    
    def check_value_ranges(self, df: DataFrame) -> QualityCheck:
        """
        Check if numeric values fall within acceptable ranges
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Executing value range check")
        
        range_rules = self.quality_rules.get('value_ranges', {})
        out_of_range_count = 0
        range_violations = {}
        
        for field, range_config in range_rules.items():
            if field in df.columns:
                min_val = range_config.get('min', float('-inf'))
                max_val = range_config.get('max', float('inf'))
                
                violations = df.filter(
                    (F.col(field) < min_val) | (F.col(field) > max_val)
                ).count()
                
                if violations > 0:
                    out_of_range_count += violations
                    range_violations[field] = {
                        'violations': violations,
                        'min': min_val,
                        'max': max_val
                    }
        
        passed = out_of_range_count == 0
        message = (f"All values in range" if passed 
                  else f"Found {out_of_range_count} out-of-range values")
        
        return QualityCheck(
            check_name="Value Range Check",
            check_type="VALUE_RANGE",
            passed=passed,
            failed_count=out_of_range_count,
            message=message,
            details=range_violations
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
        
        # Calculate null counts across all columns
        null_counts = {}
        for col_name in df.columns:
            null_count = df.filter(F.col(col_name).isNull()).count()
            null_counts[col_name] = null_count
        
        total_nulls = sum(null_counts.values())
        
        # Calculate duplicate count
        key_fields = self.quality_rules.get('unique_keys', ['id'])
        distinct_count = df.select(key_fields).distinct().count()
        duplicate_count = total_records - distinct_count
        
        # Statistical measures for numeric columns
        stats = df.select(
            F.min('value').alias('min_value'),
            F.max('value').alias('max_value'),
            F.avg('value').alias('avg_value'),
            F.stddev('value').alias('std_deviation')
        ).collect()[0]
        
        # Count unique categories
        unique_categories = 0
        if 'category' in df.columns:
            unique_categories = df.select('category').distinct().count()
        
        profile = DataProfile(
            total_records=total_records,
            null_count=total_nulls,
            duplicate_count=duplicate_count,
            min_value=float(stats['min_value']) if stats['min_value'] else 0.0,
            max_value=float(stats['max_value']) if stats['max_value'] else 0.0,
            avg_value=float(stats['avg_value']) if stats['avg_value'] else 0.0,
            std_deviation=float(stats['std_deviation']) if stats['std_deviation'] else 0.0,
            unique_categories=unique_categories
        )
        
        self.logger.info(f"Profile generated: {total_records} records, "
                        f"{total_nulls} nulls, {duplicate_count} duplicates")
        
        return profile
    
    def detect_anomalies(self, df: DataFrame, profile: DataProfile) -> List[str]:
        """
        Detect statistical anomalies and outliers
        
        Args:
            df: Input DataFrame
            profile: Data profile with statistics
            
        Returns:
            List of anomaly descriptions
        """
        self.logger.info("Detecting anomalies")
        
        anomalies = []
        
        # Check for outliers using standard deviation
        if 'value' in df.columns and profile.std_deviation > 0:
            outlier_threshold = profile.avg_value + (3 * profile.std_deviation)
            outliers = df.filter(F.col('value') > outlier_threshold).count()
            
            if outliers > 0:
                anomalies.append(
                    f"Found {outliers} statistical outliers "
                    f"(> {outlier_threshold:.2f})"
                )
        
        # Check for unusual distribution patterns
        if 'category' in df.columns:
            category_counts = df.groupBy('category').count()
            total = profile.total_records
            
            # Check for highly skewed distributions
            max_category = category_counts.agg(F.max('count')).collect()[0][0]
            if max_category and (max_category / total) > 0.8:
                anomalies.append(
                    f"Highly skewed category distribution: "
                    f"one category represents {max_category/total*100:.1f}% of data"
                )
        
        # Check for sudden changes in value patterns
        if 'transformed_value' in df.columns:
            transform_ratio = df.select(
                (F.col('transformed_value') / F.col('value')).alias('ratio')
            ).filter(F.col('value') > 0)
            
            ratio_stats = transform_ratio.select(
                F.avg('ratio').alias('avg_ratio'),
                F.stddev('ratio').alias('std_ratio')
            ).collect()[0]
            
            if ratio_stats['std_ratio'] and ratio_stats['std_ratio'] > 0.5:
                anomalies.append(
                    f"High variance in transformation ratios "
                    f"(std dev: {ratio_stats['std_ratio']:.2f})"
                )
        
        # Check for timestamp anomalies
        if 'processed_at' in df.columns:
            time_range = df.select(
                F.min('processed_at').alias('min_time'),
                F.max('processed_at').alias('max_time')
            ).collect()[0]
            
            if time_range['min_time'] and time_range['max_time']:
                time_span = (time_range['max_time'] - time_range['min_time']).total_seconds()
                if time_span > 86400:  # More than 24 hours
                    anomalies.append(
                        f"Records span {time_span/3600:.1f} hours, "
                        "possible batch processing delay"
                    )
        
        self.logger.info(f"Detected {len(anomalies)} anomalies")
        
        return anomalies
    
    def _log_validation_summary(self, result: ValidationResult):
        """Log validation summary"""
        passed_count = sum(1 for check in result.checks if check.passed)
        failed_count = len(result.checks) - passed_count
        
        self.logger.info(f"Validation Summary for run {result.run_id}:")
        self.logger.info(f"  Overall Status: {'PASSED' if result.overall_passed else 'FAILED'}")
        self.logger.info(f"  Checks Passed: {passed_count}/{len(result.checks)}")
        self.logger.info(f"  Checks Failed: {failed_count}/{len(result.checks)}")
        self.logger.info(f"  Total Records: {result.profile.total_records}")
        self.logger.info(f"  Anomalies Found: {len(result.anomalies)}")
        
        for check in result.checks:
            status = "✓" if check.passed else "✗"
            self.logger.info(f"  {status} {check.check_name}: {check.message}")


def create_quality_engine(spark: SparkSession, config_path: str, run_id: str) -> DataQualityEngine:
    """
    Factory function to create DataQualityEngine
    
    Args:
        spark: SparkSession
        config_path: Path to configuration file
        run_id: Unique run identifier
        
    Returns:
        Configured DataQualityEngine instance
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return DataQualityEngine(spark, config, run_id)