"""
PySpark Data Quality Validation Framework
Implements comprehensive data quality checks including completeness, uniqueness, validity, and consistency
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, field
from datetime import datetime
import logging


@dataclass
class QualityCheck:
    """Data class representing a single quality check result"""
    check_name: str
    check_type: str
    passed: bool
    failed_count: int
    message: str
    details: Dict = field(default_factory=dict)


@dataclass
class DataProfile:
    """Data class representing data profiling results"""
    total_records: int
    null_count: int
    duplicate_count: int
    min_value: float
    max_value: float
    avg_value: float
    std_deviation: float
    unique_categories: int
    additional_metrics: Dict = field(default_factory=dict)


class DataQualityValidator:
    """
    Comprehensive data quality validation framework for PySpark
    Provides null/empty field checks, duplicate detection, and business rule validations
    """
    
    def __init__(self, spark: SparkSession, run_id: str, config: Dict):
        """
        Initialize the data quality validator
        
        Args:
            spark: SparkSession instance
            run_id: Unique identifier for this run
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Define critical fields that should not be null
        self.critical_fields = config.get('critical_fields', [
            'id', 'name', 'value'
        ])
        
        # Define valid value ranges
        self.value_ranges = config.get('value_ranges', {
            'value': {'min': 0, 'max': 1000000},
            'transformed_value': {'min': 0, 'max': 1000000},
            'priority': {'min': 1, 'max': 5}
        })
        
        # Define valid categories
        self.valid_categories = config.get('valid_categories', [
            'PREMIUM', 'STANDARD', 'BASIC', 'VIP', 'TRIAL', 'UNCATEGORIZED'
        ])
    
    def perform_quality_checks(self, df: DataFrame) -> List[QualityCheck]:
        """
        Perform all quality checks on the DataFrame
        
        Args:
            df: Input DataFrame to validate
            
        Returns:
            List of QualityCheck results
        """
        self.logger.info(f"Starting data quality checks for run {self.run_id}")
        
        checks = []
        
        # Run all quality check methods
        checks.append(self.check_completeness(df))
        checks.append(self.check_uniqueness(df))
        checks.append(self.check_validity(df))
        checks.append(self.check_consistency(df))
        checks.append(self.check_value_ranges(df))
        checks.append(self.check_category_validity(df))
        checks.append(self.check_data_freshness(df))
        
        # Count passed/failed
        passed_count = sum(1 for check in checks if check.passed)
        failed_count = len(checks) - passed_count
        
        self.logger.info(
            f"Quality checks complete: {passed_count} passed, {failed_count} failed"
        )
        
        return checks
    
    def check_completeness(self, df: DataFrame) -> QualityCheck:
        """
        Check for null or empty values in critical fields
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Running completeness check")
        
        # Build condition to check for null or empty in critical fields
        null_conditions = []
        for field in self.critical_fields:
            if field in df.columns:
                null_conditions.append(
                    F.col(field).isNull() | 
                    (F.col(field) == "") |
                    (F.trim(F.col(field)) == "")
                )
        
        if not null_conditions:
            return QualityCheck(
                check_name="Completeness Check",
                check_type="COMPLETENESS",
                passed=True,
                failed_count=0,
                message="No critical fields to check"
            )
        
        # Count records with any null/empty critical field
        combined_condition = null_conditions[0]
        for condition in null_conditions[1:]:
            combined_condition = combined_condition | condition
        
        null_count = df.filter(combined_condition).count()
        
        # Get detailed breakdown by field
        field_null_counts = {}
        for field in self.critical_fields:
            if field in df.columns:
                field_null_count = df.filter(
                    F.col(field).isNull() | 
                    (F.col(field) == "") |
                    (F.trim(F.col(field)) == "")
                ).count()
                field_null_counts[field] = field_null_count
        
        passed = null_count == 0
        message = (
            "All required fields are complete" if passed 
            else f"{null_count} records with incomplete data"
        )
        
        return QualityCheck(
            check_name="Completeness Check",
            check_type="COMPLETENESS",
            passed=passed,
            failed_count=null_count,
            message=message,
            details={'field_null_counts': field_null_counts}
        )
    
    def check_uniqueness(self, df: DataFrame) -> QualityCheck:
        """
        Check for duplicate IDs
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Running uniqueness check")
        
        if 'id' not in df.columns:
            return QualityCheck(
                check_name="Uniqueness Check",
                check_type="UNIQUENESS",
                passed=True,
                failed_count=0,
                message="No ID field to check"
            )
        
        total_count = df.count()
        unique_count = df.select('id').distinct().count()
        duplicate_count = total_count - unique_count
        
        # Get examples of duplicate IDs
        duplicate_ids = []
        if duplicate_count > 0:
            duplicate_df = (
                df.groupBy('id')
                .count()
                .filter(F.col('count') > 1)
                .orderBy(F.desc('count'))
                .limit(10)
            )
            duplicate_ids = [
                {'id': row['id'], 'count': row['count']} 
                for row in duplicate_df.collect()
            ]
        
        passed = duplicate_count == 0
        message = (
            "All IDs are unique" if passed 
            else f"{duplicate_count} duplicate IDs found"
        )
        
        return QualityCheck(
            check_name="Uniqueness Check",
            check_type="UNIQUENESS",
            passed=passed,
            failed_count=duplicate_count,
            message=message,
            details={
                'total_records': total_count,
                'unique_records': unique_count,
                'duplicate_examples': duplicate_ids
            }
        )
    
    def check_validity(self, df: DataFrame) -> QualityCheck:
        """
        Check for invalid values (negative numbers, invalid priorities, etc.)
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Running validity check")
        
        invalid_conditions = []
        
        # Check for negative values
        if 'value' in df.columns:
            invalid_conditions.append(F.col('value') < 0)
        
        if 'transformed_value' in df.columns:
            invalid_conditions.append(F.col('transformed_value') < 0)
        
        # Check for invalid priority
        if 'priority' in df.columns:
            invalid_conditions.append(
                (F.col('priority') < 1) | (F.col('priority') > 5)
            )
        
        # Check for empty category
        if 'category' in df.columns:
            invalid_conditions.append(
                F.col('category').isNull() | 
                (F.col('category') == "") |
                (F.trim(F.col('category')) == "")
            )
        
        if not invalid_conditions:
            return QualityCheck(
                check_name="Validity Check",
                check_type="VALIDITY",
                passed=True,
                failed_count=0,
                message="No validity rules to check"
            )
        
        # Combine all conditions
        combined_condition = invalid_conditions[0]
        for condition in invalid_conditions[1:]:
            combined_condition = combined_condition | condition
        
        invalid_count = df.filter(combined_condition).count()
        
        passed = invalid_count == 0
        message = (
            "All values are valid" if passed 
            else f"{invalid_count} records with invalid values"
        )
        
        return QualityCheck(
            check_name="Validity Check",
            check_type="VALIDITY",
            passed=passed,
            failed_count=invalid_count,
            message=message
        )
    
    def check_consistency(self, df: DataFrame) -> QualityCheck:
        """
        Check for data consistency (e.g., transformed_value should be >= value)
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Running consistency check")
        
        inconsistent_conditions = []
        
        # Check if transformed_value makes sense relative to value
        if 'value' in df.columns and 'transformed_value' in df.columns:
            # Transformed value should generally be within reasonable range of original
            # For example, not more than 10x the original value
            inconsistent_conditions.append(
                (F.col('transformed_value') > F.col('value') * 10) |
                (F.col('transformed_value') < F.col('value') * 0.1)
            )
        
        # Check status consistency with value ranges
        if 'status' in df.columns and 'transformed_value' in df.columns:
            # HIGH_VALUE status should have high transformed_value
            inconsistent_conditions.append(
                (F.col('status') == 'HIGH_VALUE') & (F.col('transformed_value') < 750)
            )
            inconsistent_conditions.append(
                (F.col('status') == 'LOW_VALUE') & (F.col('transformed_value') >= 300)
            )
        
        # Check priority consistency with transformed_value
        if 'priority' in df.columns and 'transformed_value' in df.columns:
            # Priority 1 should have high values
            inconsistent_conditions.append(
                (F.col('priority') == 1) & (F.col('transformed_value') < 1000)
            )
        
        if not inconsistent_conditions:
            return QualityCheck(
                check_name="Consistency Check",
                check_type="CONSISTENCY",
                passed=True,
                failed_count=0,
                message="No consistency rules to check"
            )
        
        # Combine all conditions
        combined_condition = inconsistent_conditions[0]
        for condition in inconsistent_conditions[1:]:
            combined_condition = combined_condition | condition
        
        inconsistent_count = df.filter(combined_condition).count()
        
        passed = inconsistent_count == 0
        message = (
            "All values are consistent" if passed 
            else f"{inconsistent_count} records with inconsistent values"
        )
        
        return QualityCheck(
            check_name="Consistency Check",
            check_type="CONSISTENCY",
            passed=passed,
            failed_count=inconsistent_count,
            message=message
        )
    
    def check_value_ranges(self, df: DataFrame) -> QualityCheck:
        """
        Check if values are within expected ranges
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Running value range check")
        
        out_of_range_conditions = []
        
        for field, range_config in self.value_ranges.items():
            if field in df.columns:
                min_val = range_config.get('min')
                max_val = range_config.get('max')
                
                if min_val is not None:
                    out_of_range_conditions.append(F.col(field) < min_val)
                if max_val is not None:
                    out_of_range_conditions.append(F.col(field) > max_val)
        
        if not out_of_range_conditions:
            return QualityCheck(
                check_name="Value Range Check",
                check_type="VALUE_RANGE",
                passed=True,
                failed_count=0,
                message="No value ranges to check"
            )
        
        combined_condition = out_of_range_conditions[0]
        for condition in out_of_range_conditions[1:]:
            combined_condition = combined_condition | condition
        
        out_of_range_count = df.filter(combined_condition).count()
        
        passed = out_of_range_count == 0
        message = (
            "All values within expected ranges" if passed 
            else f"{out_of_range_count} records with values out of range"
        )
        
        return QualityCheck(
            check_name="Value Range Check",
            check_type="VALUE_RANGE",
            passed=passed,
            failed_count=out_of_range_count,
            message=message
        )
    
    def check_category_validity(self, df: DataFrame) -> QualityCheck:
        """
        Check if categories are from valid set
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Running category validity check")
        
        if 'category' not in df.columns:
            return QualityCheck(
                check_name="Category Validity Check",
                check_type="CATEGORY_VALIDITY",
                passed=True,
                failed_count=0,
                message="No category field to check"
            )
        
        invalid_category_count = df.filter(
            ~F.col('category').isin(self.valid_categories)
        ).count()
        
        # Get examples of invalid categories
        invalid_categories = []
        if invalid_category_count > 0:
            invalid_df = (
                df.filter(~F.col('category').isin(self.valid_categories))
                .groupBy('category')
                .count()
                .orderBy(F.desc('count'))
                .limit(10)
            )
            invalid_categories = [
                {'category': row['category'], 'count': row['count']} 
                for row in invalid_df.collect()
            ]
        
        passed = invalid_category_count == 0
        message = (
            "All categories are valid" if passed 
            else f"{invalid_category_count} records with invalid categories"
        )
        
        return QualityCheck(
            check_name="Category Validity Check",
            check_type="CATEGORY_VALIDITY",
            passed=passed,
            failed_count=invalid_category_count,
            message=message,
            details={'invalid_categories': invalid_categories}
        )
    
    def check_data_freshness(self, df: DataFrame) -> QualityCheck:
        """
        Check if data is fresh (recently processed)
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Running data freshness check")
        
        if 'processed_at' not in df.columns:
            return QualityCheck(
                check_name="Data Freshness Check",
                check_type="FRESHNESS",
                passed=True,
                failed_count=0,
                message="No timestamp field to check"
            )
        
        # Check for records older than configured threshold (default 24 hours)
        freshness_hours = self.config.get('freshness_threshold_hours', 24)
        threshold = F.current_timestamp() - F.expr(f"INTERVAL {freshness_hours} HOURS")
        
        stale_count = df.filter(F.col('processed_at') < threshold).count()
        
        passed = stale_count == 0
        message = (
            f"All data processed within {freshness_hours} hours" if passed 
            else f"{stale_count} stale records (older than {freshness_hours} hours)"
        )
        
        return QualityCheck(
            check_name="Data Freshness Check",
            check_type="FRESHNESS",
            passed=passed,
            failed_count=stale_count,
            message=message
        )
    
    def profile_data(self, df: DataFrame) -> DataProfile:
        """
        Generate comprehensive data profile
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataProfile with statistics
        """
        self.logger.info("Generating data profile")
        
        # Basic counts
        total_records = df.count()
        
        # Count nulls across all columns
        null_counts = {}
        for col_name in df.columns:
            null_count = df.filter(F.col(col_name).isNull()).count()
            null_counts[col_name] = null_count
        
        total_null_count = sum(null_counts.values())
        
        # Check for duplicates
        duplicate_count = 0
        if 'id' in df.columns:
            total = df.count()
            unique = df.select('id').distinct().count()
            duplicate_count = total - unique
        
        # Statistics on numeric columns
        stats = {}
        if 'transformed_value' in df.columns:
            stats_df = df.select(
                F.min('transformed_value').alias('min_value'),
                F.max('transformed_value').alias('max_value'),
                F.avg('transformed_value').alias('avg_value'),
                F.stddev('transformed_value').alias('std_dev')
            ).first()
            
            min_value = stats_df['min_value'] if stats_df['min_value'] is not None else 0.0
            max_value = stats_df['max_value'] if stats_df['max_value'] is not None else 0.0
            avg_value = stats_df['avg_value'] if stats_df['avg_value'] is not None else 0.0
            std_dev = stats_df['std_dev'] if stats_df['std_dev'] is not None else 0.0
        else:
            min_value = max_value = avg_value = std_dev = 0.0
        
        # Count unique categories
        unique_categories = 0
        if 'category' in df.columns:
            unique_categories = df.select('category').distinct().count()
        
        # Additional metrics
        additional_metrics = {
            'null_counts_by_field': null_counts,
            'total_columns': len(df.columns),
            'column_names': df.columns
        }
        
        return DataProfile(
            total_records=total_records,
            null_count=total_null_count,
            duplicate_count=duplicate_count,
            min_value=float(min_value),
            max_value=float(max_value),
            avg_value=float(avg_value),
            std_deviation=float(std_dev),
            unique_categories=unique_categories,
            additional_metrics=additional_metrics
        )
    
    def detect_anomalies(self, df: DataFrame) -> List[str]:
        """
        Detect anomalies in the data using statistical methods
        
        Args:
            df: Input DataFrame
            
        Returns:
            List of anomaly descriptions
        """
        self.logger.info("Detecting data anomalies")
        
        anomalies = []
        
        if 'transformed_value' not in df.columns:
            return anomalies
        
        # Calculate statistics
        stats = df.select(
            F.avg('transformed_value').alias('mean'),
            F.stddev('transformed_value').alias('stddev')
        ).first()
        
        mean = stats['mean']
        stddev = stats['stddev']
        
        if mean is None or stddev is None:
            return anomalies
        
        # Find outliers (values beyond 3 standard deviations)
        outlier_threshold = self.config.get('outlier_std_devs', 3)
        lower_bound = mean - (outlier_threshold * stddev)
        upper_bound = mean + (outlier_threshold * stddev)
        
        outlier_count = df.filter(
            (F.col('transformed_value') < lower_bound) | 
            (F.col('transformed_value') > upper_bound)
        ).count()
        
        if outlier_count > 0:
            anomalies.append(
                f"Found {outlier_count} outliers beyond {outlier_threshold} standard deviations "
                f"(mean={mean:.2f}, stddev={stddev:.2f})"
            )
        
        # Check for sudden spike in record count
        if 'processed_at' in df.columns:
            # Group by hour and count
            hourly_counts = df.groupBy(
                F.date_trunc('hour', 'processed_at').alias('hour')
            ).count()
            
            hourly_stats = hourly_counts.select(
                F.avg('count').alias('avg_count'),
                F.stddev('count').alias('stddev_count')
            ).first()
            
            if hourly_stats['avg_count'] is not None and hourly_stats['stddev_count'] is not None:
                spike_threshold = hourly_stats['avg_count'] + (3 * hourly_stats['stddev_count'])
                
                spike_count = hourly_counts.filter(
                    F.col('count') > spike_threshold
                ).count()
                
                if spike_count > 0:
                    anomalies.append(
                        f"Found {spike_count} hours with unusually high record counts"
                    )
        
        # Check for suspicious patterns in categories
        if 'category' in df.columns:
            category_dist = df.groupBy('category').count()
            total = df.count()
            
            for row in category_dist.collect():
                percentage = (row['count'] / total) * 100
                
                # Flag if any category has more than 80% or less than 1%
                if percentage > 80:
                    anomalies.append(
                        f"Category '{row['category']}' dominates with {percentage:.1f}% of records"
                    )
                elif percentage < 1 and row['count'] > 10:
                    anomalies.append(
                        f"Category '{row['category']}' has unusually low representation: {percentage:.2f}%"
                    )
        
        self.logger.info(f"Detected {len(anomalies)} anomalies")
        return anomalies
    
    def generate_quality_report(self, df: DataFrame) -> Dict:
        """
        Generate comprehensive quality report
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary containing all quality metrics
        """
        self.logger.info("Generating comprehensive quality report")
        
        checks = self.perform_quality_checks(df)
        profile = self.profile_data(df)
        anomalies = self.detect_anomalies(df)
        
        # Calculate overall quality score
        passed_checks = sum(1 for check in checks if check.passed)
        total_checks = len(checks)
        quality_score = (passed_checks / total_checks * 100) if total_checks > 0 else 0
        
        report = {
            'run_id': self.run_id,
            'timestamp': datetime.now().isoformat(),
            'quality_score': quality_score,
            'checks': [
                {
                    'check_name': check.check_name,
                    'check_type': check.check_type,
                    'passed': check.passed,
                    'failed_count': check.failed_count,
                    'message': check.message,
                    'details': check.details
                }
                for check in checks
            ],
            'profile': {
                'total_records': profile.total_records,
                'null_count': profile.null_count,
                'duplicate_count': profile.duplicate_count,
                'min_value': profile.min_value,
                'max_value': profile.max_value,
                'avg_value': profile.avg_value,
                'std_deviation': profile.std_deviation,
                'unique_categories': profile.unique_categories,
                'additional_metrics': profile.additional_metrics
            },
            'anomalies': anomalies,
            'summary': {
                'total_checks': total_checks,
                'passed_checks': passed_checks,
                'failed_checks': total_checks - passed_checks,
                'has_anomalies': len(anomalies) > 0
            }
        }
        
        return report


def create_validator(spark: SparkSession, run_id: str, config_path: str = None) -> DataQualityValidator:
    """
    Factory function to create DataQualityValidator instance
    
    Args:
        spark: SparkSession instance
        run_id: Unique run identifier
        config_path: Optional path to config file
        
    Returns:
        DataQualityValidator instance
    """
    if config_path:
        import yaml
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f).get('data_quality', {})
    else:
        config = {}
    
    return DataQualityValidator(spark, run_id, config)