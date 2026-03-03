"""
PySpark Data Quality Validation Framework
Implements comprehensive data quality checks including null/empty field validation,
duplicate detection, and business rule validations using DataFrame operations.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, field
from datetime import datetime
import logging


@dataclass
class QualityCheckResult:
    """Result of a data quality check"""
    check_name: str
    check_type: str
    passed: bool
    failed_count: int
    message: str
    details: Optional[Dict] = None


@dataclass
class DataProfile:
    """Statistical profile of dataset"""
    total_records: int
    null_count: int
    duplicate_count: int
    min_value: float
    max_value: float
    avg_value: float
    std_deviation: float
    unique_categories: int


class DataQualityValidator:
    """
    Comprehensive data quality validation framework for PySpark DataFrames.
    Performs completeness, uniqueness, validity, and consistency checks.
    """
    
    def __init__(self, spark: SparkSession, run_id: str, config: Dict):
        """
        Initialize data quality validator.
        
        Args:
            spark: SparkSession instance
            run_id: Unique identifier for ETL run
            config: Configuration dictionary with validation rules
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Load validation rules from config
        self.required_fields = config.get('quality', {}).get('required_fields', [])
        self.unique_keys = config.get('quality', {}).get('unique_keys', [])
        self.value_ranges = config.get('quality', {}).get('value_ranges', {})
        self.business_rules = config.get('quality', {}).get('business_rules', {})
        
    def perform_quality_checks(self, df: DataFrame) -> List[QualityCheckResult]:
        """
        Execute all configured data quality checks.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            List of QualityCheckResult objects
        """
        self.logger.info(f"Starting data quality checks for run_id: {self.run_id}")
        
        results = []
        
        # Execute all quality checks
        results.append(self.check_completeness(df))
        results.append(self.check_uniqueness(df))
        results.append(self.check_validity(df))
        results.append(self.check_consistency(df))
        results.append(self.check_business_rules(df))
        
        # Count passed/failed
        passed = sum(1 for r in results if r.passed)
        failed = sum(1 for r in results if not r.passed)
        
        self.logger.info(f"Quality checks complete: {passed} passed, {failed} failed")
        
        return results
    
    def check_completeness(self, df: DataFrame) -> QualityCheckResult:
        """
        Check for null or empty values in required fields.
        
        Args:
            df: DataFrame to check
            
        Returns:
            QualityCheckResult with completeness validation results
        """
        self.logger.info("Performing completeness check")
        
        required_fields = self.required_fields or ['id', 'name', 'value']
        
        # Build condition to check all required fields
        null_conditions = []
        for field in required_fields:
            if field in df.columns:
                # Check for null or empty string
                null_conditions.append(
                    F.col(field).isNull() | 
                    (F.col(field) == '') |
                    (F.trim(F.col(field)) == '')
                )
        
        if not null_conditions:
            return QualityCheckResult(
                check_name="Completeness Check",
                check_type="COMPLETENESS",
                passed=True,
                failed_count=0,
                message="No required fields configured"
            )
        
        # Combine all conditions with OR
        combined_condition = null_conditions[0]
        for condition in null_conditions[1:]:
            combined_condition = combined_condition | condition
        
        # Count records with null/empty required fields
        null_count = df.filter(combined_condition).count()
        
        # Get detailed null counts per field
        null_details = {}
        for field in required_fields:
            if field in df.columns:
                field_null_count = df.filter(
                    F.col(field).isNull() | 
                    (F.col(field) == '') |
                    (F.trim(F.col(field)) == '')
                ).count()
                if field_null_count > 0:
                    null_details[field] = field_null_count
        
        passed = null_count == 0
        message = (
            "All required fields are complete" if passed 
            else f"{null_count} records with incomplete data"
        )
        
        return QualityCheckResult(
            check_name="Completeness Check",
            check_type="COMPLETENESS",
            passed=passed,
            failed_count=null_count,
            message=message,
            details=null_details if null_details else None
        )
    
    def check_uniqueness(self, df: DataFrame) -> QualityCheckResult:
        """
        Check for duplicate records based on unique key fields.
        
        Args:
            df: DataFrame to check
            
        Returns:
            QualityCheckResult with uniqueness validation results
        """
        self.logger.info("Performing uniqueness check")
        
        unique_keys = self.unique_keys or ['id']
        
        # Filter out keys that don't exist in the DataFrame
        existing_keys = [key for key in unique_keys if key in df.columns]
        
        if not existing_keys:
            return QualityCheckResult(
                check_name="Uniqueness Check",
                check_type="UNIQUENESS",
                passed=True,
                failed_count=0,
                message="No unique keys configured or found"
            )
        
        total_count = df.count()
        unique_count = df.select(existing_keys).distinct().count()
        duplicate_count = total_count - unique_count
        
        # Find actual duplicate IDs for reporting
        duplicate_details = {}
        if duplicate_count > 0:
            duplicates_df = (
                df.groupBy(existing_keys)
                .count()
                .filter(F.col('count') > 1)
                .orderBy(F.desc('count'))
                .limit(10)
            )
            
            duplicate_details['sample_duplicates'] = [
                row.asDict() for row in duplicates_df.collect()
            ]
        
        passed = duplicate_count == 0
        message = (
            "All IDs are unique" if passed 
            else f"{duplicate_count} duplicate records found"
        )
        
        return QualityCheckResult(
            check_name="Uniqueness Check",
            check_type="UNIQUENESS",
            passed=passed,
            failed_count=duplicate_count,
            message=message,
            details=duplicate_details if duplicate_details else None
        )
    
    def check_validity(self, df: DataFrame) -> QualityCheckResult:
        """
        Check for invalid values based on defined ranges and constraints.
        
        Args:
            df: DataFrame to check
            
        Returns:
            QualityCheckResult with validity validation results
        """
        self.logger.info("Performing validity check")
        
        invalid_conditions = []
        validation_details = {}
        
        # Check numeric value ranges
        if 'value' in df.columns:
            # Values must be positive
            min_value = self.value_ranges.get('value', {}).get('min', 0)
            max_value = self.value_ranges.get('value', {}).get('max', float('inf'))
            
            invalid_conditions.append(
                (F.col('value') < min_value) | (F.col('value') > max_value)
            )
            
            # Count invalid values
            invalid_value_count = df.filter(
                (F.col('value') < min_value) | (F.col('value') > max_value)
            ).count()
            if invalid_value_count > 0:
                validation_details['invalid_value_count'] = invalid_value_count
        
        if 'transformed_value' in df.columns:
            min_value = self.value_ranges.get('transformed_value', {}).get('min', 0)
            invalid_conditions.append(F.col('transformed_value') < min_value)
            
            invalid_transformed_count = df.filter(
                F.col('transformed_value') < min_value
            ).count()
            if invalid_transformed_count > 0:
                validation_details['invalid_transformed_count'] = invalid_transformed_count
        
        # Check priority range (1-5)
        if 'priority' in df.columns:
            priority_min = self.value_ranges.get('priority', {}).get('min', 1)
            priority_max = self.value_ranges.get('priority', {}).get('max', 5)
            
            invalid_conditions.append(
                (F.col('priority') < priority_min) | 
                (F.col('priority') > priority_max)
            )
            
            invalid_priority_count = df.filter(
                (F.col('priority') < priority_min) | 
                (F.col('priority') > priority_max)
            ).count()
            if invalid_priority_count > 0:
                validation_details['invalid_priority_count'] = invalid_priority_count
        
        # Check category is not null/empty
        if 'category' in df.columns:
            invalid_conditions.append(
                F.col('category').isNull() | 
                (F.col('category') == '')
            )
            
            invalid_category_count = df.filter(
                F.col('category').isNull() | (F.col('category') == '')
            ).count()
            if invalid_category_count > 0:
                validation_details['invalid_category_count'] = invalid_category_count
        
        # Count total invalid records
        if invalid_conditions:
            combined_condition = invalid_conditions[0]
            for condition in invalid_conditions[1:]:
                combined_condition = combined_condition | condition
            
            invalid_count = df.filter(combined_condition).count()
        else:
            invalid_count = 0
        
        passed = invalid_count == 0
        message = (
            "All values are valid" if passed 
            else f"{invalid_count} records with invalid values"
        )
        
        return QualityCheckResult(
            check_name="Validity Check",
            check_type="VALIDITY",
            passed=passed,
            failed_count=invalid_count,
            message=message,
            details=validation_details if validation_details else None
        )
    
    def check_consistency(self, df: DataFrame) -> QualityCheckResult:
        """
        Check for logical consistency between related fields.
        
        Args:
            df: DataFrame to check
            
        Returns:
            QualityCheckResult with consistency validation results
        """
        self.logger.info("Performing consistency check")
        
        inconsistent_conditions = []
        consistency_details = {}
        
        # Check that transformed_value >= value (after transformation)
        if 'value' in df.columns and 'transformed_value' in df.columns:
            # Transformed value should typically be >= original value
            # (depends on business logic, adjust as needed)
            inconsistent_conditions.append(
                F.col('transformed_value') < (F.col('value') * 0.5)
            )
            
            inconsistent_transform_count = df.filter(
                F.col('transformed_value') < (F.col('value') * 0.5)
            ).count()
            if inconsistent_transform_count > 0:
                consistency_details['inconsistent_transformation'] = inconsistent_transform_count
        
        # Check status consistency with value
        if 'status' in df.columns and 'transformed_value' in df.columns:
            # High value items should have appropriate status
            high_value_wrong_status = df.filter(
                (F.col('transformed_value') >= 1000) & 
                (F.col('status') == 'LOW_VALUE')
            ).count()
            
            if high_value_wrong_status > 0:
                consistency_details['status_inconsistency'] = high_value_wrong_status
                inconsistent_conditions.append(
                    (F.col('transformed_value') >= 1000) & 
                    (F.col('status') == 'LOW_VALUE')
                )
        
        # Check priority consistency with value
        if 'priority' in df.columns and 'transformed_value' in df.columns:
            # High value items should have high priority (1 or 2)
            priority_inconsistency = df.filter(
                (F.col('transformed_value') >= 1000) & 
                (F.col('priority') > 2)
            ).count()
            
            if priority_inconsistency > 0:
                consistency_details['priority_inconsistency'] = priority_inconsistency
                inconsistent_conditions.append(
                    (F.col('transformed_value') >= 1000) & 
                    (F.col('priority') > 2)
                )
        
        # Count total inconsistent records
        if inconsistent_conditions:
            combined_condition = inconsistent_conditions[0]
            for condition in inconsistent_conditions[1:]:
                combined_condition = combined_condition | condition
            
            inconsistent_count = df.filter(combined_condition).count()
        else:
            inconsistent_count = 0
        
        passed = inconsistent_count == 0
        message = (
            "All records are logically consistent" if passed 
            else f"{inconsistent_count} records with logical inconsistencies"
        )
        
        return QualityCheckResult(
            check_name="Consistency Check",
            check_type="CONSISTENCY",
            passed=passed,
            failed_count=inconsistent_count,
            message=message,
            details=consistency_details if consistency_details else None
        )
    
    def check_business_rules(self, df: DataFrame) -> QualityCheckResult:
        """
        Validate custom business rules defined in configuration.
        
        Args:
            df: DataFrame to check
            
        Returns:
            QualityCheckResult with business rule validation results
        """
        self.logger.info("Performing business rules check")
        
        rule_violations = []
        rule_details = {}
        
        # Business Rule 1: Premium category must have value >= 500
        if 'category' in df.columns and 'value' in df.columns:
            premium_threshold = self.business_rules.get('premium_min_value', 500)
            premium_violations = df.filter(
                (F.col('category') == 'PREMIUM') & 
                (F.col('value') < premium_threshold)
            ).count()
            
            if premium_violations > 0:
                rule_violations.append(premium_violations)
                rule_details['premium_value_violations'] = premium_violations
        
        # Business Rule 2: VIP category must have priority 1 or 2
        if 'category' in df.columns and 'priority' in df.columns:
            vip_priority_violations = df.filter(
                (F.col('category') == 'VIP') & 
                (F.col('priority') > 2)
            ).count()
            
            if vip_priority_violations > 0:
                rule_violations.append(vip_priority_violations)
                rule_details['vip_priority_violations'] = vip_priority_violations
        
        # Business Rule 3: Name must not contain special characters (beyond basic punctuation)
        if 'name' in df.columns:
            invalid_name_pattern = self.business_rules.get(
                'invalid_name_pattern', 
                r'[^A-Za-z0-9\s\-_.]'
            )
            name_violations = df.filter(
                F.col('name').rlike(invalid_name_pattern)
            ).count()
            
            if name_violations > 0:
                rule_violations.append(name_violations)
                rule_details['name_pattern_violations'] = name_violations
        
        # Business Rule 4: Status must be from allowed list
        if 'status' in df.columns:
            allowed_statuses = self.business_rules.get(
                'allowed_statuses',
                ['ACTIVE', 'TRANSFORMED', 'HIGH_VALUE', 'MEDIUM_VALUE', 'LOW_VALUE', 'NORMAL']
            )
            status_violations = df.filter(
                ~F.col('status').isin(allowed_statuses)
            ).count()
            
            if status_violations > 0:
                rule_violations.append(status_violations)
                rule_details['invalid_status_violations'] = status_violations
        
        total_violations = sum(rule_violations)
        passed = total_violations == 0
        message = (
            "All business rules satisfied" if passed 
            else f"{total_violations} business rule violations found"
        )
        
        return QualityCheckResult(
            check_name="Business Rules Check",
            check_type="BUSINESS_RULES",
            passed=passed,
            failed_count=total_violations,
            message=message,
            details=rule_details if rule_details else None
        )
    
    def profile_data(self, df: DataFrame) -> DataProfile:
        """
        Generate statistical profile of the dataset.
        
        Args:
            df: DataFrame to profile
            
        Returns:
            DataProfile with statistical summary
        """
        self.logger.info("Generating data profile")
        
        total_records = df.count()
        
        # Count nulls across all required fields
        null_count = df.filter(
            F.col('id').isNull() | 
            F.col('name').isNull() | 
            F.col('value').isNull()
        ).count() if all(col in df.columns for col in ['id', 'name', 'value']) else 0
        
        # Count duplicates
        if 'id' in df.columns:
            unique_ids = df.select('id').distinct().count()
            duplicate_count = total_records - unique_ids
        else:
            duplicate_count = 0
        
        # Calculate value statistics
        if 'value' in df.columns:
            stats = df.select(
                F.min('value').alias('min_value'),
                F.max('value').alias('max_value'),
                F.avg('value').alias('avg_value'),
                F.stddev('value').alias('std_deviation')
            ).first()
            
            min_value = float(stats['min_value']) if stats['min_value'] else 0.0
            max_value = float(stats['max_value']) if stats['max_value'] else 0.0
            avg_value = float(stats['avg_value']) if stats['avg_value'] else 0.0
            std_deviation = float(stats['std_deviation']) if stats['std_deviation'] else 0.0
        else:
            min_value = max_value = avg_value = std_deviation = 0.0
        
        # Count unique categories
        if 'category' in df.columns:
            unique_categories = df.select('category').distinct().count()
        else:
            unique_categories = 0
        
        profile = DataProfile(
            total_records=total_records,
            null_count=null_count,
            duplicate_count=duplicate_count,
            min_value=min_value,
            max_value=max_value,
            avg_value=avg_value,
            std_deviation=std_deviation,
            unique_categories=unique_categories
        )
        
        self.logger.info(f"Data profile generated: {profile}")
        return profile
    
    def detect_anomalies(self, df: DataFrame) -> List[str]:
        """
        Detect statistical anomalies in the dataset.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            List of anomaly descriptions
        """
        self.logger.info("Detecting anomalies")
        
        anomalies = []
        
        # Detect outliers in value field using IQR method
        if 'value' in df.columns:
            quartiles = df.approxQuantile('value', [0.25, 0.75], 0.05)
            if len(quartiles) == 2:
                q1, q3 = quartiles
                iqr = q3 - q1
                lower_bound = q1 - (1.5 * iqr)
                upper_bound = q3 + (1.5 * iqr)
                
                outlier_count = df.filter(
                    (F.col('value') < lower_bound) | 
                    (F.col('value') > upper_bound)
                ).count()
                
                if outlier_count > 0:
                    anomalies.append(
                        f"Found {outlier_count} outlier values "
                        f"(outside range [{lower_bound:.2f}, {upper_bound:.2f}])"
                    )
        
        # Detect skewed category distribution
        if 'category' in df.columns:
            category_counts = df.groupBy('category').count()
            total = df.count()
            
            skewed_categories = category_counts.filter(
                (F.col('count') / total) > 0.5
            ).collect()
            
            if skewed_categories:
                for row in skewed_categories:
                    percentage = (row['count'] / total) * 100
                    anomalies.append(
                        f"Category '{row['category']}' represents {percentage:.1f}% "
                        f"of data (potential skew)"
                    )
        
        # Detect unusual priority distribution
        if 'priority' in df.columns:
            priority_dist = df.groupBy('priority').count().collect()
            if len(priority_dist) == 1:
                anomalies.append(
                    f"All records have the same priority value "
                    f"({priority_dist[0]['priority']})"
                )
        
        # Detect missing timestamps
        if 'processed_at' in df.columns:
            null_timestamps = df.filter(F.col('processed_at').isNull()).count()
            if null_timestamps > 0:
                anomalies.append(f"Found {null_timestamps} records with null timestamps")
        
        self.logger.info(f"Detected {len(anomalies)} anomalies")
        return anomalies
    
    def get_failed_records(self, df: DataFrame, check_type: str) -> DataFrame:
        """
        Get records that failed specific quality check.
        
        Args:
            df: Original DataFrame
            check_type: Type of check to filter failures
            
        Returns:
            DataFrame containing only failed records
        """
        if check_type == "COMPLETENESS":
            required_fields = self.required_fields or ['id', 'name', 'value']
            condition = None
            for field in required_fields:
                if field in df.columns:
                    field_condition = (
                        F.col(field).isNull() | 
                        (F.col(field) == '')
                    )
                    condition = field_condition if condition is None else (condition | field_condition)
            return df.filter(condition) if condition else df.limit(0)
        
        elif check_type == "UNIQUENESS":
            unique_keys = self.unique_keys or ['id']
            existing_keys = [key for key in unique_keys if key in df.columns]
            if existing_keys:
                return (df.groupBy(existing_keys)
                       .count()
                       .filter(F.col('count') > 1))
            return df.limit(0)
        
        elif check_type == "VALIDITY":
            invalid_conditions = []
            if 'value' in df.columns:
                invalid_conditions.append(F.col('value') < 0)
            if 'priority' in df.columns:
                invalid_conditions.append(
                    (F.col('priority') < 1) | (F.col('priority') > 5)
                )
            
            if invalid_conditions:
                combined = invalid_conditions[0]
                for cond in invalid_conditions[1:]:
                    combined = combined | cond
                return df.filter(combined)
            return df.limit(0)
        
        else:
            return df.limit(0)


def create_quality_report(
    results: List[QualityCheckResult], 
    profile: DataProfile,
    anomalies: List[str]
) -> Dict:
    """
    Create comprehensive quality report from check results.
    
    Args:
        results: List of quality check results
        profile: Data profile statistics
        anomalies: List of detected anomalies
        
    Returns:
        Dictionary containing formatted quality report
    """
    total_checks = len(results)
    passed_checks = sum(1 for r in results if r.passed)
    failed_checks = total_checks - passed_checks
    
    report = {
        'summary': {
            'total_checks': total_checks,
            'passed': passed_checks,
            'failed': failed_checks,
            'pass_rate': (passed_checks / total_checks * 100) if total_checks > 0 else 0
        },
        'checks': [
            {
                'name': r.check_name,
                'type': r.check_type,
                'passed': r.passed,
                'failed_count': r.failed_count,
                'message': r.message,
                'details': r.details
            }
            for r in results
        ],
        'profile': {
            'total_records': profile.total_records,
            'null_count': profile.null_count,
            'duplicate_count': profile.duplicate_count,
            'min_value': profile.min_value,
            'max_value': profile.max_value,
            'avg_value': profile.avg_value,
            'std_deviation': profile.std_deviation,
            'unique_categories': profile.unique_categories
        },
        'anomalies': anomalies,
        'timestamp': datetime.now().isoformat()
    }
    
    return report