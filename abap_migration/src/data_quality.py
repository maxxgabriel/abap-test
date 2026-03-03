"""
PySpark Data Quality Validation Framework
Implements comprehensive data quality checks including null/empty field validation,
duplicate ID detection, and business rule validations.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import logging


class DataQualityCheck:
    """Represents a single data quality check result"""
    
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


class DataQualityValidator:
    """
    Main data quality validation framework for PySpark DataFrames.
    Performs completeness, uniqueness, validity, and consistency checks.
    """
    
    def __init__(self, spark: SparkSession, run_id: str, config: Dict):
        """
        Initialize the data quality validator
        
        Args:
            spark: SparkSession instance
            run_id: Unique identifier for this validation run
            config: Configuration dictionary with validation rules
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = logging.getLogger(__name__)
        
    def perform_quality_checks(self, df: DataFrame) -> List[DataQualityCheck]:
        """
        Execute all configured data quality checks
        
        Args:
            df: DataFrame to validate
            
        Returns:
            List of DataQualityCheck results
        """
        self.logger.info(f"Starting data quality checks for run_id: {self.run_id}")
        
        checks = []
        
        # Run all quality checks
        checks.append(self.check_completeness(df))
        checks.append(self.check_uniqueness(df))
        checks.append(self.check_validity(df))
        checks.append(self.check_consistency(df))
        
        # Count passed/failed
        passed = sum(1 for check in checks if check.passed)
        failed = len(checks) - passed
        
        self.logger.info(f"Quality checks complete: {passed} passed, {failed} failed")
        
        return checks
    
    def check_completeness(self, df: DataFrame) -> DataQualityCheck:
        """
        Check for null or empty values in critical fields
        
        Args:
            df: DataFrame to check
            
        Returns:
            DataQualityCheck result
        """
        required_fields = self.config.get('required_fields', ['id', 'name', 'value'])
        
        # Build condition for null/empty checks
        null_conditions = []
        for field in required_fields:
            if field in df.columns:
                null_conditions.append(
                    F.col(field).isNull() | 
                    (F.trim(F.col(field)) == "") |
                    (F.col(field) == 0)
                )
        
        if not null_conditions:
            return DataQualityCheck(
                check_name='Completeness Check',
                check_type='COMPLETENESS',
                passed=True,
                failed_count=0,
                message='No required fields to check'
            )
        
        # Combine conditions with OR
        combined_condition = null_conditions[0]
        for condition in null_conditions[1:]:
            combined_condition = combined_condition | condition
        
        # Count records with null/empty values
        null_count = df.filter(combined_condition).count()
        
        passed = (null_count == 0)
        message = 'All required fields are complete' if passed else \
                  f'{null_count} records with incomplete data'
        
        return DataQualityCheck(
            check_name='Completeness Check',
            check_type='COMPLETENESS',
            passed=passed,
            failed_count=null_count,
            message=message
        )
    
    def check_uniqueness(self, df: DataFrame) -> DataQualityCheck:
        """
        Check for duplicate IDs
        
        Args:
            df: DataFrame to check
            
        Returns:
            DataQualityCheck result
        """
        id_field = self.config.get('id_field', 'id')
        
        if id_field not in df.columns:
            return DataQualityCheck(
                check_name='Uniqueness Check',
                check_type='UNIQUENESS',
                passed=False,
                failed_count=0,
                message=f'ID field {id_field} not found in DataFrame'
            )
        
        # Count total records
        total_count = df.count()
        
        # Count unique IDs
        unique_count = df.select(id_field).distinct().count()
        
        # Calculate duplicates
        duplicate_count = total_count - unique_count
        
        passed = (duplicate_count == 0)
        message = 'All IDs are unique' if passed else \
                  f'{duplicate_count} duplicate IDs found'
        
        return DataQualityCheck(
            check_name='Uniqueness Check',
            check_type='UNIQUENESS',
            passed=passed,
            failed_count=duplicate_count,
            message=message
        )
    
    def check_validity(self, df: DataFrame) -> DataQualityCheck:
        """
        Check for invalid values based on business rules
        
        Args:
            df: DataFrame to check
            
        Returns:
            DataQualityCheck result
        """
        validity_rules = self.config.get('validity_rules', {})
        
        invalid_conditions = []
        
        # Check value must be positive
        if 'value' in df.columns:
            invalid_conditions.append(F.col('value') < 0)
        
        if 'transformed_value' in df.columns:
            invalid_conditions.append(F.col('transformed_value') < 0)
        
        # Check priority must be between 1-5
        if 'priority' in df.columns:
            invalid_conditions.append(
                (F.col('priority') < 1) | (F.col('priority') > 5)
            )
        
        # Check category must not be empty
        if 'category' in df.columns:
            invalid_conditions.append(
                F.col('category').isNull() | 
                (F.trim(F.col('category')) == "")
            )
        
        # Check status against allowed values
        if 'status' in df.columns and 'allowed_statuses' in validity_rules:
            allowed_statuses = validity_rules['allowed_statuses']
            invalid_conditions.append(
                ~F.col('status').isin(allowed_statuses)
            )
        
        if not invalid_conditions:
            return DataQualityCheck(
                check_name='Validity Check',
                check_type='VALIDITY',
                passed=True,
                failed_count=0,
                message='All values are valid'
            )
        
        # Combine conditions with OR
        combined_condition = invalid_conditions[0]
        for condition in invalid_conditions[1:]:
            combined_condition = combined_condition | condition
        
        # Count invalid records
        invalid_count = df.filter(combined_condition).count()
        
        passed = (invalid_count == 0)
        message = 'All values are valid' if passed else \
                  f'{invalid_count} records with invalid values'
        
        return DataQualityCheck(
            check_name='Validity Check',
            check_type='VALIDITY',
            passed=passed,
            failed_count=invalid_count,
            message=message
        )
    
    def check_consistency(self, df: DataFrame) -> DataQualityCheck:
        """
        Check for data consistency issues
        
        Args:
            df: DataFrame to check
            
        Returns:
            DataQualityCheck result
        """
        inconsistent_conditions = []
        
        # Check transformed_value consistency with value
        if 'value' in df.columns and 'transformed_value' in df.columns:
            # Transformed value should be >= original value
            inconsistent_conditions.append(
                F.col('transformed_value') < F.col('value')
            )
        
        # Check status consistency with value
        if 'status' in df.columns and 'transformed_value' in df.columns:
            # HIGH_VALUE status should have high transformed_value
            inconsistent_conditions.append(
                (F.col('status') == 'HIGH_VALUE') & 
                (F.col('transformed_value') < 750)
            )
            
            # LOW_VALUE status should have low transformed_value
            inconsistent_conditions.append(
                (F.col('status') == 'LOW_VALUE') & 
                (F.col('transformed_value') >= 300)
            )
        
        # Check priority consistency with transformed_value
        if 'priority' in df.columns and 'transformed_value' in df.columns:
            # Priority 1 should be for high values (>= 1000)
            inconsistent_conditions.append(
                (F.col('priority') == 1) & 
                (F.col('transformed_value') < 1000)
            )
        
        if not inconsistent_conditions:
            return DataQualityCheck(
                check_name='Consistency Check',
                check_type='CONSISTENCY',
                passed=True,
                failed_count=0,
                message='All data is consistent'
            )
        
        # Combine conditions with OR
        combined_condition = inconsistent_conditions[0]
        for condition in inconsistent_conditions[1:]:
            combined_condition = combined_condition | condition
        
        # Count inconsistent records
        inconsistent_count = df.filter(combined_condition).count()
        
        passed = (inconsistent_count == 0)
        message = 'All data is consistent' if passed else \
                  f'{inconsistent_count} records with consistency issues'
        
        return DataQualityCheck(
            check_name='Consistency Check',
            check_type='CONSISTENCY',
            passed=passed,
            failed_count=inconsistent_count,
            message=message
        )
    
    def profile_data(self, df: DataFrame) -> DataProfile:
        """
        Generate comprehensive data profile
        
        Args:
            df: DataFrame to profile
            
        Returns:
            DataProfile with statistics
        """
        self.logger.info("Generating data profile")
        
        # Calculate basic statistics
        total_records = df.count()
        
        # Count null values across critical fields
        null_count = 0
        critical_fields = self.config.get('required_fields', ['id', 'name', 'value'])
        
        for field in critical_fields:
            if field in df.columns:
                null_count += df.filter(
                    F.col(field).isNull() | (F.trim(F.col(field)) == "")
                ).count()
        
        # Calculate duplicate count
        if 'id' in df.columns:
            unique_ids = df.select('id').distinct().count()
            duplicate_count = total_records - unique_ids
        else:
            duplicate_count = 0
        
        # Calculate value statistics
        if 'transformed_value' in df.columns:
            stats = df.select(
                F.min('transformed_value').alias('min_value'),
                F.max('transformed_value').alias('max_value'),
                F.avg('transformed_value').alias('avg_value'),
                F.stddev('transformed_value').alias('std_deviation')
            ).collect()[0]
            
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
        
        return DataProfile(
            total_records=total_records,
            null_count=null_count,
            duplicate_count=duplicate_count,
            min_value=min_value,
            max_value=max_value,
            avg_value=avg_value,
            std_deviation=std_deviation,
            unique_categories=unique_categories
        )
    
    def detect_anomalies(self, df: DataFrame) -> List[str]:
        """
        Detect data anomalies using statistical methods
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            List of anomaly descriptions
        """
        self.logger.info("Detecting data anomalies")
        
        anomalies = []
        
        # Check for outliers in value field
        if 'transformed_value' in df.columns:
            stats = df.select(
                F.avg('transformed_value').alias('mean'),
                F.stddev('transformed_value').alias('stddev')
            ).collect()[0]
            
            mean = float(stats['mean']) if stats['mean'] else 0.0
            stddev = float(stats['stddev']) if stats['stddev'] else 0.0
            
            if stddev > 0:
                # Values beyond 3 standard deviations
                threshold_low = mean - (3 * stddev)
                threshold_high = mean + (3 * stddev)
                
                outlier_count = df.filter(
                    (F.col('transformed_value') < threshold_low) |
                    (F.col('transformed_value') > threshold_high)
                ).count()
                
                if outlier_count > 0:
                    anomalies.append(
                        f'Found {outlier_count} outliers in transformed_value '
                        f'(beyond 3 standard deviations)'
                    )
        
        # Check for unexpected category distributions
        if 'category' in df.columns:
            category_counts = df.groupBy('category').count().collect()
            total = df.count()
            
            for row in category_counts:
                percentage = (row['count'] / total) * 100
                if percentage > 50:
                    anomalies.append(
                        f'Category {row["category"]} represents {percentage:.1f}% '
                        f'of records (possible data skew)'
                    )
        
        # Check for date anomalies
        if 'processed_at' in df.columns:
            future_dates = df.filter(
                F.col('processed_at') > F.current_timestamp()
            ).count()
            
            if future_dates > 0:
                anomalies.append(f'Found {future_dates} records with future dates')
        
        # Check for unusual status patterns
        if 'status' in df.columns:
            status_counts = df.groupBy('status').count().collect()
            
            for row in status_counts:
                if row['status'] and 'INVALID' in row['status']:
                    anomalies.append(
                        f'Found {row["count"]} records with INVALID status'
                    )
        
        return anomalies
    
    def validate_business_rules(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate custom business rules
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []
        business_rules = self.config.get('business_rules', {})
        
        # Rule: Premium category should have multiplier applied
        if business_rules.get('premium_multiplier_check', False):
            if 'category' in df.columns and 'value' in df.columns and 'transformed_value' in df.columns:
                premium_df = df.filter(F.col('category') == 'PREMIUM')
                invalid_premium = premium_df.filter(
                    F.col('transformed_value') <= F.col('value')
                ).count()
                
                if invalid_premium > 0:
                    errors.append(
                        f'{invalid_premium} PREMIUM records without value transformation'
                    )
        
        # Rule: High priority items must have high values
        if business_rules.get('priority_value_check', False):
            if 'priority' in df.columns and 'transformed_value' in df.columns:
                invalid_priority = df.filter(
                    (F.col('priority') == 1) & 
                    (F.col('transformed_value') < 1000)
                ).count()
                
                if invalid_priority > 0:
                    errors.append(
                        f'{invalid_priority} high priority records with low values'
                    )
        
        # Rule: All records must have ETL run ID
        if business_rules.get('run_id_check', True):
            if 'etl_run_id' in df.columns:
                missing_run_id = df.filter(
                    F.col('etl_run_id').isNull() | 
                    (F.trim(F.col('etl_run_id')) == "")
                ).count()
                
                if missing_run_id > 0:
                    errors.append(f'{missing_run_id} records missing ETL run ID')
        
        return (len(errors) == 0, errors)
    
    def generate_quality_report(self, df: DataFrame) -> Dict:
        """
        Generate comprehensive data quality report
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary containing complete quality report
        """
        self.logger.info("Generating comprehensive quality report")
        
        # Perform all checks
        checks = self.perform_quality_checks(df)
        profile = self.profile_data(df)
        anomalies = self.detect_anomalies(df)
        is_valid, business_rule_errors = self.validate_business_rules(df)
        
        report = {
            'run_id': self.run_id,
            'timestamp': datetime.now().isoformat(),
            'quality_checks': [check.to_dict() for check in checks],
            'data_profile': profile.to_dict(),
            'anomalies': anomalies,
            'business_rule_validation': {
                'is_valid': is_valid,
                'errors': business_rule_errors
            },
            'summary': {
                'total_checks': len(checks),
                'passed_checks': sum(1 for check in checks if check.passed),
                'failed_checks': sum(1 for check in checks if not check.passed),
                'total_anomalies': len(anomalies),
                'business_rules_valid': is_valid
            }
        }
        
        return report


def create_quality_validator(spark: SparkSession, config_path: str, run_id: str) -> DataQualityValidator:
    """
    Factory function to create DataQualityValidator with configuration
    
    Args:
        spark: SparkSession instance
        config_path: Path to configuration file
        run_id: Unique run identifier
        
    Returns:
        Configured DataQualityValidator instance
    """
    import yaml
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    quality_config = config.get('data_quality', {})
    
    return DataQualityValidator(spark, run_id, quality_config)