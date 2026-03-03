"""
PySpark Data Quality Validation Framework
Implements comprehensive data quality checks including null/empty field validation,
duplicate detection, and business rule validations.
"""

from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
import logging


@dataclass
class QualityCheck:
    """Data class representing a quality check result"""
    check_name: str
    check_type: str
    passed: bool
    failed_count: int
    message: str
    severity: str = "ERROR"
    details: Optional[Dict] = None


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
    completeness_pct: float
    uniqueness_pct: float


class DataQualityValidator:
    """
    Comprehensive data quality validation framework for PySpark
    Implements null checks, duplicate detection, and business rule validations
    """
    
    def __init__(self, spark: SparkSession, run_id: str, config: Dict):
        """
        Initialize the Data Quality Validator
        
        Args:
            spark: SparkSession instance
            run_id: Unique identifier for this ETL run
            config: Configuration dictionary with validation rules
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.quality_results = []
        
    def perform_quality_checks(self, df: DataFrame) -> Tuple[List[QualityCheck], bool]:
        """
        Perform all configured quality checks on the DataFrame
        
        Args:
            df: Input DataFrame to validate
            
        Returns:
            Tuple of (list of quality checks, overall pass/fail)
        """
        self.logger.info(f"Starting data quality checks for run_id: {self.run_id}")
        
        checks = []
        
        # Run all quality checks
        checks.append(self.check_completeness(df))
        checks.append(self.check_uniqueness(df))
        checks.append(self.check_validity(df))
        checks.append(self.check_consistency(df))
        checks.append(self.check_business_rules(df))
        checks.append(self.check_referential_integrity(df))
        checks.append(self.check_data_types(df))
        
        # Count passed/failed
        passed_count = sum(1 for check in checks if check.passed)
        failed_count = len(checks) - passed_count
        
        overall_pass = all(check.passed or check.severity != "ERROR" for check in checks)
        
        self.logger.info(
            f"Quality checks complete: {passed_count} passed, {failed_count} failed"
        )
        
        self.quality_results = checks
        return checks, overall_pass
    
    def check_completeness(self, df: DataFrame) -> QualityCheck:
        """
        Check for null/empty values in critical fields
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Running completeness check")
        
        required_fields = self.config.get('required_fields', [
            'id', 'name', 'value', 'category'
        ])
        
        # Count nulls for each required field
        null_counts = {}
        total_nulls = 0
        
        for field in required_fields:
            if field in df.columns:
                null_count = df.filter(
                    F.col(field).isNull() | 
                    (F.col(field) == "") |
                    (F.trim(F.col(field)) == "")
                ).count()
                null_counts[field] = null_count
                total_nulls += null_count
        
        # Calculate completeness percentage
        total_checks = len(required_fields) * df.count()
        completeness_pct = ((total_checks - total_nulls) / total_checks * 100) if total_checks > 0 else 0
        
        threshold = self.config.get('completeness_threshold', 95.0)
        passed = completeness_pct >= threshold
        
        return QualityCheck(
            check_name="Completeness Check",
            check_type="COMPLETENESS",
            passed=passed,
            failed_count=total_nulls,
            message=f"Completeness: {completeness_pct:.2f}% (threshold: {threshold}%)",
            severity="ERROR",
            details={
                "null_counts": null_counts,
                "completeness_percentage": completeness_pct,
                "required_fields": required_fields
            }
        )
    
    def check_uniqueness(self, df: DataFrame) -> QualityCheck:
        """
        Check for duplicate IDs and records
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Running uniqueness check")
        
        unique_key = self.config.get('unique_key', 'id')
        
        if unique_key not in df.columns:
            return QualityCheck(
                check_name="Uniqueness Check",
                check_type="UNIQUENESS",
                passed=False,
                failed_count=0,
                message=f"Unique key field '{unique_key}' not found",
                severity="ERROR"
            )
        
        total_records = df.count()
        unique_records = df.select(unique_key).distinct().count()
        duplicate_count = total_records - unique_records
        
        # Find actual duplicate IDs
        duplicates_df = df.groupBy(unique_key).count().filter(F.col("count") > 1)
        duplicate_ids = duplicates_df.select(unique_key).limit(100).collect()
        
        uniqueness_pct = (unique_records / total_records * 100) if total_records > 0 else 0
        threshold = self.config.get('uniqueness_threshold', 100.0)
        
        passed = duplicate_count == 0 or uniqueness_pct >= threshold
        
        return QualityCheck(
            check_name="Uniqueness Check",
            check_type="UNIQUENESS",
            passed=passed,
            failed_count=duplicate_count,
            message=f"Found {duplicate_count} duplicate IDs ({uniqueness_pct:.2f}% unique)",
            severity="ERROR",
            details={
                "total_records": total_records,
                "unique_records": unique_records,
                "duplicate_count": duplicate_count,
                "sample_duplicates": [row[unique_key] for row in duplicate_ids[:10]],
                "uniqueness_percentage": uniqueness_pct
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
        self.logger.info("Running validity check")
        
        validity_rules = self.config.get('validity_rules', {})
        invalid_count = 0
        violations = {}
        
        # Check value ranges
        if 'value' in df.columns:
            min_value = validity_rules.get('min_value', 0)
            max_value = validity_rules.get('max_value', 999999)
            
            invalid_values = df.filter(
                (F.col('value') < min_value) | 
                (F.col('value') > max_value) |
                F.col('value').isNull()
            ).count()
            
            if invalid_values > 0:
                violations['value_range'] = invalid_values
                invalid_count += invalid_values
        
        # Check priority range
        if 'priority' in df.columns:
            invalid_priority = df.filter(
                (F.col('priority') < 1) | 
                (F.col('priority') > 5) |
                F.col('priority').isNull()
            ).count()
            
            if invalid_priority > 0:
                violations['priority_range'] = invalid_priority
                invalid_count += invalid_priority
        
        # Check category values
        if 'category' in df.columns:
            valid_categories = validity_rules.get('valid_categories', [])
            if valid_categories:
                invalid_category = df.filter(
                    ~F.col('category').isin(valid_categories) |
                    F.col('category').isNull()
                ).count()
                
                if invalid_category > 0:
                    violations['invalid_category'] = invalid_category
                    invalid_count += invalid_category
        
        # Check status values
        if 'status' in df.columns:
            valid_statuses = validity_rules.get('valid_statuses', [])
            if valid_statuses:
                invalid_status = df.filter(
                    ~F.col('status').isin(valid_statuses)
                ).count()
                
                if invalid_status > 0:
                    violations['invalid_status'] = invalid_status
                    invalid_count += invalid_status
        
        passed = invalid_count == 0
        
        return QualityCheck(
            check_name="Validity Check",
            check_type="VALIDITY",
            passed=passed,
            failed_count=invalid_count,
            message=f"Found {invalid_count} records with invalid values",
            severity="ERROR",
            details={
                "violations": violations,
                "rules_checked": list(validity_rules.keys())
            }
        )
    
    def check_consistency(self, df: DataFrame) -> QualityCheck:
        """
        Check for logical consistency between fields
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Running consistency check")
        
        inconsistent_count = 0
        issues = {}
        
        # Check if transformed_value is calculated correctly
        if 'value' in df.columns and 'transformed_value' in df.columns:
            # Transformed value should be >= original value for most cases
            invalid_transform = df.filter(
                (F.col('transformed_value') < 0) |
                ((F.col('transformed_value') > 0) & (F.col('value') <= 0))
            ).count()
            
            if invalid_transform > 0:
                issues['invalid_transformation'] = invalid_transform
                inconsistent_count += invalid_transform
        
        # Check timestamp consistency
        if 'created_at' in df.columns and 'processed_at' in df.columns:
            invalid_timestamps = df.filter(
                F.col('processed_at') < F.col('created_at')
            ).count()
            
            if invalid_timestamps > 0:
                issues['invalid_timestamps'] = invalid_timestamps
                inconsistent_count += invalid_timestamps
        
        # Check priority vs value consistency
        if 'priority' in df.columns and 'transformed_value' in df.columns:
            # High values should have high priority (1 or 2)
            high_value_threshold = self.config.get('high_value_threshold', 750)
            inconsistent_priority = df.filter(
                (F.col('transformed_value') >= high_value_threshold) &
                (F.col('priority') > 2)
            ).count()
            
            if inconsistent_priority > 0:
                issues['priority_value_mismatch'] = inconsistent_priority
                inconsistent_count += inconsistent_priority
        
        passed = inconsistent_count == 0
        
        return QualityCheck(
            check_name="Consistency Check",
            check_type="CONSISTENCY",
            passed=passed,
            failed_count=inconsistent_count,
            message=f"Found {inconsistent_count} consistency violations",
            severity="WARNING",
            details={"issues": issues}
        )
    
    def check_business_rules(self, df: DataFrame) -> QualityCheck:
        """
        Validate business-specific rules
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Running business rules check")
        
        business_rules = self.config.get('business_rules', {})
        violations_count = 0
        rule_violations = {}
        
        # Rule 1: Premium category items must have value >= minimum
        if business_rules.get('premium_min_value', None):
            min_premium_value = business_rules['premium_min_value']
            premium_violations = df.filter(
                (F.col('category') == 'PREMIUM') &
                (F.col('value') < min_premium_value)
            ).count()
            
            if premium_violations > 0:
                rule_violations['premium_min_value'] = premium_violations
                violations_count += premium_violations
        
        # Rule 2: High priority items must have high values
        if business_rules.get('high_priority_min_value', None):
            min_high_priority_value = business_rules['high_priority_min_value']
            priority_violations = df.filter(
                (F.col('priority') == 1) &
                (F.col('transformed_value') < min_high_priority_value)
            ).count()
            
            if priority_violations > 0:
                rule_violations['high_priority_value'] = priority_violations
                violations_count += priority_violations
        
        # Rule 3: Status must be appropriate for the values
        if business_rules.get('enforce_status_rules', False):
            status_violations = df.filter(
                ((F.col('status') == 'HIGH_VALUE') & (F.col('transformed_value') < 750)) |
                ((F.col('status') == 'LOW_VALUE') & (F.col('transformed_value') >= 300))
            ).count()
            
            if status_violations > 0:
                rule_violations['status_rules'] = status_violations
                violations_count += status_violations
        
        # Rule 4: Name must be properly formatted
        if business_rules.get('validate_name_format', True):
            name_violations = df.filter(
                (F.col('name').rlike('\\s{2,}')) |  # Multiple spaces
                (F.col('name') != F.trim(F.col('name')))  # Leading/trailing spaces
            ).count()
            
            if name_violations > 0:
                rule_violations['name_format'] = name_violations
                violations_count += name_violations
        
        passed = violations_count == 0
        severity = business_rules.get('violation_severity', 'ERROR')
        
        return QualityCheck(
            check_name="Business Rules Check",
            check_type="BUSINESS_RULES",
            passed=passed,
            failed_count=violations_count,
            message=f"Found {violations_count} business rule violations",
            severity=severity,
            details={
                "rule_violations": rule_violations,
                "rules_checked": list(business_rules.keys())
            }
        )
    
    def check_referential_integrity(self, df: DataFrame) -> QualityCheck:
        """
        Check referential integrity with lookup tables
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Running referential integrity check")
        
        ref_checks = self.config.get('referential_integrity', {})
        violation_count = 0
        ref_violations = {}
        
        # Check if category exists in reference table
        if ref_checks.get('validate_category_reference', False):
            try:
                # Load category reference data
                categories_df = self.spark.read.format("delta").load(
                    ref_checks.get('category_reference_path', '')
                ).select('category')
                
                # Find categories not in reference
                invalid_categories = df.join(
                    categories_df,
                    df.category == categories_df.category,
                    'left_anti'
                ).filter(F.col('category').isNotNull()).count()
                
                if invalid_categories > 0:
                    ref_violations['invalid_categories'] = invalid_categories
                    violation_count += invalid_categories
                    
            except Exception as e:
                self.logger.warning(f"Could not validate category reference: {str(e)}")
        
        passed = violation_count == 0
        
        return QualityCheck(
            check_name="Referential Integrity Check",
            check_type="REFERENTIAL_INTEGRITY",
            passed=passed,
            failed_count=violation_count,
            message=f"Found {violation_count} referential integrity violations",
            severity="WARNING",
            details={"violations": ref_violations}
        )
    
    def check_data_types(self, df: DataFrame) -> QualityCheck:
        """
        Validate data types and formats
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        self.logger.info("Running data type check")
        
        type_issues = {}
        issue_count = 0
        
        expected_schema = self.config.get('expected_schema', {})
        
        for field_name, expected_type in expected_schema.items():
            if field_name in df.columns:
                actual_type = str(df.schema[field_name].dataType)
                if expected_type not in actual_type:
                    type_issues[field_name] = {
                        'expected': expected_type,
                        'actual': actual_type
                    }
                    issue_count += 1
        
        passed = issue_count == 0
        
        return QualityCheck(
            check_name="Data Type Check",
            check_type="DATA_TYPE",
            passed=passed,
            failed_count=issue_count,
            message=f"Found {issue_count} data type mismatches",
            severity="ERROR",
            details={"type_issues": type_issues}
        )
    
    def profile_data(self, df: DataFrame) -> DataProfile:
        """
        Generate comprehensive data profile
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataProfile object with statistics
        """
        self.logger.info("Generating data profile")
        
        total_records = df.count()
        
        # Calculate null counts across all columns
        null_counts = df.select([
            F.count(F.when(F.col(c).isNull(), c)).alias(c) 
            for c in df.columns
        ]).collect()[0].asDict()
        
        total_nulls = sum(null_counts.values())
        
        # Check for duplicates
        unique_ids = df.select('id').distinct().count() if 'id' in df.columns else total_records
        duplicate_count = total_records - unique_ids
        
        # Calculate value statistics
        value_stats = df.select(
            F.min('value').alias('min_value'),
            F.max('value').alias('max_value'),
            F.mean('value').alias('avg_value'),
            F.stddev('value').alias('std_deviation')
        ).collect()[0] if 'value' in df.columns else None
        
        # Count unique categories
        unique_categories = df.select('category').distinct().count() if 'category' in df.columns else 0
        
        # Calculate percentages
        total_cells = total_records * len(df.columns)
        completeness_pct = ((total_cells - total_nulls) / total_cells * 100) if total_cells > 0 else 0
        uniqueness_pct = (unique_ids / total_records * 100) if total_records > 0 else 0
        
        profile = DataProfile(
            total_records=total_records,
            null_count=total_nulls,
            duplicate_count=duplicate_count,
            min_value=value_stats.min_value if value_stats else 0.0,
            max_value=value_stats.max_value if value_stats else 0.0,
            avg_value=value_stats.avg_value if value_stats else 0.0,
            std_deviation=value_stats.std_deviation if value_stats else 0.0,
            unique_categories=unique_categories,
            completeness_pct=completeness_pct,
            uniqueness_pct=uniqueness_pct
        )
        
        self.logger.info(f"Data profile generated: {total_records} records")
        return profile
    
    def detect_anomalies(self, df: DataFrame) -> List[str]:
        """
        Detect statistical anomalies in the data
        
        Args:
            df: Input DataFrame
            
        Returns:
            List of anomaly descriptions
        """
        self.logger.info("Detecting anomalies")
        
        anomalies = []
        
        if 'value' not in df.columns:
            return anomalies
        
        # Calculate statistics
        stats = df.select(
            F.mean('value').alias('mean'),
            F.stddev('value').alias('stddev'),
            F.percentile_approx('value', 0.25).alias('q1'),
            F.percentile_approx('value', 0.75).alias('q3')
        ).collect()[0]
        
        mean = stats['mean']
        stddev = stats['stddev']
        q1 = stats['q1']
        q3 = stats['q3']
        
        # Detect outliers using IQR method
        iqr = q3 - q1
        lower_bound = q1 - (1.5 * iqr)
        upper_bound = q3 + (1.5 * iqr)
        
        outliers = df.filter(
            (F.col('value') < lower_bound) | 
            (F.col('value') > upper_bound)
        ).count()
        
        if outliers > 0:
            outlier_pct = (outliers / df.count()) * 100
            anomalies.append(
                f"Found {outliers} outliers ({outlier_pct:.2f}%) using IQR method "
                f"(bounds: {lower_bound:.2f} - {upper_bound:.2f})"
            )
        
        # Detect outliers using Z-score method (3 standard deviations)
        if stddev and stddev > 0:
            z_score_outliers = df.filter(
                F.abs((F.col('value') - mean) / stddev) > 3
            ).count()
            
            if z_score_outliers > 0:
                anomalies.append(
                    f"Found {z_score_outliers} extreme outliers (>3 std deviations)"
                )
        
        # Check for suspicious patterns
        if 'id' in df.columns:
            # Check for sequential ID gaps
            ids_df = df.select('id').orderBy('id')
            # This is a simplified check - in production you'd want more sophisticated gap detection
        
        # Check for timestamp anomalies
        if 'processed_at' in df.columns:
            future_dates = df.filter(
                F.col('processed_at') > F.current_timestamp()
            ).count()
            
            if future_dates > 0:
                anomalies.append(f"Found {future_dates} records with future timestamps")
        
        return anomalies
    
    def generate_quality_report(self) -> Dict:
        """
        Generate comprehensive quality report
        
        Returns:
            Dictionary containing quality report
        """
        if not self.quality_results:
            return {"error": "No quality checks have been performed"}
        
        passed_checks = [c for c in self.quality_results if c.passed]
        failed_checks = [c for c in self.quality_results if not c.passed]
        
        report = {
            "run_id": self.run_id,
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total_checks": len(self.quality_results),
                "passed": len(passed_checks),
                "failed": len(failed_checks),
                "overall_status": "PASSED" if len(failed_checks) == 0 else "FAILED"
            },
            "checks": [
                {
                    "name": check.check_name,
                    "type": check.check_type,
                    "passed": check.passed,
                    "failed_count": check.failed_count,
                    "message": check.message,
                    "severity": check.severity,
                    "details": check.details
                }
                for check in self.quality_results
            ]
        }
        
        return report
    
    def save_quality_metrics(self, df: DataFrame, output_path: str):
        """
        Save quality metrics to storage
        
        Args:
            df: Input DataFrame
            output_path: Path to save metrics
        """
        profile = self.profile_data(df)
        report = self.generate_quality_report()
        
        metrics_df = self.spark.createDataFrame([{
            "run_id": self.run_id,
            "timestamp": datetime.now(),
            "total_records": profile.total_records,
            "null_count": profile.null_count,
            "duplicate_count": profile.duplicate_count,
            "completeness_pct": profile.completeness_pct,
            "uniqueness_pct": profile.uniqueness_pct,
            "total_checks": report['summary']['total_checks'],
            "checks_passed": report['summary']['passed'],
            "checks_failed": report['summary']['failed'],
            "overall_status": report['summary']['overall_status']
        }])
        
        metrics_df.write.mode("append").parquet(output_path)
        self.logger.info(f"Quality metrics saved to {output_path}")


def create_quality_validator(spark: SparkSession, run_id: str, config_path: str) -> DataQualityValidator:
    """
    Factory function to create DataQualityValidator from config file
    
    Args:
        spark: SparkSession instance
        run_id: Unique run identifier
        config_path: Path to configuration file
        
    Returns:
        Configured DataQualityValidator instance
    """
    import yaml
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return DataQualityValidator(
        spark=spark,
        run_id=run_id,
        config=config.get('data_quality', {})
    )