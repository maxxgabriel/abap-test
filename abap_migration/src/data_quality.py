"""
Data Quality Validation Module for PySpark ETL
Provides quality check engine, data profiling, and validation against thresholds
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, field
from datetime import datetime
import logging
import yaml


@dataclass
class QualityCheckResult:
    """Result of a single quality check"""
    check_name: str
    check_type: str
    passed: bool
    failed_count: int
    message: str
    timestamp: datetime = field(default_factory=datetime.now)
    severity: str = "INFO"


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
    distinct_ids: int


@dataclass
class ValidationThreshold:
    """Threshold configuration for validation"""
    metric_name: str
    min_threshold: Optional[float] = None
    max_threshold: Optional[float] = None
    expected_value: Optional[float] = None
    tolerance_percent: float = 5.0


class DataQualityValidator:
    """
    Comprehensive data quality validation engine
    Executes validation rules, generates profiling statistics, validates thresholds
    """

    def __init__(self, spark: SparkSession, config_path: str = "config.yaml"):
        """
        Initialize validator with Spark session and configuration
        
        Args:
            spark: Active SparkSession
            config_path: Path to configuration file
        """
        self.spark = spark
        self.logger = logging.getLogger(__name__)
        self._setup_logging()
        
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.quality_config = self.config.get('data_quality', {})
        self.thresholds = self._load_thresholds()
        
    def _setup_logging(self):
        """Configure logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
    def _load_thresholds(self) -> Dict[str, ValidationThreshold]:
        """Load validation thresholds from configuration"""
        thresholds = {}
        threshold_config = self.quality_config.get('thresholds', {})
        
        for name, config in threshold_config.items():
            thresholds[name] = ValidationThreshold(
                metric_name=name,
                min_threshold=config.get('min'),
                max_threshold=config.get('max'),
                expected_value=config.get('expected'),
                tolerance_percent=config.get('tolerance_percent', 5.0)
            )
        
        return thresholds
    
    def perform_quality_checks(self, df: DataFrame, run_id: str) -> List[QualityCheckResult]:
        """
        Execute all quality checks on the dataset
        
        Args:
            df: DataFrame to validate
            run_id: Unique identifier for this validation run
            
        Returns:
            List of QualityCheckResult objects
        """
        self.logger.info(f"Starting quality checks for run {run_id}")
        
        results = []
        
        # Execute all validation checks
        results.append(self.check_completeness(df))
        results.append(self.check_uniqueness(df))
        results.append(self.check_validity(df))
        results.append(self.check_consistency(df))
        results.append(self.check_record_count_threshold(df))
        
        # Log summary
        passed_count = sum(1 for r in results if r.passed)
        failed_count = len(results) - passed_count
        
        self.logger.info(
            f"Quality checks complete: {passed_count} passed, {failed_count} failed"
        )
        
        return results
    
    def check_completeness(self, df: DataFrame) -> QualityCheckResult:
        """
        Check for null/empty values in critical fields
        
        Args:
            df: DataFrame to validate
            
        Returns:
            QualityCheckResult indicating pass/fail
        """
        self.logger.info("Running completeness check")
        
        critical_fields = self.quality_config.get('critical_fields', [
            'id', 'name', 'value'
        ])
        
        # Count null values across critical fields
        null_conditions = [F.col(field).isNull() for field in critical_fields if field in df.columns]
        
        if not null_conditions:
            return QualityCheckResult(
                check_name="Completeness Check",
                check_type="COMPLETENESS",
                passed=True,
                failed_count=0,
                message="No critical fields to check"
            )
        
        null_count = df.filter(
            F.expr(" OR ".join([f"{field} IS NULL" for field in critical_fields if field in df.columns]))
        ).count()
        
        passed = null_count == 0
        severity = "CRITICAL" if null_count > 0 else "INFO"
        
        return QualityCheckResult(
            check_name="Completeness Check",
            check_type="COMPLETENESS",
            passed=passed,
            failed_count=null_count,
            message=f"All required fields are complete" if passed else f"{null_count} records with incomplete data",
            severity=severity
        )
    
    def check_uniqueness(self, df: DataFrame) -> QualityCheckResult:
        """
        Check for duplicate records based on ID field
        
        Args:
            df: DataFrame to validate
            
        Returns:
            QualityCheckResult indicating duplicate count
        """
        self.logger.info("Running uniqueness check")
        
        id_field = self.quality_config.get('id_field', 'id')
        
        if id_field not in df.columns:
            return QualityCheckResult(
                check_name="Uniqueness Check",
                check_type="UNIQUENESS",
                passed=False,
                failed_count=0,
                message=f"ID field '{id_field}' not found in dataset",
                severity="WARNING"
            )
        
        total_count = df.count()
        distinct_count = df.select(id_field).distinct().count()
        duplicate_count = total_count - distinct_count
        
        passed = duplicate_count == 0
        severity = "HIGH" if duplicate_count > total_count * 0.01 else "INFO"
        
        return QualityCheckResult(
            check_name="Uniqueness Check",
            check_type="UNIQUENESS",
            passed=passed,
            failed_count=duplicate_count,
            message="All IDs are unique" if passed else f"{duplicate_count} duplicate IDs found",
            severity=severity
        )
    
    def check_validity(self, df: DataFrame) -> QualityCheckResult:
        """
        Check for invalid values (negative numbers, out of range, etc.)
        
        Args:
            df: DataFrame to validate
            
        Returns:
            QualityCheckResult indicating validity
        """
        self.logger.info("Running validity check")
        
        invalid_count = 0
        validation_rules = self.quality_config.get('validation_rules', {})
        
        # Check numeric fields are positive
        numeric_fields = validation_rules.get('positive_fields', ['value', 'transformed_value'])
        for field in numeric_fields:
            if field in df.columns:
                invalid_count += df.filter(F.col(field) < 0).count()
        
        # Check priority is in valid range
        if 'priority' in df.columns:
            invalid_count += df.filter(
                (F.col('priority') < 1) | (F.col('priority') > 5)
            ).count()
        
        # Check category is not empty
        if 'category' in df.columns:
            invalid_count += df.filter(
                F.col('category').isNull() | (F.col('category') == '')
            ).count()
        
        passed = invalid_count == 0
        severity = "HIGH" if invalid_count > 0 else "INFO"
        
        return QualityCheckResult(
            check_name="Validity Check",
            check_type="VALIDITY",
            passed=passed,
            failed_count=invalid_count,
            message="All values are valid" if passed else f"{invalid_count} records with invalid values",
            severity=severity
        )
    
    def check_consistency(self, df: DataFrame) -> QualityCheckResult:
        """
        Check for data consistency (e.g., derived values match calculations)
        
        Args:
            df: DataFrame to validate
            
        Returns:
            QualityCheckResult indicating consistency
        """
        self.logger.info("Running consistency check")
        
        inconsistent_count = 0
        
        # Check if transformed_value is within expected range of value
        if 'value' in df.columns and 'transformed_value' in df.columns:
            # Transformed value should be between 0.5x and 2x of original value
            inconsistent_count = df.filter(
                (F.col('transformed_value') < F.col('value') * 0.5) |
                (F.col('transformed_value') > F.col('value') * 2.0)
            ).count()
        
        # Check status consistency with value
        if 'status' in df.columns and 'transformed_value' in df.columns:
            # High value items should have HIGH_VALUE status
            inconsistent_status = df.filter(
                (F.col('transformed_value') >= 1000) & (F.col('status') != 'HIGH_VALUE')
            ).count()
            inconsistent_count += inconsistent_status
        
        passed = inconsistent_count == 0
        severity = "MEDIUM" if inconsistent_count > 0 else "INFO"
        
        return QualityCheckResult(
            check_name="Consistency Check",
            check_type="CONSISTENCY",
            passed=passed,
            failed_count=inconsistent_count,
            message="Data is consistent" if passed else f"{inconsistent_count} records with inconsistencies",
            severity=severity
        )
    
    def check_record_count_threshold(self, df: DataFrame) -> QualityCheckResult:
        """
        Validate record count against expected thresholds
        
        Args:
            df: DataFrame to validate
            
        Returns:
            QualityCheckResult indicating if count is within threshold
        """
        self.logger.info("Running record count threshold check")
        
        actual_count = df.count()
        
        threshold = self.thresholds.get('record_count')
        if not threshold:
            return QualityCheckResult(
                check_name="Record Count Threshold",
                check_type="THRESHOLD",
                passed=True,
                failed_count=0,
                message=f"No threshold configured. Actual count: {actual_count}"
            )
        
        passed = True
        message_parts = []
        severity = "INFO"
        
        # Check minimum threshold
        if threshold.min_threshold is not None:
            if actual_count < threshold.min_threshold:
                passed = False
                severity = "CRITICAL"
                message_parts.append(
                    f"Below minimum threshold: {actual_count} < {threshold.min_threshold}"
                )
        
        # Check maximum threshold
        if threshold.max_threshold is not None:
            if actual_count > threshold.max_threshold:
                passed = False
                severity = "HIGH"
                message_parts.append(
                    f"Above maximum threshold: {actual_count} > {threshold.max_threshold}"
                )
        
        # Check expected value with tolerance
        if threshold.expected_value is not None:
            tolerance = threshold.expected_value * (threshold.tolerance_percent / 100.0)
            min_expected = threshold.expected_value - tolerance
            max_expected = threshold.expected_value + tolerance
            
            if actual_count < min_expected or actual_count > max_expected:
                passed = False
                severity = "MEDIUM"
                message_parts.append(
                    f"Outside expected range: {actual_count} not in [{min_expected:.0f}, {max_expected:.0f}]"
                )
        
        message = " | ".join(message_parts) if message_parts else f"Record count within threshold: {actual_count}"
        
        return QualityCheckResult(
            check_name="Record Count Threshold",
            check_type="THRESHOLD",
            passed=passed,
            failed_count=0 if passed else 1,
            message=message,
            severity=severity
        )
    
    def profile_data(self, df: DataFrame) -> DataProfile:
        """
        Generate comprehensive statistical profile of dataset
        
        Args:
            df: DataFrame to profile
            
        Returns:
            DataProfile object with statistics
        """
        self.logger.info("Generating data profile")
        
        # Calculate basic statistics
        total_records = df.count()
        
        # Null counts across all columns
        null_counts = df.select([
            F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
            for c in df.columns
        ]).collect()[0]
        total_nulls = sum(null_counts.asDict().values())
        
        # Duplicate count
        id_field = self.quality_config.get('id_field', 'id')
        distinct_ids = df.select(id_field).distinct().count() if id_field in df.columns else total_records
        duplicate_count = total_records - distinct_ids
        
        # Value statistics
        value_field = 'transformed_value' if 'transformed_value' in df.columns else 'value'
        value_stats = df.select(
            F.min(value_field).alias('min_value'),
            F.max(value_field).alias('max_value'),
            F.avg(value_field).alias('avg_value'),
            F.stddev(value_field).alias('std_deviation')
        ).collect()[0]
        
        # Category statistics
        unique_categories = 0
        if 'category' in df.columns:
            unique_categories = df.select('category').distinct().count()
        
        profile = DataProfile(
            total_records=total_records,
            null_count=total_nulls,
            duplicate_count=duplicate_count,
            min_value=float(value_stats['min_value']) if value_stats['min_value'] else 0.0,
            max_value=float(value_stats['max_value']) if value_stats['max_value'] else 0.0,
            avg_value=float(value_stats['avg_value']) if value_stats['avg_value'] else 0.0,
            std_deviation=float(value_stats['std_deviation']) if value_stats['std_deviation'] else 0.0,
            unique_categories=unique_categories,
            distinct_ids=distinct_ids
        )
        
        self.logger.info(f"Data profile generated: {total_records} records, {duplicate_count} duplicates")
        
        return profile
    
    def detect_anomalies(self, df: DataFrame, std_threshold: float = 3.0) -> List[str]:
        """
        Detect statistical anomalies using standard deviation method
        
        Args:
            df: DataFrame to analyze
            std_threshold: Number of standard deviations for anomaly detection
            
        Returns:
            List of anomaly descriptions
        """
        self.logger.info(f"Detecting anomalies with {std_threshold} std threshold")
        
        anomalies = []
        value_field = 'transformed_value' if 'transformed_value' in df.columns else 'value'
        
        if value_field not in df.columns:
            return anomalies
        
        # Calculate statistics
        stats = df.select(
            F.avg(value_field).alias('mean'),
            F.stddev(value_field).alias('std')
        ).collect()[0]
        
        mean = float(stats['mean']) if stats['mean'] else 0.0
        std = float(stats['std']) if stats['std'] else 0.0
        
        if std == 0:
            return anomalies
        
        # Find outliers
        lower_bound = mean - (std_threshold * std)
        upper_bound = mean + (std_threshold * std)
        
        outlier_count = df.filter(
            (F.col(value_field) < lower_bound) | (F.col(value_field) > upper_bound)
        ).count()
        
        if outlier_count > 0:
            anomalies.append(
                f"{outlier_count} outlier values outside [{lower_bound:.2f}, {upper_bound:.2f}]"
            )
        
        # Check for sudden spikes in category distribution
        if 'category' in df.columns:
            category_dist = df.groupBy('category').count().collect()
            total = sum(row['count'] for row in category_dist)
            
            for row in category_dist:
                percentage = (row['count'] / total) * 100
                if percentage > 80:
                    anomalies.append(
                        f"Category '{row['category']}' represents {percentage:.1f}% of data"
                    )
        
        self.logger.info(f"Detected {len(anomalies)} anomalies")
        
        return anomalies
    
    def validate_schema(self, df: DataFrame, expected_schema: StructType) -> Tuple[bool, List[str]]:
        """
        Validate DataFrame schema matches expected structure
        
        Args:
            df: DataFrame to validate
            expected_schema: Expected StructType schema
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        expected_fields = {field.name: field for field in expected_schema.fields}
        actual_fields = {field.name: field for field in df.schema.fields}
        
        # Check missing fields
        missing = set(expected_fields.keys()) - set(actual_fields.keys())
        if missing:
            errors.append(f"Missing fields: {', '.join(missing)}")
        
        # Check extra fields
        extra = set(actual_fields.keys()) - set(expected_fields.keys())
        if extra:
            errors.append(f"Extra fields: {', '.join(extra)}")
        
        # Check field types
        for field_name in expected_fields.keys() & actual_fields.keys():
            if expected_fields[field_name].dataType != actual_fields[field_name].dataType:
                errors.append(
                    f"Type mismatch for '{field_name}': "
                    f"expected {expected_fields[field_name].dataType}, "
                    f"got {actual_fields[field_name].dataType}"
                )
        
        return len(errors) == 0, errors
    
    def generate_quality_report(
        self,
        check_results: List[QualityCheckResult],
        profile: DataProfile,
        anomalies: List[str]
    ) -> Dict:
        """
        Generate comprehensive quality report
        
        Args:
            check_results: List of quality check results
            profile: Data profile statistics
            anomalies: List of detected anomalies
            
        Returns:
            Dictionary containing full quality report
        """
        passed_checks = [r for r in check_results if r.passed]
        failed_checks = [r for r in check_results if not r.passed]
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_checks': len(check_results),
                'passed_checks': len(passed_checks),
                'failed_checks': len(failed_checks),
                'success_rate': (len(passed_checks) / len(check_results) * 100) if check_results else 0
            },
            'check_results': [
                {
                    'name': r.check_name,
                    'type': r.check_type,
                    'passed': r.passed,
                    'failed_count': r.failed_count,
                    'message': r.message,
                    'severity': r.severity
                }
                for r in check_results
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
                'distinct_ids': profile.distinct_ids
            },
            'anomalies': anomalies,
            'recommendations': self._generate_recommendations(failed_checks, anomalies)
        }
        
        return report
    
    def _generate_recommendations(
        self,
        failed_checks: List[QualityCheckResult],
        anomalies: List[str]
    ) -> List[str]:
        """Generate actionable recommendations based on quality issues"""
        recommendations = []
        
        for check in failed_checks:
            if check.check_type == "COMPLETENESS":
                recommendations.append(
                    "Review data source for missing values and implement null handling strategy"
                )
            elif check.check_type == "UNIQUENESS":
                recommendations.append(
                    "Investigate duplicate records and implement deduplication logic"
                )
            elif check.check_type == "VALIDITY":
                recommendations.append(
                    "Add validation rules to data ingestion pipeline"
                )
            elif check.check_type == "CONSISTENCY":
                recommendations.append(
                    "Review transformation logic for calculation inconsistencies"
                )
            elif check.check_type == "THRESHOLD":
                recommendations.append(
                    "Investigate record count deviation and verify data source"
                )
        
        if anomalies:
            recommendations.append(
                "Review detected anomalies and consider adjusting business rules"
            )
        
        return list(set(recommendations))  # Remove duplicates


def main():
    """Example usage of DataQualityValidator"""
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("DataQualityValidation") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    # Create sample data
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("value", DoubleType(), False),
        StructField("transformed_value", DoubleType(), False),
        StructField("status", StringType(), False),
        StructField("category", StringType(), False),
        StructField("priority", IntegerType(), False),
    ])
    
    data = [
        ("ID001", "Product 1", 100.0, 150.0, "NORMAL", "PREMIUM", 3),
        ("ID002", "Product 2", 200.0, 240.0, "NORMAL", "STANDARD", 2),
        ("ID003", "Product 3", 1500.0, 1800.0, "HIGH_VALUE", "VIP", 1),
        ("ID004", "Product 4", 50.0, 60.0, "LOW_VALUE", "BASIC", 4),
    ]
    
    df = spark.createDataFrame(data, schema)
    
    # Initialize validator
    validator = DataQualityValidator(spark)
    
    # Perform quality checks
    results = validator.perform_quality_checks(df, "RUN001")
    
    # Generate profile
    profile = validator.profile_data(df)
    
    # Detect anomalies
    anomalies = validator.detect_anomalies(df)
    
    # Generate report
    report = validator.generate_quality_report(results, profile, anomalies)
    
    # Print report
    import json
    print(json.dumps(report, indent=2))
    
    spark.stop()


if __name__ == "__main__":
    main()