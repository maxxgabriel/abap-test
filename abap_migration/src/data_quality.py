"""
Data Quality Module for ETL Pipeline
Provides profiling, validation, and quality check execution
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, countDistinct, avg, stddev, min as spark_min, 
    max as spark_max, sum as spark_sum, when, isnan, isnull
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@dataclass
class QualityCheck:
    """Represents a single quality check result"""
    check_name: str
    check_type: str
    passed: bool
    failed_count: int = 0
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataProfile:
    """Statistical profile of dataset"""
    total_records: int = 0
    null_count: int = 0
    duplicate_count: int = 0
    min_value: float = 0.0
    max_value: float = 0.0
    avg_value: float = 0.0
    std_deviation: float = 0.0
    unique_categories: int = 0
    column_stats: Dict[str, Dict[str, Any]] = field(default_factory=dict)


class DataQualityChecker:
    """
    Manages data quality checks, profiling, and validation
    """
    
    def __init__(self, spark: SparkSession, run_id: str, config: Dict[str, Any]):
        """
        Initialize data quality checker
        
        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.quality_thresholds = config.get('quality_thresholds', {})
        
    def perform_quality_checks(self, df: DataFrame) -> List[QualityCheck]:
        """
        Execute all quality checks on dataset
        
        Args:
            df: Input DataFrame to check
            
        Returns:
            List of QualityCheck results
        """
        logger.info(f"Starting data quality checks for run {self.run_id}")
        
        checks = []
        
        # Execute all check methods
        checks.append(self.check_completeness(df))
        checks.append(self.check_uniqueness(df))
        checks.append(self.check_validity(df))
        checks.append(self.check_consistency(df))
        checks.append(self.check_record_count(df))
        
        # Summary statistics
        passed = sum(1 for check in checks if check.passed)
        failed = len(checks) - passed
        
        logger.info(f"Quality checks complete: {passed} passed, {failed} failed")
        
        return checks
    
    def check_completeness(self, df: DataFrame) -> QualityCheck:
        """
        Check for null/empty values in critical fields
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        check_name = "Completeness Check"
        check_type = "COMPLETENESS"
        
        required_fields = self.config.get('required_fields', ['id', 'name', 'value'])
        
        # Count nulls in required fields
        null_counts = {}
        total_nulls = 0
        
        for field in required_fields:
            if field in df.columns:
                null_count = df.filter(
                    col(field).isNull() | (col(field) == "")
                ).count()
                null_counts[field] = null_count
                total_nulls += null_count
        
        passed = total_nulls == 0
        message = (
            "All required fields are complete" 
            if passed 
            else f"{total_nulls} records with incomplete data"
        )
        
        return QualityCheck(
            check_name=check_name,
            check_type=check_type,
            passed=passed,
            failed_count=total_nulls,
            message=message,
            details={'null_counts': null_counts}
        )
    
    def check_uniqueness(self, df: DataFrame) -> QualityCheck:
        """
        Check for duplicate records based on ID field
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        check_name = "Uniqueness Check"
        check_type = "UNIQUENESS"
        
        id_field = self.config.get('id_field', 'id')
        
        if id_field not in df.columns:
            return QualityCheck(
                check_name=check_name,
                check_type=check_type,
                passed=False,
                message=f"ID field '{id_field}' not found in DataFrame"
            )
        
        total_records = df.count()
        unique_records = df.select(id_field).distinct().count()
        duplicate_count = total_records - unique_records
        
        passed = duplicate_count == 0
        message = (
            "All IDs are unique" 
            if passed 
            else f"{duplicate_count} duplicate IDs found"
        )
        
        return QualityCheck(
            check_name=check_name,
            check_type=check_type,
            passed=passed,
            failed_count=duplicate_count,
            message=message,
            details={
                'total_records': total_records,
                'unique_records': unique_records,
                'duplicate_count': duplicate_count
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
        check_name = "Validity Check"
        check_type = "VALIDITY"
        
        invalid_count = 0
        validation_rules = []
        
        # Value must be positive
        if 'value' in df.columns:
            negative_values = df.filter(col('value') < 0).count()
            invalid_count += negative_values
            validation_rules.append(f"Negative values: {negative_values}")
        
        if 'transformed_value' in df.columns:
            negative_transformed = df.filter(col('transformed_value') < 0).count()
            invalid_count += negative_transformed
            validation_rules.append(f"Negative transformed values: {negative_transformed}")
        
        # Priority must be 1-5
        if 'priority' in df.columns:
            invalid_priority = df.filter(
                (col('priority') < 1) | (col('priority') > 5)
            ).count()
            invalid_count += invalid_priority
            validation_rules.append(f"Invalid priority: {invalid_priority}")
        
        # Category must not be empty
        if 'category' in df.columns:
            empty_category = df.filter(
                col('category').isNull() | (col('category') == "")
            ).count()
            invalid_count += empty_category
            validation_rules.append(f"Empty categories: {empty_category}")
        
        passed = invalid_count == 0
        message = (
            "All values are valid" 
            if passed 
            else f"{invalid_count} records with invalid values"
        )
        
        return QualityCheck(
            check_name=check_name,
            check_type=check_type,
            passed=passed,
            failed_count=invalid_count,
            message=message,
            details={'validation_rules': validation_rules}
        )
    
    def check_consistency(self, df: DataFrame) -> QualityCheck:
        """
        Check for data consistency across related fields
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        check_name = "Consistency Check"
        check_type = "CONSISTENCY"
        
        inconsistent_count = 0
        consistency_issues = []
        
        # Check transformed_value is >= value for PREMIUM category
        if all(col_name in df.columns for col_name in ['value', 'transformed_value', 'category']):
            premium_inconsistent = df.filter(
                (col('category') == 'PREMIUM') & 
                (col('transformed_value') < col('value'))
            ).count()
            inconsistent_count += premium_inconsistent
            consistency_issues.append(
                f"Premium items with transformed_value < value: {premium_inconsistent}"
            )
        
        # Check status consistency with transformed_value
        if all(col_name in df.columns for col_name in ['transformed_value', 'status']):
            high_value_wrong_status = df.filter(
                (col('transformed_value') >= 750) & 
                (col('status') != 'HIGH_VALUE')
            ).count()
            inconsistent_count += high_value_wrong_status
            consistency_issues.append(
                f"High value items with wrong status: {high_value_wrong_status}"
            )
        
        # Check priority consistency with transformed_value
        if all(col_name in df.columns for col_name in ['transformed_value', 'priority']):
            very_high_value_low_priority = df.filter(
                (col('transformed_value') >= 1000) & 
                (col('priority') > 1)
            ).count()
            inconsistent_count += very_high_value_low_priority
            consistency_issues.append(
                f"Very high value items with low priority: {very_high_value_low_priority}"
            )
        
        passed = inconsistent_count == 0
        message = (
            "Data is consistent" 
            if passed 
            else f"{inconsistent_count} consistency issues found"
        )
        
        return QualityCheck(
            check_name=check_name,
            check_type=check_type,
            passed=passed,
            failed_count=inconsistent_count,
            message=message,
            details={'consistency_issues': consistency_issues}
        )
    
    def check_record_count(self, df: DataFrame) -> QualityCheck:
        """
        Validate record count against expected thresholds
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        check_name = "Record Count Validation"
        check_type = "RECORD_COUNT"
        
        actual_count = df.count()
        min_expected = self.quality_thresholds.get('min_records', 0)
        max_expected = self.quality_thresholds.get('max_records', float('inf'))
        
        passed = min_expected <= actual_count <= max_expected
        
        if actual_count < min_expected:
            message = f"Record count {actual_count} below minimum {min_expected}"
        elif actual_count > max_expected:
            message = f"Record count {actual_count} exceeds maximum {max_expected}"
        else:
            message = f"Record count {actual_count} within expected range"
        
        return QualityCheck(
            check_name=check_name,
            check_type=check_type,
            passed=passed,
            failed_count=0 if passed else 1,
            message=message,
            details={
                'actual_count': actual_count,
                'min_expected': min_expected,
                'max_expected': max_expected
            }
        )
    
    def profile_data(self, df: DataFrame) -> DataProfile:
        """
        Generate comprehensive statistical profile of dataset
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataProfile with statistics
        """
        logger.info(f"Generating data profile for run {self.run_id}")
        
        profile = DataProfile()
        
        # Basic counts
        profile.total_records = df.count()
        
        # Calculate nulls across all columns
        null_counts = []
        for field in df.schema.fields:
            null_count = df.filter(col(field.name).isNull()).count()
            null_counts.append(null_count)
        profile.null_count = sum(null_counts)
        
        # Check for duplicates on ID field
        if 'id' in df.columns:
            unique_ids = df.select('id').distinct().count()
            profile.duplicate_count = profile.total_records - unique_ids
        
        # Numeric field statistics
        if 'value' in df.columns:
            stats = df.select(
                spark_min('value').alias('min_val'),
                spark_max('value').alias('max_val'),
                avg('value').alias('avg_val'),
                stddev('value').alias('std_val')
            ).first()
            
            profile.min_value = float(stats['min_val']) if stats['min_val'] else 0.0
            profile.max_value = float(stats['max_val']) if stats['max_val'] else 0.0
            profile.avg_value = float(stats['avg_val']) if stats['avg_val'] else 0.0
            profile.std_deviation = float(stats['std_val']) if stats['std_val'] else 0.0
        
        # Category statistics
        if 'category' in df.columns:
            profile.unique_categories = df.select('category').distinct().count()
        
        # Per-column statistics
        profile.column_stats = self._generate_column_stats(df)
        
        logger.info(f"Data profile generated: {profile.total_records} records, "
                   f"{profile.null_count} nulls, {profile.duplicate_count} duplicates")
        
        return profile
    
    def _generate_column_stats(self, df: DataFrame) -> Dict[str, Dict[str, Any]]:
        """
        Generate statistics for each column
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary of column statistics
        """
        column_stats = {}
        
        for field in df.schema.fields:
            col_name = field.name
            col_type = str(field.dataType)
            
            stats = {
                'type': col_type,
                'null_count': df.filter(col(col_name).isNull()).count(),
                'distinct_count': df.select(col_name).distinct().count()
            }
            
            # Numeric columns
            if 'Integer' in col_type or 'Double' in col_type or 'Decimal' in col_type:
                numeric_stats = df.select(
                    spark_min(col_name).alias('min'),
                    spark_max(col_name).alias('max'),
                    avg(col_name).alias('avg'),
                    stddev(col_name).alias('stddev')
                ).first()
                
                stats.update({
                    'min': numeric_stats['min'],
                    'max': numeric_stats['max'],
                    'avg': numeric_stats['avg'],
                    'stddev': numeric_stats['stddev']
                })
            
            # String columns
            elif 'String' in col_type:
                stats['empty_count'] = df.filter(col(col_name) == "").count()
            
            column_stats[col_name] = stats
        
        return column_stats
    
    def detect_anomalies(self, df: DataFrame) -> List[str]:
        """
        Detect anomalies in dataset using statistical methods
        
        Args:
            df: Input DataFrame
            
        Returns:
            List of anomaly descriptions
        """
        logger.info(f"Detecting anomalies for run {self.run_id}")
        
        anomalies = []
        
        # Numeric outlier detection (values beyond 3 standard deviations)
        if 'value' in df.columns:
            stats = df.select(
                avg('value').alias('avg'),
                stddev('value').alias('std')
            ).first()
            
            if stats['avg'] and stats['std']:
                mean = float(stats['avg'])
                std = float(stats['std'])
                
                outlier_count = df.filter(
                    (col('value') < (mean - 3 * std)) | 
                    (col('value') > (mean + 3 * std))
                ).count()
                
                if outlier_count > 0:
                    anomalies.append(
                        f"Found {outlier_count} outlier values "
                        f"(> 3 std deviations from mean {mean:.2f})"
                    )
        
        # Check for unusual category distribution
        if 'category' in df.columns:
            total_count = df.count()
            category_counts = df.groupBy('category').count().collect()
            
            for row in category_counts:
                category = row['category']
                count = row['count']
                percentage = (count / total_count) * 100
                
                # Flag if any category is < 1% or > 80% of total
                if percentage < 1:
                    anomalies.append(
                        f"Category '{category}' has unusually low representation: {percentage:.2f}%"
                    )
                elif percentage > 80:
                    anomalies.append(
                        f"Category '{category}' dominates dataset: {percentage:.2f}%"
                    )
        
        # Check for unexpected null patterns
        null_threshold = self.quality_thresholds.get('max_null_percentage', 10)
        total_count = df.count()
        
        for field in df.schema.fields:
            null_count = df.filter(col(field.name).isNull()).count()
            null_percentage = (null_count / total_count) * 100
            
            if null_percentage > null_threshold:
                anomalies.append(
                    f"Column '{field.name}' has high null rate: {null_percentage:.2f}%"
                )
        
        logger.info(f"Anomaly detection complete: {len(anomalies)} anomalies found")
        
        return anomalies
    
    def validate_pipeline_counts(
        self, 
        extracted_count: int, 
        transformed_count: int, 
        loaded_count: int
    ) -> Tuple[bool, str]:
        """
        Validate record counts across pipeline stages
        
        Args:
            extracted_count: Number of records extracted
            transformed_count: Number of records transformed
            loaded_count: Number of records loaded
            
        Returns:
            Tuple of (validation_passed, message)
        """
        logger.info(
            f"Validating pipeline counts - Extracted: {extracted_count}, "
            f"Transformed: {transformed_count}, Loaded: {loaded_count}"
        )
        
        # Allow some data loss but flag significant discrepancies
        max_loss_percentage = self.quality_thresholds.get('max_pipeline_loss_pct', 5)
        
        # Check extraction to transformation
        if extracted_count > 0:
            transform_loss_pct = ((extracted_count - transformed_count) / extracted_count) * 100
            if transform_loss_pct > max_loss_percentage:
                return (
                    False, 
                    f"Excessive data loss in transformation: {transform_loss_pct:.2f}% "
                    f"(threshold: {max_loss_percentage}%)"
                )
        
        # Check transformation to loading
        if transformed_count > 0:
            load_loss_pct = ((transformed_count - loaded_count) / transformed_count) * 100
            if load_loss_pct > max_loss_percentage:
                return (
                    False,
                    f"Excessive data loss in loading: {load_loss_pct:.2f}% "
                    f"(threshold: {max_loss_percentage}%)"
                )
        
        # Overall pipeline check
        if extracted_count > 0:
            total_loss_pct = ((extracted_count - loaded_count) / extracted_count) * 100
            if total_loss_pct > max_loss_percentage:
                return (
                    False,
                    f"Excessive overall data loss: {total_loss_pct:.2f}% "
                    f"(threshold: {max_loss_percentage}%)"
                )
        
        return (
            True, 
            f"Pipeline counts validated successfully. "
            f"Loss rate: {total_loss_pct:.2f}%"
        )


def create_quality_checker(
    spark: SparkSession, 
    run_id: str, 
    config: Dict[str, Any]
) -> DataQualityChecker:
    """
    Factory function to create DataQualityChecker instance
    
    Args:
        spark: SparkSession instance
        run_id: Unique run identifier
        config: Configuration dictionary
        
    Returns:
        DataQualityChecker instance
    """
    return DataQualityChecker(spark, run_id, config)