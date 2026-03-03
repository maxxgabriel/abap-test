===FILE: src/extract.py===
"""
PySpark Data Extraction Module
Reads source data from various sources for quality validation
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, 
    IntegerType, TimestampType
)
from typing import Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class DataExtractor:
    """Extract data from various sources for quality validation"""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize data extractor
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.source_schema = self._get_source_schema()
        
    def _get_source_schema(self) -> StructType:
        """Define source data schema"""
        return StructType([
            StructField("id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("value", DecimalType(15, 2), nullable=True),
            StructField("status", StringType(), nullable=True),
            StructField("category", StringType(), nullable=True),
            StructField("source_system", StringType(), nullable=True),
            StructField("created_at", TimestampType(), nullable=True),
            StructField("created_by", StringType(), nullable=True),
            StructField("changed_at", TimestampType(), nullable=True),
            StructField("changed_by", StringType(), nullable=True)
        ])
    
    def extract_from_source(
        self, 
        source_type: str = "database",
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data from specified source
        
        Args:
            source_type: Type of source (database, file, staging)
            filter_condition: Optional filter SQL condition
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        logger.info(f"Starting extraction from {source_type}")
        
        if source_type == "database":
            df = self._extract_from_database(filter_condition)
        elif source_type == "file":
            df = self._extract_from_file()
        elif source_type == "staging":
            df = self._extract_from_staging()
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
        
        # Apply max records limit
        if max_records > 0:
            df = df.limit(max_records)
        
        record_count = df.count()
        logger.info(f"Extracted {record_count} records from {source_type}")
        
        return df
    
    def _extract_from_database(
        self, 
        filter_condition: Optional[str] = None
    ) -> DataFrame:
        """Extract data from database source"""
        jdbc_config = self.config.get("jdbc", {})
        
        query = f"(SELECT * FROM {jdbc_config.get('source_table', 'etl_source_data')}"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        query += ") as source_data"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config.get("url")) \
            .option("dbtable", query) \
            .option("user", jdbc_config.get("user")) \
            .option("password", jdbc_config.get("password")) \
            .option("driver", jdbc_config.get("driver", "org.postgresql.Driver")) \
            .load()
        
        return df
    
    def _extract_from_file(self) -> DataFrame:
        """Extract data from file source"""
        file_config = self.config.get("file", {})
        file_path = file_config.get("input_path")
        file_format = file_config.get("format", "parquet")
        
        df = self.spark.read \
            .format(file_format) \
            .schema(self.source_schema) \
            .load(file_path)
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract data from staging area"""
        staging_config = self.config.get("staging", {})
        staging_path = staging_config.get("path")
        
        df = self.spark.read \
            .format("parquet") \
            .schema(self.source_schema) \
            .load(staging_path)
        
        return df
    
    def extract_incremental(
        self, 
        last_run_timestamp: datetime
    ) -> DataFrame:
        """
        Extract only records changed since last run
        
        Args:
            last_run_timestamp: Timestamp of last successful run
            
        Returns:
            DataFrame with incremental data
        """
        logger.info(f"Extracting incremental data since {last_run_timestamp}")
        
        filter_condition = f"changed_at > '{last_run_timestamp}'"
        df = self._extract_from_database(filter_condition)
        
        return df


===FILE: src/quality_validator.py===
"""
PySpark Data Quality Validation Framework
Performs comprehensive data quality checks with actionable reporting
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Dict, List, Tuple
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class QualityCheck:
    """Data quality check result"""
    
    def __init__(
        self, 
        check_name: str, 
        check_type: str, 
        passed: bool,
        failed_count: int = 0,
        message: str = "",
        details: Dict = None
    ):
        self.check_name = check_name
        self.check_type = check_type
        self.passed = passed
        self.failed_count = failed_count
        self.message = message
        self.details = details or {}
        self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict:
        """Convert check to dictionary"""
        return {
            "check_name": self.check_name,
            "check_type": self.check_type,
            "passed": self.passed,
            "failed_count": self.failed_count,
            "message": self.message,
            "details": self.details,
            "timestamp": self.timestamp.isoformat()
        }


class DataQualityValidator:
    """Comprehensive data quality validation framework"""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize quality validator
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary with quality rules
        """
        self.spark = spark
        self.config = config
        self.quality_rules = config.get("quality_rules", {})
        self.checks_results: List[QualityCheck] = []
        
    def perform_all_checks(self, df: DataFrame) -> Tuple[List[QualityCheck], DataFrame]:
        """
        Run all quality checks on DataFrame
        
        Args:
            df: Input DataFrame to validate
            
        Returns:
            Tuple of (check results list, flagged DataFrame with quality issues)
        """
        logger.info("Starting comprehensive data quality validation")
        
        self.checks_results = []
        
        # Core quality checks
        self.checks_results.append(self.check_completeness(df))
        self.checks_results.append(self.check_uniqueness(df))
        self.checks_results.append(self.check_validity(df))
        self.checks_results.append(self.check_consistency(df))
        self.checks_results.append(self.check_accuracy(df))
        self.checks_results.append(self.check_business_rules(df))
        
        # Add quality flags to DataFrame
        flagged_df = self._add_quality_flags(df)
        
        # Log summary
        passed_count = sum(1 for check in self.checks_results if check.passed)
        failed_count = len(self.checks_results) - passed_count
        
        logger.info(
            f"Quality validation complete: {passed_count} passed, "
            f"{failed_count} failed"
        )
        
        return self.checks_results, flagged_df
    
    def check_completeness(self, df: DataFrame) -> QualityCheck:
        """
        Check for null/missing values in critical fields
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        critical_fields = self.quality_rules.get("critical_fields", [
            "id", "name", "value"
        ])
        
        null_counts = {}
        total_nulls = 0
        
        for field in critical_fields:
            if field in df.columns:
                null_count = df.filter(F.col(field).isNull()).count()
                null_counts[field] = null_count
                total_nulls += null_count
        
        total_records = df.count()
        completeness_rate = (
            (total_records * len(critical_fields) - total_nulls) / 
            (total_records * len(critical_fields)) * 100
        ) if total_records > 0 else 0
        
        threshold = self.quality_rules.get("completeness_threshold", 95.0)
        passed = completeness_rate >= threshold
        
        message = (
            f"Completeness: {completeness_rate:.2f}% "
            f"({'PASS' if passed else 'FAIL'} - threshold: {threshold}%)"
        )
        
        return QualityCheck(
            check_name="Completeness Check",
            check_type="COMPLETENESS",
            passed=passed,
            failed_count=total_nulls,
            message=message,
            details={
                "null_counts": null_counts,
                "completeness_rate": completeness_rate,
                "threshold": threshold
            }
        )
    
    def check_uniqueness(self, df: DataFrame) -> QualityCheck:
        """
        Check for duplicate records based on key fields
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        key_fields = self.quality_rules.get("key_fields", ["id"])
        
        total_records = df.count()
        unique_records = df.select(key_fields).distinct().count()
        duplicate_count = total_records - unique_records
        
        passed = duplicate_count == 0
        
        # Find duplicate keys
        duplicate_keys = []
        if duplicate_count > 0:
            dup_df = df.groupBy(key_fields).count().filter(F.col("count") > 1)
            duplicate_keys = [
                row.asDict() for row in dup_df.limit(10).collect()
            ]
        
        message = (
            f"Found {duplicate_count} duplicate records "
            f"({'PASS' if passed else 'FAIL'})"
        )
        
        return QualityCheck(
            check_name="Uniqueness Check",
            check_type="UNIQUENESS",
            passed=passed,
            failed_count=duplicate_count,
            message=message,
            details={
                "total_records": total_records,
                "unique_records": unique_records,
                "duplicate_count": duplicate_count,
                "sample_duplicates": duplicate_keys
            }
        )
    
    def check_validity(self, df: DataFrame) -> QualityCheck:
        """
        Check field values against validity rules
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        validity_rules = self.quality_rules.get("validity_rules", {})
        invalid_count = 0
        invalid_details = {}
        
        # Check value ranges
        if "value" in df.columns:
            min_value = validity_rules.get("min_value", 0)
            max_value = validity_rules.get("max_value", 999999)
            
            invalid_values = df.filter(
                (F.col("value") < min_value) | (F.col("value") > max_value)
            ).count()
            invalid_count += invalid_values
            invalid_details["invalid_values"] = invalid_values
        
        # Check status values
        if "status" in df.columns:
            valid_statuses = validity_rules.get("valid_statuses", [
                "ACTIVE", "INACTIVE", "PENDING"
            ])
            invalid_statuses = df.filter(
                ~F.col("status").isin(valid_statuses)
            ).count()
            invalid_count += invalid_statuses
            invalid_details["invalid_statuses"] = invalid_statuses
        
        # Check category values
        if "category" in df.columns:
            valid_categories = validity_rules.get("valid_categories", [
                "PREMIUM", "STANDARD", "BASIC", "VIP", "TRIAL"
            ])
            invalid_categories = df.filter(
                ~F.col("category").isin(valid_categories) & 
                F.col("category").isNotNull()
            ).count()
            invalid_count += invalid_categories
            invalid_details["invalid_categories"] = invalid_categories
        
        passed = invalid_count == 0
        message = (
            f"Found {invalid_count} records with invalid values "
            f"({'PASS' if passed else 'FAIL'})"
        )
        
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
        Check data consistency and referential integrity
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        inconsistent_count = 0
        consistency_details = {}
        
        # Check timestamp consistency
        if "created_at" in df.columns and "changed_at" in df.columns:
            invalid_timestamps = df.filter(
                F.col("changed_at") < F.col("created_at")
            ).count()
            inconsistent_count += invalid_timestamps
            consistency_details["invalid_timestamps"] = invalid_timestamps
        
        # Check value consistency (transformed >= original)
        if "value" in df.columns and "transformed_value" in df.columns:
            invalid_transformations = df.filter(
                (F.col("transformed_value") < 0) |
                (F.col("transformed_value") < F.col("value") * 0.5) |
                (F.col("transformed_value") > F.col("value") * 10)
            ).count()
            inconsistent_count += invalid_transformations
            consistency_details["invalid_transformations"] = invalid_transformations
        
        passed = inconsistent_count == 0
        message = (
            f"Found {inconsistent_count} consistency violations "
            f"({'PASS' if passed else 'FAIL'})"
        )
        
        return QualityCheck(
            check_name="Consistency Check",
            check_type="CONSISTENCY",
            passed=passed,
            failed_count=inconsistent_count,
            message=message,
            details=consistency_details
        )
    
    def check_accuracy(self, df: DataFrame) -> QualityCheck:
        """
        Check data accuracy using statistical methods
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        if "value" not in df.columns:
            return QualityCheck(
                check_name="Accuracy Check",
                check_type="ACCURACY",
                passed=True,
                message="Accuracy check skipped - no numeric columns"
            )
        
        # Calculate statistics
        stats = df.select(
            F.mean("value").alias("mean"),
            F.stddev("value").alias("stddev"),
            F.min("value").alias("min"),
            F.max("value").alias("max")
        ).first()
        
        mean_val = stats["mean"]
        stddev_val = stats["stddev"]
        
        # Detect outliers (beyond 3 standard deviations)
        outlier_count = 0
        if mean_val is not None and stddev_val is not None:
            lower_bound = mean_val - (3 * stddev_val)
            upper_bound = mean_val + (3 * stddev_val)
            
            outlier_count = df.filter(
                (F.col("value") < lower_bound) | (F.col("value") > upper_bound)
            ).count()
        
        total_records = df.count()
        outlier_rate = (outlier_count / total_records * 100) if total_records > 0 else 0
        threshold = self.quality_rules.get("outlier_threshold", 5.0)
        
        passed = outlier_rate <= threshold
        
        message = (
            f"Outlier rate: {outlier_rate:.2f}% "
            f"({'PASS' if passed else 'FAIL'} - threshold: {threshold}%)"
        )
        
        return QualityCheck(
            check_name="Accuracy Check",
            check_type="ACCURACY",
            passed=passed,
            failed_count=outlier_count,
            message=message,
            details={
                "mean": float(mean_val) if mean_val else None,
                "stddev": float(stddev_val) if stddev_val else None,
                "min": float(stats["min"]) if stats["min"] else None,
                "max": float(stats["max"]) if stats["max"] else None,
                "outlier_count": outlier_count,
                "outlier_rate": outlier_rate
            }
        )
    
    def check_business_rules(self, df: DataFrame) -> QualityCheck:
        """
        Validate business rules
        
        Args:
            df: Input DataFrame
            
        Returns:
            QualityCheck result
        """
        business_rules = self.quality_rules.get("business_rules", {})
        violations_count = 0
        violations_details = {}
        
        # Rule: Premium category must have value > threshold
        if business_rules.get("premium_minimum"):
            premium_min = business_rules["premium_minimum"]
            violations = df.filter(
                (F.col("category") == "PREMIUM") & 
                (F.col("value") < premium_min)
            ).count()
            violations_count += violations
            violations_details["premium_minimum_violations"] = violations
        
        # Rule: Status must align with value
        if business_rules.get("status_value_alignment"):
            high_value_threshold = business_rules.get("high_value_threshold", 750)
            status_violations = df.filter(
                (F.col("value") >= high_value_threshold) &
                (F.col("status") != "HIGH_VALUE")
            ).count()
            violations_count += status_violations
            violations_details["status_alignment_violations"] = status_violations
        
        passed = violations_count == 0
        message = (
            f"Found {violations_count} business rule violations "
            f"({'PASS' if passed else 'FAIL'})"
        )
        
        return QualityCheck(
            check_name="Business Rules Check",
            check_type="BUSINESS_RULES",
            passed=passed,
            failed_count=violations_count,
            message=message,
            details=violations_details
        )
    
    def _add_quality_flags(self, df: DataFrame) -> DataFrame:
        """
        Add quality flag columns to DataFrame
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with quality flags
        """
        # Add completeness flag
        critical_fields = self.quality_rules.get("critical_fields", ["id", "name", "value"])
        completeness_condition = F.lit(True)
        for field in critical_fields:
            if field in df.columns:
                completeness_condition = completeness_condition & F.col(field).isNotNull()
        
        df = df.withColumn("is_complete", completeness_condition)
        
        # Add validity flag
        validity_condition = F.lit(True)
        if "value" in df.columns:
            validity_condition = validity_condition & (F.col("value") >= 0)
        
        df = df.withColumn("is_valid", validity_condition)
        
        # Add overall quality score
        df = df.withColumn(
            "quality_score",
            (
                F.when(F.col("is_complete"), 50).otherwise(0) +
                F.when(F.col("is_valid"), 50).otherwise(0)
            )
        )
        
        return df
    
    def profile_data(self, df: DataFrame) -> Dict:
        """
        Generate data profiling statistics
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary with profiling statistics
        """
        total_records = df.count()
        
        profile = {
            "total_records": total_records,
            "column_count": len(df.columns),
            "columns": df.columns
        }
        
        # Null counts per column
        null_counts = {}
        for col_name in df.columns:
            null_count = df.filter(F.col(col_name).isNull()).count()
            null_counts[col_name] = {
                "null_count": null_count,
                "null_percentage": (null_count / total_records * 100) if total_records > 0 else 0
            }
        profile["null_analysis"] = null_counts
        
        # Numeric column statistics
        numeric_cols = [
            field.name for field in df.schema.fields 
            if str(field.dataType) in ["DoubleType", "IntegerType", "DecimalType(15,2)"]
        ]
        
        numeric_stats = {}
        for col_name in numeric_cols:
            stats = df.select(
                F.min(col_name).alias("min"),
                F.max(col_name).alias("max"),
                F.mean(col_name).alias("mean"),
                F.stddev(col_name).alias("stddev")
            ).first()
            
            numeric_stats[col_name] = {
                "min": float(stats["min"]) if stats["min"] else None,
                "max": float(stats["max"]) if stats["max"] else None,
                "mean": float(stats["mean"]) if stats["mean"] else None,
                "stddev": float(stats["stddev"]) if stats["stddev"] else None
            }
        profile["numeric_statistics"] = numeric_stats
        
        # Categorical column distributions
        categorical_cols = [
            field.name for field in df.schema.fields 
            if str(field.dataType) == "StringType" and field.name not in ["id", "name"]
        ]
        
        categorical_dist = {}
        for col_name in categorical_cols:
            dist = df.groupBy(col_name).count() \
                .orderBy(F.desc("count")) \
                .limit(10) \
                .collect()
            
            categorical_dist[col_name] = [
                {"value": row[col_name], "count": row["count"]} 
                for row in dist
            ]
        profile["categorical_distributions"] = categorical_dist
        
        return profile
    
    def detect_anomalies(self, df: DataFrame) -> DataFrame:
        """
        Detect anomalies using statistical methods
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with records flagged as anomalies
        """
        if "value" not in df.columns:
            return df.limit(0)
        
        # Calculate z-score for value column
        stats = df.select(
            F.mean("value").alias("mean"),
            F.stddev("value").alias("stddev")
        ).first()
        
        mean_val = stats["mean"]
        stddev_val = stats["stddev"]
        
        if mean_val is None or stddev_val is None or stddev_val == 0:
            return df.limit(0)
        
        # Add z-score and flag anomalies
        anomaly_threshold = self.quality_rules.get("anomaly_z_score", 3.0)
        
        anomalies_df = df.withColumn(
            "z_score",
            F.abs((F.col("value") - F.lit(mean_val)) / F.lit(stddev_val))
        ).filter(
            F.col("z_score") > anomaly_threshold
        ).withColumn(
            "anomaly_reason",
            F.lit("Statistical outlier detected")
        )
        
        return anomalies_df
    
    def generate_quality_report(self) -> Dict:
        """
        Generate comprehensive quality report
        
        Returns:
            Dictionary with quality report
        """
        report = {
            "timestamp": datetime.now().isoformat(),
            "total_checks": len(self.checks_results),
            "passed_checks": sum(1 for check in self.checks_results if check.passed),
            "failed_checks": sum(1 for check in self.checks_results if not check.passed),
            "checks": [check.to_dict() for check in self.checks_results]
        }
        
        # Calculate overall quality score
        if report["total_checks"] > 0:
            report["overall_quality_score"] = (
                report["passed_checks"] / report["total_checks"] * 100
            )
        else:
            report["overall_quality_score"] = 0
        
        # Determine overall status
        if report["overall_quality_score"] >= 90:
            report["status"] = "EXCELLENT"
        elif report["overall_quality_score"] >= 75:
            report["status"] = "GOOD"
        elif report["overall_quality_score"] >= 50:
            report["status"] = "FAIR"
        else:
            report["status"] = "POOR"
        
        return report


===FILE: src/report_generator.py===
"""
PySpark Quality Report Generator
Creates actionable reports from data quality validation results
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict, List
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class QualityReportGenerator:
    """Generate comprehensive quality validation reports"""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize report generator
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.output_path = config.get("reporting", {}).get("output_path", "output/reports")
        
    def generate_html_report(
        self, 
        quality_report: Dict,
        profile_data: Dict,
        anomalies_df: DataFrame,
        output_file: str = None
    ) -> str:
        """
        Generate HTML quality report
        
        Args:
            quality_report: Quality validation report dict
            profile_data: Data profiling statistics
            anomalies_df: DataFrame with detected anomalies
            output_file: Optional output file path
            
        Returns:
            HTML report string
        """
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"{self.output_path}/quality_report_{timestamp}.html"
        
        anomaly_count = anomalies_df.count()
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Data Quality Validation Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
                .header {{ background-color: #2c3e50; color: white; padding: 20px; border-radius: 5px; }}
                .summary {{ background-color: white; padding: 20px; margin: 20px 0; border-radius: 5px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                .metric {{ display: inline-block; margin: 10px 20px; text-align: center; }}
                .metric-value {{ font-size: 36px; font-weight: bold; color: #3498db; }}
                .metric-label {{ font-size: 14px; color: #7f8c8d; }}
                .check {{ background-color: white; padding: 15px; margin: 10px 0; border-radius: 5px; border-left: 4px solid #95a5a6; }}
                .check.passed {{ border-left-color: #27ae60; }}
                .check.failed {{ border-left-color: #e74c3c; }}
                .status-badge {{ padding: 5px 10px; border-radius: 3px; font-weight: bold; }}
                .status-excellent {{ background-color: #27ae60; color: white; }}
                .status-good {{ background-color: #2ecc71; color: white; }}
                .status-fair {{ background-color: #f39c12; color: white; }}
                .status-poor {{ background-color: #e74c3c; color: white; }}
                table {{ width: 100%; border-collapse: collapse; background-color: white; }}
                th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
                th {{ background-color: #34495e; color: white; }}
                .profile-section {{ background-color: white; padding: 20px; margin: 20px 0; border-radius: 5px; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>Data Quality Validation Report</h1>
                <p>Generated: {quality_report.get('timestamp', 'N/A')}</p>
            </div>
            
            <div class="summary">
                <h2>Overall Quality Score: 
                    <span class="status-badge status-{quality_report.get('status', 'poor').lower()}">
                        {quality_report.get('overall_quality_score', 0):.1f}% - {quality_report.get('status', 'UNKNOWN')}
                    </span>
                </h2>
                
                <div class="metric">
                    <div class="metric-value">{quality_report.get('total_checks', 0)}</div>
                    <div class="metric-label">Total Checks</div>
                </div>
                
                <div class="metric">
                    <div class="metric-value" style="color: #27ae60;">{quality_report.get('passed_checks', 0)}</div>
                    <div class="metric-label">Passed</div>
                </div>
                
                <div class="metric">
                    <div class="metric-value" style="color: #e74c3c;">{quality_report.get('failed_checks', 0)}</div>
                    <div class="metric-label">Failed</div>
                </div>
                
                <div class="metric">
                    <div class="metric-value" style="color: #f39c12;">{anomaly_count}</div>
                    <div class="metric-label">Anomalies Detected</div>
                </div>
            </div>
            
            <div class="profile-section">
                <h2>Quality Checks Details</h2>
        """
        
        # Add individual check results
        for check in quality_report.get('checks', []):
            check_class = "passed" if check.get('passed') else "failed"
            status_icon = "✓"