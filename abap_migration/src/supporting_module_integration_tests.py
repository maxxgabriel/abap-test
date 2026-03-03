I'll generate production-ready PySpark code for supporting module integration tests covering data initialization, quality management, logger, monitoring, and test framework modules.

===FILE: src/data_init.py===
"""
Data initialization module for ETL testing and setup.
Handles creation of sample data, configuration, and schedules.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from datetime import datetime, timedelta
from typing import Dict, List
import random


class DataInitializer:
    """Initialize sample data for ETL testing and operations."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def get_source_schema(self) -> StructType:
        """Define source data schema."""
        return StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("value", DecimalType(15, 2), False),
            StructField("status", StringType(), False),
            StructField("category", StringType(), False),
            StructField("source_system", StringType(), False),
            StructField("created_at", TimestampType(), False),
            StructField("created_by", StringType(), False),
            StructField("changed_at", TimestampType(), False),
            StructField("changed_by", StringType(), False)
        ])
    
    def get_config_schema(self) -> StructType:
        """Define configuration schema."""
        return StructType([
            StructField("config_key", StringType(), False),
            StructField("config_value", StringType(), False),
            StructField("description", StringType(), True),
            StructField("config_type", StringType(), False),
            StructField("is_active", StringType(), False),
            StructField("changed_at", TimestampType(), False),
            StructField("changed_by", StringType(), False)
        ])
    
    def get_schedule_schema(self) -> StructType:
        """Define schedule schema."""
        return StructType([
            StructField("schedule_id", StringType(), False),
            StructField("schedule_name", StringType(), False),
            StructField("etl_type", StringType(), False),
            StructField("frequency", StringType(), False),
            StructField("start_date", StringType(), False),
            StructField("start_time", StringType(), False),
            StructField("is_active", StringType(), False),
            StructField("created_by", StringType(), False),
            StructField("created_at", TimestampType(), False)
        ])
    
    def create_sample_data(self, num_rows: int = 100) -> DataFrame:
        """
        Create sample source data.
        
        Args:
            num_rows: Number of sample records to create
            
        Returns:
            DataFrame with sample source data
        """
        categories = ["PREMIUM", "STANDARD", "BASIC", "VIP", "TRIAL"]
        current_time = datetime.now()
        
        data = []
        for i in range(1, num_rows + 1):
            value = random.uniform(50, 1000)
            category = categories[i % len(categories)]
            
            record = {
                "id": f"{i:010d}",
                "name": f"Product {i}",
                "value": round(value, 2),
                "status": "ACTIVE",
                "category": category,
                "source_system": "SAP_ERP",
                "created_at": current_time,
                "created_by": "system",
                "changed_at": current_time,
                "changed_by": "system"
            }
            data.append(record)
        
        df = self.spark.createDataFrame(data, schema=self.get_source_schema())
        return df
    
    def create_config_data(self) -> DataFrame:
        """Create sample configuration data."""
        current_time = datetime.now()
        
        configs = [
            {
                "config_key": "BATCH_SIZE",
                "config_value": "1000",
                "description": "Default batch size for data loading",
                "config_type": "PERFORMANCE",
                "is_active": "X",
                "changed_at": current_time,
                "changed_by": "admin"
            },
            {
                "config_key": "MAX_RETRIES",
                "config_value": "3",
                "description": "Maximum number of retries on error",
                "config_type": "ERROR_HANDLING",
                "is_active": "X",
                "changed_at": current_time,
                "changed_by": "admin"
            },
            {
                "config_key": "ALERT_EMAIL",
                "config_value": "admin@example.com",
                "description": "Email address for alerts",
                "config_type": "NOTIFICATION",
                "is_active": "X",
                "changed_at": current_time,
                "changed_by": "admin"
            },
            {
                "config_key": "LOG_RETENTION_DAYS",
                "config_value": "90",
                "description": "Number of days to retain logs",
                "config_type": "MAINTENANCE",
                "is_active": "X",
                "changed_at": current_time,
                "changed_by": "admin"
            },
            {
                "config_key": "ENABLE_RECONCILIATION",
                "config_value": "X",
                "description": "Enable data reconciliation after load",
                "config_type": "DATA_QUALITY",
                "is_active": "X",
                "changed_at": current_time,
                "changed_by": "admin"
            },
            {
                "config_key": "PREMIUM_MULTIPLIER",
                "config_value": "1.5",
                "description": "Value multiplier for premium category",
                "config_type": "BUSINESS_RULE",
                "is_active": "X",
                "changed_at": current_time,
                "changed_by": "admin"
            }
        ]
        
        return self.spark.createDataFrame(configs, schema=self.get_config_schema())
    
    def create_schedule_data(self) -> DataFrame:
        """Create sample schedule data."""
        current_time = datetime.now()
        current_date = current_time.strftime("%Y-%m-%d")
        
        schedules = [
            {
                "schedule_id": "SCHED001",
                "schedule_name": "Daily Full Load",
                "etl_type": "FULL",
                "frequency": "DAILY",
                "start_date": current_date,
                "start_time": "02:00:00",
                "is_active": "X",
                "created_by": "admin",
                "created_at": current_time
            },
            {
                "schedule_id": "SCHED002",
                "schedule_name": "Hourly Incremental",
                "etl_type": "INCREMENTAL",
                "frequency": "HOURLY",
                "start_date": current_date,
                "start_time": "00:00:00",
                "is_active": "X",
                "created_by": "admin",
                "created_at": current_time
            },
            {
                "schedule_id": "SCHED003",
                "schedule_name": "Weekly Reconciliation",
                "etl_type": "RECONCILIATION",
                "frequency": "WEEKLY",
                "start_date": current_date,
                "start_time": "18:00:00",
                "is_active": "",
                "created_by": "admin",
                "created_at": current_time
            }
        ]
        
        return self.spark.createDataFrame(schedules, schema=self.get_schedule_schema())
    
    def initialize_all(self, num_rows: int = 100) -> Dict[str, DataFrame]:
        """
        Initialize all data types.
        
        Args:
            num_rows: Number of sample records to create
            
        Returns:
            Dictionary containing all initialized DataFrames
        """
        return {
            "source_data": self.create_sample_data(num_rows),
            "config_data": self.create_config_data(),
            "schedule_data": self.create_schedule_data()
        }
    
    def write_to_storage(self, data_dict: Dict[str, DataFrame], base_path: str, mode: str = "overwrite"):
        """
        Write initialized data to storage.
        
        Args:
            data_dict: Dictionary of DataFrames to write
            base_path: Base path for storage
            mode: Write mode (overwrite, append, etc.)
        """
        for name, df in data_dict.items():
            path = f"{base_path}/{name}"
            df.write.mode(mode).parquet(path)


===FILE: src/data_quality.py===
"""
Data quality management module for ETL processes.
Provides comprehensive data quality checks and profiling.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, DecimalType
from typing import Dict, List, Tuple
from dataclasses import dataclass
import math


@dataclass
class QualityCheck:
    """Quality check result."""
    check_name: str
    check_type: str
    passed: bool
    failed_count: int
    message: str


@dataclass
class DataProfile:
    """Data profile statistics."""
    total_records: int
    null_count: int
    duplicate_count: int
    min_value: float
    max_value: float
    avg_value: float
    std_deviation: float
    unique_categories: int


class DataQualityManager:
    """Manage data quality checks and profiling."""
    
    def __init__(self, spark: SparkSession, run_id: str):
        self.spark = spark
        self.run_id = run_id
    
    def perform_quality_checks(self, df: DataFrame) -> List[QualityCheck]:
        """
        Perform all quality checks on the data.
        
        Args:
            df: DataFrame to check
            
        Returns:
            List of quality check results
        """
        checks = [
            self.check_completeness(df),
            self.check_uniqueness(df),
            self.check_validity(df),
            self.check_consistency(df)
        ]
        
        passed_count = sum(1 for check in checks if check.passed)
        failed_count = len(checks) - passed_count
        
        print(f"Quality checks complete: {passed_count} passed, {failed_count} failed")
        
        return checks
    
    def check_completeness(self, df: DataFrame) -> QualityCheck:
        """
        Check for null/empty values in critical fields.
        
        Args:
            df: DataFrame to check
            
        Returns:
            QualityCheck result
        """
        null_count = df.filter(
            F.col("id").isNull() | 
            F.col("name").isNull() | 
            F.col("value").isNull() |
            (F.col("id") == "") |
            (F.col("name") == "")
        ).count()
        
        passed = null_count == 0
        message = "All required fields are complete" if passed else f"{null_count} records with incomplete data"
        
        return QualityCheck(
            check_name="Completeness Check",
            check_type="COMPLETENESS",
            passed=passed,
            failed_count=null_count,
            message=message
        )
    
    def check_uniqueness(self, df: DataFrame) -> QualityCheck:
        """
        Check for duplicate IDs.
        
        Args:
            df: DataFrame to check
            
        Returns:
            QualityCheck result
        """
        total_count = df.count()
        unique_count = df.select("id").distinct().count()
        dup_count = total_count - unique_count
        
        passed = dup_count == 0
        message = "All IDs are unique" if passed else f"{dup_count} duplicate IDs found"
        
        return QualityCheck(
            check_name="Uniqueness Check",
            check_type="UNIQUENESS",
            passed=passed,
            failed_count=dup_count,
            message=message
        )
    
    def check_validity(self, df: DataFrame) -> QualityCheck:
        """
        Check for invalid values.
        
        Args:
            df: DataFrame to check
            
        Returns:
            QualityCheck result
        """
        invalid_count = df.filter(
            (F.col("value") < 0) |
            (F.col("transformed_value") < 0) |
            (F.col("priority") < 1) |
            (F.col("priority") > 5) |
            F.col("category").isNull() |
            (F.col("category") == "")
        ).count()
        
        passed = invalid_count == 0
        message = "All values are valid" if passed else f"{invalid_count} records with invalid values"
        
        return QualityCheck(
            check_name="Validity Check",
            check_type="VALIDITY",
            passed=passed,
            failed_count=invalid_count,
            message=message
        )
    
    def check_consistency(self, df: DataFrame) -> QualityCheck:
        """
        Check for data consistency issues.
        
        Args:
            df: DataFrame to check
            
        Returns:
            QualityCheck result
        """
        inconsistent_count = df.filter(
            (F.col("transformed_value") < F.col("value") * 0.5) |
            (F.col("transformed_value") > F.col("value") * 3)
        ).count()
        
        passed = inconsistent_count == 0
        message = "Data is consistent" if passed else f"{inconsistent_count} records with consistency issues"
        
        return QualityCheck(
            check_name="Consistency Check",
            check_type="CONSISTENCY",
            passed=passed,
            failed_count=inconsistent_count,
            message=message
        )
    
    def profile_data(self, df: DataFrame) -> DataProfile:
        """
        Generate data profile statistics.
        
        Args:
            df: DataFrame to profile
            
        Returns:
            DataProfile with statistics
        """
        stats = df.agg(
            F.count("*").alias("total_records"),
            F.sum(F.when(F.col("value").isNull(), 1).otherwise(0)).alias("null_count"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
            F.avg("value").alias("avg_value"),
            F.stddev("value").alias("std_deviation"),
            F.countDistinct("category").alias("unique_categories")
        ).collect()[0]
        
        # Check for duplicates
        total_count = df.count()
        unique_count = df.select("id").distinct().count()
        duplicate_count = total_count - unique_count
        
        return DataProfile(
            total_records=stats["total_records"],
            null_count=stats["null_count"],
            duplicate_count=duplicate_count,
            min_value=float(stats["min_value"]) if stats["min_value"] else 0.0,
            max_value=float(stats["max_value"]) if stats["max_value"] else 0.0,
            avg_value=float(stats["avg_value"]) if stats["avg_value"] else 0.0,
            std_deviation=float(stats["std_deviation"]) if stats["std_deviation"] else 0.0,
            unique_categories=stats["unique_categories"]
        )
    
    def detect_anomalies(self, df: DataFrame, threshold_std: float = 3.0) -> List[str]:
        """
        Detect anomalies in the data using standard deviation.
        
        Args:
            df: DataFrame to check
            threshold_std: Number of standard deviations for anomaly detection
            
        Returns:
            List of anomaly descriptions
        """
        anomalies = []
        
        # Calculate statistics
        stats = df.select(
            F.mean("value").alias("mean"),
            F.stddev("value").alias("stddev")
        ).collect()[0]
        
        mean_value = float(stats["mean"]) if stats["mean"] else 0.0
        std_value = float(stats["stddev"]) if stats["stddev"] else 0.0
        
        if std_value > 0:
            # Find outliers
            outliers = df.filter(
                (F.col("value") > mean_value + (threshold_std * std_value)) |
                (F.col("value") < mean_value - (threshold_std * std_value))
            )
            
            outlier_count = outliers.count()
            if outlier_count > 0:
                anomalies.append(f"Found {outlier_count} statistical outliers (>{threshold_std} std dev)")
                
                # Sample some outliers
                sample_outliers = outliers.select("id", "value").limit(5).collect()
                for row in sample_outliers:
                    anomalies.append(f"  - ID {row['id']}: value={row['value']}")
        
        # Check for category imbalances
        category_counts = df.groupBy("category").count().collect()
        total = df.count()
        
        for row in category_counts:
            percentage = (row["count"] / total) * 100
            if percentage < 1:
                anomalies.append(f"Category '{row['category']}' has only {percentage:.2f}% of records")
        
        return anomalies
    
    def generate_quality_report(self, df: DataFrame) -> Dict:
        """
        Generate comprehensive quality report.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary with quality report
        """
        checks = self.perform_quality_checks(df)
        profile = self.profile_data(df)
        anomalies = self.detect_anomalies(df)
        
        return {
            "run_id": self.run_id,
            "checks": [
                {
                    "name": check.check_name,
                    "type": check.check_type,
                    "passed": check.passed,
                    "failed_count": check.failed_count,
                    "message": check.message
                }
                for check in checks
            ],
            "profile": {
                "total_records": profile.total_records,
                "null_count": profile.null_count,
                "duplicate_count": profile.duplicate_count,
                "min_value": profile.min_value,
                "max_value": profile.max_value,
                "avg_value": profile.avg_value,
                "std_deviation": profile.std_deviation,
                "unique_categories": profile.unique_categories
            },
            "anomalies": anomalies
        }


===FILE: src/logger.py===
"""
Logging module for ETL processes.
Provides structured logging with different severity levels.
"""
from datetime import datetime
from typing import List, Dict, Optional
from enum import Enum
from dataclasses import dataclass, asdict
import json


class LogLevel(Enum):
    """Log severity levels."""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    DEBUG = "DEBUG"


@dataclass
class LogEntry:
    """Structured log entry."""
    timestamp: str
    level: str
    component: str
    message: str
    details: Optional[str] = None
    run_id: Optional[str] = None


class ETLLogger:
    """Singleton logger for ETL processes."""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self.logs: List[LogEntry] = []
            self._initialized = True
    
    @classmethod
    def get_instance(cls) -> 'ETLLogger':
        """Get singleton logger instance."""
        return cls()
    
    def _add_log_entry(self, level: LogLevel, component: str, message: str, 
                       details: Optional[str] = None, run_id: Optional[str] = None):
        """Add a log entry."""
        entry = LogEntry(
            timestamp=datetime.now().isoformat(),
            level=level.value,
            component=component,
            message=message,
            details=details,
            run_id=run_id
        )
        
        self.logs.append(entry)
        
        # Also print to console
        log_str = f"[{entry.timestamp}] {entry.level}: {entry.component} - {entry.message}"
        if entry.details:
            log_str += f"\n  Details: {entry.details}"
        print(log_str)
    
    def log_info(self, component: str, message: str, details: Optional[str] = None, 
                 run_id: Optional[str] = None):
        """Log info message."""
        self._add_log_entry(LogLevel.INFO, component, message, details, run_id)
    
    def log_warning(self, component: str, message: str, details: Optional[str] = None,
                    run_id: Optional[str] = None):
        """Log warning message."""
        self._add_log_entry(LogLevel.WARNING, component, message, details, run_id)
    
    def log_error(self, component: str, message: str, details: Optional[str] = None,
                  run_id: Optional[str] = None):
        """Log error message."""
        self._add_log_entry(LogLevel.ERROR, component, message, details, run_id)
    
    def log_debug(self, component: str, message: str, details: Optional[str] = None,
                  run_id: Optional[str] = None):
        """Log debug message."""
        self._add_log_entry(LogLevel.DEBUG, component, message, details, run_id)
    
    def get_logs(self, level: Optional[LogLevel] = None, component: Optional[str] = None) -> List[LogEntry]:
        """
        Get logs with optional filtering.
        
        Args:
            level: Filter by log level
            component: Filter by component
            
        Returns:
            Filtered list of log entries
        """
        filtered_logs = self.logs
        
        if level:
            filtered_logs = [log for log in filtered_logs if log.level == level.value]
        
        if component:
            filtered_logs = [log for log in filtered_logs if log.component == component]
        
        return filtered_logs
    
    def get_logs_as_dict(self) -> List[Dict]:
        """Get logs as list of dictionaries."""
        return [asdict(log) for log in self.logs]
    
    def get_logs_as_json(self) -> str:
        """Get logs as JSON string."""
        return json.dumps(self.get_logs_as_dict(), indent=2)
    
    def clear_logs(self):
        """Clear all logs."""
        self.logs.clear()
    
    def get_error_count(self) -> int:
        """Get count of error logs."""
        return len([log for log in self.logs if log.level == LogLevel.ERROR.value])
    
    def get_warning_count(self) -> int:
        """Get count of warning logs."""
        return len([log for log in self.logs if log.level == LogLevel.WARNING.value])
    
    def has_errors(self) -> bool:
        """Check if any errors were logged."""
        return self.get_error_count() > 0


===FILE: src/monitor.py===
"""
Monitoring module for ETL processes.
Provides health checks, performance metrics, and alerting.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass
from src.logger import ETLLogger


@dataclass
class DashboardData:
    """ETL dashboard metrics."""
    total_runs: int
    successful_runs: int
    failed_runs: int
    running_jobs: int
    avg_duration: float
    total_records: int
    error_rate: float
    last_run_time: Optional[str]
    last_run_status: Optional[str]


@dataclass
class PerformanceMetric:
    """Performance metric for a single run."""
    run_id: str
    start_time: str
    duration: int
    records_processed: int
    throughput: float
    status: str


class ETLMonitor:
    """Monitor ETL processes and provide health metrics."""
    
    _instance = None
    
    def __new__(cls, spark: SparkSession):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, spark: SparkSession):
        if not self._initialized:
            self.spark = spark
            self.logger = ETLLogger.get_instance()
            self._initialized = True
    
    @classmethod
    def get_instance(cls, spark: SparkSession) -> 'ETLMonitor':
        """Get singleton monitor instance."""
        return cls(spark)
    
    def get_dashboard_data(self, run_log_df: DataFrame, days_back: int = 7) -> DashboardData:
        """
        Get dashboard metrics.
        
        Args:
            run_log_df: DataFrame containing run log data
            days_back: Number of days to look back
            
        Returns:
            DashboardData with metrics
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        cutoff_str = cutoff_time.isoformat()
        
        # Filter recent runs
        recent_runs = run_log_df.filter(F.col("start_time") >= cutoff_str)
        
        # Calculate statistics
        stats = recent_runs.agg(
            F.count("*").alias("total_runs"),
            F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("successful"),
            F.sum(F.when((F.col("status") == "FAILED") | (F.col("status") == "ERROR"), 1).otherwise(0)).alias("failed"),
            F.sum(F.when(F.col("status") == "RUNNING", 1).otherwise(0)).alias("running"),
            F.avg("duration").alias("avg_duration"),
            F.sum("records_loaded").alias("total_records")
        ).collect()[0]
        
        total_runs = stats["total_runs"] or 0
        successful = stats["successful"] or 0
        failed = stats["failed"] or 0
        
        # Calculate error rate
        error_rate = (failed / total_runs * 100) if total_runs > 0 else 0.0
        
        # Get last run info
        last_run = run_log_df.orderBy(F.col("end_time").desc()).limit(1).collect()
        last_run_time = last_run[0]["end_time"] if last_run else None
        last_run_status = last_run[0]["status"] if last_run else None
        
        return DashboardData(
            total_runs=total_runs,
            successful_runs=successful,
            failed_runs=failed,
            running_jobs=stats["running"] or 0,
            avg_duration=float(stats["avg_duration"]) if stats["avg_duration"] else 0.0,
            total_records=stats["total_records"] or 0,
            error_rate=error_rate,
            last_run_time=last_run_time,
            last_run_status=last_run_status
        )
    
    def get_performance_metrics(self, run_log_df: DataFrame, days_back: int = 30) -> List[PerformanceMetric]:
        """
        Get performance metrics for recent runs.
        
        Args:
            run_log_df: DataFrame containing run log data
            days_back: Number of days to look back
            
        Returns:
            List of performance metrics
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        cutoff_str = cutoff_time.isoformat()
        
        recent_runs = run_log_df.filter(
            F.col("start_time") >= cutoff_str
        ).select(
            "run_id",
            "start_time",
            "duration",
            F.col("records_loaded").alias("records_processed"),
            "status"
        ).orderBy(F.col("start_time").desc()).collect()
        
        metrics = []
        for row in recent_runs:
            throughput = self._calculate_throughput(
                row["records_processed"] or 0,
                row["duration"] or 1
            )
            
            metrics.append(PerformanceMetric(
                run_id=row["run_id"],
                start_time=row["start_time"],
                duration=row["duration"] or 0,
                records_processed=row["records_processed"] or 0,
                throughput=throughput,
                status=row["status"]
            ))
        
        return metrics
    
    def _calculate_throughput(self, records: int, duration: int) -> float:
        """Calculate throughput (records per second)."""
        if duration == 0:
            return 0.0
        return round(records / duration, 2)
    
    def check_health(self, run_log_df: DataFrame) -> str:
        """
        Check ETL system health.
        
        Args:
            run_log_df: DataFrame containing run log data
            
        Returns:
            Health status string
        """
        dashboard = self.get_dashboard_data(run_log_df, days_back=1)
        
        if dashboard.running_jobs > 5:
            self.send_alert(
                alert_type="PERFORMANCE",
                message=f"Too many running jobs: {dashboard.running_jobs}",
                severity="HIGH"
            )
            return "OVERLOADED"
        
        elif dashboard.error_rate > 50:
            self.send_alert(
                alert_type="ERROR_RATE",
                message=f"High error rate: {dashboard.error_rate:.2f}%",
                severity="CRITICAL"
            )
            return "CRITICAL"
        
        elif dashboard.error_rate > 20:
            self.send_alert(
                alert_type="ERROR_RATE",
                message=f"Elevated error rate: {dashboard.error_rate:.2f}%",
                severity="MEDIUM"
            )
            return "WARNING"
        
        elif dashboard.failed_runs == 0 and dashboard.successful_runs > 0:
            return "HEALTHY"
        
        else:
            return "UNKNOWN"
    
    def send_alert(self, alert_type: str, message: str, severity: str):
        """
        Send alert (log for now, can be extended to email/Slack).
        
        Args:
            alert_type: Type of alert
            message: Alert message
            severity: Alert severity
        """
        self.logger.log_warning(
            component="MONITOR",
            message=f"[{alert_type}] {message}",
            details=f"Severity: {severity}"
        )
    
    def get_health_summary(self, run_log_df: DataFrame) -> Dict:
        """
        Get comprehensive health summary.
        
        Args:
            run_log_df: DataFrame containing run log data
            
        Returns:
            Dictionary with health summary
        """
        health_status = self.check_health(run_log_df)
        dashboard = self.get_dashboard_data(run_log_df, days_back=7)
        metrics