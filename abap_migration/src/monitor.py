"""
ETL Monitoring Module with metrics aggregation and datetime-based tracking.
Converts ABAP monitoring singleton to Python class with PySpark DataFrame operations.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    when, lit, current_timestamp, datediff, stddev, countDistinct,
    concat_ws, unix_timestamp, from_unixtime
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
import logging
from dataclasses import dataclass, asdict
import yaml


@dataclass
class DashboardData:
    """Dashboard summary metrics."""
    total_runs: int
    successful_runs: int
    failed_runs: int
    running_jobs: int
    avg_duration: float
    total_records: int
    error_rate: float
    last_run_time: Optional[datetime]
    last_run_status: Optional[str]


@dataclass
class PerformanceMetric:
    """Performance metrics for a single ETL run."""
    run_id: str
    start_time: datetime
    duration: int
    records_processed: int
    throughput: float
    status: str


@dataclass
class HealthStatus:
    """System health status."""
    status: str
    timestamp: datetime
    running_jobs: int
    error_rate: float
    alerts: List[str]


class ETLMonitor:
    """
    Singleton ETL monitoring class with PySpark DataFrame operations.
    Provides metrics aggregation, health checks, and alerting.
    """
    
    _instance: Optional['ETLMonitor'] = None
    _initialized: bool = False
    
    def __new__(cls, spark: Optional[SparkSession] = None, config: Optional[Dict] = None):
        """Ensure singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, spark: Optional[SparkSession] = None, config: Optional[Dict] = None):
        """Initialize monitor with Spark session and configuration."""
        if not self._initialized:
            self.spark = spark or SparkSession.builder.appName("ETLMonitor").getOrCreate()
            self.config = config or {}
            self.logger = logging.getLogger(__name__)
            
            # Load configuration
            self.alert_threshold_error_rate = self.config.get('alert_threshold_error_rate', 50.0)
            self.alert_threshold_jobs = self.config.get('alert_threshold_jobs', 5)
            self.default_lookback_days = self.config.get('default_lookback_days', 7)
            
            # Define schemas
            self._define_schemas()
            
            ETLMonitor._initialized = True
            self.logger.info("ETL Monitor initialized")
    
    def _define_schemas(self):
        """Define DataFrame schemas for ETL logs."""
        self.run_log_schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("status", StringType(), False),
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), True),
            StructField("duration", IntegerType(), True),
            StructField("records_extracted", IntegerType(), True),
            StructField("records_transformed", IntegerType(), True),
            StructField("records_loaded", IntegerType(), True),
            StructField("records_failed", IntegerType(), True),
            StructField("error_count", IntegerType(), True),
            StructField("warning_count", IntegerType(), True)
        ])
        
        self.error_log_schema = StructType([
            StructField("error_id", StringType(), False),
            StructField("run_id", StringType(), False),
            StructField("component", StringType(), False),
            StructField("error_type", StringType(), False),
            StructField("message", StringType(), False),
            StructField("details", StringType(), True),
            StructField("created_at", TimestampType(), False),
            StructField("resolved", BooleanType(), False)
        ])
    
    @classmethod
    def get_instance(cls, spark: Optional[SparkSession] = None, config: Optional[Dict] = None) -> 'ETLMonitor':
        """Get singleton instance of ETL Monitor."""
        if cls._instance is None:
            cls._instance = cls(spark, config)
        return cls._instance
    
    def get_dashboard_data(self, days_back: int = None) -> DashboardData:
        """
        Get dashboard summary metrics for specified time period.
        
        Args:
            days_back: Number of days to look back (default from config)
            
        Returns:
            DashboardData object with aggregated metrics
        """
        days_back = days_back or self.default_lookback_days
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        self.logger.info(f"Calculating dashboard data for last {days_back} days")
        
        # Load run log data
        run_log_df = self._load_run_log_data()
        
        # Filter by time period
        filtered_df = run_log_df.filter(col("start_time") >= lit(cutoff_time))
        
        # Calculate aggregated metrics
        stats = filtered_df.agg(
            count("*").alias("total_runs"),
            spark_sum(when(col("status") == "SUCCESS", 1).otherwise(0)).alias("successful_runs"),
            spark_sum(
                when(
                    (col("status") == "FAILED") | (col("status") == "ERROR"),
                    1
                ).otherwise(0)
            ).alias("failed_runs"),
            spark_sum(when(col("status") == "RUNNING", 1).otherwise(0)).alias("running_jobs"),
            avg("duration").alias("avg_duration"),
            spark_sum("records_loaded").alias("total_records")
        ).collect()[0]
        
        # Calculate error rate
        total_runs = stats["total_runs"] or 0
        error_rate = (stats["failed_runs"] / total_runs * 100) if total_runs > 0 else 0.0
        
        # Get last run info
        last_run = filtered_df.orderBy(col("end_time").desc()).select("end_time", "status").first()
        last_run_time = last_run["end_time"] if last_run else None
        last_run_status = last_run["status"] if last_run else None
        
        dashboard = DashboardData(
            total_runs=stats["total_runs"] or 0,
            successful_runs=stats["successful_runs"] or 0,
            failed_runs=stats["failed_runs"] or 0,
            running_jobs=stats["running_jobs"] or 0,
            avg_duration=float(stats["avg_duration"] or 0.0),
            total_records=stats["total_records"] or 0,
            error_rate=round(error_rate, 2),
            last_run_time=last_run_time,
            last_run_status=last_run_status
        )
        
        self.logger.info(f"Dashboard data calculated: {dashboard.total_runs} total runs, {dashboard.error_rate}% error rate")
        return dashboard
    
    def get_performance_metrics(self, days_back: int = 30) -> List[PerformanceMetric]:
        """
        Get performance metrics for individual runs.
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            List of PerformanceMetric objects
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        self.logger.info(f"Retrieving performance metrics for last {days_back} days")
        
        # Load and filter run log
        run_log_df = self._load_run_log_data()
        filtered_df = run_log_df.filter(col("start_time") >= lit(cutoff_time))
        
        # Calculate throughput (records per second)
        metrics_df = filtered_df.withColumn(
            "throughput",
            when(col("duration") > 0, col("records_loaded") / col("duration")).otherwise(0.0)
        ).select(
            "run_id",
            "start_time",
            "duration",
            col("records_loaded").alias("records_processed"),
            "throughput",
            "status"
        ).orderBy(col("start_time").desc())
        
        # Convert to list of PerformanceMetric objects
        metrics = []
        for row in metrics_df.collect():
            metrics.append(PerformanceMetric(
                run_id=row["run_id"],
                start_time=row["start_time"],
                duration=row["duration"] or 0,
                records_processed=row["records_processed"] or 0,
                throughput=round(float(row["throughput"] or 0.0), 2),
                status=row["status"]
            ))
        
        self.logger.info(f"Retrieved {len(metrics)} performance metrics")
        return metrics
    
    def get_error_summary(self, days_back: int = 7) -> DataFrame:
        """
        Get summary of errors for specified time period.
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            DataFrame with error summary
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        self.logger.info(f"Retrieving error summary for last {days_back} days")
        
        # Load error log
        error_log_df = self._load_error_log_data()
        
        # Filter unresolved errors in time period
        error_summary = error_log_df.filter(
            (col("created_at") >= lit(cutoff_time)) &
            (col("resolved") == False)
        ).orderBy(col("created_at").desc())
        
        error_count = error_summary.count()
        self.logger.info(f"Found {error_count} unresolved errors")
        
        return error_summary
    
    def check_health(self) -> HealthStatus:
        """
        Check overall system health and generate alerts.
        
        Returns:
            HealthStatus object with health status and alerts
        """
        self.logger.info("Performing health check")
        
        # Get current dashboard data (last 24 hours)
        dashboard = self.get_dashboard_data(days_back=1)
        
        alerts = []
        status = "HEALTHY"
        
        # Check for overloaded system
        if dashboard.running_jobs > self.alert_threshold_jobs:
            status = "OVERLOADED"
            alerts.append(f"Too many running jobs: {dashboard.running_jobs}")
            self._send_alert(
                alert_type="PERFORMANCE",
                message=f"Too many running jobs: {dashboard.running_jobs}",
                severity="HIGH"
            )
        
        # Check error rate
        if dashboard.error_rate > self.alert_threshold_error_rate:
            status = "CRITICAL"
            alerts.append(f"High error rate: {dashboard.error_rate}%")
            self._send_alert(
                alert_type="ERROR_RATE",
                message=f"Critical error rate: {dashboard.error_rate}%",
                severity="CRITICAL"
            )
        elif dashboard.error_rate > 20.0:
            if status == "HEALTHY":
                status = "WARNING"
            alerts.append(f"Elevated error rate: {dashboard.error_rate}%")
            self._send_alert(
                alert_type="ERROR_RATE",
                message=f"Elevated error rate: {dashboard.error_rate}%",
                severity="MEDIUM"
            )
        
        # Check if no failures and has successful runs
        if dashboard.failed_runs == 0 and dashboard.successful_runs > 0:
            status = "HEALTHY"
        
        health_status = HealthStatus(
            status=status,
            timestamp=datetime.now(),
            running_jobs=dashboard.running_jobs,
            error_rate=dashboard.error_rate,
            alerts=alerts
        )
        
        self.logger.info(f"Health check complete: {status}")
        return health_status
    
    def get_metrics_by_component(self, component: str, days_back: int = 7) -> DataFrame:
        """
        Get error metrics grouped by component.
        
        Args:
            component: Component name (EXTRACTOR, TRANSFORMER, LOADER)
            days_back: Number of days to look back
            
        Returns:
            DataFrame with component metrics
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        error_log_df = self._load_error_log_data()
        
        component_metrics = error_log_df.filter(
            (col("created_at") >= lit(cutoff_time)) &
            (col("component") == component)
        ).groupBy("error_type").agg(
            count("*").alias("error_count"),
            countDistinct("run_id").alias("affected_runs")
        ).orderBy(col("error_count").desc())
        
        return component_metrics
    
    def calculate_sla_compliance(self, sla_duration_seconds: int = 3600, days_back: int = 30) -> Dict:
        """
        Calculate SLA compliance based on run duration.
        
        Args:
            sla_duration_seconds: Maximum allowed duration in seconds
            days_back: Number of days to analyze
            
        Returns:
            Dictionary with SLA compliance metrics
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        run_log_df = self._load_run_log_data()
        filtered_df = run_log_df.filter(col("start_time") >= lit(cutoff_time))
        
        sla_stats = filtered_df.agg(
            count("*").alias("total_runs"),
            spark_sum(when(col("duration") <= sla_duration_seconds, 1).otherwise(0)).alias("within_sla"),
            spark_sum(when(col("duration") > sla_duration_seconds, 1).otherwise(0)).alias("breached_sla"),
            avg("duration").alias("avg_duration"),
            spark_max("duration").alias("max_duration")
        ).collect()[0]
        
        total_runs = sla_stats["total_runs"] or 0
        compliance_rate = (sla_stats["within_sla"] / total_runs * 100) if total_runs > 0 else 0.0
        
        return {
            "total_runs": total_runs,
            "within_sla": sla_stats["within_sla"] or 0,
            "breached_sla": sla_stats["breached_sla"] or 0,
            "compliance_rate": round(compliance_rate, 2),
            "avg_duration_seconds": float(sla_stats["avg_duration"] or 0.0),
            "max_duration_seconds": sla_stats["max_duration"] or 0,
            "sla_target_seconds": sla_duration_seconds
        }
    
    def get_trend_analysis(self, days_back: int = 30) -> DataFrame:
        """
        Analyze trends in ETL performance over time.
        
        Args:
            days_back: Number of days to analyze
            
        Returns:
            DataFrame with daily trend metrics
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        run_log_df = self._load_run_log_data()
        
        trend_df = run_log_df.filter(
            col("start_time") >= lit(cutoff_time)
        ).withColumn(
            "run_date",
            from_unixtime(unix_timestamp("start_time"), "yyyy-MM-dd")
        ).groupBy("run_date").agg(
            count("*").alias("total_runs"),
            spark_sum(when(col("status") == "SUCCESS", 1).otherwise(0)).alias("successful_runs"),
            spark_sum(when((col("status") == "FAILED") | (col("status") == "ERROR"), 1).otherwise(0)).alias("failed_runs"),
            avg("duration").alias("avg_duration"),
            spark_sum("records_loaded").alias("total_records"),
            avg("records_loaded").alias("avg_records")
        ).orderBy("run_date")
        
        return trend_df
    
    def _load_run_log_data(self) -> DataFrame:
        """Load ETL run log data from configured source."""
        # In production, this would read from actual data source
        # For now, create sample data
        run_log_path = self.config.get('run_log_path', 'data/etl_run_log')
        
        try:
            df = self.spark.read.schema(self.run_log_schema).parquet(run_log_path)
        except Exception:
            # Return empty DataFrame if no data exists
            df = self.spark.createDataFrame([], self.run_log_schema)
        
        return df
    
    def _load_error_log_data(self) -> DataFrame:
        """Load ETL error log data from configured source."""
        error_log_path = self.config.get('error_log_path', 'data/etl_error_log')
        
        try:
            df = self.spark.read.schema(self.error_log_schema).parquet(error_log_path)
        except Exception:
            df = self.spark.createDataFrame([], self.error_log_schema)
        
        return df
    
    def _send_alert(self, alert_type: str, message: str, severity: str):
        """
        Send alert notification.
        
        Args:
            alert_type: Type of alert
            message: Alert message
            severity: Alert severity level
        """
        self.logger.warning(f"ALERT [{severity}] {alert_type}: {message}")
        
        # In production, integrate with alerting system (email, Slack, PagerDuty, etc.)
        alert_config = self.config.get('alerting', {})
        if alert_config.get('enabled', False):
            # Implement actual alerting logic here
            pass
    
    def write_metrics_snapshot(self, output_path: str):
        """
        Write current metrics snapshot to storage.
        
        Args:
            output_path: Path to write metrics snapshot
        """
        dashboard = self.get_dashboard_data()
        health = self.check_health()
        
        metrics_data = {
            **asdict(dashboard),
            "health_status": health.status,
            "health_alerts": health.alerts,
            "snapshot_time": datetime.now().isoformat()
        }
        
        # Convert to DataFrame and write
        metrics_df = self.spark.createDataFrame([metrics_data])
        metrics_df.write.mode("append").partitionBy("snapshot_time").parquet(output_path)
        
        self.logger.info(f"Metrics snapshot written to {output_path}")