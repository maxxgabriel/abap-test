"""
ETL Monitoring Module - PySpark Implementation
Converts ABAP monitoring singleton to Python class with DataFrame aggregations
"""
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    TimestampType, DoubleType, LongType
)
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging


class ETLMonitor:
    """
    Singleton ETL Monitor with PySpark DataFrame aggregations
    Replaces ABAP zcl_etl_monitor with groupBy/agg operations
    """
    
    _instance = None
    _lock = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ETLMonitor, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self.spark = SparkSession.builder.getOrCreate()
        self.logger = logging.getLogger(__name__)
        self._initialized = True
        
        # Define schemas for monitoring tables
        self.run_log_schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("status", StringType(), False),
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), True),
            StructField("duration", IntegerType(), True),
            StructField("records_extracted", LongType(), True),
            StructField("records_transformed", LongType(), True),
            StructField("records_loaded", LongType(), True),
            StructField("records_failed", LongType(), True),
            StructField("error_count", IntegerType(), True),
            StructField("warning_count", IntegerType(), True)
        ])
        
        self.error_log_schema = StructType([
            StructField("error_id", StringType(), False),
            StructField("run_id", StringType(), False),
            StructField("component", StringType(), False),
            StructField("error_type", StringType(), False),
            StructField("error_message", StringType(), False),
            StructField("error_details", StringType(), True),
            StructField("created_at", TimestampType(), False),
            StructField("resolved", StringType(), True)
        ])
    
    @classmethod
    def get_instance(cls) -> 'ETLMonitor':
        """Get singleton instance of ETL Monitor"""
        return cls()
    
    def get_dashboard_data(self, days_back: int = 7) -> Dict:
        """
        Get dashboard statistics using PySpark aggregations
        Replaces ABAP SELECT with groupBy/agg operations
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            Dictionary with dashboard metrics
        """
        self.logger.info(f"Fetching dashboard data for last {days_back} days")
        
        # Calculate cutoff timestamp
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        # Load run log data (would be from Delta/Parquet in production)
        run_log_df = self._load_run_log()
        
        # Filter by time window
        filtered_df = run_log_df.filter(F.col("start_time") >= cutoff_time)
        
        # Perform aggregations - replaces ABAP SQL aggregations
        dashboard_stats = filtered_df.agg(
            F.count("*").alias("total_runs"),
            F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("successful_runs"),
            F.sum(F.when(
                (F.col("status") == "FAILED") | (F.col("status") == "ERROR"), 
                1
            ).otherwise(0)).alias("failed_runs"),
            F.sum(F.when(F.col("status") == "RUNNING", 1).otherwise(0)).alias("running_jobs"),
            F.avg("duration").alias("avg_duration"),
            F.sum("records_loaded").alias("total_records")
        ).collect()[0]
        
        # Calculate error rate
        total_runs = dashboard_stats["total_runs"] or 0
        failed_runs = dashboard_stats["failed_runs"] or 0
        error_rate = (failed_runs / total_runs * 100) if total_runs > 0 else 0.0
        
        # Get last run info using window function
        window_spec = Window.orderBy(F.col("end_time").desc())
        last_run = filtered_df.withColumn("rn", F.row_number().over(window_spec)) \
            .filter(F.col("rn") == 1) \
            .select("end_time", "status") \
            .collect()
        
        last_run_time = last_run[0]["end_time"] if last_run else None
        last_run_status = last_run[0]["status"] if last_run else None
        
        return {
            "total_runs": total_runs,
            "successful_runs": dashboard_stats["successful_runs"] or 0,
            "failed_runs": failed_runs,
            "running_jobs": dashboard_stats["running_jobs"] or 0,
            "avg_duration": int(dashboard_stats["avg_duration"] or 0),
            "total_records": dashboard_stats["total_records"] or 0,
            "error_rate": round(error_rate, 2),
            "last_run_time": last_run_time,
            "last_run_status": last_run_status
        }
    
    def get_performance_metrics(self, days_back: int = 30) -> DataFrame:
        """
        Get performance metrics using DataFrame operations
        Replaces ABAP SELECT with PySpark transformations
        
        Args:
            days_back: Number of days to analyze
            
        Returns:
            DataFrame with performance metrics
        """
        self.logger.info(f"Calculating performance metrics for {days_back} days")
        
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        run_log_df = self._load_run_log()
        
        # Calculate throughput (records per second)
        metrics_df = run_log_df.filter(F.col("start_time") >= cutoff_time) \
            .withColumn(
                "throughput",
                F.when(
                    F.col("duration") > 0,
                    F.col("records_loaded") / F.col("duration")
                ).otherwise(0)
            ) \
            .select(
                "run_id",
                "start_time",
                "duration",
                "records_loaded",
                F.round("throughput", 2).alias("throughput"),
                "status"
            ) \
            .orderBy(F.col("start_time").desc())
        
        return metrics_df
    
    def get_error_summary(self, days_back: int = 7) -> DataFrame:
        """
        Get error summary using groupBy aggregations
        Replaces ABAP error log query with DataFrame operations
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            DataFrame with error summary grouped by type
        """
        self.logger.info(f"Fetching error summary for {days_back} days")
        
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        error_log_df = self._load_error_log()
        
        # Group by error type and component - replaces ABAP GROUP BY
        error_summary = error_log_df.filter(F.col("created_at") >= cutoff_time) \
            .filter(F.col("resolved").isNull() | (F.col("resolved") == "")) \
            .groupBy("error_type", "component") \
            .agg(
                F.count("*").alias("error_count"),
                F.min("created_at").alias("first_occurrence"),
                F.max("created_at").alias("last_occurrence"),
                F.collect_list("error_message").alias("sample_messages")
            ) \
            .orderBy(F.col("error_count").desc())
        
        return error_summary
    
    def get_hourly_statistics(self, days_back: int = 1) -> DataFrame:
        """
        Calculate hourly aggregated statistics
        Uses window functions for time-based grouping
        
        Args:
            days_back: Number of days to analyze
            
        Returns:
            DataFrame with hourly statistics
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        run_log_df = self._load_run_log()
        
        # Extract hour from timestamp and aggregate
        hourly_stats = run_log_df.filter(F.col("start_time") >= cutoff_time) \
            .withColumn("hour", F.date_trunc("hour", "start_time")) \
            .groupBy("hour") \
            .agg(
                F.count("*").alias("runs_per_hour"),
                F.sum("records_loaded").alias("total_records"),
                F.avg("duration").alias("avg_duration"),
                F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("successful"),
                F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed")
            ) \
            .withColumn("success_rate", 
                F.round((F.col("successful") / F.col("runs_per_hour")) * 100, 2)
            ) \
            .orderBy("hour")
        
        return hourly_stats
    
    def check_health(self) -> Tuple[str, Dict]:
        """
        Perform health check using aggregations
        Replaces ABAP health check logic with DataFrame operations
        
        Returns:
            Tuple of (health_status, details_dict)
        """
        self.logger.info("Performing health check")
        
        dashboard = self.get_dashboard_data(days_back=1)
        
        health_status = "HEALTHY"
        issues = []
        
        # Check for overload
        if dashboard["running_jobs"] > 5:
            health_status = "OVERLOADED"
            issues.append(f"Too many running jobs: {dashboard['running_jobs']}")
            self._send_alert(
                alert_type="PERFORMANCE",
                message=f"Too many running jobs: {dashboard['running_jobs']}",
                severity="HIGH"
            )
        
        # Check error rate
        if dashboard["error_rate"] > 50:
            health_status = "CRITICAL"
            issues.append(f"Critical error rate: {dashboard['error_rate']}%")
            self._send_alert(
                alert_type="ERROR_RATE",
                message=f"High error rate: {dashboard['error_rate']}%",
                severity="CRITICAL"
            )
        elif dashboard["error_rate"] > 20:
            health_status = "WARNING"
            issues.append(f"Elevated error rate: {dashboard['error_rate']}%")
            self._send_alert(
                alert_type="ERROR_RATE",
                message=f"Elevated error rate: {dashboard['error_rate']}%",
                severity="MEDIUM"
            )
        
        # Check for stalled jobs
        if dashboard["running_jobs"] > 0:
            stalled_jobs = self._check_stalled_jobs()
            if stalled_jobs > 0:
                health_status = "WARNING"
                issues.append(f"Detected {stalled_jobs} stalled jobs")
        
        return health_status, {
            "status": health_status,
            "issues": issues,
            "dashboard": dashboard
        }
    
    def get_performance_trends(self, days_back: int = 30) -> DataFrame:
        """
        Calculate performance trends using window functions
        Analyzes throughput and duration trends over time
        
        Args:
            days_back: Number of days to analyze
            
        Returns:
            DataFrame with trend analysis
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        run_log_df = self._load_run_log()
        
        # Define window for moving averages
        days_window = Window.orderBy("day").rowsBetween(-6, 0)
        
        # Calculate daily aggregates with moving averages
        trends_df = run_log_df.filter(F.col("start_time") >= cutoff_time) \
            .withColumn("day", F.to_date("start_time")) \
            .groupBy("day") \
            .agg(
                F.avg("duration").alias("avg_duration"),
                F.sum("records_loaded").alias("records"),
                F.count("*").alias("runs"),
                F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("successful")
            ) \
            .withColumn("success_rate", (F.col("successful") / F.col("runs")) * 100) \
            .withColumn("avg_duration_7day", F.avg("avg_duration").over(days_window)) \
            .withColumn("records_7day", F.avg("records").over(days_window)) \
            .orderBy("day")
        
        return trends_df
    
    def get_category_statistics(self) -> DataFrame:
        """
        Get statistics grouped by category
        Uses multiple groupBy operations for hierarchical aggregation
        
        Returns:
            DataFrame with category-level statistics
        """
        run_log_df = self._load_run_log()
        
        # Join with execution details to get category information
        # (In production, this would join with additional metadata)
        category_stats = run_log_df.groupBy("status") \
            .agg(
                F.count("*").alias("count"),
                F.avg("duration").alias("avg_duration"),
                F.sum("records_loaded").alias("total_records"),
                F.min("start_time").alias("first_run"),
                F.max("start_time").alias("last_run")
            ) \
            .orderBy(F.col("count").desc())
        
        return category_stats
    
    def detect_anomalies(self, threshold_std: float = 2.0) -> DataFrame:
        """
        Detect anomalies using statistical aggregations
        Uses standard deviation and z-scores
        
        Args:
            threshold_std: Standard deviation threshold for anomaly detection
            
        Returns:
            DataFrame with detected anomalies
        """
        run_log_df = self._load_run_log()
        
        # Calculate statistics for anomaly detection
        stats = run_log_df.filter(F.col("status") == "SUCCESS") \
            .agg(
                F.mean("duration").alias("mean_duration"),
                F.stddev("duration").alias("stddev_duration"),
                F.mean("records_loaded").alias("mean_records"),
                F.stddev("records_loaded").alias("stddev_records")
            ).collect()[0]
        
        mean_duration = stats["mean_duration"] or 0
        stddev_duration = stats["stddev_duration"] or 1
        mean_records = stats["mean_records"] or 0
        stddev_records = stats["stddev_records"] or 1
        
        # Calculate z-scores and flag anomalies
        anomalies_df = run_log_df \
            .withColumn("duration_zscore",
                (F.col("duration") - F.lit(mean_duration)) / F.lit(stddev_duration)
            ) \
            .withColumn("records_zscore",
                (F.col("records_loaded") - F.lit(mean_records)) / F.lit(stddev_records)
            ) \
            .filter(
                (F.abs(F.col("duration_zscore")) > threshold_std) |
                (F.abs(F.col("records_zscore")) > threshold_std)
            ) \
            .select(
                "run_id",
                "start_time",
                "duration",
                "records_loaded",
                F.round("duration_zscore", 2).alias("duration_zscore"),
                F.round("records_zscore", 2).alias("records_zscore")
            ) \
            .orderBy(F.col("start_time").desc())
        
        return anomalies_df
    
    def _load_run_log(self) -> DataFrame:
        """
        Load run log data from storage
        In production, this would read from Delta Lake or Parquet
        """
        # Placeholder - would load from actual storage
        return self.spark.createDataFrame([], self.run_log_schema)
    
    def _load_error_log(self) -> DataFrame:
        """
        Load error log data from storage
        """
        # Placeholder - would load from actual storage
        return self.spark.createDataFrame([], self.error_log_schema)
    
    def _check_stalled_jobs(self) -> int:
        """
        Check for stalled jobs (running > 2 hours)
        Uses timestamp arithmetic
        """
        cutoff_time = datetime.now() - timedelta(hours=2)
        
        run_log_df = self._load_run_log()
        
        stalled_count = run_log_df.filter(
            (F.col("status") == "RUNNING") &
            (F.col("start_time") < cutoff_time)
        ).count()
        
        return stalled_count
    
    def _send_alert(self, alert_type: str, message: str, severity: str):
        """
        Send monitoring alert
        
        Args:
            alert_type: Type of alert
            message: Alert message
            severity: Alert severity level
        """
        self.logger.warning(f"ALERT [{severity}] {alert_type}: {message}")
        # In production, integrate with alerting system (PagerDuty, Slack, etc.)
    
    def calculate_throughput(self, records: int, duration: int) -> float:
        """
        Calculate throughput in records per second
        
        Args:
            records: Number of records processed
            duration: Duration in seconds
            
        Returns:
            Throughput value
        """
        if duration > 0:
            return round(records / duration, 2)
        return 0.0


===FILE: config.yaml===
# ETL Monitor Configuration

spark:
  app_name: "ETL_Monitor"
  master: "local[*]"
  config:
    spark.sql.adaptive.enabled: "true"
    spark.sql.adaptive.coalescePartitions.enabled: "true"
    spark.sql.shuffle.partitions: "200"

monitoring:
  dashboard:
    default_days_back: 7
    max_days_back: 90
    refresh_interval_seconds: 300
  
  performance:
    threshold_std_dev: 2.0
    slow_job_threshold_seconds: 3600
    stalled_job_threshold_hours: 2
  
  health_check:
    running_jobs_threshold: 5
    error_rate_warning: 20.0
    error_rate_critical: 50.0
  
  alerts:
    enabled: true
    email_recipients:
      - "admin@example.com"
      - "ops-team@example.com"
    severity_levels:
      - "LOW"
      - "MEDIUM"
      - "HIGH"
      - "CRITICAL"

storage:
  run_log_path: "s3://etl-bucket/logs/run_log/"
  error_log_path: "s3://etl-bucket/logs/error_log/"
  metrics_path: "s3://etl-bucket/metrics/"
  format: "delta"
  
  retention:
    run_log_days: 90
    error_log_days: 180
    metrics_days: 365

aggregations:
  hourly_stats_window: "1 hour"
  daily_stats_window: "1 day"
  weekly_stats_window: "7 days"
  
  metrics:
    - name: "throughput"
      type: "gauge"
      unit: "records_per_second"
    - name: "duration"
      type: "histogram"
      unit: "seconds"
    - name: "error_rate"
      type: "gauge"
      unit: "percentage"

logging:
  level: "INFO"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  handlers:
    console: true
    file:
      enabled: true
      path: "/var/log/etl/monitor.log"
      max_bytes: 10485760
      backup_count: 5