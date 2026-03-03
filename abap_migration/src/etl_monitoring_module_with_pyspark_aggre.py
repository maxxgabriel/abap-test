I'll generate production-ready PySpark code for an ETL monitoring module with DataFrame aggregations, converting the ABAP monitoring singleton to Python.

===FILE: src/monitor.py===
"""
ETL Monitoring Module with PySpark Aggregations
Provides runtime metrics, performance tracking, and health monitoring
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging


class ETLMonitor:
    """Singleton monitoring class for ETL runtime metrics with PySpark aggregations"""
    
    _instance = None
    
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
        
    def get_dashboard_data(self, days_back: int = 7) -> Dict:
        """
        Get aggregated dashboard metrics using PySpark DataFrame operations
        
        Args:
            days_back: Number of days to look back for statistics
            
        Returns:
            Dictionary with aggregated metrics
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        # Load run log data
        run_log_df = self._load_run_log()
        
        # Filter by date range
        filtered_df = run_log_df.filter(F.col("start_time") >= F.lit(cutoff_time))
        
        # Aggregate statistics using groupBy/agg
        stats = filtered_df.agg(
            F.count("*").alias("total_runs"),
            F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("successful_runs"),
            F.sum(F.when(F.col("status").isin(["FAILED", "ERROR"]), 1).otherwise(0)).alias("failed_runs"),
            F.sum(F.when(F.col("status") == "RUNNING", 1).otherwise(0)).alias("running_jobs"),
            F.avg("duration").alias("avg_duration"),
            F.sum("records_loaded").alias("total_records")
        ).collect()[0]
        
        # Calculate error rate
        error_rate = 0.0
        if stats["total_runs"] > 0:
            error_rate = (stats["failed_runs"] / stats["total_runs"]) * 100
        
        # Get last run info using window functions
        window_spec = Window.orderBy(F.col("end_time").desc())
        last_run = filtered_df.withColumn("row_num", F.row_number().over(window_spec)) \
                             .filter(F.col("row_num") == 1) \
                             .select("end_time", "status") \
                             .collect()
        
        return {
            "total_runs": stats["total_runs"],
            "successful_runs": stats["successful_runs"],
            "failed_runs": stats["failed_runs"],
            "running_jobs": stats["running_jobs"],
            "avg_duration": stats["avg_duration"],
            "total_records": stats["total_records"],
            "error_rate": round(error_rate, 2),
            "last_run_time": last_run[0]["end_time"] if last_run else None,
            "last_run_status": last_run[0]["status"] if last_run else None
        }
    
    def get_performance_metrics(self, days_back: int = 30) -> DataFrame:
        """
        Get performance metrics with calculated throughput using PySpark
        
        Args:
            days_back: Number of days to analyze
            
        Returns:
            DataFrame with performance metrics
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        run_log_df = self._load_run_log()
        
        # Calculate throughput: records per second
        metrics_df = run_log_df.filter(F.col("start_time") >= F.lit(cutoff_time)) \
            .withColumn(
                "throughput",
                F.when(F.col("duration") > 0, 
                      F.col("records_loaded") / F.col("duration"))
                .otherwise(0.0)
            ) \
            .select(
                "run_id",
                "start_time",
                "duration",
                F.col("records_loaded").alias("records_processed"),
                "throughput",
                "status"
            ) \
            .orderBy(F.col("start_time").desc())
        
        return metrics_df
    
    def get_error_summary(self, days_back: int = 7) -> DataFrame:
        """
        Get error summary with aggregations by error type
        
        Args:
            days_back: Number of days to analyze
            
        Returns:
            DataFrame with error statistics
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        error_log_df = self._load_error_log()
        
        # Aggregate errors by type and component
        error_summary = error_log_df.filter(
            (F.col("created_at") >= F.lit(cutoff_time)) &
            (F.col("resolved") == False)
        ).groupBy("error_type", "component") \
         .agg(
             F.count("*").alias("error_count"),
             F.min("created_at").alias("first_occurrence"),
             F.max("created_at").alias("last_occurrence"),
             F.collect_list("error_message").alias("sample_messages")
         ) \
         .orderBy(F.col("error_count").desc())
        
        return error_summary
    
    def get_runtime_statistics(self, group_by: str = "status") -> DataFrame:
        """
        Get runtime statistics grouped by specified dimension
        
        Args:
            group_by: Column to group by (status, etl_type, etc.)
            
        Returns:
            DataFrame with aggregated runtime stats
        """
        run_log_df = self._load_run_log()
        
        # Aggregate runtime metrics
        stats_df = run_log_df.groupBy(group_by) \
            .agg(
                F.count("*").alias("run_count"),
                F.avg("duration").alias("avg_duration"),
                F.min("duration").alias("min_duration"),
                F.max("duration").alias("max_duration"),
                F.stddev("duration").alias("std_dev_duration"),
                F.sum("records_loaded").alias("total_records"),
                F.avg("records_loaded").alias("avg_records")
            ) \
            .orderBy(F.col("run_count").desc())
        
        return stats_df
    
    def check_health(self) -> Tuple[str, Dict]:
        """
        Check system health using aggregated metrics
        
        Returns:
            Tuple of (health_status, details_dict)
        """
        dashboard = self.get_dashboard_data(days_back=1)
        
        health_status = "HEALTHY"
        alerts = []
        
        # Check for too many running jobs
        if dashboard["running_jobs"] > 5:
            health_status = "OVERLOADED"
            alerts.append({
                "type": "PERFORMANCE",
                "message": f"Too many running jobs: {dashboard['running_jobs']}",
                "severity": "HIGH"
            })
        
        # Check error rate
        elif dashboard["error_rate"] > 50:
            health_status = "CRITICAL"
            alerts.append({
                "type": "ERROR_RATE",
                "message": f"High error rate: {dashboard['error_rate']}%",
                "severity": "CRITICAL"
            })
        
        elif dashboard["error_rate"] > 20:
            health_status = "WARNING"
            alerts.append({
                "type": "ERROR_RATE",
                "message": f"Elevated error rate: {dashboard['error_rate']}%",
                "severity": "MEDIUM"
            })
        
        # Check for stale runs
        if dashboard["last_run_time"]:
            time_since_last = datetime.now() - dashboard["last_run_time"]
            if time_since_last > timedelta(hours=24):
                health_status = "WARNING" if health_status == "HEALTHY" else health_status
                alerts.append({
                    "type": "STALE_DATA",
                    "message": f"No successful run in {time_since_last.days} days",
                    "severity": "MEDIUM"
                })
        
        return health_status, {
            "status": health_status,
            "alerts": alerts,
            "dashboard": dashboard
        }
    
    def get_trend_analysis(self, metric: str = "duration", days: int = 30) -> DataFrame:
        """
        Perform trend analysis on specified metric using window functions
        
        Args:
            metric: Metric to analyze (duration, records_loaded, etc.)
            days: Number of days for analysis
            
        Returns:
            DataFrame with trend calculations
        """
        cutoff_time = datetime.now() - timedelta(days=days)
        
        run_log_df = self._load_run_log()
        
        # Create date column for grouping
        df_with_date = run_log_df.filter(F.col("start_time") >= F.lit(cutoff_time)) \
            .withColumn("date", F.to_date("start_time"))
        
        # Calculate daily aggregates
        daily_agg = df_with_date.groupBy("date") \
            .agg(
                F.avg(metric).alias(f"avg_{metric}"),
                F.min(metric).alias(f"min_{metric}"),
                F.max(metric).alias(f"max_{metric}"),
                F.count("*").alias("run_count")
            )
        
        # Calculate moving averages using window functions
        window_7day = Window.orderBy("date").rowsBetween(-6, 0)
        
        trend_df = daily_agg.withColumn(
            f"ma_7day_{metric}",
            F.avg(f"avg_{metric}").over(window_7day)
        ).orderBy("date")
        
        return trend_df
    
    def get_component_metrics(self) -> DataFrame:
        """
        Get metrics aggregated by ETL component (extract, transform, load)
        
        Returns:
            DataFrame with component-level metrics
        """
        error_log_df = self._load_error_log()
        
        # Aggregate by component
        component_metrics = error_log_df.groupBy("component") \
            .agg(
                F.count("*").alias("total_errors"),
                F.countDistinct("error_type").alias("unique_error_types"),
                F.avg(F.unix_timestamp("resolved_at") - F.unix_timestamp("created_at"))
                    .alias("avg_resolution_time_sec"),
                F.sum(F.when(F.col("resolved") == True, 1).otherwise(0)).alias("resolved_count"),
                F.sum(F.when(F.col("resolved") == False, 1).otherwise(0)).alias("unresolved_count")
            ) \
            .withColumn(
                "resolution_rate",
                F.when(F.col("total_errors") > 0,
                      (F.col("resolved_count") / F.col("total_errors")) * 100)
                .otherwise(0.0)
            ) \
            .orderBy(F.col("total_errors").desc())
        
        return component_metrics
    
    def calculate_sla_compliance(self, sla_threshold_minutes: int = 60) -> DataFrame:
        """
        Calculate SLA compliance based on duration threshold
        
        Args:
            sla_threshold_minutes: SLA threshold in minutes
            
        Returns:
            DataFrame with SLA compliance metrics
        """
        run_log_df = self._load_run_log()
        
        sla_threshold_seconds = sla_threshold_minutes * 60
        
        # Calculate SLA compliance
        sla_df = run_log_df.withColumn(
            "sla_met",
            F.when(
                (F.col("duration") <= sla_threshold_seconds) & 
                (F.col("status") == "SUCCESS"),
                True
            ).otherwise(False)
        )
        
        # Aggregate by etl_type
        compliance_df = sla_df.groupBy("etl_type") \
            .agg(
                F.count("*").alias("total_runs"),
                F.sum(F.when(F.col("sla_met") == True, 1).otherwise(0)).alias("sla_met_count"),
                F.avg("duration").alias("avg_duration_sec"),
                F.percentile_approx("duration", 0.95).alias("p95_duration_sec")
            ) \
            .withColumn(
                "sla_compliance_pct",
                F.when(F.col("total_runs") > 0,
                      (F.col("sla_met_count") / F.col("total_runs")) * 100)
                .otherwise(0.0)
            ) \
            .orderBy(F.col("sla_compliance_pct").desc())
        
        return compliance_df
    
    def _load_run_log(self) -> DataFrame:
        """Load ETL run log data with proper schema"""
        schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("etl_type", StringType(), True),
            StructField("status", StringType(), True),
            StructField("start_time", TimestampType(), True),
            StructField("end_time", TimestampType(), True),
            StructField("duration", IntegerType(), True),
            StructField("records_extracted", IntegerType(), True),
            StructField("records_loaded", IntegerType(), True),
            StructField("records_failed", IntegerType(), True),
            StructField("error_count", IntegerType(), True),
            StructField("warning_count", IntegerType(), True)
        ])
        
        # In production, load from actual data source
        # For now, return empty DataFrame with schema
        return self.spark.createDataFrame([], schema)
    
    def _load_error_log(self) -> DataFrame:
        """Load error log data with proper schema"""
        schema = StructType([
            StructField("error_id", StringType(), False),
            StructField("run_id", StringType(), True),
            StructField("component", StringType(), True),
            StructField("error_type", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("error_details", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("resolved", StringType(), True),
            StructField("resolved_at", TimestampType(), True)
        ])
        
        return self.spark.createDataFrame([], schema)
    
    def send_alert(self, alert_type: str, message: str, severity: str):
        """Send alert notification"""
        self.logger.warning(f"ALERT [{severity}] {alert_type}: {message}")
        
        # In production, integrate with alerting system
        # (email, Slack, PagerDuty, etc.)


def get_monitor_instance() -> ETLMonitor:
    """Get singleton monitor instance"""
    return ETLMonitor()


===FILE: src/metrics_aggregator.py===
"""
Metrics aggregation utilities for ETL monitoring
Provides specialized aggregation functions for different metric types
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Dict, Optional
import logging


class MetricsAggregator:
    """Aggregation utilities for ETL metrics"""
    
    def __init__(self, spark):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
    
    def aggregate_by_time_window(
        self, 
        df: DataFrame, 
        timestamp_col: str = "start_time",
        window_duration: str = "1 hour",
        metrics: List[str] = None
    ) -> DataFrame:
        """
        Aggregate metrics by time window
        
        Args:
            df: Input DataFrame
            timestamp_col: Timestamp column name
            window_duration: Window size (e.g., "1 hour", "1 day")
            metrics: List of metrics to aggregate
            
        Returns:
            Aggregated DataFrame by time window
        """
        if metrics is None:
            metrics = ["duration", "records_loaded", "records_failed"]
        
        # Create time window
        windowed_df = df.withColumn(
            "time_window",
            F.window(F.col(timestamp_col), window_duration)
        )
        
        # Build aggregation expressions
        agg_exprs = [
            F.count("*").alias("run_count"),
            F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
            F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_count")
        ]
        
        for metric in metrics:
            agg_exprs.extend([
                F.avg(metric).alias(f"avg_{metric}"),
                F.min(metric).alias(f"min_{metric}"),
                F.max(metric).alias(f"max_{metric}"),
                F.sum(metric).alias(f"total_{metric}")
            ])
        
        # Perform aggregation
        result_df = windowed_df.groupBy("time_window") \
            .agg(*agg_exprs) \
            .withColumn("window_start", F.col("time_window.start")) \
            .withColumn("window_end", F.col("time_window.end")) \
            .drop("time_window") \
            .orderBy("window_start")
        
        return result_df
    
    def calculate_percentiles(
        self,
        df: DataFrame,
        metric_col: str,
        group_by_cols: List[str] = None,
        percentiles: List[float] = None
    ) -> DataFrame:
        """
        Calculate percentile metrics
        
        Args:
            df: Input DataFrame
            metric_col: Metric column to analyze
            group_by_cols: Columns to group by
            percentiles: List of percentiles (0.0 to 1.0)
            
        Returns:
            DataFrame with percentile calculations
        """
        if percentiles is None:
            percentiles = [0.5, 0.75, 0.90, 0.95, 0.99]
        
        if group_by_cols is None:
            group_by_cols = ["status"]
        
        # Calculate percentiles
        agg_exprs = [
            F.count("*").alias("count"),
            F.avg(metric_col).alias("mean")
        ]
        
        for p in percentiles:
            agg_exprs.append(
                F.percentile_approx(metric_col, p).alias(f"p{int(p*100)}")
            )
        
        result_df = df.groupBy(*group_by_cols).agg(*agg_exprs)
        
        return result_df
    
    def detect_outliers(
        self,
        df: DataFrame,
        metric_col: str,
        method: str = "iqr",
        threshold: float = 1.5
    ) -> DataFrame:
        """
        Detect outliers in metric data
        
        Args:
            df: Input DataFrame
            metric_col: Metric column to analyze
            method: Detection method ("iqr" or "zscore")
            threshold: Threshold for outlier detection
            
        Returns:
            DataFrame with outlier flag
        """
        if method == "iqr":
            # IQR method
            quantiles = df.stat.approxQuantile(metric_col, [0.25, 0.75], 0.01)
            q1, q3 = quantiles[0], quantiles[1]
            iqr = q3 - q1
            
            lower_bound = q1 - threshold * iqr
            upper_bound = q3 + threshold * iqr
            
            result_df = df.withColumn(
                "is_outlier",
                (F.col(metric_col) < lower_bound) | (F.col(metric_col) > upper_bound)
            ).withColumn(
                "outlier_type",
                F.when(F.col(metric_col) < lower_bound, "LOW")
                .when(F.col(metric_col) > upper_bound, "HIGH")
                .otherwise("NORMAL")
            )
        
        elif method == "zscore":
            # Z-score method
            stats = df.select(
                F.mean(metric_col).alias("mean"),
                F.stddev(metric_col).alias("stddev")
            ).collect()[0]
            
            mean_val = stats["mean"]
            stddev_val = stats["stddev"]
            
            result_df = df.withColumn(
                "zscore",
                (F.col(metric_col) - mean_val) / stddev_val
            ).withColumn(
                "is_outlier",
                F.abs(F.col("zscore")) > threshold
            ).withColumn(
                "outlier_type",
                F.when(F.col("zscore") < -threshold, "LOW")
                .when(F.col("zscore") > threshold, "HIGH")
                .otherwise("NORMAL")
            )
        
        else:
            raise ValueError(f"Unknown outlier detection method: {method}")
        
        return result_df
    
    def calculate_moving_averages(
        self,
        df: DataFrame,
        metric_col: str,
        order_col: str = "start_time",
        windows: List[int] = None
    ) -> DataFrame:
        """
        Calculate moving averages for time series data
        
        Args:
            df: Input DataFrame
            metric_col: Metric to calculate MA for
            order_col: Column to order by
            windows: List of window sizes
            
        Returns:
            DataFrame with moving averages
        """
        if windows is None:
            windows = [7, 14, 30]
        
        result_df = df
        
        for window_size in windows:
            window_spec = Window.orderBy(order_col).rowsBetween(-(window_size-1), 0)
            
            result_df = result_df.withColumn(
                f"ma_{window_size}_{metric_col}",
                F.avg(metric_col).over(window_spec)
            )
        
        return result_df
    
    def aggregate_by_category(
        self,
        df: DataFrame,
        category_col: str,
        metrics: List[str] = None
    ) -> DataFrame:
        """
        Aggregate metrics by category with statistical measures
        
        Args:
            df: Input DataFrame
            category_col: Column to group by
            metrics: List of metrics to aggregate
            
        Returns:
            Aggregated DataFrame
        """
        if metrics is None:
            metrics = ["duration", "records_loaded"]
        
        agg_exprs = [F.count("*").alias("count")]
        
        for metric in metrics:
            agg_exprs.extend([
                F.avg(metric).alias(f"avg_{metric}"),
                F.stddev(metric).alias(f"stddev_{metric}"),
                F.min(metric).alias(f"min_{metric}"),
                F.max(metric).alias(f"max_{metric}")
            ])
        
        result_df = df.groupBy(category_col).agg(*agg_exprs)
        
        return result_df


===FILE: config.yaml===
# ETL Monitoring Configuration

spark:
  app_name: "ETL_Monitoring"
  master: "local[*]"
  config:
    spark.sql.adaptive.enabled: "true"
    spark.sql.adaptive.coalescePartitions.enabled: "true"
    spark.sql.shuffle.partitions: "200"
    spark.driver.memory: "4g"
    spark.executor.memory: "4g"

monitoring:
  dashboard:
    default_days_back: 7
    refresh_interval_seconds: 300
  
  performance:
    sla_threshold_minutes: 60
    slow_job_threshold_minutes: 30
    throughput_warning_threshold: 100  # records per second
  
  health_check:
    max_running_jobs: 5
    critical_error_rate: 50.0
    warning_error_rate: 20.0
    stale_data_hours: 24
  
  alerts:
    enabled: true
    channels:
      - email
      - slack
    severity_levels:
      - CRITICAL
      - HIGH
      - MEDIUM
      - LOW

data_sources:
  run_log:
    path: "data/etl_run_log"
    format: "parquet"
    schema_validation: true
  
  error_log:
    path: "data/etl_error_log"
    format: "parquet"
    schema_validation: true

aggregation:
  time_windows:
    - "1 hour"
    - "1 day"
    - "1 week"
  
  metrics:
    - duration
    - records_loaded
    - records_failed
    - error_count
  
  percentiles:
    - 0.50
    - 0.75
    - 0.90
    - 0.95
    - 0.99

trends:
  moving_average_windows:
    - 7
    - 14
    - 30
  
  outlier_detection:
    method: "iqr"  # or "zscore"
    threshold: 1.5

retention:
  run_log_days: 90
  error_log_days: 180
  metrics_cache_hours: 24

logging:
  level: "INFO"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  file: "logs/etl_monitoring.log"


===FILE: src/main.py===
"""
Main ETL monitoring application
Demonstrates usage of monitoring module with PySpark aggregations
"""
from pyspark.sql import SparkSession
import yaml
import logging
from datetime import datetime
from monitor import get_monitor_instance
from metrics_aggregator import MetricsAggregator


def setup_logging(config: dict):
    """Configure logging"""
    logging.basicConfig(
        level=config['logging']['level'],
        format=config['logging']['format'],
        handlers=[
            logging.FileHandler(config['logging']['file']),
            logging.StreamHandler()
        ]
    )


def load_config(config_path: str = "config.yaml") -> dict:
    """Load configuration from YAML file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def create_spark_session(config: dict) -> SparkSession:
    """Create and configure Spark session"""
    builder = SparkSession.builder.appName(config['spark']['app_name'])
    
    if 'master' in config['spark']:
        builder = builder.master(config['spark']['master'])
    
    for key, value in config['spark']['config'].items():
        builder = builder.config(key, value)
    
    return builder.getOrCreate()


def display_dashboard(monitor, config):
    """Display monitoring dashboard"""
    days_back = config['monitoring']['dashboard']['default_days_back']
    
    print("=" * 80)
    print(f"ETL MONITORING DASHBOARD - Last {days_back} Days")
    print("=" * 80)
    
    # Get dashboard data
    dashboard = monitor.get_dashboard_data(days_back=days_back)
    
    print(f"\nOverall Statistics:")
    print(f"  Total Runs:        {dashboard['total_runs']}")
    print(f"  Successful:        {dashboard['successful_runs']}")
    print(f"  Failed:            {dashboard['failed_runs']}")
    print(f"  Running:           {dashboard['running_jobs']}")
    print(f"  Avg Duration:      {dashboard['avg_duration']:.2f} seconds")
    print(f"  Total Records:     {dashboard['total_records']:,}")
    print(f"  Error Rate:        {dashboard['error_rate']:.2f}%")
    
    if dashboard['last_run_time']:
        print(f"\nLast Run:")
        print(f"  Time:   {dashboard['last_run_time']}")
        print(f"  Status: {dashboard['last_run_status']}")
    
    print("\n" + "-" * 80)


def display_performance_metrics(monitor, spark, config):
    """Display performance metrics"""
    print("\nPerformance Metrics:")
    print("-" * 80)
    
    days_back = config['monitoring']['dashboard']['default_days_back']
    metrics_df = monitor.get_performance_metrics(days_back=days_back)
    
    # Show top 10 most recent runs
    metrics_df.select(
        "run_id",
        "start_time",
        "duration",
        "records_processed",
        "throughput",
        "status"
    ).show(10, truncate=False)
    
    # Show aggregated statistics by status
    print("\nStatistics by Status:")
    stats_df = monitor.get_runtime_statistics(group_by="status")
    stats_df.show(truncate=False)


def display_error_summary(monitor):
    """Display error summary"""
    print("\nError Summary:")
    print("-" * 80)
    
    error_summary = monitor.get_error_summary(days_back=7)
    
    if error_summary.count() > 0:
        error_summary.select(
            "error_type",
            "component",
            "error_count",
            "first_occurrence",
            "last_occurrence"
        ).show(10, truncate=False)
    else:
        print("No unresolved errors found.")


def display_health_check(monitor):
    """Display health check results"""
    print("\nSystem Health Check:")
    print("-" * 80)
    
    health_status, details = monitor.check_health()
    
    status_emoji = {
        "HEALTHY": "✓",
        "WARNING": "⚠",
        "CRITICAL": "✗",
        "OVERLOADED": "⚠"
    }
    
    print(f"\nHealth Status: {status_emoji.get(health_status, '?')} {health_status}")
    
    if details['alerts']:
        print("\nAlerts:")
        for alert in details['alerts']:
            print(f"  [{alert['severity']}] {alert['type']}: {alert['message']}")
    else:
        print("\nNo alerts - system is healthy")


def display_sla_compliance(monitor, config):
    """Display SLA compliance metrics"""
    print("\nSLA Compliance:")
    print("-" * 80)
    
    threshold = config['monitoring']['performance']['sla_threshold_minutes']
    sla_df = monitor.calculate_sla_compliance(sla_threshold_minutes=threshold)
    
    sla_df.show(truncate=False)


def display_trend_analysis(monitor, spark, config):
    """Display trend analysis"""
    print("\nTrend Analysis (Duration):")
    print("-" * 80)
    
    days = config['monitoring']['dashboard']['default_days_back']
    trend_df = monitor.get_trend_analysis(metric="duration", days=days)
    
    trend_df.