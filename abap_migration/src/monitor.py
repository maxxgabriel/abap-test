"""
ETL Monitoring Module with PySpark Aggregations
Provides runtime metrics, performance tracking, and health monitoring
"""
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    TimestampType, DoubleType, LongType
)

from logger import ETLLogger


@dataclass
class DashboardData:
    """Dashboard metrics data class"""
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
    """Performance metric data class"""
    run_id: str
    start_time: datetime
    duration: int
    records_processed: int
    throughput: float
    status: str


class ETLMonitor:
    """
    Singleton ETL monitoring class with PySpark DataFrame aggregations
    Replaces ABAP SQL aggregations with groupBy/agg operations
    """
    
    _instance = None
    
    def __new__(cls, spark: SparkSession = None):
        if cls._instance is None:
            cls._instance = super(ETLMonitor, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, spark: SparkSession = None):
        """Initialize monitor with SparkSession"""
        if self._initialized:
            return
            
        if spark is None:
            raise ValueError("SparkSession is required for first initialization")
            
        self.spark = spark
        self.logger = ETLLogger.get_instance()
        self._initialized = True
    
    @classmethod
    def get_instance(cls, spark: SparkSession = None) -> 'ETLMonitor':
        """Get singleton instance"""
        if cls._instance is None:
            if spark is None:
                raise ValueError("SparkSession required for first initialization")
            cls._instance = ETLMonitor(spark)
        return cls._instance
    
    def get_dashboard_data(self, days_back: int = 7) -> DashboardData:
        """
        Get dashboard metrics using PySpark aggregations
        Replaces ABAP SELECT with COUNT/SUM/AVG aggregations
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            DashboardData with aggregated metrics
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        self.logger.log_info(
            component="MONITOR",
            message=f"Fetching dashboard data for last {days_back} days"
        )
        
        try:
            # Read run log data
            run_log_df = self._read_run_log()
            
            # Filter by date range
            filtered_df = run_log_df.filter(
                F.col("start_time") >= F.lit(cutoff_time)
            )
            
            # Perform aggregations - replaces ABAP SQL aggregations
            stats_df = filtered_df.agg(
                F.count("*").alias("total_runs"),
                F.sum(
                    F.when(F.col("status") == "SUCCESS", 1).otherwise(0)
                ).alias("successful_runs"),
                F.sum(
                    F.when(
                        (F.col("status") == "FAILED") | (F.col("status") == "ERROR"),
                        1
                    ).otherwise(0)
                ).alias("failed_runs"),
                F.sum(
                    F.when(F.col("status") == "RUNNING", 1).otherwise(0)
                ).alias("running_jobs"),
                F.avg("duration").alias("avg_duration"),
                F.sum("records_loaded").alias("total_records")
            )
            
            # Collect results
            stats = stats_df.collect()[0]
            
            # Calculate error rate
            total_runs = stats["total_runs"] or 0
            failed_runs = stats["failed_runs"] or 0
            error_rate = (failed_runs / total_runs * 100) if total_runs > 0 else 0.0
            
            # Get last run info
            last_run_df = run_log_df.orderBy(F.col("end_time").desc()).limit(1)
            last_run = last_run_df.collect()
            
            last_run_time = last_run[0]["end_time"] if last_run else None
            last_run_status = last_run[0]["status"] if last_run else None
            
            dashboard_data = DashboardData(
                total_runs=stats["total_runs"] or 0,
                successful_runs=stats["successful_runs"] or 0,
                failed_runs=failed_runs,
                running_jobs=stats["running_jobs"] or 0,
                avg_duration=float(stats["avg_duration"] or 0),
                total_records=stats["total_records"] or 0,
                error_rate=error_rate,
                last_run_time=last_run_time,
                last_run_status=last_run_status
            )
            
            self.logger.log_info(
                component="MONITOR",
                message=f"Dashboard data retrieved: {dashboard_data.total_runs} runs"
            )
            
            return dashboard_data
            
        except Exception as e:
            self.logger.log_error(
                component="MONITOR",
                message="Failed to get dashboard data",
                details=str(e)
            )
            raise
    
    def get_performance_metrics(
        self, 
        days_back: int = 30
    ) -> List[PerformanceMetric]:
        """
        Get performance metrics using PySpark operations
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            List of performance metrics
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        try:
            run_log_df = self._read_run_log()
            
            # Filter and calculate throughput
            metrics_df = run_log_df.filter(
                F.col("start_time") >= F.lit(cutoff_time)
            ).withColumn(
                "throughput",
                F.when(
                    F.col("duration") > 0,
                    F.col("records_loaded") / F.col("duration")
                ).otherwise(0.0)
            ).orderBy(
                F.col("start_time").desc()
            )
            
            # Convert to PerformanceMetric objects
            metrics = []
            for row in metrics_df.collect():
                metric = PerformanceMetric(
                    run_id=row["run_id"],
                    start_time=row["start_time"],
                    duration=row["duration"],
                    records_processed=row["records_loaded"],
                    throughput=float(row["throughput"]),
                    status=row["status"]
                )
                metrics.append(metric)
            
            self.logger.log_info(
                component="MONITOR",
                message=f"Retrieved {len(metrics)} performance metrics"
            )
            
            return metrics
            
        except Exception as e:
            self.logger.log_error(
                component="MONITOR",
                message="Failed to get performance metrics",
                details=str(e)
            )
            raise
    
    def get_error_summary(self, days_back: int = 7) -> DataFrame:
        """
        Get error summary using PySpark aggregations
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            DataFrame with error summary
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        try:
            error_log_df = self._read_error_log()
            
            # Filter unresolved errors
            errors_df = error_log_df.filter(
                (F.col("created_at") >= F.lit(cutoff_time)) &
                (F.col("resolved") == False)
            ).orderBy(
                F.col("created_at").desc()
            )
            
            # Group by error type for summary
            error_summary = errors_df.groupBy("error_type").agg(
                F.count("*").alias("error_count"),
                F.collect_list("error_message").alias("messages")
            ).orderBy(
                F.col("error_count").desc()
            )
            
            return error_summary
            
        except Exception as e:
            self.logger.log_error(
                component="MONITOR",
                message="Failed to get error summary",
                details=str(e)
            )
            raise
    
    def check_health(self) -> str:
        """
        Check system health based on metrics
        
        Returns:
            Health status string
        """
        try:
            dashboard = self.get_dashboard_data(days_back=1)
            
            # Health check logic
            if dashboard.running_jobs > 5:
                health_status = "OVERLOADED"
                self.send_alert(
                    alert_type="PERFORMANCE",
                    message=f"Too many running jobs: {dashboard.running_jobs}",
                    severity="HIGH"
                )
            elif dashboard.error_rate > 50:
                health_status = "CRITICAL"
                self.send_alert(
                    alert_type="ERROR_RATE",
                    message=f"High error rate: {dashboard.error_rate:.2f}%",
                    severity="CRITICAL"
                )
            elif dashboard.error_rate > 20:
                health_status = "WARNING"
                self.send_alert(
                    alert_type="ERROR_RATE",
                    message=f"Elevated error rate: {dashboard.error_rate:.2f}%",
                    severity="MEDIUM"
                )
            elif dashboard.failed_runs == 0 and dashboard.successful_runs > 0:
                health_status = "HEALTHY"
            else:
                health_status = "UNKNOWN"
            
            self.logger.log_info(
                component="MONITOR",
                message=f"Health check complete: {health_status}"
            )
            
            return health_status
            
        except Exception as e:
            self.logger.log_error(
                component="MONITOR",
                message="Health check failed",
                details=str(e)
            )
            return "ERROR"
    
    def send_alert(
        self, 
        alert_type: str, 
        message: str, 
        severity: str
    ) -> None:
        """
        Send alert notification
        
        Args:
            alert_type: Type of alert
            message: Alert message
            severity: Alert severity level
        """
        self.logger.log_warning(
            component="ALERT",
            message=f"[{severity}] {alert_type}: {message}"
        )
        
        # In production, implement actual alerting mechanism
        # (email, Slack, PagerDuty, etc.)
    
    def get_aggregated_stats_by_status(
        self, 
        days_back: int = 30
    ) -> DataFrame:
        """
        Get aggregated statistics grouped by status
        Uses PySpark groupBy and agg operations
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            DataFrame with aggregated stats by status
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        try:
            run_log_df = self._read_run_log()
            
            # Group by status and aggregate metrics
            stats_by_status = run_log_df.filter(
                F.col("start_time") >= F.lit(cutoff_time)
            ).groupBy("status").agg(
                F.count("*").alias("run_count"),
                F.avg("duration").alias("avg_duration"),
                F.min("duration").alias("min_duration"),
                F.max("duration").alias("max_duration"),
                F.sum("records_loaded").alias("total_records"),
                F.avg("records_loaded").alias("avg_records")
            ).orderBy(
                F.col("run_count").desc()
            )
            
            return stats_by_status
            
        except Exception as e:
            self.logger.log_error(
                component="MONITOR",
                message="Failed to get aggregated stats",
                details=str(e)
            )
            raise
    
    def get_hourly_throughput(
        self, 
        days_back: int = 7
    ) -> DataFrame:
        """
        Calculate hourly throughput using window functions
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            DataFrame with hourly throughput metrics
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        try:
            run_log_df = self._read_run_log()
            
            # Extract hour from timestamp and calculate throughput
            hourly_df = run_log_df.filter(
                F.col("start_time") >= F.lit(cutoff_time)
            ).withColumn(
                "hour",
                F.hour(F.col("start_time"))
            ).withColumn(
                "date",
                F.to_date(F.col("start_time"))
            ).withColumn(
                "throughput",
                F.when(
                    F.col("duration") > 0,
                    F.col("records_loaded") / F.col("duration")
                ).otherwise(0.0)
            )
            
            # Aggregate by hour
            hourly_stats = hourly_df.groupBy("date", "hour").agg(
                F.count("*").alias("run_count"),
                F.avg("throughput").alias("avg_throughput"),
                F.max("throughput").alias("max_throughput"),
                F.sum("records_loaded").alias("total_records")
            ).orderBy("date", "hour")
            
            return hourly_stats
            
        except Exception as e:
            self.logger.log_error(
                component="MONITOR",
                message="Failed to calculate hourly throughput",
                details=str(e)
            )
            raise
    
    def get_trend_analysis(
        self, 
        days_back: int = 30
    ) -> Dict[str, float]:
        """
        Perform trend analysis using window functions
        
        Args:
            days_back: Number of days to analyze
            
        Returns:
            Dictionary with trend metrics
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        try:
            run_log_df = self._read_run_log()
            
            # Calculate daily aggregates
            daily_df = run_log_df.filter(
                F.col("start_time") >= F.lit(cutoff_time)
            ).withColumn(
                "date",
                F.to_date(F.col("start_time"))
            ).groupBy("date").agg(
                F.count("*").alias("daily_runs"),
                F.avg("duration").alias("avg_duration"),
                F.sum("records_loaded").alias("daily_records"),
                F.sum(
                    F.when(F.col("status") == "SUCCESS", 1).otherwise(0)
                ).alias("success_count")
            )
            
            # Calculate moving averages using window functions
            window_spec = Window.orderBy("date").rowsBetween(-6, 0)
            
            trend_df = daily_df.withColumn(
                "ma_7_runs",
                F.avg("daily_runs").over(window_spec)
            ).withColumn(
                "ma_7_duration",
                F.avg("avg_duration").over(window_spec)
            ).withColumn(
                "ma_7_records",
                F.avg("daily_records").over(window_spec)
            )
            
            # Get latest values
            latest = trend_df.orderBy(F.col("date").desc()).limit(1).collect()
            
            if latest:
                row = latest[0]
                trends = {
                    "avg_daily_runs": float(row["ma_7_runs"] or 0),
                    "avg_duration": float(row["ma_7_duration"] or 0),
                    "avg_daily_records": float(row["ma_7_records"] or 0),
                    "latest_date": row["date"].isoformat()
                }
            else:
                trends = {
                    "avg_daily_runs": 0.0,
                    "avg_duration": 0.0,
                    "avg_daily_records": 0.0,
                    "latest_date": None
                }
            
            return trends
            
        except Exception as e:
            self.logger.log_error(
                component="MONITOR",
                message="Failed to perform trend analysis",
                details=str(e)
            )
            raise
    
    def calculate_throughput(
        self, 
        records: int, 
        duration: int
    ) -> float:
        """
        Calculate throughput (records per second)
        
        Args:
            records: Number of records processed
            duration: Duration in seconds
            
        Returns:
            Throughput value
        """
        if duration > 0:
            return records / duration
        return 0.0
    
    def _read_run_log(self) -> DataFrame:
        """Read run log data with schema"""
        schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), True),
            StructField("duration", IntegerType(), True),
            StructField("status", StringType(), False),
            StructField("records_extracted", IntegerType(), True),
            StructField("records_loaded", IntegerType(), True),
            StructField("records_failed", IntegerType(), True),
            StructField("error_count", IntegerType(), True)
        ])
        
        # Read from configured source (file, table, etc.)
        # This is a placeholder - implement based on your storage
        return self.spark.read.schema(schema).parquet("data/run_log")
    
    def _read_error_log(self) -> DataFrame:
        """Read error log data with schema"""
        schema = StructType([
            StructField("error_id", StringType(), False),
            StructField("run_id", StringType(), False),
            StructField("error_type", StringType(), False),
            StructField("error_message", StringType(), False),
            StructField("created_at", TimestampType(), False),
            StructField("resolved", StringType(), False),
            StructField("component", StringType(), True)
        ])
        
        df = self.spark.read.schema(schema).parquet("data/error_log")
        
        # Convert resolved to boolean
        df = df.withColumn(
            "resolved",
            F.when(F.col("resolved") == "X", True).otherwise(False)
        )
        
        return df


def create_monitor_report(
    monitor: ETLMonitor,
    output_path: str
) -> None:
    """
    Generate comprehensive monitoring report
    
    Args:
        monitor: ETLMonitor instance
        output_path: Path to save report
    """
    try:
        # Get all metrics
        dashboard = monitor.get_dashboard_data(days_back=7)
        performance = monitor.get_performance_metrics(days_back=30)
        health = monitor.check_health()
        trends = monitor.get_trend_analysis(days_back=30)
        
        # Create report DataFrame
        report_data = {
            "dashboard": asdict(dashboard),
            "health_status": health,
            "trends": trends,
            "performance_samples": len(performance)
        }
        
        # Save as JSON
        import json
        with open(output_path, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
        
        monitor.logger.log_info(
            component="MONITOR",
            message=f"Report generated: {output_path}"
        )
        
    except Exception as e:
        monitor.logger.log_error(
            component="MONITOR",
            message="Failed to create report",
            details=str(e)
        )
        raise