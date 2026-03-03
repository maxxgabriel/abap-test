"""
ETL Monitoring Module
Provides real-time monitoring, metrics aggregation, and health checks
for ETL pipeline execution with PySpark DataFrame operations.
"""

from pyspark.sql import SparkSession, DataFrame
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
    Singleton ETL monitoring class for metrics aggregation and tracking.
    Replaces ABAP zcl_etl_monitor with PySpark DataFrame operations.
    """
    
    _instance = None
    
    def __new__(cls, spark: SparkSession):
        if cls._instance is None:
            cls._instance = super(ETLMonitor, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, spark: SparkSession):
        """Initialize monitoring with SparkSession."""
        if self._initialized:
            return
            
        self.spark = spark
        self.logger = logging.getLogger(__name__)
        self._initialized = True
        
        # Define schemas
        self.run_log_schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), True),
            StructField("duration", IntegerType(), True),
            StructField("status", StringType(), False),
            StructField("records_extracted", LongType(), True),
            StructField("records_transformed", LongType(), True),
            StructField("records_loaded", LongType(), True),
            StructField("records_failed", LongType(), True),
            StructField("error_count", IntegerType(), True),
            StructField("warning_count", IntegerType(), True),
            StructField("run_type", StringType(), True),
            StructField("source_type", StringType(), True),
            StructField("target_type", StringType(), True)
        ])
        
        self.error_log_schema = StructType([
            StructField("error_id", StringType(), False),
            StructField("run_id", StringType(), False),
            StructField("component", StringType(), False),
            StructField("error_type", StringType(), False),
            StructField("error_message", StringType(), False),
            StructField("error_details", StringType(), True),
            StructField("record_id", StringType(), True),
            StructField("created_at", TimestampType(), False),
            StructField("resolved", StringType(), True),
            StructField("resolved_at", TimestampType(), True),
            StructField("resolved_by", StringType(), True)
        ])
        
        self.alert_schema = StructType([
            StructField("alert_id", StringType(), False),
            StructField("alert_type", StringType(), False),
            StructField("severity", StringType(), False),
            StructField("message", StringType(), False),
            StructField("created_at", TimestampType(), False),
            StructField("acknowledged", StringType(), True),
            StructField("acknowledged_at", TimestampType(), True),
            StructField("acknowledged_by", StringType(), True)
        ])
    
    @classmethod
    def get_instance(cls, spark: SparkSession) -> 'ETLMonitor':
        """Get singleton instance of ETL Monitor."""
        return cls(spark)
    
    def get_dashboard_data(
        self, 
        run_log_path: str,
        days_back: int = 7
    ) -> Dict:
        """
        Generate dashboard metrics from run logs.
        
        Args:
            run_log_path: Path to ETL run log data
            days_back: Number of days to look back
            
        Returns:
            Dictionary with aggregated dashboard metrics
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        self.logger.info(f"Generating dashboard data for last {days_back} days")
        
        # Read run logs
        run_logs_df = self.spark.read.schema(self.run_log_schema).parquet(run_log_path)
        
        # Filter by date range
        filtered_df = run_logs_df.filter(F.col("start_time") >= F.lit(cutoff_time))
        
        # Aggregate metrics
        metrics_df = filtered_df.agg(
            F.count("*").alias("total_runs"),
            F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("successful_runs"),
            F.sum(F.when(F.col("status").isin(["FAILED", "ERROR"]), 1).otherwise(0)).alias("failed_runs"),
            F.sum(F.when(F.col("status") == "RUNNING", 1).otherwise(0)).alias("running_jobs"),
            F.avg("duration").alias("avg_duration"),
            F.sum("records_loaded").alias("total_records"),
            F.sum("error_count").alias("total_errors"),
            F.sum("warning_count").alias("total_warnings")
        ).collect()[0]
        
        # Calculate error rate
        total_runs = metrics_df["total_runs"] or 0
        failed_runs = metrics_df["failed_runs"] or 0
        error_rate = (failed_runs / total_runs * 100) if total_runs > 0 else 0.0
        
        # Get last run info
        last_run_df = filtered_df.orderBy(F.col("end_time").desc()).limit(1)
        last_run = last_run_df.select("end_time", "status").collect()
        
        last_run_time = last_run[0]["end_time"] if last_run else None
        last_run_status = last_run[0]["status"] if last_run else None
        
        dashboard_data = {
            "total_runs": total_runs,
            "successful_runs": metrics_df["successful_runs"] or 0,
            "failed_runs": failed_runs,
            "running_jobs": metrics_df["running_jobs"] or 0,
            "avg_duration": round(metrics_df["avg_duration"] or 0, 2),
            "total_records": metrics_df["total_records"] or 0,
            "error_rate": round(error_rate, 2),
            "last_run_time": last_run_time,
            "last_run_status": last_run_status,
            "total_errors": metrics_df["total_errors"] or 0,
            "total_warnings": metrics_df["total_warnings"] or 0
        }
        
        self.logger.info(f"Dashboard data generated: {dashboard_data}")
        return dashboard_data
    
    def get_performance_metrics(
        self,
        run_log_path: str,
        days_back: int = 30
    ) -> DataFrame:
        """
        Get performance metrics with throughput calculations.
        
        Args:
            run_log_path: Path to ETL run log data
            days_back: Number of days to look back
            
        Returns:
            DataFrame with performance metrics
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        self.logger.info(f"Calculating performance metrics for last {days_back} days")
        
        # Read and filter logs
        run_logs_df = self.spark.read.schema(self.run_log_schema).parquet(run_log_path)
        filtered_df = run_logs_df.filter(F.col("start_time") >= F.lit(cutoff_time))
        
        # Calculate throughput (records per second)
        metrics_df = filtered_df.withColumn(
            "throughput",
            F.when(
                F.col("duration") > 0,
                F.col("records_loaded") / F.col("duration")
            ).otherwise(0.0)
        ).select(
            "run_id",
            "start_time",
            "duration",
            "records_loaded",
            F.round("throughput", 2).alias("throughput"),
            "status"
        ).orderBy(F.col("start_time").desc())
        
        return metrics_df
    
    def get_error_summary(
        self,
        error_log_path: str,
        days_back: int = 7,
        include_resolved: bool = False
    ) -> DataFrame:
        """
        Get error summary for specified time period.
        
        Args:
            error_log_path: Path to error log data
            days_back: Number of days to look back
            include_resolved: Whether to include resolved errors
            
        Returns:
            DataFrame with error details
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        self.logger.info(f"Retrieving error summary for last {days_back} days")
        
        # Read error logs
        error_logs_df = self.spark.read.schema(self.error_log_schema).parquet(error_log_path)
        
        # Filter by date and resolution status
        filtered_df = error_logs_df.filter(F.col("created_at") >= F.lit(cutoff_time))
        
        if not include_resolved:
            filtered_df = filtered_df.filter(
                (F.col("resolved").isNull()) | (F.col("resolved") != "X")
            )
        
        # Order by most recent first
        result_df = filtered_df.orderBy(F.col("created_at").desc())
        
        return result_df
    
    def aggregate_error_metrics(
        self,
        error_log_path: str,
        days_back: int = 7
    ) -> DataFrame:
        """
        Aggregate error metrics by component and error type.
        
        Args:
            error_log_path: Path to error log data
            days_back: Number of days to look back
            
        Returns:
            DataFrame with aggregated error metrics
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        error_logs_df = self.spark.read.schema(self.error_log_schema).parquet(error_log_path)
        filtered_df = error_logs_df.filter(F.col("created_at") >= F.lit(cutoff_time))
        
        # Aggregate by component and error type
        aggregated_df = filtered_df.groupBy("component", "error_type").agg(
            F.count("*").alias("error_count"),
            F.countDistinct("run_id").alias("affected_runs"),
            F.max("created_at").alias("last_occurrence"),
            F.sum(F.when(F.col("resolved") == "X", 1).otherwise(0)).alias("resolved_count")
        ).withColumn(
            "resolution_rate",
            F.round(F.col("resolved_count") / F.col("error_count") * 100, 2)
        ).orderBy(F.col("error_count").desc())
        
        return aggregated_df
    
    def check_health(
        self,
        run_log_path: str,
        alert_output_path: Optional[str] = None
    ) -> Tuple[str, List[Dict]]:
        """
        Perform health check on ETL system.
        
        Args:
            run_log_path: Path to run log data
            alert_output_path: Optional path to write alerts
            
        Returns:
            Tuple of (health_status, list of alerts)
        """
        self.logger.info("Performing system health check")
        
        dashboard = self.get_dashboard_data(run_log_path, days_back=1)
        alerts = []
        
        # Check for overload
        if dashboard["running_jobs"] > 5:
            health_status = "OVERLOADED"
            alert = {
                "alert_type": "PERFORMANCE",
                "severity": "HIGH",
                "message": f"Too many running jobs: {dashboard['running_jobs']}",
                "created_at": datetime.now()
            }
            alerts.append(alert)
            self.logger.warning(alert["message"])
        
        # Check error rate
        elif dashboard["error_rate"] > 50:
            health_status = "CRITICAL"
            alert = {
                "alert_type": "ERROR_RATE",
                "severity": "CRITICAL",
                "message": f"High error rate: {dashboard['error_rate']}%",
                "created_at": datetime.now()
            }
            alerts.append(alert)
            self.logger.error(alert["message"])
        
        elif dashboard["error_rate"] > 20:
            health_status = "WARNING"
            alert = {
                "alert_type": "ERROR_RATE",
                "severity": "MEDIUM",
                "message": f"Elevated error rate: {dashboard['error_rate']}%",
                "created_at": datetime.now()
            }
            alerts.append(alert)
            self.logger.warning(alert["message"])
        
        # Check for successful runs
        elif dashboard["failed_runs"] == 0 and dashboard["successful_runs"] > 0:
            health_status = "HEALTHY"
            self.logger.info("System health: HEALTHY")
        
        else:
            health_status = "UNKNOWN"
            self.logger.warning("System health: UNKNOWN")
        
        # Write alerts if path provided
        if alert_output_path and alerts:
            alerts_df = self.spark.createDataFrame(alerts)
            alerts_df.write.mode("append").parquet(alert_output_path)
        
        return health_status, alerts
    
    def calculate_time_series_metrics(
        self,
        run_log_path: str,
        days_back: int = 30,
        time_bucket: str = "hour"
    ) -> DataFrame:
        """
        Calculate time-series metrics for trend analysis.
        
        Args:
            run_log_path: Path to run log data
            days_back: Number of days to analyze
            time_bucket: Time bucket size ('hour', 'day', 'week')
            
        Returns:
            DataFrame with time-series metrics
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        self.logger.info(f"Calculating time-series metrics with {time_bucket} buckets")
        
        run_logs_df = self.spark.read.schema(self.run_log_schema).parquet(run_log_path)
        filtered_df = run_logs_df.filter(F.col("start_time") >= F.lit(cutoff_time))
        
        # Create time bucket
        if time_bucket == "hour":
            time_expr = F.date_trunc("hour", F.col("start_time"))
        elif time_bucket == "day":
            time_expr = F.date_trunc("day", F.col("start_time"))
        elif time_bucket == "week":
            time_expr = F.date_trunc("week", F.col("start_time"))
        else:
            time_expr = F.date_trunc("hour", F.col("start_time"))
        
        # Aggregate by time bucket
        time_series_df = filtered_df.withColumn("time_bucket", time_expr).groupBy("time_bucket").agg(
            F.count("*").alias("total_runs"),
            F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("successful_runs"),
            F.sum(F.when(F.col("status").isin(["FAILED", "ERROR"]), 1).otherwise(0)).alias("failed_runs"),
            F.avg("duration").alias("avg_duration"),
            F.sum("records_loaded").alias("total_records"),
            F.avg(F.when(F.col("duration") > 0, F.col("records_loaded") / F.col("duration")).otherwise(0)).alias("avg_throughput")
        ).withColumn(
            "success_rate",
            F.round(F.col("successful_runs") / F.col("total_runs") * 100, 2)
        ).orderBy("time_bucket")
        
        return time_series_df
    
    def generate_run_comparison(
        self,
        run_log_path: str,
        run_id_1: str,
        run_id_2: str
    ) -> Dict:
        """
        Compare two ETL runs side by side.
        
        Args:
            run_log_path: Path to run log data
            run_id_1: First run ID
            run_id_2: Second run ID
            
        Returns:
            Dictionary with comparison metrics
        """
        self.logger.info(f"Comparing runs: {run_id_1} vs {run_id_2}")
        
        run_logs_df = self.spark.read.schema(self.run_log_schema).parquet(run_log_path)
        
        # Get both runs
        comparison_df = run_logs_df.filter(
            F.col("run_id").isin([run_id_1, run_id_2])
        ).collect()
        
        if len(comparison_df) != 2:
            self.logger.error(f"Could not find both runs for comparison")
            return {}
        
        run1 = next((r.asDict() for r in comparison_df if r["run_id"] == run_id_1), None)
        run2 = next((r.asDict() for r in comparison_df if r["run_id"] == run_id_2), None)
        
        comparison = {
            "run_1": run1,
            "run_2": run2,
            "duration_diff": run2["duration"] - run1["duration"],
            "duration_diff_pct": round(
                (run2["duration"] - run1["duration"]) / run1["duration"] * 100, 2
            ) if run1["duration"] > 0 else 0,
            "records_diff": run2["records_loaded"] - run1["records_loaded"],
            "throughput_1": round(
                run1["records_loaded"] / run1["duration"], 2
            ) if run1["duration"] > 0 else 0,
            "throughput_2": round(
                run2["records_loaded"] / run2["duration"], 2
            ) if run2["duration"] > 0 else 0
        }
        
        return comparison
    
    def export_metrics_report(
        self,
        run_log_path: str,
        error_log_path: str,
        output_path: str,
        days_back: int = 30,
        format: str = "parquet"
    ) -> None:
        """
        Export comprehensive metrics report.
        
        Args:
            run_log_path: Path to run log data
            error_log_path: Path to error log data
            output_path: Path to write report
            days_back: Number of days to include
            format: Output format ('parquet', 'csv', 'json')
        """
        self.logger.info(f"Generating metrics report for last {days_back} days")
        
        # Get all metrics
        dashboard = self.get_dashboard_data(run_log_path, days_back)
        performance_df = self.get_performance_metrics(run_log_path, days_back)
        error_summary_df = self.aggregate_error_metrics(error_log_path, days_back)
        time_series_df = self.calculate_time_series_metrics(run_log_path, days_back, "day")
        
        # Write summary
        summary_df = self.spark.createDataFrame([dashboard])
        
        writer = summary_df.write.mode("overwrite")
        if format == "csv":
            writer.option("header", "true").csv(f"{output_path}/summary")
        elif format == "json":
            writer.json(f"{output_path}/summary")
        else:
            writer.parquet(f"{output_path}/summary")
        
        # Write detailed metrics
        performance_df.write.mode("overwrite").format(format).save(f"{output_path}/performance")
        error_summary_df.write.mode("overwrite").format(format).save(f"{output_path}/errors")
        time_series_df.write.mode("overwrite").format(format).save(f"{output_path}/timeseries")
        
        self.logger.info(f"Metrics report exported to {output_path}")


def create_monitoring_tables(spark: SparkSession, base_path: str) -> None:
    """
    Initialize monitoring data structures.
    
    Args:
        spark: SparkSession
        base_path: Base path for monitoring data
    """
    monitor = ETLMonitor.get_instance(spark)
    
    # Create empty DataFrames with schemas
    empty_run_log = spark.createDataFrame([], monitor.run_log_schema)
    empty_error_log = spark.createDataFrame([], monitor.error_log_schema)
    empty_alerts = spark.createDataFrame([], monitor.alert_schema)
    
    # Write initial empty tables
    empty_run_log.write.mode("overwrite").parquet(f"{base_path}/run_logs")
    empty_error_log.write.mode("overwrite").parquet(f"{base_path}/error_logs")
    empty_alerts.write.mode("overwrite").parquet(f"{base_path}/alerts")
    
    logging.info(f"Monitoring tables initialized at {base_path}")