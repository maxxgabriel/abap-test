"""
ETL Monitoring Module with PySpark DataFrame aggregations.
Provides runtime metrics, dashboard data, and performance analysis.
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
    Singleton ETL monitoring class with PySpark aggregations.
    Replaces ABAP zcl_etl_monitor with DataFrame operations.
    """
    
    _instance = None
    
    def __new__(cls, spark: Optional[SparkSession] = None):
        """Singleton pattern implementation."""
        if cls._instance is None:
            cls._instance = super(ETLMonitor, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, spark: Optional[SparkSession] = None):
        """Initialize monitor with SparkSession."""
        if self._initialized:
            return
            
        self.spark = spark or SparkSession.builder.getOrCreate()
        self.logger = logging.getLogger(__name__)
        self._initialized = True
    
    @classmethod
    def get_instance(cls, spark: Optional[SparkSession] = None) -> 'ETLMonitor':
        """Get singleton instance."""
        return cls(spark)
    
    def get_dashboard_data(
        self, 
        days_back: int = 7,
        run_log_table: str = "etl_run_log"
    ) -> Dict:
        """
        Get dashboard statistics using PySpark aggregations.
        
        Replaces ABAP SQL aggregations with groupBy/agg operations.
        
        Args:
            days_back: Number of days to look back
            run_log_table: Table/path containing run logs
            
        Returns:
            Dictionary with dashboard metrics
        """
        try:
            # Calculate cutoff timestamp
            cutoff_time = datetime.now() - timedelta(days=days_back)
            
            # Read run log data
            df_runs = self.spark.read.table(run_log_table)
            
            # Filter by date range
            df_filtered = df_runs.filter(
                F.col("start_time") >= F.lit(cutoff_time)
            )
            
            # Aggregate statistics using DataFrame operations
            stats = df_filtered.agg(
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
            ).collect()[0]
            
            # Calculate error rate
            total_runs = stats["total_runs"] or 0
            failed_runs = stats["failed_runs"] or 0
            error_rate = (failed_runs / total_runs * 100) if total_runs > 0 else 0.0
            
            # Get last run info using window functions
            from pyspark.sql.window import Window
            
            window_spec = Window.orderBy(F.col("end_time").desc())
            df_last_run = df_runs.withColumn(
                "row_num", F.row_number().over(window_spec)
            ).filter(F.col("row_num") == 1).select(
                "end_time", "status"
            ).collect()
            
            last_run_time = df_last_run[0]["end_time"] if df_last_run else None
            last_run_status = df_last_run[0]["status"] if df_last_run else None
            
            return {
                "total_runs": stats["total_runs"] or 0,
                "successful_runs": stats["successful_runs"] or 0,
                "failed_runs": failed_runs,
                "running_jobs": stats["running_jobs"] or 0,
                "avg_duration": stats["avg_duration"] or 0,
                "total_records": stats["total_records"] or 0,
                "error_rate": round(error_rate, 2),
                "last_run_time": last_run_time,
                "last_run_status": last_run_status
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get dashboard data: {str(e)}")
            raise
    
    def get_performance_metrics(
        self, 
        days_back: int = 30,
        run_log_table: str = "etl_run_log"
    ) -> DataFrame:
        """
        Get performance metrics with throughput calculations.
        
        Args:
            days_back: Number of days to analyze
            run_log_table: Table/path containing run logs
            
        Returns:
            DataFrame with performance metrics
        """
        try:
            cutoff_time = datetime.now() - timedelta(days=days_back)
            
            df_runs = self.spark.read.table(run_log_table)
            
            # Calculate throughput (records per second)
            df_metrics = df_runs.filter(
                F.col("start_time") >= F.lit(cutoff_time)
            ).select(
                "run_id",
                "start_time",
                "duration",
                F.col("records_loaded").alias("records_processed"),
                "status"
            ).withColumn(
                "throughput",
                F.when(
                    F.col("duration") > 0,
                    F.col("records_processed") / F.col("duration")
                ).otherwise(0.0)
            ).orderBy(F.col("start_time").desc())
            
            return df_metrics
            
        except Exception as e:
            self.logger.error(f"Failed to get performance metrics: {str(e)}")
            raise
    
    def get_error_summary(
        self, 
        days_back: int = 7,
        error_log_table: str = "etl_error_log"
    ) -> DataFrame:
        """
        Get error summary with aggregations.
        
        Args:
            days_back: Number of days to look back
            error_log_table: Table/path containing error logs
            
        Returns:
            DataFrame with error details
        """
        try:
            cutoff_time = datetime.now() - timedelta(days=days_back)
            
            df_errors = self.spark.read.table(error_log_table)
            
            df_filtered = df_errors.filter(
                (F.col("created_at") >= F.lit(cutoff_time)) &
                (F.col("resolved") == False)
            ).orderBy(F.col("created_at").desc())
            
            return df_filtered
            
        except Exception as e:
            self.logger.error(f"Failed to get error summary: {str(e)}")
            raise
    
    def check_health(
        self,
        alert_thresholds: Optional[Dict] = None
    ) -> Tuple[str, List[Dict]]:
        """
        Check system health and generate alerts.
        
        Args:
            alert_thresholds: Custom thresholds for alerts
            
        Returns:
            Tuple of (health_status, alerts)
        """
        if alert_thresholds is None:
            alert_thresholds = {
                "max_running_jobs": 5,
                "critical_error_rate": 50,
                "warning_error_rate": 20
            }
        
        try:
            dashboard = self.get_dashboard_data(days_back=1)
            alerts = []
            
            running_jobs = dashboard["running_jobs"]
            error_rate = dashboard["error_rate"]
            
            # Check for overload
            if running_jobs > alert_thresholds["max_running_jobs"]:
                health_status = "OVERLOADED"
                alerts.append({
                    "alert_type": "PERFORMANCE",
                    "message": f"Too many running jobs: {running_jobs}",
                    "severity": "HIGH",
                    "timestamp": datetime.now()
                })
            
            # Check error rate
            elif error_rate > alert_thresholds["critical_error_rate"]:
                health_status = "CRITICAL"
                alerts.append({
                    "alert_type": "ERROR_RATE",
                    "message": f"High error rate: {error_rate}%",
                    "severity": "CRITICAL",
                    "timestamp": datetime.now()
                })
            
            elif error_rate > alert_thresholds["warning_error_rate"]:
                health_status = "WARNING"
                alerts.append({
                    "alert_type": "ERROR_RATE",
                    "message": f"Elevated error rate: {error_rate}%",
                    "severity": "MEDIUM",
                    "timestamp": datetime.now()
                })
            
            elif (dashboard["failed_runs"] == 0 and 
                  dashboard["successful_runs"] > 0):
                health_status = "HEALTHY"
            
            else:
                health_status = "UNKNOWN"
            
            return health_status, alerts
            
        except Exception as e:
            self.logger.error(f"Health check failed: {str(e)}")
            return "ERROR", [{
                "alert_type": "SYSTEM",
                "message": f"Health check error: {str(e)}",
                "severity": "CRITICAL",
                "timestamp": datetime.now()
            }]
    
    def send_alert(
        self,
        alert_type: str,
        message: str,
        severity: str,
        alert_table: str = "etl_alerts"
    ):
        """
        Send/log alert to alert table.
        
        Args:
            alert_type: Type of alert
            message: Alert message
            severity: Alert severity level
            alert_table: Table to store alerts
        """
        try:
            alert_data = [{
                "alert_id": f"ALERT_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                "alert_type": alert_type,
                "message": message,
                "severity": severity,
                "created_at": datetime.now(),
                "resolved": False
            }]
            
            df_alert = self.spark.createDataFrame(alert_data)
            df_alert.write.mode("append").saveAsTable(alert_table)
            
            self.logger.warning(
                f"Alert sent - Type: {alert_type}, Severity: {severity}, "
                f"Message: {message}"
            )
            
        except Exception as e:
            self.logger.error(f"Failed to send alert: {str(e)}")
    
    def get_aggregated_metrics(
        self,
        run_log_table: str = "etl_run_log",
        group_by: str = "day"
    ) -> DataFrame:
        """
        Get aggregated metrics grouped by time period.
        
        Args:
            run_log_table: Table containing run logs
            group_by: Time period ('hour', 'day', 'week', 'month')
            
        Returns:
            DataFrame with aggregated metrics
        """
        try:
            df_runs = self.spark.read.table(run_log_table)
            
            # Create time grouping column based on period
            if group_by == "hour":
                df_grouped = df_runs.withColumn(
                    "period", 
                    F.date_trunc("hour", F.col("start_time"))
                )
            elif group_by == "day":
                df_grouped = df_runs.withColumn(
                    "period", 
                    F.date_trunc("day", F.col("start_time"))
                )
            elif group_by == "week":
                df_grouped = df_runs.withColumn(
                    "period", 
                    F.date_trunc("week", F.col("start_time"))
                )
            elif group_by == "month":
                df_grouped = df_runs.withColumn(
                    "period", 
                    F.date_trunc("month", F.col("start_time"))
                )
            else:
                raise ValueError(f"Invalid group_by value: {group_by}")
            
            # Aggregate metrics by period
            df_metrics = df_grouped.groupBy("period").agg(
                F.count("*").alias("total_runs"),
                F.sum(
                    F.when(F.col("status") == "SUCCESS", 1).otherwise(0)
                ).alias("successful_runs"),
                F.sum(
                    F.when(F.col("status").isin(["FAILED", "ERROR"]), 1).otherwise(0)
                ).alias("failed_runs"),
                F.avg("duration").alias("avg_duration"),
                F.min("duration").alias("min_duration"),
                F.max("duration").alias("max_duration"),
                F.sum("records_loaded").alias("total_records_loaded"),
                F.avg("records_loaded").alias("avg_records_loaded")
            ).withColumn(
                "success_rate",
                F.when(
                    F.col("total_runs") > 0,
                    (F.col("successful_runs") / F.col("total_runs") * 100)
                ).otherwise(0.0)
            ).orderBy(F.col("period").desc())
            
            return df_metrics
            
        except Exception as e:
            self.logger.error(f"Failed to get aggregated metrics: {str(e)}")
            raise
    
    def analyze_run_patterns(
        self,
        run_log_table: str = "etl_run_log",
        days_back: int = 30
    ) -> Dict:
        """
        Analyze ETL run patterns and trends.
        
        Args:
            run_log_table: Table containing run logs
            days_back: Number of days to analyze
            
        Returns:
            Dictionary with pattern analysis
        """
        try:
            cutoff_time = datetime.now() - timedelta(days=days_back)
            
            df_runs = self.spark.read.table(run_log_table).filter(
                F.col("start_time") >= F.lit(cutoff_time)
            )
            
            # Add hour and day of week
            df_enriched = df_runs.withColumn(
                "hour_of_day", F.hour("start_time")
            ).withColumn(
                "day_of_week", F.dayofweek("start_time")
            )
            
            # Peak hours analysis
            df_peak_hours = df_enriched.groupBy("hour_of_day").agg(
                F.count("*").alias("run_count"),
                F.avg("duration").alias("avg_duration")
            ).orderBy(F.col("run_count").desc())
            
            peak_hours = df_peak_hours.limit(3).collect()
            
            # Day of week analysis
            df_day_patterns = df_enriched.groupBy("day_of_week").agg(
                F.count("*").alias("run_count"),
                F.avg("duration").alias("avg_duration"),
                F.sum(
                    F.when(F.col("status") == "SUCCESS", 1).otherwise(0)
                ).alias("success_count")
            ).withColumn(
                "success_rate",
                F.col("success_count") / F.col("run_count") * 100
            ).orderBy("day_of_week")
            
            day_patterns = df_day_patterns.collect()
            
            # Duration trends
            duration_stats = df_runs.agg(
                F.avg("duration").alias("avg_duration"),
                F.stddev("duration").alias("std_duration"),
                F.percentile_approx("duration", 0.5).alias("median_duration"),
                F.percentile_approx("duration", 0.95).alias("p95_duration")
            ).collect()[0]
            
            return {
                "peak_hours": [
                    {
                        "hour": row["hour_of_day"],
                        "run_count": row["run_count"],
                        "avg_duration": row["avg_duration"]
                    }
                    for row in peak_hours
                ],
                "day_patterns": [
                    {
                        "day_of_week": row["day_of_week"],
                        "run_count": row["run_count"],
                        "success_rate": row["success_rate"],
                        "avg_duration": row["avg_duration"]
                    }
                    for row in day_patterns
                ],
                "duration_stats": {
                    "avg_duration": duration_stats["avg_duration"],
                    "std_deviation": duration_stats["std_duration"],
                    "median_duration": duration_stats["median_duration"],
                    "p95_duration": duration_stats["p95_duration"]
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to analyze run patterns: {str(e)}")
            raise
    
    def calculate_throughput(
        self,
        records: int,
        duration: int
    ) -> float:
        """
        Calculate throughput (records per second).
        
        Args:
            records: Number of records processed
            duration: Duration in seconds
            
        Returns:
            Throughput value
        """
        if duration > 0:
            return round(records / duration, 2)
        return 0.0
    
    def get_run_statistics_summary(
        self,
        run_log_table: str = "etl_run_log",
        run_id: Optional[str] = None
    ) -> DataFrame:
        """
        Get run statistics with optional filtering by run_id.
        
        Args:
            run_log_table: Table containing run logs
            run_id: Optional specific run ID to filter
            
        Returns:
            DataFrame with run statistics
        """
        try:
            df_runs = self.spark.read.table(run_log_table)
            
            if run_id:
                df_runs = df_runs.filter(F.col("run_id") == run_id)
            
            return df_runs.select(
                "run_id",
                "start_time",
                "end_time",
                "duration",
                "status",
                "records_extracted",
                "records_transformed",
                "records_loaded",
                "records_failed",
                "error_count"
            ).orderBy(F.col("start_time").desc())
            
        except Exception as e:
            self.logger.error(f"Failed to get run statistics: {str(e)}")
            raise


def create_monitor_tables(spark: SparkSession):
    """
    Create monitoring tables with proper schemas.
    
    Args:
        spark: SparkSession instance
    """
    # Run log schema
    run_log_schema = StructType([
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
        StructField("created_by", StringType(), True)
    ])
    
    # Error log schema
    error_log_schema = StructType([
        StructField("error_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("component", StringType(), False),
        StructField("error_type", StringType(), False),
        StructField("error_message", StringType(), False),
        StructField("error_details", StringType(), True),
        StructField("record_id", StringType(), True),
        StructField("created_at", TimestampType(), False),
        StructField("resolved", StringType(), False)
    ])
    
    # Alert schema
    alert_schema = StructType([
        StructField("alert_id", StringType(), False),
        StructField("alert_type", StringType(), False),
        StructField("message", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("resolved", StringType(), False)
    ])
    
    # Create empty DataFrames with schemas
    df_run_log = spark.createDataFrame([], run_log_schema)
    df_error_log = spark.createDataFrame([], error_log_schema)
    df_alerts = spark.createDataFrame([], alert_schema)
    
    # Save as tables
    df_run_log.write.mode("overwrite").saveAsTable("etl_run_log")
    df_error_log.write.mode("overwrite").saveAsTable("etl_error_log")
    df_alerts.write.mode("overwrite").saveAsTable("etl_alerts")