"""
ETL Monitoring Module with PySpark Aggregations
Provides runtime metrics, performance tracking, and health monitoring
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    TimestampType, DoubleType, LongType
)
from typing import Dict, Optional, List
from datetime import datetime, timedelta
import logging


class ETLMonitor:
    """
    Singleton ETL monitoring class for tracking and aggregating runtime metrics
    """
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
    
    @classmethod
    def get_instance(cls) -> 'ETLMonitor':
        """Get singleton instance"""
        return cls()
    
    def get_dashboard_data(self, days_back: int = 7) -> Dict:
        """
        Get aggregated dashboard metrics using PySpark
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            Dictionary with dashboard metrics
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        # Read run log data
        run_log_df = self._read_run_log()
        
        if run_log_df.isEmpty():
            return self._empty_dashboard()
        
        # Filter by time window
        filtered_df = run_log_df.filter(F.col("start_time") >= F.lit(cutoff_time))
        
        # Aggregate metrics using groupBy/agg
        dashboard_metrics = filtered_df.agg(
            F.count("*").alias("total_runs"),
            F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("successful_runs"),
            F.sum(F.when(F.col("status").isin(["FAILED", "ERROR"]), 1).otherwise(0)).alias("failed_runs"),
            F.sum(F.when(F.col("status") == "RUNNING", 1).otherwise(0)).alias("running_jobs"),
            F.avg("duration").alias("avg_duration"),
            F.sum("records_loaded").alias("total_records")
        ).first()
        
        # Calculate error rate
        total_runs = dashboard_metrics["total_runs"] or 0
        failed_runs = dashboard_metrics["failed_runs"] or 0
        error_rate = (failed_runs / total_runs * 100) if total_runs > 0 else 0.0
        
        # Get last run info
        last_run = run_log_df.orderBy(F.col("end_time").desc()).first()
        
        return {
            "total_runs": total_runs,
            "successful_runs": dashboard_metrics["successful_runs"] or 0,
            "failed_runs": failed_runs,
            "running_jobs": dashboard_metrics["running_jobs"] or 0,
            "avg_duration": dashboard_metrics["avg_duration"] or 0.0,
            "total_records": dashboard_metrics["total_records"] or 0,
            "error_rate": round(error_rate, 2),
            "last_run_time": last_run["end_time"] if last_run else None,
            "last_run_status": last_run["status"] if last_run else None
        }
    
    def get_performance_metrics(self, days_back: int = 30) -> List[Dict]:
        """
        Get detailed performance metrics with throughput calculations
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            List of performance metric dictionaries
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        run_log_df = self._read_run_log()
        
        if run_log_df.isEmpty():
            return []
        
        # Filter and calculate throughput
        metrics_df = (
            run_log_df
            .filter(F.col("start_time") >= F.lit(cutoff_time))
            .withColumn(
                "throughput",
                F.when(
                    F.col("duration") > 0,
                    F.col("records_loaded") / F.col("duration")
                ).otherwise(0.0)
            )
            .select(
                "run_id",
                "start_time",
                "duration",
                F.col("records_loaded").alias("records_processed"),
                "throughput",
                "status"
            )
            .orderBy(F.col("start_time").desc())
        )
        
        return [row.asDict() for row in metrics_df.collect()]
    
    def get_error_summary(self, days_back: int = 7) -> List[Dict]:
        """
        Get summary of unresolved errors
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            List of error records
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        error_log_df = self._read_error_log()
        
        if error_log_df.isEmpty():
            return []
        
        errors = (
            error_log_df
            .filter(
                (F.col("created_at") >= F.lit(cutoff_time)) &
                (F.col("resolved") == False)
            )
            .orderBy(F.col("created_at").desc())
        )
        
        return [row.asDict() for row in errors.collect()]
    
    def get_aggregated_stats_by_category(self) -> DataFrame:
        """
        Get aggregated statistics grouped by category
        
        Returns:
            DataFrame with category-level aggregations
        """
        run_log_df = self._read_run_log()
        
        if run_log_df.isEmpty():
            return self.spark.createDataFrame([], self._stats_schema())
        
        # Group by category with multiple aggregations
        stats_df = (
            run_log_df
            .groupBy("etl_type")
            .agg(
                F.count("*").alias("run_count"),
                F.sum("records_loaded").alias("total_records"),
                F.avg("duration").alias("avg_duration"),
                F.min("duration").alias("min_duration"),
                F.max("duration").alias("max_duration"),
                F.stddev("duration").alias("std_duration"),
                F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
                F.sum(F.when(F.col("status").isin(["FAILED", "ERROR"]), 1).otherwise(0)).alias("failure_count")
            )
            .withColumn(
                "success_rate",
                F.when(
                    F.col("run_count") > 0,
                    (F.col("success_count") / F.col("run_count")) * 100
                ).otherwise(0.0)
            )
            .orderBy(F.col("run_count").desc())
        )
        
        return stats_df
    
    def get_hourly_throughput_trend(self, days_back: int = 7) -> DataFrame:
        """
        Calculate hourly throughput trends using window functions
        
        Args:
            days_back: Number of days to analyze
            
        Returns:
            DataFrame with hourly throughput metrics
        """
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        run_log_df = self._read_run_log()
        
        if run_log_df.isEmpty():
            return self.spark.createDataFrame([], self._trend_schema())
        
        # Extract hour and calculate throughput
        trend_df = (
            run_log_df
            .filter(F.col("start_time") >= F.lit(cutoff_time))
            .withColumn("hour", F.hour("start_time"))
            .withColumn("date", F.to_date("start_time"))
            .withColumn(
                "throughput",
                F.when(
                    F.col("duration") > 0,
                    F.col("records_loaded") / F.col("duration")
                ).otherwise(0.0)
            )
            .groupBy("date", "hour")
            .agg(
                F.count("*").alias("run_count"),
                F.sum("records_loaded").alias("total_records"),
                F.avg("throughput").alias("avg_throughput"),
                F.max("throughput").alias("max_throughput"),
                F.avg("duration").alias("avg_duration")
            )
            .orderBy("date", "hour")
        )
        
        return trend_df
    
    def check_health(self) -> str:
        """
        Perform health check based on current metrics
        
        Returns:
            Health status string
        """
        dashboard = self.get_dashboard_data(days_back=1)
        
        running_jobs = dashboard["running_jobs"]
        error_rate = dashboard["error_rate"]
        
        if running_jobs > 5:
            self.logger.warning(f"Too many running jobs: {running_jobs}")
            self.send_alert(
                alert_type="PERFORMANCE",
                message=f"Too many running jobs: {running_jobs}",
                severity="HIGH"
            )
            return "OVERLOADED"
        
        if error_rate > 50:
            self.logger.error(f"Critical error rate: {error_rate}%")
            self.send_alert(
                alert_type="ERROR_RATE",
                message=f"High error rate: {error_rate}%",
                severity="CRITICAL"
            )
            return "CRITICAL"
        
        if error_rate > 20:
            self.logger.warning(f"Elevated error rate: {error_rate}%")
            self.send_alert(
                alert_type="ERROR_RATE",
                message=f"Elevated error rate: {error_rate}%",
                severity="MEDIUM"
            )
            return "WARNING"
        
        if dashboard["failed_runs"] == 0 and dashboard["successful_runs"] > 0:
            return "HEALTHY"
        
        return "NORMAL"
    
    def send_alert(self, alert_type: str, message: str, severity: str):
        """
        Send monitoring alert
        
        Args:
            alert_type: Type of alert
            message: Alert message
            severity: Alert severity level
        """
        self.logger.info(f"ALERT [{severity}] {alert_type}: {message}")
        # In production, integrate with alerting system (email, Slack, PagerDuty, etc.)
    
    def calculate_sla_compliance(self, sla_threshold_minutes: int = 60) -> DataFrame:
        """
        Calculate SLA compliance based on duration threshold
        
        Args:
            sla_threshold_minutes: Maximum allowed duration in minutes
            
        Returns:
            DataFrame with SLA compliance metrics
        """
        run_log_df = self._read_run_log()
        
        if run_log_df.isEmpty():
            return self.spark.createDataFrame([], self._sla_schema())
        
        sla_threshold_seconds = sla_threshold_minutes * 60
        
        sla_df = (
            run_log_df
            .withColumn(
                "sla_met",
                F.when(
                    (F.col("duration") <= sla_threshold_seconds) & 
                    (F.col("status") == "SUCCESS"),
                    True
                ).otherwise(False)
            )
            .groupBy("etl_type")
            .agg(
                F.count("*").alias("total_runs"),
                F.sum(F.when(F.col("sla_met") == True, 1).otherwise(0)).alias("sla_met_count"),
                F.avg("duration").alias("avg_duration")
            )
            .withColumn(
                "sla_compliance_pct",
                F.when(
                    F.col("total_runs") > 0,
                    (F.col("sla_met_count") / F.col("total_runs")) * 100
                ).otherwise(0.0)
            )
        )
        
        return sla_df
    
    def _read_run_log(self) -> DataFrame:
        """Read ETL run log with proper schema"""
        schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("etl_type", StringType(), True),
            StructField("status", StringType(), True),
            StructField("start_time", TimestampType(), True),
            StructField("end_time", TimestampType(), True),
            StructField("duration", IntegerType(), True),
            StructField("records_extracted", LongType(), True),
            StructField("records_loaded", LongType(), True),
            StructField("records_failed", LongType(), True),
            StructField("error_message", StringType(), True)
        ])
        
        try:
            # In production, read from actual data source (database, Delta Lake, etc.)
            return self.spark.read.format("delta").schema(schema).load("path/to/run_log")
        except Exception as e:
            self.logger.warning(f"Could not read run log: {e}")
            return self.spark.createDataFrame([], schema)
    
    def _read_error_log(self) -> DataFrame:
        """Read error log with proper schema"""
        schema = StructType([
            StructField("error_id", StringType(), False),
            StructField("run_id", StringType(), True),
            StructField("component", StringType(), True),
            StructField("error_type", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("resolved", StringType(), True)
        ])
        
        try:
            return self.spark.read.format("delta").schema(schema).load("path/to/error_log")
        except Exception as e:
            self.logger.warning(f"Could not read error log: {e}")
            return self.spark.createDataFrame([], schema)
    
    def _empty_dashboard(self) -> Dict:
        """Return empty dashboard structure"""
        return {
            "total_runs": 0,
            "successful_runs": 0,
            "failed_runs": 0,
            "running_jobs": 0,
            "avg_duration": 0.0,
            "total_records": 0,
            "error_rate": 0.0,
            "last_run_time": None,
            "last_run_status": None
        }
    
    def _stats_schema(self) -> StructType:
        """Schema for aggregated statistics"""
        return StructType([
            StructField("etl_type", StringType(), True),
            StructField("run_count", LongType(), True),
            StructField("total_records", LongType(), True),
            StructField("avg_duration", DoubleType(), True),
            StructField("min_duration", IntegerType(), True),
            StructField("max_duration", IntegerType(), True),
            StructField("std_duration", DoubleType(), True),
            StructField("success_count", LongType(), True),
            StructField("failure_count", LongType(), True),
            StructField("success_rate", DoubleType(), True)
        ])
    
    def _trend_schema(self) -> StructType:
        """Schema for trend analysis"""
        return StructType([
            StructField("date", StringType(), True),
            StructField("hour", IntegerType(), True),
            StructField("run_count", LongType(), True),
            StructField("total_records", LongType(), True),
            StructField("avg_throughput", DoubleType(), True),
            StructField("max_throughput", DoubleType(), True),
            StructField("avg_duration", DoubleType(), True)
        ])
    
    def _sla_schema(self) -> StructType:
        """Schema for SLA compliance"""
        return StructType([
            StructField("etl_type", StringType(), True),
            StructField("total_runs", LongType(), True),
            StructField("sla_met_count", LongType(), True),
            StructField("avg_duration", DoubleType(), True),
            StructField("sla_compliance_pct", DoubleType(), True)
        ])