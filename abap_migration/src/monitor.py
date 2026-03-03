"""
ETL Monitoring Module
Provides monitoring, health checks, and performance metrics
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass
import logging

from src.logger import ETLLogger

logger = logging.getLogger(__name__)


@dataclass
class DashboardData:
    """Dashboard statistics"""
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
    """Performance metric"""
    run_id: str
    start_time: datetime
    duration: int
    records_processed: int
    throughput: float
    status: str


class ETLMonitor:
    """Monitor ETL processes and health"""
    
    _instance = None
    
    def __new__(cls, spark: SparkSession = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, spark: SparkSession = None):
        if self._initialized:
            return
            
        self._initialized = True
        self.spark = spark
        self.etl_logger = ETLLogger.get_instance()
        
    @classmethod
    def get_instance(cls, spark: SparkSession = None) -> 'ETLMonitor':
        """Get monitor singleton instance"""
        return cls(spark)
        
    def get_dashboard_data(self, days_back: int = 7, run_log_path: str = None) -> DashboardData:
        """Get dashboard statistics"""
        logger.info(f"Generating dashboard data for last {days_back} days...")
        
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        # Read run log
        if not run_log_path or not self.spark:
            return self._get_mock_dashboard_data()
            
        try:
            df = self.spark.read.parquet(run_log_path)
            df = df.filter(F.col("start_time") >= F.lit(cutoff_time))
            
            stats = df.agg(
                F.count("*").alias("total_runs"),
                F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("successful"),
                F.sum(F.when(F.col("status").isin(["FAILED", "ERROR"]), 1).otherwise(0)).alias("failed"),
                F.sum(F.when(F.col("status") == "RUNNING", 1).otherwise(0)).alias("running"),
                F.avg("duration").alias("avg_duration"),
                F.sum("records_loaded").alias("total_records")
            ).collect()[0]
            
            total_runs = stats["total_runs"]
            error_rate = (stats["failed"] / total_runs * 100) if total_runs > 0 else 0.0
            
            # Get last run
            last_run = df.orderBy(F.col("end_time").desc()).first()
            
            dashboard = DashboardData(
                total_runs=total_runs,
                successful_runs=stats["successful"],
                failed_runs=stats["failed"],
                running_jobs=stats["running"],
                avg_duration=float(stats["avg_duration"]) if stats["avg_duration"] else 0.0,
                total_records=stats["total_records"],
                error_rate=error_rate,
                last_run_time=last_run["end_time"] if last_run else None,
                last_run_status=last_run["status"] if last_run else None
            )
            
            logger.info(f"Dashboard: {total_runs} runs, {error_rate:.2f}% error rate")
            
            return dashboard
            
        except Exception as e:
            logger.error(f"Failed to generate dashboard data: {str(e)}")
            return self._get_mock_dashboard_data()
            
    def _get_mock_dashboard_data(self) -> DashboardData:
        """Get mock dashboard data for testing"""
        return DashboardData(
            total_runs=0,
            successful_runs=0,
            failed_runs=0,
            running_jobs=0,
            avg_duration=0.0,
            total_records=0,
            error_rate=0.0,
            last_run_time=None,
            last_run_status=None
        )
        
    def get_performance_metrics(self, days_back: int = 30, run_log_path: str = None) -> List[PerformanceMetric]:
        """Get performance metrics"""
        logger.info(f"Getting performance metrics for last {days_back} days...")
        
        if not run_log_path or not self.spark:
            return []
            
        try:
            cutoff_time = datetime.now() - timedelta(days=days_back)
            
            df = self.spark.read.parquet(run_log_path)
            df = df.filter(F.col("start_time") >= F.lit(cutoff_time))
            df = df.orderBy(F.col("start_time").desc())
            
            metrics = []
            for row in df.collect():
                throughput = self._calculate_throughput(
                    row["records_loaded"],
                    row["duration"]
                )
                
                metrics.append(PerformanceMetric(
                    run_id=row["run_id"],
                    start_time=row["start_time"],
                    duration=row["duration"],
                    records_processed=row["records_loaded"],
                    throughput=throughput,
                    status=row["status"]
                ))
                
            logger.info(f"Retrieved {len(metrics)} performance metrics")
            
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to get performance metrics: {str(e)}")
            return []
            
    def _calculate_throughput(self, records: int, duration: int) -> float:
        """Calculate throughput (records per second)"""
        if duration == 0:
            return 0.0
        return records / duration
        
    def check_health(self, run_log_path: str = None) -> str:
        """Check system health status"""
        logger.info("Performing health check...")
        
        dashboard = self.get_dashboard_data(days_back=1, run_log_path=run_log_path)
        
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
            
    def send_alert(self, alert_type: str, message: str, severity: str) -> None:
        """Send alert notification"""
        logger.warning(f"ALERT [{severity}] {alert_type}: {message}")
        
        self.etl_logger.log_warning(
            component="MONITOR",
            message=f"Alert: {alert_type}",
            details=f"Severity: {severity}, Message: {message}"
        )
        
    def get_error_summary(self, days_back: int = 7, error_log_path: str = None) -> List[Dict]:
        """Get error summary"""
        logger.info(f"Getting error summary for last {days_back} days...")
        
        if not error_log_path or not self.spark:
            return []
            
        try:
            cutoff_time = datetime.now() - timedelta(days=days_back)
            
            df = self.spark.read.parquet(error_log_path)
            df = df.filter(
                (F.col("created_at") >= F.lit(cutoff_time)) &
                (F.col("resolved") == False)
            )
            df = df.orderBy(F.col("created_at").desc())
            
            errors = [row.asDict() for row in df.collect()]
            
            logger.info(f"Found {len(errors)} unresolved errors")
            
            return errors
            
        except Exception as e:
            logger.error(f"Failed to get error summary: {str(e)}")
            return []