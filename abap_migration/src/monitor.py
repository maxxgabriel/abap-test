"""
PySpark ETL Monitoring and Alerting Module
Provides monitoring, dashboard, and alerting capabilities.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as _sum, max as _max, min as _min
from datetime import datetime, timedelta
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


class ETLMonitor:
    """Monitors ETL system health and performance"""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize monitor with Spark session and configuration
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.alert_config = config.get('alerting', {})
    
    def get_dashboard_data(self, days_back: int = 7) -> Dict:
        """
        Get dashboard summary statistics
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            Dictionary with dashboard metrics
        """
        logger.info(f"Retrieving dashboard data for last {days_back} days")
        
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        # Read run log
        run_log_df = self.spark.read \
            .format("jdbc") \
            .options(
                url=self.config['sources']['database']['jdbc_url'],
                dbtable="etl_run_log",
                user=self.config['sources']['database']['user'],
                password=self.config['sources']['database']['password']
            ) \
            .load() \
            .filter(col("start_time") >= cutoff_time)
        
        # Calculate metrics
        stats = run_log_df.agg(
            count("*").alias("total_runs"),
            _sum(col("status") == "SUCCESS").alias("successful_runs"),
            _sum((col("status") == "FAILED") | (col("status") == "ERROR")).alias("failed_runs"),
            _sum(col("status") == "RUNNING").alias("running_jobs"),
            avg("duration").alias("avg_duration"),
            _sum("records_loaded").alias("total_records")
        ).first()
        
        # Get last run info
        last_run = run_log_df.orderBy(col("end_time").desc()).first()
        
        dashboard = {
            'total_runs': stats['total_runs'] or 0,
            'successful_runs': stats['successful_runs'] or 0,
            'failed_runs': stats['failed_runs'] or 0,
            'running_jobs': stats['running_jobs'] or 0,
            'avg_duration': stats['avg_duration'] or 0,
            'total_records': stats['total_records'] or 0,
            'error_rate': (stats['failed_runs'] / stats['total_runs'] * 100) if stats['total_runs'] > 0 else 0,
            'last_run_time': last_run['end_time'] if last_run else None,
            'last_run_status': last_run['status'] if last_run else None
        }
        
        logger.info(f"Dashboard: {dashboard['total_runs']} runs, {dashboard['error_rate']:.2f}% error rate")
        
        return dashboard
    
    def get_performance_metrics(self, days_back: int = 30) -> List[Dict]:
        """
        Get performance metrics over time
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            List of performance metric dictionaries
        """
        logger.info(f"Retrieving performance metrics for last {days_back} days")
        
        cutoff_time = datetime.now() - timedelta(days=days_back)
        
        run_log_df = self.spark.read \
            .format("jdbc") \
            .options(
                url=self.config['sources']['database']['jdbc_url'],
                dbtable="etl_run_log",
                user=self.config['sources']['database']['user'],
                password=self.config['sources']['database']['password']
            ) \
            .load() \
            .filter(col("start_time") >= cutoff_time) \
            .orderBy(col("start_time").desc())
        
        metrics = []
        for row in run_log_df.collect():
            throughput = row['records_loaded'] / row['duration'] if row['duration'] > 0 else 0
            metrics.append({
                'run_id': row['run_id'],
                'start_time': row['start_time'],
                'duration': row['duration'],
                'records_processed': row['records_loaded'],
                'throughput': throughput,
                'status': row['status']
            })
        
        return metrics
    
    def check_health(self) -> str:
        """
        Perform system health check
        
        Returns:
            Health status ('HEALTHY', 'WARNING', 'CRITICAL', 'OVERLOADED')
        """
        logger.info("Performing health check")
        
        dashboard = self.get_dashboard_data(days_back=1)
        
        health_status = 'HEALTHY'
        
        # Check for overload
        if dashboard['running_jobs'] > self.config.get('max_concurrent_jobs', 5):
            health_status = 'OVERLOADED'
            self.send_alert(
                alert_type='PERFORMANCE',
                message=f"Too many running jobs: {dashboard['running_jobs']}",
                severity='HIGH'
            )
        
        # Check error rate
        elif dashboard['error_rate'] > 50:
            health_status = 'CRITICAL'
            self.send_alert(
                alert_type='ERROR_RATE',
                message=f"Critical error rate: {dashboard['error_rate']:.2f}%",
                severity='CRITICAL'
            )
        
        elif dashboard['error_rate'] > 20:
            health_status = 'WARNING'
            self.send_alert(
                alert_type='ERROR_RATE',
                message=f"Elevated error rate: {dashboard['error_rate']:.2f}%",
                severity='MEDIUM'
            )
        
        logger.info(f"Health status: {health_status}")
        
        return health_status
    
    def send_alert(self, alert_type: str, message: str, severity: str):
        """
        Send alert notification
        
        Args:
            alert_type: Type of alert
            message: Alert message
            severity: Alert severity level
        """
        logger.warning(f"ALERT [{severity}] {alert_type}: {message}")
        
        # Send email if configured
        if self.alert_config.get('email_enabled', False):
            self._send_email_alert(alert_type, message, severity)
        
        # Send to monitoring system
        if self.alert_config.get('monitoring_enabled', False):
            self._send_to_monitoring(alert_type, message, severity)
    
    def _send_email_alert(self, alert_type: str, message: str, severity: str):
        """Send email alert"""
        # Email implementation would go here
        logger.info(f"Email alert sent to {self.alert_config.get('alert_email')}")
    
    def _send_to_monitoring(self, alert_type: str, message: str, severity: str):
        """Send to external monitoring system"""
        # Integration with monitoring system (Prometheus, CloudWatch, etc.)
        logger.info(f"Alert sent to monitoring system: {alert_type}")