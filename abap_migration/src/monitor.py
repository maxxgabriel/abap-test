"""
Production monitoring module for ETL system.
Provides real-time monitoring, metrics collection, and alerting.
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict
import logging
from pyspark.sql import SparkSession
from prometheus_client import Counter, Gauge, Histogram, start_http_server
import time
import threading

logger = logging.getLogger(__name__)


@dataclass
class MetricData:
    """Container for metric data."""
    timestamp: datetime
    metric_name: str
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class Alert:
    """Alert definition."""
    alert_id: str
    severity: str  # CRITICAL, HIGH, MEDIUM, LOW
    message: str
    timestamp: datetime
    component: str
    resolved: bool = False


class MonitoringManager:
    """Manages production monitoring and alerting."""
    
    # Prometheus metrics
    jobs_total = Counter('etl_jobs_total', 'Total ETL jobs', ['status'])
    records_processed = Counter('etl_records_processed_total', 'Total records processed', ['stage'])
    job_duration = Histogram('etl_job_duration_seconds', 'Job duration', ['job_type'])
    active_jobs = Gauge('etl_active_jobs', 'Currently active jobs')
    error_rate = Gauge('etl_error_rate', 'Error rate percentage')
    data_quality_score = Gauge('etl_data_quality_score', 'Data quality score', ['check_type'])
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize monitoring manager.
        
        Args:
            config: Monitoring configuration
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.alerts: List[Alert] = []
        self.metrics_history: Dict[str, List[MetricData]] = defaultdict(list)
        self._running = False
        self._monitor_thread: Optional[threading.Thread] = None
        
    def initialize(self):
        """Initialize monitoring systems."""
        self.logger.info("Initializing monitoring systems")
        
        # Start Prometheus metrics server
        if self.config.get('prometheus', {}).get('enabled', True):
            port = self.config.get('prometheus', {}).get('port', 8000)
            start_http_server(port)
            self.logger.info(f"Prometheus metrics server started on port {port}")
        
        # Start background monitoring
        self._running = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        
        self.logger.info("Monitoring initialized successfully")
    
    def shutdown(self):
        """Shutdown monitoring systems."""
        self.logger.info("Shutting down monitoring")
        self._running = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
    
    def record_job_start(self, job_id: str, job_type: str):
        """Record job start."""
        self.logger.info(f"Job started: {job_id} ({job_type})")
        self.active_jobs.inc()
        self.jobs_total.labels(status='started').inc()
    
    def record_job_completion(self, job_id: str, job_type: str, 
                            duration: float, status: str, 
                            records_processed: int):
        """
        Record job completion.
        
        Args:
            job_id: Job identifier
            job_type: Type of job
            duration: Job duration in seconds
            status: Job status (SUCCESS, FAILED, PARTIAL)
            records_processed: Number of records processed
        """
        self.logger.info(
            f"Job completed: {job_id} - Status: {status}, "
            f"Duration: {duration:.2f}s, Records: {records_processed}"
        )
        
        self.active_jobs.dec()
        self.jobs_total.labels(status=status).inc()
        self.job_duration.labels(job_type=job_type).observe(duration)
        self.records_processed.labels(stage='total').inc(records_processed)
        
        # Check for performance issues
        threshold = self.config.get('thresholds', {}).get('job_duration_seconds', 300)
        if duration > threshold:
            self._create_alert(
                alert_id=f"PERF_{job_id}",
                severity='MEDIUM',
                message=f"Job {job_id} exceeded duration threshold: {duration:.2f}s > {threshold}s",
                component='performance'
            )
    
    def record_stage_metrics(self, stage: str, records: int, duration: float):
        """
        Record metrics for ETL stage.
        
        Args:
            stage: Stage name (extract, transform, load)
            records: Number of records processed
            duration: Stage duration in seconds
        """
        self.records_processed.labels(stage=stage).inc(records)
        
        metric = MetricData(
            timestamp=datetime.now(),
            metric_name=f'{stage}_duration',
            value=duration,
            labels={'stage': stage}
        )
        self.metrics_history[f'{stage}_duration'].append(metric)
        
        # Keep only recent history
        self._cleanup_old_metrics(f'{stage}_duration')
    
    def record_error(self, component: str, error_type: str, message: str):
        """
        Record error occurrence.
        
        Args:
            component: Component where error occurred
            error_type: Type of error
            message: Error message
        """
        self.logger.error(f"Error in {component}: {error_type} - {message}")
        
        # Update error rate
        self._update_error_rate()
        
        # Create alert for critical errors
        if error_type in ['CRITICAL', 'FATAL']:
            self._create_alert(
                alert_id=f"ERROR_{component}_{int(time.time())}",
                severity='CRITICAL',
                message=f"{component}: {message}",
                component=component
            )
    
    def record_data_quality(self, check_type: str, score: float, details: Dict[str, Any]):
        """
        Record data quality metrics.
        
        Args:
            check_type: Type of quality check
            score: Quality score (0-100)
            details: Additional details
        """
        self.logger.info(f"Data quality check: {check_type} = {score:.2f}")
        self.data_quality_score.labels(check_type=check_type).set(score)
        
        # Alert on low quality scores
        threshold = self.config.get('thresholds', {}).get('quality_score', 80)
        if score < threshold:
            self._create_alert(
                alert_id=f"QUALITY_{check_type}_{int(time.time())}",
                severity='HIGH',
                message=f"Data quality score below threshold: {score:.2f} < {threshold}",
                component='data_quality'
            )
    
    def get_dashboard_data(self, hours_back: int = 24) -> Dict[str, Any]:
        """
        Get dashboard data for visualization.
        
        Args:
            hours_back: Hours of history to include
            
        Returns:
            Dashboard data dictionary
        """
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        
        # Calculate statistics
        recent_metrics = {
            name: [m for m in metrics if m.timestamp >= cutoff_time]
            for name, metrics in self.metrics_history.items()
        }
        
        # Get active alerts
        active_alerts = [a for a in self.alerts if not a.resolved]
        
        return {
            'timestamp': datetime.now().isoformat(),
            'period_hours': hours_back,
            'summary': {
                'active_jobs': int(self.active_jobs._value.get()),
                'total_jobs': sum(self.jobs_total._metrics.values()),
                'active_alerts': len(active_alerts),
                'critical_alerts': len([a for a in active_alerts if a.severity == 'CRITICAL'])
            },
            'alerts': [
                {
                    'id': a.alert_id,
                    'severity': a.severity,
                    'message': a.message,
                    'component': a.component,
                    'timestamp': a.timestamp.isoformat()
                }
                for a in active_alerts[:10]  # Last 10 alerts
            ],
            'metrics': {
                name: {
                    'count': len(metrics),
                    'avg': sum(m.value for m in metrics) / len(metrics) if metrics else 0,
                    'min': min((m.value for m in metrics), default=0),
                    'max': max((m.value for m in metrics), default=0)
                }
                for name, metrics in recent_metrics.items()
            }
        }
    
    def get_performance_report(self) -> Dict[str, Any]:
        """
        Generate performance report.
        
        Returns:
            Performance report dictionary
        """
        return {
            'timestamp': datetime.now().isoformat(),
            'job_statistics': {
                'total_jobs': sum(self.jobs_total._metrics.values()),
                'active_jobs': int(self.active_jobs._value.get()),
                'success_rate': self._calculate_success_rate()
            },
            'throughput': {
                'records_per_hour': self._calculate_throughput(),
                'avg_job_duration': self._calculate_avg_duration()
            },
            'quality': {
                'overall_score': self._calculate_overall_quality_score(),
                'checks_performed': len(self.data_quality_score._metrics)
            },
            'alerts': {
                'total': len(self.alerts),
                'active': len([a for a in self.alerts if not a.resolved]),
                'by_severity': self._group_alerts_by_severity()
            }
        }
    
    def _monitor_loop(self):
        """Background monitoring loop."""
        interval = self.config.get('monitor_interval_seconds', 60)
        
        while self._running:
            try:
                self._update_error_rate()
                self._check_system_health()
                self._cleanup_resolved_alerts()
                time.sleep(interval)
            except Exception as e:
                self.logger.error(f"Error in monitor loop: {str(e)}")
    
    def _update_error_rate(self):
        """Update error rate metric."""
        try:
            total_jobs = sum(self.jobs_total._metrics.values())
            if total_jobs > 0:
                failed_jobs = self.jobs_total.labels(status='FAILED')._value.get()
                rate = (failed_jobs / total_jobs) * 100
                self.error_rate.set(rate)
                
                # Alert on high error rate
                threshold = self.config.get('thresholds', {}).get('error_rate_percent', 10)
                if rate > threshold:
                    self._create_alert(
                        alert_id=f"ERROR_RATE_{int(time.time())}",
                        severity='HIGH',
                        message=f"Error rate exceeded threshold: {rate:.2f}% > {threshold}%",
                        component='system'
                    )
        except Exception as e:
            self.logger.error(f"Error updating error rate: {str(e)}")
    
    def _check_system_health(self):
        """Check overall system health."""
        try:
            # Check Spark cluster
            spark = SparkSession.builder.getOrCreate()
            if not spark.sparkContext._jsc.sc().isStopped():
                # Spark is healthy
                pass
            
            # Check active jobs
            active = int(self.active_jobs._value.get())
            max_concurrent = self.config.get('thresholds', {}).get('max_concurrent_jobs', 10)
            if active > max_concurrent:
                self._create_alert(
                    alert_id=f"CAPACITY_{int(time.time())}",
                    severity='MEDIUM',
                    message=f"Too many concurrent jobs: {active} > {max_concurrent}",
                    component='capacity'
                )
        except Exception as e:
            self.logger.error(f"Health check failed: {str(e)}")
            self._create_alert(
                alert_id=f"HEALTH_{int(time.time())}",
                severity='CRITICAL',
                message=f"System health check failed: {str(e)}",
                component='system'
            )
    
    def _create_alert(self, alert_id: str, severity: str, 
                     message: str, component: str):
        """Create new alert."""
        # Check if similar alert already exists
        existing = next(
            (a for a in self.alerts 
             if a.component == component and a.message == message and not a.resolved),
            None
        )
        
        if existing:
            return  # Don't create duplicate
        
        alert = Alert(
            alert_id=alert_id,
            severity=severity,
            message=message,
            timestamp=datetime.now(),
            component=component
        )
        
        self.alerts.append(alert)
        self.logger.warning(f"Alert created: [{severity}] {component}: {message}")
        
        # Send notifications if configured
        self._send_alert_notification(alert)
    
    def _send_alert_notification(self, alert: Alert):
        """Send alert notification."""
        notification_config = self.config.get('notifications', {})
        
        if notification_config.get('enabled', False):
            # Implement notification logic (email, Slack, PagerDuty, etc.)
            self.logger.info(f"Alert notification sent: {alert.alert_id}")
    
    def _cleanup_old_metrics(self, metric_name: str, hours: int = 24):
        """Cleanup old metric data."""
        cutoff = datetime.now() - timedelta(hours=hours)
        self.metrics_history[metric_name] = [
            m for m in self.metrics_history[metric_name]
            if m.timestamp >= cutoff
        ]
    
    def _cleanup_resolved_alerts(self, hours: int = 24):
        """Remove old resolved alerts."""
        cutoff = datetime.now() - timedelta(hours=hours)
        self.alerts = [
            a for a in self.alerts
            if not a.resolved or a.timestamp >= cutoff
        ]
    
    def _calculate_success_rate(self) -> float:
        """Calculate job success rate."""
        total = sum(self.jobs_total._metrics.values())
        if total == 0:
            return 100.0
        success = self.jobs_total.labels(status='SUCCESS')._value.get()
        return (success / total) * 100
    
    def _calculate_throughput(self) -> float:
        """Calculate records processed per hour."""
        # Get metrics from last hour
        cutoff = datetime.now() - timedelta(hours=1)
        recent = [
            m for metrics in self.metrics_history.values()
            for m in metrics
            if m.timestamp >= cutoff
        ]
        
        if not recent:
            return 0.0
        
        total_records = sum(m.value for m in recent if 'records' in m.metric_name)
        return total_records
    
    def _calculate_avg_duration(self) -> float:
        """Calculate average job duration."""
        durations = self.metrics_history.get('job_duration', [])
        if not durations:
            return 0.0
        
        recent = [
            m.value for m in durations
            if m.timestamp >= datetime.now() - timedelta(hours=1)
        ]
        
        return sum(recent) / len(recent) if recent else 0.0
    
    def _calculate_overall_quality_score(self) -> float:
        """Calculate overall data quality score."""
        scores = [
            metric._value.get()
            for metric in self.data_quality_score._metrics.values()
        ]
        
        return sum(scores) / len(scores) if scores else 100.0
    
    def _group_alerts_by_severity(self) -> Dict[str, int]:
        """Group alerts by severity."""
        active_alerts = [a for a in self.alerts if not a.resolved]
        return {
            'CRITICAL': len([a for a in active_alerts if a.severity == 'CRITICAL']),
            'HIGH': len([a for a in active_alerts if a.severity == 'HIGH']),
            'MEDIUM': len([a for a in active_alerts if a.severity == 'MEDIUM']),
            'LOW': len([a for a in active_alerts if a.severity == 'LOW'])
        }