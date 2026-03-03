"""
ETL Orchestration Module
Manages three-phase ETL execution with error handling and scheduling
"""

from pyspark.sql import SparkSession
from typing import Dict, Any, Optional, List
from datetime import datetime
import logging
import uuid
from dataclasses import dataclass, asdict
from enum import Enum

from src.extract import Extractor
from src.transform import Transformer
from src.load import Loader
from src.data_quality import DataQualityChecker
from src.monitor import ETLMonitor
from src.utils import setup_logger, load_config


class RunStatus(Enum):
    """ETL run status enumeration"""
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    FAILED = "FAILED"
    ERROR = "ERROR"
    NO_DATA = "NO_DATA"
    VALIDATION_FAILED = "VALIDATION_FAILED"


@dataclass
class ETLResult:
    """ETL execution result"""
    run_id: str
    status: str
    start_time: datetime
    end_time: Optional[datetime]
    duration: Optional[float]
    records_extracted: int
    records_transformed: int
    records_loaded: int
    records_failed: int
    error_count: int
    warning_count: int
    errors: List[str]


class ETLOrchestrator:
    """
    Orchestrates ETL pipeline execution with comprehensive error handling
    and monitoring capabilities
    """
    
    def __init__(self, config_path: str = "config.yaml", run_type: str = "MANUAL"):
        """
        Initialize ETL Orchestrator
        
        Args:
            config_path: Path to configuration file
            run_type: Type of run (MANUAL, SCHEDULED, INCREMENTAL)
        """
        self.config = load_config(config_path)
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        self.logger = setup_logger("ETLOrchestrator", self.config)
        
        # Initialize Spark session
        self.spark = self._create_spark_session()
        
        # Initialize components
        self.extractor = Extractor(self.spark, self.config, self.run_id)
        self.transformer = Transformer(self.spark, self.config, self.run_id)
        self.loader = Loader(self.spark, self.config, self.run_id)
        self.quality_checker = DataQualityChecker(self.spark, self.config, self.run_id)
        self.monitor = ETLMonitor(self.spark, self.config)
        
        self.logger.info(f"ETL Orchestrator initialized - Run ID: {self.run_id}")
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        spark_config = self.config.get("spark", {})
        
        builder = SparkSession.builder.appName(
            spark_config.get("app_name", "ETL_Pipeline")
        )
        
        # Apply Spark configurations
        for key, value in spark_config.get("config", {}).items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel(
            spark_config.get("log_level", "WARN")
        )
        
        return spark
    
    def _generate_run_id(self) -> str:
        """Generate unique run identifier"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"RUN_{timestamp}_{unique_id}"
    
    def execute_etl(
        self,
        source_type: str = "database",
        target_type: str = "database",
        filter_clause: Optional[str] = None,
        batch_size: Optional[int] = None,
        max_records: int = 0,
        mode: str = "upsert"
    ) -> ETLResult:
        """
        Execute complete ETL pipeline
        
        Args:
            source_type: Type of data source (database, staging, incremental)
            target_type: Type of target (database, file, warehouse)
            filter_clause: Optional filter for extraction
            batch_size: Batch size for loading
            max_records: Maximum records to process (0 for unlimited)
            mode: Load mode (insert, update, upsert)
            
        Returns:
            ETLResult with execution statistics
        """
        start_time = datetime.now()
        result = ETLResult(
            run_id=self.run_id,
            status=RunStatus.RUNNING.value,
            start_time=start_time,
            end_time=None,
            duration=None,
            records_extracted=0,
            records_transformed=0,
            records_loaded=0,
            records_failed=0,
            error_count=0,
            warning_count=0,
            errors=[]
        )
        
        self.logger.info(f"Starting ETL execution - Run ID: {self.run_id}")
        self._log_run_start(start_time)
        
        try:
            # ===== PHASE 1: EXTRACT =====
            self.logger.info("Phase 1: Extraction started")
            df_source = self.extractor.extract_data(
                source_type=source_type,
                filter_clause=filter_clause,
                max_records=max_records
            )
            
            if df_source is None or df_source.count() == 0:
                self.logger.warning("No data extracted - stopping ETL pipeline")
                result.status = RunStatus.NO_DATA.value
                result.end_time = datetime.now()
                result.duration = (result.end_time - start_time).total_seconds()
                self._log_run_end(result)
                return result
            
            result.records_extracted = df_source.count()
            self.logger.info(f"Extracted {result.records_extracted} records")
            
            # ===== PHASE 2: TRANSFORM =====
            self.logger.info("Phase 2: Transformation started")
            df_transformed = self.transformer.transform_data(df_source)
            
            # Data quality checks
            if self.config.get("enable_quality_checks", True):
                self.logger.info("Running data quality checks")
                quality_results = self.quality_checker.perform_quality_checks(
                    df_transformed
                )
                
                # Check if critical validations passed
                critical_failures = [
                    check for check in quality_results 
                    if not check["passed"] and check.get("critical", False)
                ]
                
                if critical_failures:
                    error_msg = f"Critical quality checks failed: {len(critical_failures)}"
                    self.logger.error(error_msg)
                    result.status = RunStatus.VALIDATION_FAILED.value
                    result.error_count = len(critical_failures)
                    result.errors = [check["message"] for check in critical_failures]
                    result.end_time = datetime.now()
                    result.duration = (result.end_time - start_time).total_seconds()
                    self._log_run_end(result)
                    return result
            
            # Data validation
            validation_errors = self.transformer.validate_data(df_transformed)
            
            if validation_errors:
                self.logger.error(f"Validation failed with {len(validation_errors)} errors")
                result.status = RunStatus.VALIDATION_FAILED.value
                result.error_count = len(validation_errors)
                result.errors = validation_errors
                result.end_time = datetime.now()
                result.duration = (result.end_time - start_time).total_seconds()
                self._log_run_end(result)
                return result
            
            result.records_transformed = df_transformed.count()
            self.logger.info(f"Transformed {result.records_transformed} records")
            
            # ===== PHASE 3: LOAD =====
            self.logger.info("Phase 3: Loading started")
            
            # Use batch size from config if not provided
            if batch_size is None:
                batch_size = self.config.get("batch_size", 1000)
            
            load_result = self.loader.load_data(
                df_transformed,
                target_type=target_type,
                mode=mode,
                batch_size=batch_size
            )
            
            result.records_loaded = load_result["success_count"]
            result.records_failed = load_result["error_count"]
            result.error_count = load_result["error_count"]
            
            if load_result.get("errors"):
                result.errors.extend(load_result["errors"])
            
            # Determine final status
            if result.records_failed == 0:
                result.status = RunStatus.SUCCESS.value
            elif result.records_loaded > 0:
                result.status = RunStatus.PARTIAL_SUCCESS.value
            else:
                result.status = RunStatus.FAILED.value
            
            # Calculate duration
            result.end_time = datetime.now()
            result.duration = (result.end_time - start_time).total_seconds()
            
            # Log completion
            self._log_run_end(result)
            
            self.logger.info(
                f"ETL execution completed - Status: {result.status}, "
                f"Duration: {result.duration:.2f}s"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"ETL execution failed: {str(e)}", exc_info=True)
            result.status = RunStatus.ERROR.value
            result.error_count += 1
            result.errors.append(str(e))
            result.end_time = datetime.now()
            result.duration = (result.end_time - start_time).total_seconds()
            
            self._handle_orchestration_error(e, result)
            self._log_run_end(result)
            
            return result
    
    def execute_incremental(
        self,
        last_run_time: Optional[datetime] = None,
        **kwargs
    ) -> ETLResult:
        """
        Execute incremental ETL load
        
        Args:
            last_run_time: Timestamp of last successful run
            **kwargs: Additional parameters for execute_etl
            
        Returns:
            ETLResult with execution statistics
        """
        if last_run_time is None:
            # Get last successful run time from monitor
            last_run_time = self.monitor.get_last_successful_run_time()
        
        if last_run_time:
            self.logger.info(f"Running incremental load from {last_run_time}")
            return self.execute_etl(
                source_type="incremental",
                filter_clause=f"changed_at > '{last_run_time}'",
                **kwargs
            )
        else:
            self.logger.warning("No previous run found, executing full load")
            return self.execute_etl(source_type="database", **kwargs)
    
    def schedule_etl(self, schedule_id: str) -> bool:
        """
        Execute ETL based on schedule configuration
        
        Args:
            schedule_id: Schedule identifier
            
        Returns:
            True if schedule execution was successful
        """
        try:
            # Load schedule configuration
            schedules = self.config.get("schedules", {})
            schedule = schedules.get(schedule_id)
            
            if not schedule:
                self.logger.error(f"Schedule not found: {schedule_id}")
                return False
            
            if not schedule.get("is_active", False):
                self.logger.warning(f"Schedule is not active: {schedule_id}")
                return False
            
            self.logger.info(f"Executing scheduled job: {schedule_id}")
            
            # Execute ETL with schedule parameters
            result = self.execute_etl(
                source_type=schedule.get("source_type", "database"),
                target_type=schedule.get("target_type", "database"),
                filter_clause=schedule.get("filter_clause"),
                batch_size=schedule.get("batch_size"),
                mode=schedule.get("mode", "upsert")
            )
            
            # Send alerts if configured
            if schedule.get("alert_on_failure") and result.status in [
                RunStatus.FAILED.value,
                RunStatus.ERROR.value
            ]:
                self._send_alert(
                    alert_type="SCHEDULE_FAILURE",
                    message=f"Scheduled job {schedule_id} failed",
                    severity="HIGH",
                    details=asdict(result)
                )
            
            return result.status == RunStatus.SUCCESS.value
            
        except Exception as e:
            self.logger.error(f"Schedule execution failed: {str(e)}", exc_info=True)
            return False
    
    def get_run_statistics(self, run_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get execution statistics for runs
        
        Args:
            run_id: Optional specific run ID
            
        Returns:
            List of run statistics
        """
        return self.monitor.get_run_statistics(run_id)
    
    def _log_run_start(self, start_time: datetime):
        """Log run start to tracking table"""
        try:
            log_data = {
                "run_id": self.run_id,
                "run_type": self.run_type,
                "status": RunStatus.RUNNING.value,
                "start_time": start_time,
                "created_by": "system"
            }
            
            # Write to run log table/file
            self.monitor.log_run_start(log_data)
            
        except Exception as e:
            self.logger.warning(f"Failed to log run start: {str(e)}")
    
    def _log_run_end(self, result: ETLResult):
        """Log run completion to tracking table"""
        try:
            log_data = asdict(result)
            self.monitor.log_run_end(log_data)
            
        except Exception as e:
            self.logger.warning(f"Failed to log run end: {str(e)}")
    
    def _handle_orchestration_error(self, error: Exception, result: ETLResult):
        """Handle orchestration errors"""
        try:
            error_data = {
                "run_id": self.run_id,
                "error_type": type(error).__name__,
                "error_message": str(error),
                "component": "ORCHESTRATOR",
                "timestamp": datetime.now(),
                "severity": "HIGH"
            }
            
            self.monitor.log_error(error_data)
            
            # Send alert if configured
            if self.config.get("alert_on_error", True):
                self._send_alert(
                    alert_type="ORCHESTRATION_ERROR",
                    message=f"ETL orchestration failed: {str(error)}",
                    severity="CRITICAL",
                    details=error_data
                )
            
        except Exception as e:
            self.logger.error(f"Error handling failed: {str(e)}")
    
    def _send_alert(
        self,
        alert_type: str,
        message: str,
        severity: str,
        details: Optional[Dict[str, Any]] = None
    ):
        """Send alert notification"""
        try:
            alert_config = self.config.get("alerts", {})
            
            if not alert_config.get("enabled", False):
                return
            
            self.logger.warning(f"ALERT [{severity}] {alert_type}: {message}")
            
            # Implementation would integrate with actual alerting service
            # (Email, Slack, PagerDuty, etc.)
            
        except Exception as e:
            self.logger.error(f"Failed to send alert: {str(e)}")
    
    def cleanup(self):
        """Cleanup resources"""
        try:
            if self.spark:
                self.spark.stop()
            self.logger.info("Orchestrator cleanup completed")
        except Exception as e:
            self.logger.error(f"Cleanup failed: {str(e)}")