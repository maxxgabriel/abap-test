"""
ETL Orchestration Framework
Coordinates ETL workflow execution with run tracking and status monitoring
"""

from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict, Optional, Tuple
import uuid
import logging
from dataclasses import dataclass, asdict

from src.extract import Extractor
from src.transform import Transformer
from src.load import Loader
from src.data_quality import DataQualityChecker
from src.logger import ETLLogger
from src.monitor import ETLMonitor
from src.config import Config


@dataclass
class ETLResult:
    """Container for ETL execution results"""
    run_id: str
    status: str
    start_time: datetime
    end_time: Optional[datetime]
    duration_seconds: int
    records_extracted: int
    records_transformed: int
    records_loaded: int
    records_failed: int
    error_count: int
    warning_count: int
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for logging"""
        result = asdict(self)
        result['start_time'] = self.start_time.isoformat()
        result['end_time'] = self.end_time.isoformat() if self.end_time else None
        return result


class ETLOrchestrator:
    """
    Main orchestrator for ETL pipeline execution
    Manages workflow coordination, run tracking, and status monitoring
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Config,
        run_type: str = "MANUAL"
    ):
        """
        Initialize ETL orchestrator
        
        Args:
            spark: Active SparkSession
            config: Configuration object
            run_type: Type of run (MANUAL, SCHEDULED, TEST)
        """
        self.spark = spark
        self.config = config
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        self.logger = ETLLogger(run_id=self.run_id)
        self.monitor = ETLMonitor(spark=spark, config=config)
        
        self.logger.log_info(
            component="ORCHESTRATOR",
            message=f"Orchestrator initialized - Run ID: {self.run_id}, Type: {run_type}"
        )
    
    def _generate_run_id(self) -> str:
        """
        Generate unique run identifier
        
        Returns:
            Unique run ID string
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"RUN_{timestamp}_{unique_id}"
    
    def execute_etl(
        self,
        source_type: str = "DATABASE",
        target_type: str = "DATABASE",
        filter_condition: Optional[str] = None,
        batch_size: int = 1000,
        max_records: int = 0,
        enable_validation: bool = True,
        enable_reconciliation: bool = True
    ) -> ETLResult:
        """
        Execute complete ETL pipeline
        
        Args:
            source_type: Source data type (DATABASE, STAGING, INCREMENTAL)
            target_type: Target data type (DATABASE, PARQUET, DELTA)
            filter_condition: Optional SQL filter condition
            batch_size: Batch size for loading
            max_records: Maximum records to process (0 = unlimited)
            enable_validation: Enable data quality validation
            enable_reconciliation: Enable post-load reconciliation
            
        Returns:
            ETLResult object with execution statistics
        """
        start_time = datetime.now()
        
        result = ETLResult(
            run_id=self.run_id,
            status="RUNNING",
            start_time=start_time,
            end_time=None,
            duration_seconds=0,
            records_extracted=0,
            records_transformed=0,
            records_loaded=0,
            records_failed=0,
            error_count=0,
            warning_count=0
        )
        
        self.logger.log_info(
            component="ORCHESTRATOR",
            message=f"ETL execution started - Source: {source_type}, Target: {target_type}"
        )
        
        # Log run start to tracking table
        self._log_run_start(start_time, source_type, target_type)
        
        try:
            # STEP 1: EXTRACT
            self.logger.log_info(
                component="ORCHESTRATOR",
                message="Stage 1/4: Data Extraction"
            )
            
            extractor = Extractor(
                spark=self.spark,
                config=self.config,
                run_id=self.run_id,
                source_type=source_type
            )
            
            source_df = extractor.extract_data(
                filter_condition=filter_condition,
                max_records=max_records
            )
            
            result.records_extracted = source_df.count()
            
            if result.records_extracted == 0:
                self.logger.log_warning(
                    component="ORCHESTRATOR",
                    message="No data extracted - ETL process stopping"
                )
                result.status = "NO_DATA"
                result.end_time = datetime.now()
                result.duration_seconds = int((result.end_time - start_time).total_seconds())
                self._log_run_end(result)
                return result
            
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"Extraction complete: {result.records_extracted} records"
            )
            
            # STEP 2: TRANSFORM
            self.logger.log_info(
                component="ORCHESTRATOR",
                message="Stage 2/4: Data Transformation"
            )
            
            transformer = Transformer(
                spark=self.spark,
                config=self.config,
                run_id=self.run_id
            )
            
            transformed_df = transformer.transform_data(source_df)
            result.records_transformed = transformed_df.count()
            
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"Transformation complete: {result.records_transformed} records"
            )
            
            # STEP 3: VALIDATION (if enabled)
            if enable_validation:
                self.logger.log_info(
                    component="ORCHESTRATOR",
                    message="Stage 3/4: Data Quality Validation"
                )
                
                quality_checker = DataQualityChecker(
                    spark=self.spark,
                    config=self.config,
                    run_id=self.run_id
                )
                
                validation_results = quality_checker.perform_quality_checks(transformed_df)
                
                failed_checks = [
                    check for check in validation_results 
                    if not check['passed']
                ]
                
                if failed_checks:
                    result.warning_count = len(failed_checks)
                    self.logger.log_warning(
                        component="ORCHESTRATOR",
                        message=f"Data quality issues detected: {len(failed_checks)} failed checks"
                    )
                    
                    # Check if we should abort on validation failure
                    if self.config.get("abort_on_validation_failure", False):
                        result.status = "VALIDATION_FAILED"
                        result.error_count = len(failed_checks)
                        result.end_time = datetime.now()
                        result.duration_seconds = int((result.end_time - start_time).total_seconds())
                        self._log_run_end(result)
                        return result
            
            # STEP 4: LOAD
            self.logger.log_info(
                component="ORCHESTRATOR",
                message="Stage 4/4: Data Loading"
            )
            
            loader = Loader(
                spark=self.spark,
                config=self.config,
                run_id=self.run_id,
                target_type=target_type,
                batch_size=batch_size
            )
            
            load_result = loader.load_data(
                transformed_df=transformed_df,
                mode="upsert"
            )
            
            result.records_loaded = load_result['success_count']
            result.records_failed = load_result['error_count']
            result.error_count += load_result['error_count']
            
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"Loading complete: {result.records_loaded} loaded, {result.records_failed} failed"
            )
            
            # STEP 5: RECONCILIATION (if enabled)
            if enable_reconciliation and result.records_loaded > 0:
                self.logger.log_info(
                    component="ORCHESTRATOR",
                    message="Stage 5/4: Data Reconciliation"
                )
                
                reconciled = loader.reconcile_data(
                    source_count=result.records_extracted,
                    loaded_count=result.records_loaded
                )
                
                if not reconciled:
                    result.warning_count += 1
                    self.logger.log_warning(
                        component="ORCHESTRATOR",
                        message="Data reconciliation detected discrepancies"
                    )
            
            # Determine final status
            if result.error_count == 0:
                result.status = "SUCCESS"
            elif result.records_loaded > 0:
                result.status = "PARTIAL_SUCCESS"
            else:
                result.status = "FAILED"
            
            result.end_time = datetime.now()
            result.duration_seconds = int((result.end_time - start_time).total_seconds())
            
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"ETL execution completed - Status: {result.status}, Duration: {result.duration_seconds}s"
            )
            
            # Log run completion
            self._log_run_end(result)
            
            # Update monitoring metrics
            self.monitor.record_run_metrics(result.to_dict())
            
            return result
            
        except Exception as e:
            result.status = "ERROR"
            result.end_time = datetime.now()
            result.duration_seconds = int((result.end_time - start_time).total_seconds())
            result.error_count += 1
            
            self.logger.log_error(
                component="ORCHESTRATOR",
                message=f"ETL execution failed: {str(e)}",
                details=str(e)
            )
            
            self._log_run_end(result)
            self._handle_orchestration_error(e)
            
            raise
    
    def schedule_etl(self, schedule_id: str) -> bool:
        """
        Execute ETL based on schedule configuration
        
        Args:
            schedule_id: Schedule identifier
            
        Returns:
            Success status
        """
        try:
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"Scheduled execution started - Schedule ID: {schedule_id}"
            )
            
            # Load schedule configuration
            schedule_config = self._load_schedule_config(schedule_id)
            
            if not schedule_config or not schedule_config.get('is_active'):
                self.logger.log_warning(
                    component="ORCHESTRATOR",
                    message=f"Schedule {schedule_id} is not active or not found"
                )
                return False
            
            # Execute ETL with schedule parameters
            result = self.execute_etl(
                source_type=schedule_config.get('source_type', 'DATABASE'),
                target_type=schedule_config.get('target_type', 'DATABASE'),
                filter_condition=schedule_config.get('filter_condition'),
                batch_size=schedule_config.get('batch_size', 1000),
                max_records=schedule_config.get('max_records', 0)
            )
            
            return result.status in ["SUCCESS", "PARTIAL_SUCCESS"]
            
        except Exception as e:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message=f"Scheduled execution failed: {str(e)}"
            )
            return False
    
    def get_run_statistics(self, run_id: Optional[str] = None) -> Dict:
        """
        Get statistics for ETL runs
        
        Args:
            run_id: Optional specific run ID, otherwise returns all recent runs
            
        Returns:
            Dictionary of run statistics
        """
        return self.monitor.get_run_statistics(run_id)
    
    def _log_run_start(
        self,
        start_time: datetime,
        source_type: str,
        target_type: str
    ) -> None:
        """
        Log ETL run start to tracking table
        
        Args:
            start_time: Run start timestamp
            source_type: Source type
            target_type: Target type
        """
        try:
            from pyspark.sql.types import StructType, StructField, StringType, TimestampType
            
            schema = StructType([
                StructField("run_id", StringType(), False),
                StructField("run_type", StringType(), True),
                StructField("status", StringType(), True),
                StructField("source_type", StringType(), True),
                StructField("target_type", StringType(), True),
                StructField("start_time", TimestampType(), True),
            ])
            
            data = [(
                self.run_id,
                self.run_type,
                "RUNNING",
                source_type,
                target_type,
                start_time
            )]
            
            df = self.spark.createDataFrame(data, schema)
            
            # Write to run log table
            df.write.format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(self.config.get("run_log_path", "/mnt/etl/run_log"))
            
        except Exception as e:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message=f"Failed to log run start: {str(e)}"
            )
    
    def _log_run_end(self, result: ETLResult) -> None:
        """
        Log ETL run completion to tracking table
        
        Args:
            result: ETL execution result
        """
        try:
            from pyspark.sql.types import (
                StructType, StructField, StringType, 
                TimestampType, IntegerType
            )
            
            schema = StructType([
                StructField("run_id", StringType(), False),
                StructField("status", StringType(), True),
                StructField("end_time", TimestampType(), True),
                StructField("duration_seconds", IntegerType(), True),
                StructField("records_extracted", IntegerType(), True),
                StructField("records_transformed", IntegerType(), True),
                StructField("records_loaded", IntegerType(), True),
                StructField("records_failed", IntegerType(), True),
                StructField("error_count", IntegerType(), True),
            ])
            
            data = [(
                result.run_id,
                result.status,
                result.end_time,
                result.duration_seconds,
                result.records_extracted,
                result.records_transformed,
                result.records_loaded,
                result.records_failed,
                result.error_count
            )]
            
            df = self.spark.createDataFrame(data, schema)
            
            # Update run log table
            df.write.format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(self.config.get("run_log_path", "/mnt/etl/run_log"))
            
        except Exception as e:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message=f"Failed to log run end: {str(e)}"
            )
    
    def _load_schedule_config(self, schedule_id: str) -> Optional[Dict]:
        """
        Load schedule configuration from storage
        
        Args:
            schedule_id: Schedule identifier
            
        Returns:
            Schedule configuration dictionary or None
        """
        try:
            schedule_df = self.spark.read.format("delta") \
                .load(self.config.get("schedule_path", "/mnt/etl/schedules")) \
                .filter(f"schedule_id = '{schedule_id}'")
            
            if schedule_df.count() == 0:
                return None
            
            return schedule_df.first().asDict()
            
        except Exception as e:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message=f"Failed to load schedule config: {str(e)}"
            )
            return None
    
    def _handle_orchestration_error(self, error: Exception) -> None:
        """
        Handle orchestration errors with proper logging and alerting
        
        Args:
            error: Exception that occurred
        """
        self.logger.log_error(
            component="ORCHESTRATOR",
            message=f"Orchestration error: {type(error).__name__}",
            details=str(error)
        )
        
        # Log to error table
        try:
            from pyspark.sql.types import StructType, StructField, StringType, TimestampType
            
            schema = StructType([
                StructField("run_id", StringType(), False),
                StructField("error_type", StringType(), True),
                StructField("error_message", StringType(), True),
                StructField("error_timestamp", TimestampType(), True),
                StructField("component", StringType(), True),
            ])
            
            data = [(
                self.run_id,
                type(error).__name__,
                str(error),
                datetime.now(),
                "ORCHESTRATOR"
            )]
            
            df = self.spark.createDataFrame(data, schema)
            
            df.write.format("delta") \
                .mode("append") \
                .save(self.config.get("error_log_path", "/mnt/etl/error_log"))
            
        except Exception as log_error:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message=f"Failed to log error: {str(log_error)}"
            )
        
        # Send alert if configured
        if self.config.get("enable_alerts", False):
            self.monitor.send_alert(
                alert_type="ORCHESTRATION_ERROR",
                message=f"ETL run {self.run_id} failed: {str(error)}",
                severity="CRITICAL"
            )