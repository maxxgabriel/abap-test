"""
ETL Orchestrator for workflow coordination and execution management.

This module provides the main orchestration logic for ETL pipelines,
including run ID generation, status tracking, and pipeline execution.
"""

from datetime import datetime
from typing import Dict, List, Optional, Tuple
import uuid
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.logger import ETLLogger
from src.monitor import ETLMonitor


class ETLOrchestrator:
    """
    Orchestrates ETL workflow execution with run tracking and status management.
    
    Coordinates extraction, transformation, and loading phases while maintaining
    comprehensive execution logs and status information.
    """
    
    def __init__(self, spark: SparkSession, run_type: str = "MANUAL"):
        """
        Initialize ETL orchestrator.
        
        Args:
            spark: Active SparkSession
            run_type: Type of run (MANUAL, SCHEDULED, INCREMENTAL)
        """
        self.spark = spark
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        self.logger = ETLLogger()
        self.monitor = ETLMonitor(spark)
        
        self.logger.log_info(
            component="ORCHESTRATOR",
            message=f"Orchestrator initialized - Run ID: {self.run_id}, Type: {run_type}"
        )
    
    def _generate_run_id(self) -> str:
        """
        Generate unique run identifier.
        
        Returns:
            Unique run ID string
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8].upper()
        return f"RUN_{timestamp}_{unique_id}"
    
    def _log_run_start(self) -> datetime:
        """
        Log ETL run start and return timestamp.
        
        Returns:
            Start timestamp
        """
        start_time = datetime.now()
        
        self.logger.log_info(
            component="ORCHESTRATOR",
            message=f"ETL run started - ID: {self.run_id}",
            details=f"Start time: {start_time}, Type: {self.run_type}"
        )
        
        return start_time
    
    def _log_run_end(
        self,
        start_time: datetime,
        status: str,
        records_extracted: int,
        records_transformed: int,
        records_loaded: int,
        records_failed: int
    ) -> None:
        """
        Log ETL run completion with statistics.
        
        Args:
            start_time: Run start timestamp
            status: Final run status
            records_extracted: Number of records extracted
            records_transformed: Number of records transformed
            records_loaded: Number of records loaded
            records_failed: Number of failed records
        """
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Create run log entry
        run_log_schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("run_type", StringType(), False),
            StructField("status", StringType(), False),
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), False),
            StructField("duration", IntegerType(), False),
            StructField("records_extracted", IntegerType(), True),
            StructField("records_transformed", IntegerType(), True),
            StructField("records_loaded", IntegerType(), True),
            StructField("records_failed", IntegerType(), True)
        ])
        
        run_log_data = [(
            self.run_id,
            self.run_type,
            status,
            start_time,
            end_time,
            int(duration),
            records_extracted,
            records_transformed,
            records_loaded,
            records_failed
        )]
        
        run_log_df = self.spark.createDataFrame(run_log_data, schema=run_log_schema)
        
        # Write to run log table
        run_log_df.write.mode("append").saveAsTable("etl_run_log")
        
        self.logger.log_info(
            component="ORCHESTRATOR",
            message=f"ETL run completed - Status: {status}",
            details=f"Duration: {duration}s, Extracted: {records_extracted}, "
                   f"Transformed: {records_transformed}, Loaded: {records_loaded}, "
                   f"Failed: {records_failed}"
        )
    
    def _handle_orchestration_error(self, error: Exception) -> None:
        """
        Handle orchestration errors with logging.
        
        Args:
            error: Exception that occurred
        """
        self.logger.log_error(
            component="ORCHESTRATOR",
            message="ETL orchestration failed",
            details=str(error)
        )
        
        # Log error to error table
        error_schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("error_type", StringType(), False),
            StructField("error_message", StringType(), False),
            StructField("component", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("resolved", StringType(), False)
        ])
        
        error_data = [(
            self.run_id,
            type(error).__name__,
            str(error),
            "ORCHESTRATOR",
            datetime.now(),
            "N"
        )]
        
        error_df = self.spark.createDataFrame(error_data, schema=error_schema)
        error_df.write.mode("append").saveAsTable("etl_error_log")
    
    def execute_etl(
        self,
        source_type: str = "DATABASE",
        target_type: str = "DATABASE",
        filter_condition: Optional[str] = None,
        batch_size: int = 1000,
        max_records: int = 0
    ) -> Dict[str, any]:
        """
        Execute complete ETL workflow.
        
        Args:
            source_type: Source data type (DATABASE, STAGING, INCREMENTAL)
            target_type: Target data type
            filter_condition: Optional filter for extraction
            batch_size: Batch size for loading
            max_records: Maximum records to process (0 = unlimited)
        
        Returns:
            Dictionary containing execution results and statistics
        """
        result = {
            "run_id": self.run_id,
            "status": "RUNNING",
            "start_time": None,
            "end_time": None,
            "duration": 0,
            "records_extracted": 0,
            "records_transformed": 0,
            "records_loaded": 0,
            "records_failed": 0,
            "error_count": 0,
            "warning_count": 0
        }
        
        start_time = self._log_run_start()
        result["start_time"] = start_time
        
        try:
            # Step 1: Extract
            self.logger.log_info(
                component="ORCHESTRATOR",
                message="Starting extraction phase"
            )
            
            extractor = ETLExtractor(
                spark=self.spark,
                source_type=source_type,
                run_id=self.run_id
            )
            
            source_df = extractor.extract_data(
                filter_condition=filter_condition,
                max_records=max_records
            )
            
            result["records_extracted"] = source_df.count()
            
            if result["records_extracted"] == 0:
                self.logger.log_warning(
                    component="ORCHESTRATOR",
                    message="No data extracted - ETL process stopping"
                )
                result["status"] = "NO_DATA"
                result["end_time"] = datetime.now()
                result["duration"] = (result["end_time"] - start_time).total_seconds()
                return result
            
            # Step 2: Transform
            self.logger.log_info(
                component="ORCHESTRATOR",
                message="Starting transformation phase"
            )
            
            transformer = ETLTransformer(spark=self.spark, run_id=self.run_id)
            transformed_df = transformer.transform_data(source_df)
            
            # Validate
            is_valid, validation_errors = transformer.validate_data(transformed_df)
            
            if not is_valid:
                self.logger.log_error(
                    component="ORCHESTRATOR",
                    message=f"Validation failed - {len(validation_errors)} errors",
                    details=str(validation_errors[:10])  # Log first 10 errors
                )
                result["status"] = "VALIDATION_FAILED"
                result["error_count"] = len(validation_errors)
                result["end_time"] = datetime.now()
                result["duration"] = (result["end_time"] - start_time).total_seconds()
                return result
            
            result["records_transformed"] = transformed_df.count()
            
            # Step 3: Load
            self.logger.log_info(
                component="ORCHESTRATOR",
                message="Starting load phase"
            )
            
            loader = ETLLoader(
                spark=self.spark,
                target_type=target_type,
                batch_size=batch_size,
                run_id=self.run_id
            )
            
            load_result = loader.load_data(
                data=transformed_df,
                mode="UPSERT"
            )
            
            result["records_loaded"] = load_result["success_count"]
            result["records_failed"] = load_result["error_count"]
            result["error_count"] = load_result["error_count"]
            
            # Determine final status
            if result["records_failed"] == 0:
                result["status"] = "SUCCESS"
            elif result["records_loaded"] > 0:
                result["status"] = "PARTIAL_SUCCESS"
            else:
                result["status"] = "FAILED"
            
            result["end_time"] = datetime.now()
            result["duration"] = (result["end_time"] - start_time).total_seconds()
            
            # Log completion
            self._log_run_end(
                start_time=start_time,
                status=result["status"],
                records_extracted=result["records_extracted"],
                records_transformed=result["records_transformed"],
                records_loaded=result["records_loaded"],
                records_failed=result["records_failed"]
            )
            
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"ETL execution completed - Status: {result['status']}"
            )
            
        except Exception as e:
            self._handle_orchestration_error(e)
            result["status"] = "ERROR"
            result["end_time"] = datetime.now()
            result["duration"] = (result["end_time"] - start_time).total_seconds()
            
            self._log_run_end(
                start_time=start_time,
                status="ERROR",
                records_extracted=result.get("records_extracted", 0),
                records_transformed=result.get("records_transformed", 0),
                records_loaded=result.get("records_loaded", 0),
                records_failed=result.get("records_failed", 0)
            )
            
            raise
        
        return result
    
    def schedule_etl(self, schedule_id: str) -> bool:
        """
        Execute ETL based on schedule configuration.
        
        Args:
            schedule_id: Schedule identifier
        
        Returns:
            True if execution successful, False otherwise
        """
        try:
            # Load schedule configuration
            schedule_df = self.spark.table("etl_schedule").filter(
                f"schedule_id = '{schedule_id}' AND is_active = 'Y'"
            )
            
            if schedule_df.count() == 0:
                self.logger.log_warning(
                    component="ORCHESTRATOR",
                    message=f"Schedule not found or inactive: {schedule_id}"
                )
                return False
            
            schedule = schedule_df.first()
            
            # Execute ETL with schedule parameters
            result = self.execute_etl(
                source_type=schedule["etl_type"],
                target_type="DATABASE",
                batch_size=1000
            )
            
            return result["status"] in ["SUCCESS", "PARTIAL_SUCCESS"]
            
        except Exception as e:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message=f"Scheduled ETL failed for {schedule_id}",
                details=str(e)
            )
            return False
    
    def get_run_statistics(self, run_id: Optional[str] = None) -> DataFrame:
        """
        Retrieve run statistics.
        
        Args:
            run_id: Optional specific run ID (default: current run)
        
        Returns:
            DataFrame with run statistics
        """
        target_run_id = run_id or self.run_id
        
        stats_df = self.spark.table("etl_run_log").filter(
            f"run_id = '{target_run_id}'"
        )
        
        return stats_df