"""
ETL Orchestration Framework
Coordinates ETL workflow execution, generates unique run IDs, and tracks pipeline status.
"""

from pyspark.sql import SparkSession
from datetime import datetime
import uuid
import logging
from typing import Dict, Optional, Tuple
from dataclasses import dataclass, asdict
import yaml

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.data_quality import DataQualityChecker
from src.logger import ETLLogger
from src.monitor import ETLMonitor


@dataclass
class ETLResult:
    """ETL execution result structure"""
    run_id: str
    status: str
    start_time: datetime
    end_time: Optional[datetime]
    duration: Optional[int]
    records_extracted: int
    records_transformed: int
    records_loaded: int
    records_failed: int
    error_count: int
    warning_count: int

    def to_dict(self) -> Dict:
        """Convert result to dictionary"""
        result = asdict(self)
        result['start_time'] = self.start_time.isoformat() if self.start_time else None
        result['end_time'] = self.end_time.isoformat() if self.end_time else None
        return result


class ETLOrchestrator:
    """
    Main orchestrator for ETL workflow execution.
    Coordinates extract, transform, and load operations with monitoring and logging.
    """

    def __init__(self, spark: SparkSession, config: Dict, run_type: str = "MANUAL"):
        """
        Initialize ETL Orchestrator
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_type: Type of run (MANUAL, SCHEDULED, etc.)
        """
        self.spark = spark
        self.config = config
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        
        # Initialize components
        self.logger = ETLLogger(self.run_id)
        self.monitor = ETLMonitor(spark, config)
        
        self.logger.log_info(
            component="ORCHESTRATOR",
            message=f"Orchestrator initialized - Run ID: {self.run_id}, Type: {run_type}"
        )

    def _generate_run_id(self) -> str:
        """
        Generate unique run ID with timestamp and UUID
        
        Returns:
            Unique run ID string
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8].upper()
        return f"RUN_{timestamp}_{unique_id}"

    def _log_run_start(self) -> datetime:
        """
        Log ETL run start and create run log entry
        
        Returns:
            Start timestamp
        """
        start_time = datetime.now()
        
        try:
            # Create run log entry in database
            run_log_data = [
                (self.run_id, self.run_type, "RUNNING", start_time, None, 0, 0, 0, 0)
            ]
            
            run_log_df = self.spark.createDataFrame(
                run_log_data,
                ["run_id", "run_type", "status", "start_time", "end_time", 
                 "duration", "records_extracted", "records_transformed", "records_loaded"]
            )
            
            run_log_df.write.mode("append").saveAsTable(
                self.config['tables']['run_log']
            )
            
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"Run started at {start_time.isoformat()}"
            )
            
        except Exception as e:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message="Failed to log run start",
                details=str(e)
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
    ):
        """
        Log ETL run completion and update run log
        
        Args:
            start_time: Run start timestamp
            status: Final run status
            records_extracted: Number of records extracted
            records_transformed: Number of records transformed
            records_loaded: Number of records loaded
            records_failed: Number of failed records
        """
        end_time = datetime.now()
        duration = int((end_time - start_time).total_seconds())
        
        try:
            # Update run log in database
            update_query = f"""
            UPDATE {self.config['tables']['run_log']}
            SET status = '{status}',
                end_time = timestamp'{end_time.isoformat()}',
                duration = {duration},
                records_extracted = {records_extracted},
                records_transformed = {records_transformed},
                records_loaded = {records_loaded}
            WHERE run_id = '{self.run_id}'
            """
            
            self.spark.sql(update_query)
            
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"Run completed - Status: {status}, Duration: {duration}s"
            )
            
        except Exception as e:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message="Failed to log run end",
                details=str(e)
            )

    def _handle_orchestration_error(self, error: Exception, start_time: datetime):
        """
        Handle orchestration errors
        
        Args:
            error: Exception that occurred
            start_time: Run start time
        """
        self.logger.log_error(
            component="ORCHESTRATOR",
            message="Orchestration error occurred",
            details=str(error)
        )
        
        # Log error to database
        try:
            error_data = [(
                self.run_id,
                "ORCHESTRATOR",
                "ORCHESTRATION_ERROR",
                str(error),
                datetime.now(),
                False
            )]
            
            error_df = self.spark.createDataFrame(
                error_data,
                ["run_id", "component", "error_type", "error_message", "created_at", "resolved"]
            )
            
            error_df.write.mode("append").saveAsTable(
                self.config['tables']['error_log']
            )
            
        except Exception as log_error:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message="Failed to log error to database",
                details=str(log_error)
            )
        
        # Update run status
        self._log_run_end(
            start_time=start_time,
            status="ERROR",
            records_extracted=0,
            records_transformed=0,
            records_loaded=0,
            records_failed=0
        )

    def execute_etl(
        self,
        source_type: str = "DATABASE",
        target_type: str = "DATABASE",
        filter_condition: Optional[str] = None,
        batch_size: int = 1000,
        max_records: int = 0,
        enable_validation: bool = True,
        enable_quality_checks: bool = True
    ) -> ETLResult:
        """
        Execute complete ETL workflow
        
        Args:
            source_type: Source data type (DATABASE, STAGING, INCREMENTAL)
            target_type: Target data type (DATABASE, PARQUET, etc.)
            filter_condition: Optional filter condition
            batch_size: Batch size for loading
            max_records: Maximum records to process (0 = unlimited)
            enable_validation: Enable data validation
            enable_quality_checks: Enable quality checks
            
        Returns:
            ETLResult object with execution statistics
        """
        self.logger.log_info(
            component="ORCHESTRATOR",
            message=f"Starting ETL execution - Source: {source_type}, Target: {target_type}"
        )
        
        # Initialize result
        result = ETLResult(
            run_id=self.run_id,
            status="RUNNING",
            start_time=datetime.now(),
            end_time=None,
            duration=None,
            records_extracted=0,
            records_transformed=0,
            records_loaded=0,
            records_failed=0,
            error_count=0,
            warning_count=0
        )
        
        start_time = self._log_run_start()
        
        try:
            # Step 1: Extract
            self.logger.log_info(
                component="ORCHESTRATOR",
                message="Phase 1: Starting data extraction"
            )
            
            extractor = ETLExtractor(self.spark, self.config, self.run_id)
            source_df = extractor.extract_data(
                source_type=source_type,
                filter_condition=filter_condition,
                max_records=max_records
            )
            
            if source_df is None or source_df.count() == 0:
                self.logger.log_warning(
                    component="ORCHESTRATOR",
                    message="No data extracted - ETL process stopping"
                )
                result.status = "NO_DATA"
                result.end_time = datetime.now()
                result.duration = int((result.end_time - start_time).total_seconds())
                return result
            
            result.records_extracted = source_df.count()
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"Extraction complete - {result.records_extracted} records"
            )
            
            # Step 2: Transform
            self.logger.log_info(
                component="ORCHESTRATOR",
                message="Phase 2: Starting data transformation"
            )
            
            transformer = ETLTransformer(self.spark, self.config, self.run_id)
            transformed_df = transformer.transform_data(source_df)
            
            result.records_transformed = transformed_df.count()
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"Transformation complete - {result.records_transformed} records"
            )
            
            # Step 3: Validate (if enabled)
            if enable_validation:
                self.logger.log_info(
                    component="ORCHESTRATOR",
                    message="Phase 3: Starting data validation"
                )
                
                is_valid, validation_errors = transformer.validate_data(transformed_df)
                
                if not is_valid:
                    result.error_count = len(validation_errors)
                    result.status = "VALIDATION_FAILED"
                    
                    self.logger.log_error(
                        component="ORCHESTRATOR",
                        message=f"Validation failed - {result.error_count} errors",
                        details="\n".join(validation_errors[:10])  # Log first 10 errors
                    )
                    
                    result.end_time = datetime.now()
                    result.duration = int((result.end_time - start_time).total_seconds())
                    return result
            
            # Step 4: Quality Checks (if enabled)
            if enable_quality_checks:
                self.logger.log_info(
                    component="ORCHESTRATOR",
                    message="Phase 4: Running quality checks"
                )
                
                quality_checker = DataQualityChecker(self.spark, self.config, self.run_id)
                quality_results = quality_checker.perform_quality_checks(transformed_df)
                
                failed_checks = [check for check in quality_results if not check['passed']]
                if failed_checks:
                    result.warning_count = len(failed_checks)
                    self.logger.log_warning(
                        component="ORCHESTRATOR",
                        message=f"Quality checks completed with {result.warning_count} warnings"
                    )
            
            # Step 5: Load
            self.logger.log_info(
                component="ORCHESTRATOR",
                message="Phase 5: Starting data load"
            )
            
            loader = ETLLoader(self.spark, self.config, self.run_id)
            load_result = loader.load_data(
                data=transformed_df,
                target_type=target_type,
                batch_size=batch_size,
                mode="upsert"
            )
            
            result.records_loaded = load_result['success_count']
            result.records_failed = load_result['error_count']
            result.error_count += load_result['error_count']
            
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"Load complete - {result.records_loaded} loaded, {result.records_failed} failed"
            )
            
            # Determine final status
            if result.error_count == 0:
                result.status = "SUCCESS"
            elif result.records_loaded > 0:
                result.status = "PARTIAL_SUCCESS"
            else:
                result.status = "FAILED"
            
            # Calculate duration
            result.end_time = datetime.now()
            result.duration = int((result.end_time - start_time).total_seconds())
            
            # Log completion
            self._log_run_end(
                start_time=start_time,
                status=result.status,
                records_extracted=result.records_extracted,
                records_transformed=result.records_transformed,
                records_loaded=result.records_loaded,
                records_failed=result.records_failed
            )
            
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"ETL execution completed - Status: {result.status}, Duration: {result.duration}s"
            )
            
        except Exception as e:
            self._handle_orchestration_error(e, start_time)
            result.status = "ERROR"
            result.end_time = datetime.now()
            result.duration = int((result.end_time - start_time).total_seconds())
            raise
        
        return result

    def schedule_etl(self, schedule_id: str) -> bool:
        """
        Execute ETL based on schedule configuration
        
        Args:
            schedule_id: Schedule identifier
            
        Returns:
            Success status
        """
        try:
            # Load schedule configuration
            schedule_df = self.spark.table(self.config['tables']['schedule'])
            schedule = schedule_df.filter(
                (schedule_df.schedule_id == schedule_id) & 
                (schedule_df.is_active == True)
            ).first()
            
            if not schedule:
                self.logger.log_error(
                    component="ORCHESTRATOR",
                    message=f"Schedule not found or inactive: {schedule_id}"
                )
                return False
            
            # Execute ETL with schedule configuration
            etl_type = schedule['etl_type']
            
            result = self.execute_etl(
                source_type=etl_type,
                target_type="DATABASE",
                batch_size=self.config['performance']['batch_size']
            )
            
            return result.status in ["SUCCESS", "PARTIAL_SUCCESS"]
            
        except Exception as e:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message=f"Scheduled ETL failed: {schedule_id}",
                details=str(e)
            )
            return False

    def get_run_statistics(self, run_id: Optional[str] = None) -> list:
        """
        Get run statistics from log
        
        Args:
            run_id: Optional specific run ID, None for all runs
            
        Returns:
            List of run statistics
        """
        try:
            run_log_df = self.spark.table(self.config['tables']['run_log'])
            
            if run_id:
                run_log_df = run_log_df.filter(run_log_df.run_id == run_id)
            
            return run_log_df.orderBy("start_time", ascending=False).collect()
            
        except Exception as e:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message="Failed to retrieve run statistics",
                details=str(e)
            )
            return []


def main():
    """Main execution function for orchestrator"""
    # Load configuration
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("ETL_Orchestrator") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Create orchestrator
        orchestrator = ETLOrchestrator(
            spark=spark,
            config=config,
            run_type="MANUAL"
        )
        
        # Execute ETL
        result = orchestrator.execute_etl(
            source_type=config['etl']['source_type'],
            target_type=config['etl']['target_type'],
            batch_size=config['performance']['batch_size'],
            enable_validation=config['data_quality']['enable_validation'],
            enable_quality_checks=config['data_quality']['enable_quality_checks']
        )
        
        # Print results
        print("\n" + "="*80)
        print("ETL EXECUTION SUMMARY")
        print("="*80)
        print(f"Run ID:           {result.run_id}")
        print(f"Status:           {result.status}")
        print(f"Duration:         {result.duration} seconds")
        print(f"Records Extracted: {result.records_extracted}")
        print(f"Records Transformed: {result.records_transformed}")
        print(f"Records Loaded:   {result.records_loaded}")
        print(f"Records Failed:   {result.records_failed}")
        print(f"Errors:           {result.error_count}")
        print(f"Warnings:         {result.warning_count}")
        print("="*80 + "\n")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()