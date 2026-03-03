"""
ETL Orchestration Framework
Coordinates workflow execution, generates unique run IDs, and tracks pipeline status.
"""

import uuid
from datetime import datetime
from typing import Dict, Optional, Any, List
from pyspark.sql import SparkSession, DataFrame
import logging

from src.extract import Extractor
from src.transform import Transformer
from src.load import Loader
from src.data_quality import DataQualityChecker
from src.monitor import ETLMonitor
from src.logger import ETLLogger
from src.config import ConfigManager


class ETLOrchestrator:
    """Main orchestrator for coordinating ETL workflow execution."""
    
    def __init__(self, spark: SparkSession, run_type: str = "MANUAL"):
        """
        Initialize ETL orchestrator.
        
        Args:
            spark: SparkSession instance
            run_type: Type of run (MANUAL, SCHEDULED, etc.)
        """
        self.spark = spark
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        self.logger = ETLLogger.get_instance()
        self.config = ConfigManager.get_instance()
        self.monitor = ETLMonitor.get_instance(spark)
        
        self.logger.log_info(
            component="ORCHESTRATOR",
            message=f"Orchestrator initialized - Run ID: {self.run_id}"
        )
    
    def _generate_run_id(self) -> str:
        """
        Generate unique run ID.
        
        Returns:
            Unique run identifier
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8].upper()
        return f"RUN_{timestamp}_{unique_id}"
    
    def execute_etl(
        self,
        source_type: str = "DATABASE",
        target_type: str = "DATABASE",
        filter_condition: Optional[str] = None,
        batch_size: int = 1000,
        max_records: int = 0,
        mode: str = "upsert"
    ) -> Dict[str, Any]:
        """
        Execute complete ETL workflow.
        
        Args:
            source_type: Type of source (DATABASE, STAGING, INCREMENTAL)
            target_type: Type of target (DATABASE, PARQUET, DELTA)
            filter_condition: Optional filter condition
            batch_size: Batch size for loading
            max_records: Maximum records to process (0 = unlimited)
            mode: Load mode (insert, update, upsert)
            
        Returns:
            Dictionary containing ETL execution results
        """
        result = {
            "run_id": self.run_id,
            "status": "RUNNING",
            "start_time": None,
            "end_time": None,
            "duration_seconds": 0,
            "records_extracted": 0,
            "records_transformed": 0,
            "records_loaded": 0,
            "records_failed": 0,
            "error_count": 0,
            "warning_count": 0,
            "quality_checks": {}
        }
        
        start_time = datetime.now()
        result["start_time"] = start_time.isoformat()
        
        self.logger.log_info(
            component="ORCHESTRATOR",
            message=f"ETL execution started - Run ID: {self.run_id}",
            details=f"Source: {source_type}, Target: {target_type}"
        )
        
        # Log run start to tracking table
        self._log_run_start(start_time)
        
        try:
            # Step 1: Extract
            self.logger.log_info(
                component="ORCHESTRATOR",
                message="Step 1: Extraction starting"
            )
            
            extractor = Extractor(
                spark=self.spark,
                source_type=source_type,
                run_id=self.run_id
            )
            
            source_df = extractor.extract_data(
                filter_condition=filter_condition,
                max_records=max_records
            )
            
            if source_df is None or source_df.count() == 0:
                self.logger.log_warning(
                    component="ORCHESTRATOR",
                    message="No data extracted - ETL process stopping"
                )
                result["status"] = "NO_DATA"
                result["end_time"] = datetime.now().isoformat()
                return result
            
            result["records_extracted"] = source_df.count()
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"Extracted {result['records_extracted']} records"
            )
            
            # Step 2: Transform
            self.logger.log_info(
                component="ORCHESTRATOR",
                message="Step 2: Transformation starting"
            )
            
            transformer = Transformer(spark=self.spark, run_id=self.run_id)
            transformed_df = transformer.transform_data(source_df)
            
            result["records_transformed"] = transformed_df.count()
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"Transformed {result['records_transformed']} records"
            )
            
            # Step 3: Validate
            self.logger.log_info(
                component="ORCHESTRATOR",
                message="Step 3: Validation starting"
            )
            
            validation_result = transformer.validate_data(transformed_df)
            
            if not validation_result["is_valid"]:
                self.logger.log_error(
                    component="ORCHESTRATOR",
                    message=f"Validation failed - {len(validation_result['errors'])} errors",
                    details="; ".join(validation_result["errors"][:5])
                )
                result["status"] = "VALIDATION_FAILED"
                result["error_count"] = len(validation_result["errors"])
                result["end_time"] = datetime.now().isoformat()
                return result
            
            # Step 4: Data Quality Checks
            self.logger.log_info(
                component="ORCHESTRATOR",
                message="Step 4: Data quality checks starting"
            )
            
            quality_checker = DataQualityChecker(
                spark=self.spark,
                run_id=self.run_id
            )
            
            quality_checks = quality_checker.perform_quality_checks(transformed_df)
            result["quality_checks"] = quality_checks
            
            failed_checks = [
                check["check_name"] 
                for check in quality_checks 
                if not check["passed"]
            ]
            
            if failed_checks:
                self.logger.log_warning(
                    component="ORCHESTRATOR",
                    message=f"Quality checks failed: {', '.join(failed_checks)}"
                )
                result["warning_count"] = len(failed_checks)
            
            # Step 5: Load
            self.logger.log_info(
                component="ORCHESTRATOR",
                message="Step 5: Loading starting"
            )
            
            loader = Loader(
                spark=self.spark,
                target_type=target_type,
                batch_size=batch_size,
                run_id=self.run_id
            )
            
            load_result = loader.load_data(
                data=transformed_df,
                mode=mode
            )
            
            result["records_loaded"] = load_result["success_count"]
            result["records_failed"] = load_result["error_count"]
            result["error_count"] += load_result["error_count"]
            
            # Determine final status
            if load_result["error_count"] == 0:
                result["status"] = "SUCCESS"
            elif load_result["success_count"] > 0:
                result["status"] = "PARTIAL_SUCCESS"
            else:
                result["status"] = "FAILED"
            
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"ETL execution completed - Status: {result['status']}"
            )
            
        except Exception as e:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message="ETL execution failed with error",
                details=str(e)
            )
            result["status"] = "ERROR"
            result["error_count"] += 1
            raise
            
        finally:
            # Calculate duration and log run end
            end_time = datetime.now()
            result["end_time"] = end_time.isoformat()
            result["duration_seconds"] = (end_time - start_time).total_seconds()
            
            self._log_run_end(
                start_time=start_time,
                end_time=end_time,
                status=result["status"],
                records_extracted=result["records_extracted"],
                records_transformed=result["records_transformed"],
                records_loaded=result["records_loaded"],
                records_failed=result["records_failed"]
            )
        
        return result
    
    def schedule_etl(self, schedule_id: str) -> bool:
        """
        Execute ETL based on schedule configuration.
        
        Args:
            schedule_id: Schedule identifier
            
        Returns:
            Success status
        """
        try:
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"Executing scheduled ETL - Schedule ID: {schedule_id}"
            )
            
            # Load schedule configuration
            schedule_config = self.config.get_schedule_config(schedule_id)
            
            if not schedule_config or not schedule_config.get("is_active"):
                self.logger.log_warning(
                    component="ORCHESTRATOR",
                    message=f"Schedule {schedule_id} not found or inactive"
                )
                return False
            
            # Execute ETL with schedule parameters
            result = self.execute_etl(
                source_type=schedule_config.get("source_type", "DATABASE"),
                target_type=schedule_config.get("target_type", "DATABASE"),
                batch_size=schedule_config.get("batch_size", 1000),
                mode=schedule_config.get("mode", "upsert")
            )
            
            return result["status"] in ["SUCCESS", "PARTIAL_SUCCESS"]
            
        except Exception as e:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message=f"Scheduled ETL failed - Schedule ID: {schedule_id}",
                details=str(e)
            )
            return False
    
    def get_run_statistics(self, run_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get execution statistics for runs.
        
        Args:
            run_id: Optional specific run ID (if None, returns recent runs)
            
        Returns:
            List of run statistics
        """
        return self.monitor.get_run_history(
            run_id=run_id,
            limit=100 if run_id is None else None
        )
    
    def _log_run_start(self, start_time: datetime) -> None:
        """Log run start to tracking table."""
        try:
            run_log_df = self.spark.createDataFrame(
                [(
                    self.run_id,
                    self.run_type,
                    "RUNNING",
                    start_time,
                    None,
                    0,
                    0,
                    0,
                    0,
                    0
                )],
                schema="""
                    run_id STRING,
                    run_type STRING,
                    status STRING,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    duration_seconds INT,
                    records_extracted BIGINT,
                    records_loaded BIGINT,
                    records_failed BIGINT,
                    error_count INT
                """
            )
            
            run_log_df.write.mode("append").saveAsTable("etl_run_log")
            
        except Exception as e:
            self.logger.log_warning(
                component="ORCHESTRATOR",
                message="Failed to log run start",
                details=str(e)
            )
    
    def _log_run_end(
        self,
        start_time: datetime,
        end_time: datetime,
        status: str,
        records_extracted: int,
        records_transformed: int,
        records_loaded: int,
        records_failed: int
    ) -> None:
        """Log run completion to tracking table."""
        try:
            duration = int((end_time - start_time).total_seconds())
            
            # Update run log
            update_query = f"""
                UPDATE etl_run_log
                SET 
                    status = '{status}',
                    end_time = '{end_time.isoformat()}',
                    duration_seconds = {duration},
                    records_extracted = {records_extracted},
                    records_loaded = {records_loaded},
                    records_failed = {records_failed}
                WHERE run_id = '{self.run_id}'
            """
            
            # Note: For production, use proper DataFrame operations
            # This is simplified for demonstration
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"Run completed - Duration: {duration}s, Status: {status}"
            )
            
        except Exception as e:
            self.logger.log_warning(
                component="ORCHESTRATOR",
                message="Failed to log run end",
                details=str(e)
            )


def main():
    """Main entry point for orchestrator execution."""
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETL-Orchestrator") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Create orchestrator
        orchestrator = ETLOrchestrator(spark=spark, run_type="MANUAL")
        
        # Execute ETL
        result = orchestrator.execute_etl(
            source_type="DATABASE",
            target_type="DATABASE",
            batch_size=1000,
            mode="upsert"
        )
        
        # Print results
        print("\n" + "="*60)
        print("ETL EXECUTION SUMMARY")
        print("="*60)
        print(f"Run ID:              {result['run_id']}")
        print(f"Status:              {result['status']}")
        print(f"Duration:            {result['duration_seconds']:.2f} seconds")
        print(f"Records Extracted:   {result['records_extracted']}")
        print(f"Records Transformed: {result['records_transformed']}")
        print(f"Records Loaded:      {result['records_loaded']}")
        print(f"Records Failed:      {result['records_failed']}")
        print(f"Error Count:         {result['error_count']}")
        print(f"Warning Count:       {result['warning_count']}")
        print("="*60 + "\n")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()