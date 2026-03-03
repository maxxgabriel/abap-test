"""
ETL orchestration module.
Coordinates extraction, transformation, and loading with monitoring.
"""

from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from typing import Dict, Optional
import uuid

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.utils.logger import ETLLogger
from src.utils.config import Config
from src.utils.monitoring import ETLMonitor
from src.utils.data_quality import DataQualityChecker


class ETLOrchestrator:
    """Orchestrates the complete ETL pipeline."""
    
    def __init__(self, spark: SparkSession, config: Config, run_type: str = "MANUAL"):
        self.spark = spark
        self.config = config
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        self.logger = ETLLogger.get_instance()
        self.monitor = ETLMonitor.get_instance()
    
    def _generate_run_id(self) -> str:
        """Generate unique run ID."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"RUN_{timestamp}_{unique_id}"
    
    def execute_etl(
        self,
        source_type: str = "database",
        target_type: str = "database",
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> Dict:
        """
        Execute complete ETL pipeline.
        
        Returns:
            Dictionary with execution results and statistics
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
        
        start_time = datetime.now()
        result["start_time"] = start_time
        
        self.logger.log_info(
            component="ORCHESTRATOR",
            message=f"ETL execution started - Run ID: {self.run_id}, Type: {self.run_type}"
        )
        
        # Log run start
        self.monitor.log_run_start(self.run_id, self.run_type)
        
        try:
            # Step 1: Extract
            self.logger.log_info(component="ORCHESTRATOR", message="Phase 1/4: Extraction")
            extractor = DataExtractor(self.spark, self.config, self.run_id)
            source_df = extractor.extract_data(
                source_type=source_type,
                filter_condition=filter_condition,
                max_records=max_records
            )
            
            result["records_extracted"] = source_df.count()
            
            if result["records_extracted"] == 0:
                self.logger.log_warning(
                    component="ORCHESTRATOR",
                    message="No data extracted - stopping ETL process"
                )
                result["status"] = "NO_DATA"
                return result
            
            # Step 2: Transform
            self.logger.log_info(component="ORCHESTRATOR", message="Phase 2/4: Transformation")
            transformer = DataTransformer(self.spark, self.config, self.run_id)
            transformed_df = transformer.transform_data(source_df)
            
            result["records_transformed"] = transformed_df.count()
            
            # Step 3: Validate
            self.logger.log_info(component="ORCHESTRATOR", message="Phase 3/4: Validation")
            is_valid, validation_errors = transformer.validate_data(transformed_df)
            
            if not is_valid:
                self.logger.log_error(
                    component="ORCHESTRATOR",
                    message=f"Validation failed - {len(validation_errors)} errors"
                )
                result["status"] = "VALIDATION_FAILED"
                result["error_count"] = len(validation_errors)
                return result
            
            # Step 3.5: Data Quality Checks
            if self.config.get("quality.enable_checks", True):
                quality_checker = DataQualityChecker(self.spark, self.config, self.run_id)
                quality_results = quality_checker.perform_quality_checks(transformed_df)
                
                failed_checks = [check for check in quality_results if not check["passed"]]
                if failed_checks:
                    self.logger.log_warning(
                        component="ORCHESTRATOR",
                        message=f"Quality checks: {len(failed_checks)} failed"
                    )
                    result["warning_count"] = len(failed_checks)
            
            # Step 4: Load
            self.logger.log_info(component="ORCHESTRATOR", message="Phase 4/4: Loading")
            loader = DataLoader(self.spark, self.config, self.run_id)
            load_result = loader.load_data(
                df=transformed_df,
                target_type=target_type,
                mode="overwrite"
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
                message="ETL execution failed",
                details=str(e)
            )
            result["status"] = "ERROR"
            result["error_count"] += 1
        
        finally:
            # Calculate duration and log completion
            end_time = datetime.now()
            result["end_time"] = end_time
            result["duration"] = int((end_time - start_time).total_seconds())
            
            # Log run completion
            self.monitor.log_run_end(
                run_id=self.run_id,
                status=result["status"],
                records_extracted=result["records_extracted"],
                records_transformed=result["records_transformed"],
                records_loaded=result["records_loaded"],
                records_failed=result["records_failed"],
                duration=result["duration"]
            )
            
            # Send alerts if configured
            if result["status"] in ["FAILED", "ERROR"]:
                self.monitor.send_alert(
                    alert_type="ETL_FAILURE",
                    message=f"ETL run {self.run_id} failed",
                    severity="HIGH"
                )
            elif result["status"] == "PARTIAL_SUCCESS":
                self.monitor.send_alert(
                    alert_type="ETL_WARNING",
                    message=f"ETL run {self.run_id} completed with errors",
                    severity="MEDIUM"
                )
        
        return result