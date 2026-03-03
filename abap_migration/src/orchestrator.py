"""
PySpark ETL Orchestrator
Converted from ABAP class zcl_etl_orchestrator
Coordinates the complete ETL pipeline
"""

from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict, Optional
import logging

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.utils.logger import ETLLogger
from src.utils.data_quality import DataQualityChecker


class ETLOrchestrator:
    """
    Orchestrates the complete ETL pipeline.
    Manages extract, transform, and load phases with error handling.
    """
    
    def __init__(self, spark: SparkSession, run_type: str = "MANUAL"):
        """
        Initialize the orchestrator.
        
        Args:
            spark: SparkSession instance
            run_type: Type of run (MANUAL, SCHEDULED, etc.)
        """
        self.spark = spark
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        self.logger = ETLLogger.get_instance()
        
    def _generate_run_id(self) -> str:
        """Generate unique run ID."""
        return f"ETL_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def execute_etl(
        self,
        source_type: str = "DATABASE",
        target_type: str = "DATABASE",
        filter_expr: Optional[str] = None,
        batch_size: int = 1000,
        max_records: int = 0
    ) -> Dict:
        """
        Execute complete ETL pipeline.
        
        Args:
            source_type: Type of data source
            target_type: Type of data target
            filter_expr: Optional filter expression
            batch_size: Batch size for loading
            max_records: Maximum records to process
            
        Returns:
            Dictionary with execution statistics
        """
        result = {
            "run_id": self.run_id,
            "status": "RUNNING",
            "start_time": datetime.now(),
            "records_extracted": 0,
            "records_transformed": 0,
            "records_loaded": 0,
            "records_failed": 0,
            "error_count": 0,
            "warning_count": 0
        }
        
        self.logger.log_info(
            component="ORCHESTRATOR",
            message=f"ETL execution started - Run ID: {self.run_id}"
        )
        
        try:
            # Step 1: Extract
            self.logger.log_info(component="ORCHESTRATOR", message="Starting extraction phase")
            extractor = DataExtractor(self.spark, source_type, self.run_id)
            source_df = extractor.extract_data(filter_expr, max_records)
            result["records_extracted"] = source_df.count()
            
            if result["records_extracted"] == 0:
                self.logger.log_warning(
                    component="ORCHESTRATOR",
                    message="No data extracted - ETL process stopping"
                )
                result["status"] = "NO_DATA"
                return result
            
            # Step 2: Transform
            self.logger.log_info(component="ORCHESTRATOR", message="Starting transformation phase")
            transformer = DataTransformer(self.spark, self.run_id)
            transformed_df = transformer.transform_data(source_df)
            result["records_transformed"] = transformed_df.count()
            
            # Validate
            is_valid, validation_errors = transformer.validate_data(transformed_df)
            if not is_valid:
                self.logger.log_error(
                    component="ORCHESTRATOR",
                    message=f"Validation failed - {len(validation_errors)} errors",
                    details="; ".join(validation_errors)
                )
                result["status"] = "VALIDATION_FAILED"
                result["error_count"] = len(validation_errors)
                return result
            
            # Data Quality Checks
            quality_checker = DataQualityChecker(self.spark, self.run_id)
            quality_checks = quality_checker.perform_quality_checks(transformed_df)
            failed_checks = [c for c in quality_checks if not c["passed"]]
            if failed_checks:
                self.logger.log_warning(
                    component="ORCHESTRATOR",
                    message=f"{len(failed_checks)} quality checks failed"
                )
                result["warning_count"] = len(failed_checks)
            
            # Step 3: Load
            self.logger.log_info(component="ORCHESTRATOR", message="Starting load phase")
            loader = DataLoader(self.spark, target_type, batch_size, self.run_id)
            load_result = loader.load_data(transformed_df, mode="UPSERT")
            
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
            
            result["end_time"] = datetime.now()
            result["duration"] = (result["end_time"] - result["start_time"]).seconds
            
            # Log completion
            self._log_run_completion(result)
            
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
            result["end_time"] = datetime.now()
            result["error_count"] += 1
            
        return result
    
    def _log_run_completion(self, result: Dict):
        """
        Log run completion to run log table.
        
        Args:
            result: Execution result dictionary
        """
        try:
            run_log_data = self.spark.createDataFrame([{
                "run_id": result["run_id"],
                "run_type": self.run_type,
                "status": result["status"],
                "start_time": result["start_time"],
                "end_time": result.get("end_time"),
                "duration": result.get("duration", 0),
                "records_extracted": result["records_extracted"],
                "records_transformed": result["records_transformed"],
                "records_loaded": result["records_loaded"],
                "records_failed": result["records_failed"],
                "error_count": result["error_count"]
            }])
            
            run_log_data.write.format("delta") \
                .mode("append") \
                .save("path/to/run_log")
                
        except Exception as e:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message="Failed to log run completion",
                details=str(e)
            )