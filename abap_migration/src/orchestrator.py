"""
ETL orchestration module.
Coordinates the entire ETL pipeline execution.
"""

from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict, Optional
import logging
import uuid

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.quality import DataQualityChecker


class ETLOrchestrator:
    """Orchestrates the complete ETL pipeline."""
    
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
        self.logger = logging.getLogger(__name__)
        
    def _generate_run_id(self) -> str:
        """Generate unique run ID."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"RUN_{timestamp}_{unique_id}"
    
    def execute_etl(
        self,
        source_type: str = "DATABASE",
        target_type: str = "DATABASE",
        filter_condition: Optional[str] = None,
        batch_size: int = 1000,
        max_records: int = 0,
        enable_quality_checks: bool = True
    ) -> Dict:
        """
        Execute complete ETL pipeline.
        
        Args:
            source_type: Source type for extraction
            target_type: Target type for loading
            filter_condition: Optional filter for extraction
            batch_size: Batch size for loading
            max_records: Maximum records to process
            enable_quality_checks: Whether to run quality checks
            
        Returns:
            Dictionary with execution results
        """
        start_time = datetime.now()
        
        self.logger.info(f"ETL execution started - Run ID: {self.run_id}")
        
        result = {
            "run_id": self.run_id,
            "status": "RUNNING",
            "start_time": start_time,
            "records_extracted": 0,
            "records_transformed": 0,
            "records_loaded": 0,
            "records_failed": 0,
            "error_count": 0,
            "warning_count": 0,
            "quality_checks": []
        }
        
        try:
            # Step 1: Extract
            self.logger.info("Step 1: Extracting data")
            extractor = ETLExtractor(self.spark, source_type, self.run_id)
            source_df = extractor.extract_data(filter_condition, max_records)
            
            result["records_extracted"] = source_df.count()
            
            if result["records_extracted"] == 0:
                self.logger.warning("No data extracted - stopping pipeline")
                result["status"] = "NO_DATA"
                result["end_time"] = datetime.now()
                return result
            
            # Step 2: Transform
            self.logger.info("Step 2: Transforming data")
            transformer = ETLTransformer(self.spark, self.run_id)
            transformed_df = transformer.transform_data(source_df)
            
            result["records_transformed"] = transformed_df.count()
            
            # Step 2.5: Validate
            self.logger.info("Step 2.5: Validating data")
            is_valid, validation_errors = transformer.validate_data(transformed_df)
            
            if not is_valid:
                self.logger.error(f"Validation failed: {len(validation_errors)} errors")
                result["status"] = "VALIDATION_FAILED"
                result["error_count"] = len(validation_errors)
                result["errors"] = validation_errors
                result["end_time"] = datetime.now()
                return result
            
            # Step 3: Quality Checks (if enabled)
            if enable_quality_checks:
                self.logger.info("Step 3: Running quality checks")
                quality_checker = DataQualityChecker(self.spark, self.run_id)
                quality_checks = quality_checker.perform_quality_checks(transformed_df)
                result["quality_checks"] = quality_checks
                
                failed_checks = [c for c in quality_checks if not c["passed"]]
                if failed_checks:
                    self.logger.warning(f"{len(failed_checks)} quality checks failed")
                    result["warning_count"] += len(failed_checks)
            
            # Step 4: Load
            self.logger.info("Step 4: Loading data")
            loader = ETLLoader(self.spark, target_type, batch_size, self.run_id)
            load_result = loader.load_data(transformed_df, mode="upsert")
            
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
            
            end_time = datetime.now()
            result["end_time"] = end_time
            result["duration_seconds"] = (end_time - start_time).total_seconds()
            
            # Log completion
            self._log_run_completion(result)
            
            self.logger.info(f"ETL execution completed - Status: {result['status']}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"ETL execution failed: {str(e)}")
            result["status"] = "ERROR"
            result["error_message"] = str(e)
            result["end_time"] = datetime.now()
            
            self._log_run_completion(result)
            
            raise
    
    def _log_run_completion(self, result: Dict):
        """
        Log run completion to database.
        
        Args:
            result: Execution result dictionary
        """
        try:
            log_data = [{
                "run_id": result["run_id"],
                "run_type": self.run_type,
                "status": result["status"],
                "start_time": result["start_time"],
                "end_time": result.get("end_time"),
                "duration": result.get("duration_seconds", 0),
                "records_extracted": result["records_extracted"],
                "records_transformed": result["records_transformed"],
                "records_loaded": result["records_loaded"],
                "records_failed": result["records_failed"],
                "error_count": result["error_count"]
            }]
            
            log_df = self.spark.createDataFrame(log_data)
            
            log_df.write \
                .format("jdbc") \
                .option("url", "${db.url}") \
                .option("dbtable", "${db.run_log_table}") \
                .option("user", "${db.user}") \
                .option("password", "${db.password}") \
                .mode("append") \
                .save()
                
        except Exception as e:
            self.logger.error(f"Failed to log run completion: {str(e)}")