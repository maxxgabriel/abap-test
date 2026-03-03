"""
ETL Orchestration Module
Orchestrates the complete ETL pipeline with monitoring and error handling.
"""

from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict, Any
import logging

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.data_quality import DataQualityChecker


class ETLOrchestrator:
    """Orchestrates end-to-end ETL execution."""
    
    def __init__(self, spark: SparkSession, run_type: str = "MANUAL"):
        """
        Initialize the orchestrator.
        
        Args:
            spark: Active SparkSession
            run_type: Type of run (MANUAL, SCHEDULED)
        """
        self.spark = spark
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        self.logger = logging.getLogger(__name__)
        
    def _generate_run_id(self) -> str:
        """Generate unique run ID."""
        return f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def execute_etl(self, source_type: str = "DATABASE",
                    target_type: str = "DATABASE",
                    filter_condition: str = None,
                    batch_size: int = 1000,
                    max_records: int = 0) -> Dict[str, Any]:
        """
        Execute complete ETL pipeline.
        
        Args:
            source_type: Source type for extraction
            target_type: Target type for loading
            filter_condition: Optional filter for extraction
            batch_size: Batch size for loading
            max_records: Maximum records to process
            
        Returns:
            Dictionary with execution results
        """
        start_time = datetime.now()
        
        result = {
            "run_id": self.run_id,
            "status": "RUNNING",
            "start_time": start_time,
            "end_time": None,
            "duration": 0,
            "records_extracted": 0,
            "records_transformed": 0,
            "records_loaded": 0,
            "records_failed": 0,
            "error_count": 0,
            "warning_count": 0
        }
        
        self.logger.info(f"ETL execution started - Run ID: {self.run_id}")
        
        try:
            # Step 1: Extract
            self.logger.info("=" * 60)
            self.logger.info("STEP 1: EXTRACTION")
            self.logger.info("=" * 60)
            
            extractor = ETLExtractor(self.spark, source_type, self.run_id)
            df_source = extractor.extract_data(filter_condition, max_records)
            result["records_extracted"] = df_source.count()
            
            if result["records_extracted"] == 0:
                self.logger.warning("No data extracted - stopping ETL process")
                result["status"] = "NO_DATA"
                return result
            
            # Step 2: Transform
            self.logger.info("=" * 60)
            self.logger.info("STEP 2: TRANSFORMATION")
            self.logger.info("=" * 60)
            
            transformer = ETLTransformer(self.spark, self.run_id)
            df_transformed = transformer.transform_data(df_source)
            result["records_transformed"] = df_transformed.count()
            
            # Validate
            self.logger.info("Validating transformed data...")
            is_valid, validation_errors = transformer.validate_data(df_transformed)
            
            if not is_valid:
                self.logger.error(f"Validation failed: {len(validation_errors)} errors")
                for error in validation_errors:
                    self.logger.error(f"  - {error}")
                result["status"] = "VALIDATION_FAILED"
                result["error_count"] = len(validation_errors)
                return result
            
            # Data Quality Checks
            self.logger.info("=" * 60)
            self.logger.info("STEP 3: DATA QUALITY CHECKS")
            self.logger.info("=" * 60)
            
            quality_checker = DataQualityChecker(self.spark, self.run_id)
            quality_checks = quality_checker.perform_quality_checks(df_transformed)
            
            failed_checks = [check for check in quality_checks if not check["passed"]]
            result["warning_count"] = len(failed_checks)
            
            # Step 3: Load
            self.logger.info("=" * 60)
            self.logger.info("STEP 4: LOADING")
            self.logger.info("=" * 60)
            
            loader = ETLLoader(self.spark, target_type, batch_size, self.run_id)
            load_result = loader.load_data(df_transformed, mode="UPSERT")
            
            result["records_loaded"] = load_result["success_count"]
            result["records_failed"] = load_result["error_count"]
            result["error_count"] = load_result["error_count"]
            
            # Determine final status
            if load_result["error_count"] == 0:
                result["status"] = "SUCCESS"
            elif load_result["success_count"] > 0:
                result["status"] = "PARTIAL_SUCCESS"
            else:
                result["status"] = "FAILED"
            
        except Exception as e:
            self.logger.error(f"ETL execution failed: {str(e)}", exc_info=True)
            result["status"] = "ERROR"
            result["error_count"] += 1
        
        finally:
            end_time = datetime.now()
            result["end_time"] = end_time
            result["duration"] = int((end_time - start_time).total_seconds())
            
            self._log_run_summary(result)
        
        return result
    
    def _log_run_summary(self, result: Dict[str, Any]):
        """Log execution summary."""
        self.logger.info("=" * 60)
        self.logger.info("ETL EXECUTION SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"Run ID:             {result['run_id']}")
        self.logger.info(f"Status:             {result['status']}")
        self.logger.info(f"Duration:           {result['duration']} seconds")
        self.logger.info(f"Records Extracted:  {result['records_extracted']}")
        self.logger.info(f"Records Transformed:{result['records_transformed']}")
        self.logger.info(f"Records Loaded:     {result['records_loaded']}")
        self.logger.info(f"Records Failed:     {result['records_failed']}")
        self.logger.info(f"Errors:             {result['error_count']}")
        self.logger.info(f"Warnings:           {result['warning_count']}")
        self.logger.info("=" * 60)