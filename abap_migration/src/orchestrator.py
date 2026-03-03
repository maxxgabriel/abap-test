"""
ETL Orchestrator Module
Main orchestration logic for ETL pipeline
"""
from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict, Optional
import logging
import time

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.data_quality import DataQualityChecker


class ETLOrchestrator:
    """Main orchestrator for ETL pipeline"""
    
    def __init__(self, spark: SparkSession, config: dict, run_type: str = "MANUAL"):
        """
        Initialize orchestrator
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_type: Type of run (MANUAL, SCHEDULED)
        """
        self.spark = spark
        self.config = config
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        self.logger = logging.getLogger(__name__)
    
    def execute_etl(
        self,
        source_type: str = "DATABASE",
        target_type: str = "DATABASE",
        filter_condition: Optional[str] = None,
        batch_size: int = 1000,
        max_records: int = 0
    ) -> Dict:
        """
        Execute complete ETL pipeline
        
        Args:
            source_type: Type of source
            target_type: Type of target
            filter_condition: Optional filter
            batch_size: Batch size for loading
            max_records: Maximum records to process
            
        Returns:
            Dictionary with execution results
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
        
        self.logger.info(f"ETL execution started - Run ID: {self.run_id}")
        start_time = time.time()
        result["start_time"] = datetime.now().isoformat()
        
        try:
            # Step 1: Extract
            self.logger.info("=" * 60)
            self.logger.info("STEP 1: EXTRACTION")
            self.logger.info("=" * 60)
            
            extractor = ETLExtractor(
                spark=self.spark,
                source_type=source_type,
                run_id=self.run_id,
                config=self.config
            )
            
            source_df = extractor.extract_data(filter_condition, max_records)
            result["records_extracted"] = source_df.count()
            
            if result["records_extracted"] == 0:
                self.logger.warning("No data extracted - ETL process stopping")
                result["status"] = "NO_DATA"
                return result
            
            # Step 2: Transform
            self.logger.info("=" * 60)
            self.logger.info("STEP 2: TRANSFORMATION")
            self.logger.info("=" * 60)
            
            transformer = ETLTransformer(
                spark=self.spark,
                run_id=self.run_id,
                config=self.config
            )
            
            transformed_df = transformer.transform_data(source_df)
            result["records_transformed"] = transformed_df.count()
            
            # Validate
            is_valid, validation_errors = transformer.validate_data(transformed_df)
            
            if not is_valid:
                self.logger.error(f"Validation failed - {len(validation_errors)} errors")
                result["status"] = "VALIDATION_FAILED"
                result["error_count"] = len(validation_errors)
                return result
            
            # Step 3: Data Quality Checks
            if self.config.get("enable_quality_checks", True):
                self.logger.info("=" * 60)
                self.logger.info("STEP 3: DATA QUALITY CHECKS")
                self.logger.info("=" * 60)
                
                quality_checker = DataQualityChecker(
                    spark=self.spark,
                    run_id=self.run_id,
                    config=self.config
                )
                
                quality_checks = quality_checker.perform_quality_checks(transformed_df)
                failed_checks = [check for check in quality_checks if not check["passed"]]
                
                if failed_checks and self.config.get("stop_on_quality_failure", False):
                    self.logger.error(f"{len(failed_checks)} quality checks failed")
                    result["status"] = "QUALITY_CHECK_FAILED"
                    result["error_count"] = len(failed_checks)
                    return result
                
                result["warning_count"] = len(failed_checks)
            
            # Step 4: Load
            self.logger.info("=" * 60)
            self.logger.info("STEP 4: LOADING")
            self.logger.info("=" * 60)
            
            loader = ETLLoader(
                spark=self.spark,
                target_type=target_type,
                run_id=self.run_id,
                batch_size=batch_size,
                config=self.config
            )
            
            load_result = loader.load_data(transformed_df, mode="UPSERT")
            
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
            self.logger.error(f"ETL execution error: {str(e)}", exc_info=True)
            result["status"] = "ERROR"
            result["error_count"] += 1
        
        finally:
            end_time = time.time()
            result["end_time"] = datetime.now().isoformat()
            result["duration"] = int(end_time - start_time)
            
            # Log run statistics
            self._log_run_statistics(result)
            
            self.logger.info("=" * 60)
            self.logger.info(f"ETL execution completed - Status: {result['status']}")
            self.logger.info("=" * 60)
        
        return result
    
    def _generate_run_id(self) -> str:
        """Generate unique run ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"RUN_{timestamp}"
    
    def _log_run_statistics(self, result: Dict):
        """Log run statistics to persistent storage"""
        try:
            run_log_path = self.config.get("run_log_path", "data/run_logs")
            
            run_log_df = self.spark.createDataFrame([result])
            
            run_log_df.write \
                .mode("append") \
                .partitionBy("status") \
                .parquet(run_log_path)
            
            self.logger.info(f"Run statistics logged to {run_log_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to log run statistics: {str(e)}")