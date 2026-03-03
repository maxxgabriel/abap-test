"""
ETL orchestration module.
Coordinates extraction, transformation, and loading processes.
"""
from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict, Optional
import logging
import uuid
from src.extract import Extractor
from src.transform import Transformer
from src.load import Loader
from src.quality import DataQuality


class ETLOrchestrator:
    """Orchestrates the complete ETL process."""
    
    def __init__(self, spark: SparkSession, config: dict, run_type: str = "manual"):
        """
        Initialize the orchestrator.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_type: Type of run (manual, scheduled, test)
        """
        self.spark = spark
        self.config = config
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.extractor = Extractor(spark, config, self.run_id)
        self.transformer = Transformer(spark, config, self.run_id)
        self.loader = Loader(spark, config, self.run_id)
        self.quality_checker = DataQuality(spark, config, self.run_id)
    
    def _generate_run_id(self) -> str:
        """Generate a unique run ID."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"RUN_{timestamp}_{unique_id}"
    
    def execute_etl(
        self,
        source_type: str = "database",
        target_type: str = "database",
        filter_condition: Optional[str] = None,
        max_records: int = 0,
        mode: str = "upsert"
    ) -> Dict:
        """
        Execute the complete ETL process.
        
        Args:
            source_type: Type of source (database, staging, incremental)
            target_type: Type of target (database, file)
            filter_condition: Optional filter condition
            max_records: Maximum records to process (0 = unlimited)
            mode: Load mode (insert, update, upsert, overwrite)
            
        Returns:
            Dictionary with execution results
        """
        start_time = datetime.now()
        
        result = {
            "run_id": self.run_id,
            "status": "RUNNING",
            "start_time": start_time,
            "end_time": None,
            "duration": None,
            "records_extracted": 0,
            "records_transformed": 0,
            "records_loaded": 0,
            "records_failed": 0,
            "error_count": 0,
            "warning_count": 0,
            "quality_checks": []
        }
        
        self.logger.info(f"ETL execution started - Run ID: {self.run_id}")
        
        try:
            # Step 1: Extract
            self.logger.info("Step 1/4: Extracting data")
            source_df = self.extractor.extract_data(
                source_type=source_type,
                filter_condition=filter_condition,
                max_records=max_records
            )
            
            result["records_extracted"] = source_df.count()
            
            if result["records_extracted"] == 0:
                self.logger.warning("No data extracted - ETL process stopping")
                result["status"] = "NO_DATA"
                return result
            
            # Step 2: Transform
            self.logger.info("Step 2/4: Transforming data")
            transformed_df = self.transformer.transform_data(source_df)
            result["records_transformed"] = transformed_df.count()
            
            # Step 3: Validate
            self.logger.info("Step 3/4: Validating data")
            is_valid, validation_errors = self.transformer.validate_data(transformed_df)
            
            if not is_valid:
                self.logger.error(f"Validation failed with {len(validation_errors)} errors")
                result["status"] = "VALIDATION_FAILED"
                result["error_count"] = len(validation_errors)
                return result
            
            # Quality checks
            if self.config.get("quality", {}).get("enable_checks", True):
                quality_checks = self.quality_checker.perform_quality_checks(transformed_df)
                result["quality_checks"] = quality_checks
                
                failed_checks = [c for c in quality_checks if not c["passed"]]
                if failed_checks:
                    result["warning_count"] += len(failed_checks)
                    self.logger.warning(f"{len(failed_checks)} quality checks failed")
            
            # Step 4: Load
            self.logger.info("Step 4/4: Loading data")
            load_result = self.loader.load_data(transformed_df, mode=mode)
            
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
            
            # Calculate duration
            end_time = datetime.now()
            result["end_time"] = end_time
            result["duration"] = (end_time - start_time).total_seconds()
            
            # Log to run history
            self._log_run_result(result)
            
            self.logger.info(f"ETL execution completed - Status: {result['status']}")
            
        except Exception as e:
            self.logger.error(f"ETL execution failed: {str(e)}")
            result["status"] = "ERROR"
            result["error_count"] += 1
            end_time = datetime.now()
            result["end_time"] = end_time
            result["duration"] = (end_time - start_time).total_seconds()
            
            self._log_run_result(result)
            raise
        
        return result
    
    def _log_run_result(self, result: Dict):
        """Log the run result to history."""
        run_log_path = self.config.get("run_log", {}).get("path", "data/run_log")
        
        try:
            log_df = self.spark.createDataFrame([result])
            
            (log_df.write
             .format("parquet")
             .mode("append")
             .save(run_log_path))
            
        except Exception as e:
            self.logger.warning(f"Failed to log run result: {str(e)}")
    
    def get_run_statistics(self, run_id: Optional[str] = None) -> Dict:
        """
        Get statistics for a specific run or all runs.
        
        Args:
            run_id: Optional run ID to filter by
            
        Returns:
            Dictionary with run statistics
        """
        run_log_path = self.config.get("run_log", {}).get("path", "data/run_log")
        
        try:
            log_df = self.spark.read.parquet(run_log_path)
            
            if run_id:
                log_df = log_df.filter(log_df.run_id == run_id)
            
            return {
                "total_runs": log_df.count(),
                "successful_runs": log_df.filter(log_df.status == "SUCCESS").count(),
                "failed_runs": log_df.filter(log_df.status.isin(["FAILED", "ERROR"])).count(),
                "avg_duration": log_df.agg({"duration": "avg"}).collect()[0][0]
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get run statistics: {str(e)}")
            return {}