"""
ETL orchestration module.
Coordinates extraction, transformation, and loading processes.
"""

from datetime import datetime
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.logger import ETLLogger


class ETLOrchestrator:
    """Orchestrates the complete ETL workflow."""
    
    def __init__(self, spark: SparkSession, run_type: str = "MANUAL"):
        """
        Initialize the ETL orchestrator.
        
        Args:
            spark: SparkSession instance
            run_type: Type of run (MANUAL, SCHEDULED, etc.)
        """
        self.spark = spark
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        self.logger = ETLLogger.get_instance()
        
    @staticmethod
    def _generate_run_id() -> str:
        """Generate a unique run ID."""
        return f"RUN{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    def execute_etl(self, source_type: str = "DATABASE", target_type: str = "DATABASE",
                    filter_condition: Optional[str] = None, batch_size: int = 1000,
                    max_records: int = 0) -> Dict[str, Any]:
        """
        Execute the complete ETL process.
        
        Args:
            source_type: Source system type
            target_type: Target system type
            filter_condition: Optional filter condition
            batch_size: Batch size for loading
            max_records: Maximum records to process (0 = no limit)
            
        Returns:
            Dictionary with execution results
        """
        result = {
            "run_id": self.run_id,
            "status": "RUNNING",
            "start_time": datetime.now(),
            "end_time": None,
            "duration": 0,
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
            extractor = DataExtractor(self.spark, source_type, self.run_id)
            source_df = extractor.extract_data(filter_condition, max_records)
            
            result["records_extracted"] = source_df.count()
            
            if result["records_extracted"] == 0:
                self.logger.log_warning(
                    component="ORCHESTRATOR",
                    message="No data extracted - ETL process stopping"
                )
                result["status"] = "NO_DATA"
                result["end_time"] = datetime.now()
                return result
            
            # Step 2: Transform
            transformer = DataTransformer(self.spark, self.run_id)
            transformed_df = transformer.transform_data(source_df)
            
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
                result["end_time"] = datetime.now()
                return result
            
            result["records_transformed"] = transformed_df.count()
            
            # Step 3: Load
            loader = DataLoader(self.spark, target_type, batch_size, self.run_id)
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
            
            result["end_time"] = datetime.now()
            result["duration"] = (result["end_time"] - result["start_time"]).total_seconds()
            
            # Log run completion
            self._log_run_completion(result)
            
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"ETL execution completed - Status: {result['status']}"
            )
            
            return result
            
        except Exception as e:
            result["status"] = "ERROR"
            result["end_time"] = datetime.now()
            result["duration"] = (result["end_time"] - result["start_time"]).total_seconds()
            
            self.logger.log_error(
                component="ORCHESTRATOR",
                message="ETL execution failed",
                details=str(e)
            )
            
            self._log_run_completion(result)
            raise
    
    def _log_run_completion(self, result: Dict[str, Any]):
        """
        Log run completion to database.
        
        Args:
            result: Execution result dictionary
        """
        try:
            run_log_data = [(
                result["run_id"],
                result["start_time"],
                result["end_time"],
                result["duration"],
                result["status"],
                result["records_extracted"],
                result["records_transformed"],
                result["records_loaded"],
                result["records_failed"]
            )]
            
            columns = [
                "run_id", "start_time", "end_time", "duration", "status",
                "records_extracted", "records_transformed", "records_loaded", "records_failed"
            ]
            
            run_log_df = self.spark.createDataFrame(run_log_data, columns)
            
            run_log_df.write \
                .format("jdbc") \
                .option("url", self.spark.conf.get("spark.etl.source.url")) \
                .option("dbtable", "etl_run_log") \
                .option("user", self.spark.conf.get("spark.etl.source.user")) \
                .option("password", self.spark.conf.get("spark.etl.source.password")) \
                .mode("append") \
                .save()
                
        except Exception as e:
            self.logger.log_warning(
                component="ORCHESTRATOR",
                message=f"Failed to log run completion: {str(e)}"
            )