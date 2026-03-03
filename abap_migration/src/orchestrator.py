"""
ETL Orchestrator Module
Coordinates the complete ETL workflow
Migrated from zcl_etl_orchestrator.abap
"""

from pyspark.sql import SparkSession
from datetime import datetime
from typing import Optional
import logging

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader


class ETLResult:
    """Container for ETL execution results"""
    def __init__(self):
        self.run_id: str = ""
        self.status: str = "RUNNING"
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.duration: int = 0
        self.records_extracted: int = 0
        self.records_transformed: int = 0
        self.records_loaded: int = 0
        self.records_failed: int = 0
        self.error_count: int = 0
        self.warning_count: int = 0


class ETLOrchestrator:
    """Orchestrates the complete ETL workflow"""
    
    def __init__(self, spark: SparkSession, run_type: str = "MANUAL"):
        """
        Initialize orchestrator
        
        Args:
            spark: SparkSession instance
            run_type: Type of run (MANUAL, SCHEDULED, etc.)
        """
        self.spark = spark
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        self.logger = logging.getLogger(__name__)
    
    def execute_etl(self, source_type: str = "DATABASE", target_type: str = "DATABASE",
                    filter_value: Optional[str] = None, batch_size: int = 1000,
                    max_records: int = 0) -> ETLResult:
        """
        Execute complete ETL workflow
        
        Args:
            source_type: Source system type
            target_type: Target system type
            filter_value: Optional filter criteria
            batch_size: Batch size for loading
            max_records: Maximum records to process
            
        Returns:
            ETLResult with execution statistics
        """
        result = ETLResult()
        result.run_id = self.run_id
        result.start_time = datetime.now()
        
        self.logger.info(f"ETL execution started - Run ID: {self.run_id}")
        
        try:
            # Step 1: Extract
            extractor = ETLExtractor(self.spark, source_type, self.run_id)
            source_df = extractor.extract_data(filter_value, max_records)
            result.records_extracted = source_df.count()
            
            if result.records_extracted == 0:
                self.logger.warning("No data extracted - ETL process stopping")
                result.status = "NO_DATA"
                return result
            
            # Step 2: Transform
            transformer = ETLTransformer(self.spark, self.run_id)
            transformed_df = transformer.transform_data(source_df)
            
            # Validate
            is_valid, validation_errors = transformer.validate_data(transformed_df)
            if not is_valid:
                self.logger.error(f"Validation failed - {len(validation_errors)} errors")
                result.status = "VALIDATION_FAILED"
                result.error_count = len(validation_errors)
                return result
            
            result.records_transformed = transformed_df.count()
            
            # Step 3: Load
            loader = ETLLoader(self.spark, target_type, batch_size, self.run_id)
            load_result = loader.load_data(transformed_df, mode="UPSERT")
            
            result.records_loaded = load_result.success_count
            result.records_failed = load_result.error_count
            result.error_count = load_result.error_count
            
            # Determine final status
            if load_result.error_count == 0:
                result.status = "SUCCESS"
            elif load_result.success_count > 0:
                result.status = "PARTIAL_SUCCESS"
            else:
                result.status = "FAILED"
            
            result.end_time = datetime.now()
            result.duration = int((result.end_time - result.start_time).total_seconds())
            
            # Log completion
            self._log_run_completion(result)
            
            self.logger.info(f"ETL execution completed - Status: {result.status}")
            
        except Exception as e:
            self.logger.error(f"ETL execution error: {str(e)}")
            result.status = "ERROR"
            result.end_time = datetime.now()
            result.duration = int((result.end_time - result.start_time).total_seconds())
            self._log_run_completion(result)
            raise
        
        return result
    
    def _generate_run_id(self) -> str:
        """Generate unique run ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"RUN_{timestamp}"
    
    def _log_run_completion(self, result: ETLResult):
        """Log run completion to database"""
        try:
            # Create log DataFrame
            log_data = [(
                result.run_id,
                result.status,
                result.start_time,
                result.end_time,
                result.duration,
                result.records_extracted,
                result.records_transformed,
                result.records_loaded,
                result.records_failed,
                result.error_count
            )]
            
            columns = [
                "run_id", "status", "start_time", "end_time", "duration",
                "records_extracted", "records_transformed", "records_loaded",
                "records_failed", "error_count"
            ]
            
            log_df = self.spark.createDataFrame(log_data, columns)
            
            # Write to log table
            log_df.write \
                .format("jdbc") \
                .option("url", self._get_jdbc_url()) \
                .option("dbtable", "zetl_run_log") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            
        except Exception as e:
            self.logger.error(f"Failed to log run completion: {str(e)}")
    
    def _get_jdbc_url(self) -> str:
        """Get JDBC connection URL"""
        return "jdbc:postgresql://localhost:5432/etl_db"