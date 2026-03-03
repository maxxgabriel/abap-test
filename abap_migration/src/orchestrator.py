"""
PySpark ETL Orchestrator
Coordinates extraction, transformation, and loading
Migrated from zcl_etl_orchestrator.abap
"""

from pyspark.sql import SparkSession
from typing import Optional
import logging
from datetime import datetime
from dataclasses import dataclass

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader


@dataclass
class ETLResult:
    """Result of ETL execution."""
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


class ETLOrchestrator:
    """Orchestrate the complete ETL process."""
    
    def __init__(self, run_type: str = "MANUAL", spark: SparkSession = None):
        """
        Initialize ETL Orchestrator.
        
        Args:
            run_type: Type of run (MANUAL, SCHEDULED, etc.)
            spark: SparkSession instance
        """
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        self.spark = spark or SparkSession.builder.getOrCreate()
        self.logger = logging.getLogger(__name__)
        
    def _generate_run_id(self) -> str:
        """Generate unique run identifier."""
        return f"ETL_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def execute_etl(
        self,
        source_type: str = "DATABASE",
        target_type: str = "DATABASE",
        filter_condition: Optional[str] = None,
        batch_size: int = 1000,
        max_records: int = 0
    ) -> ETLResult:
        """
        Execute complete ETL process.
        
        Args:
            source_type: Type of data source
            target_type: Type of data target
            filter_condition: Optional filter for extraction
            batch_size: Batch size for loading
            max_records: Maximum records to process (0 = no limit)
            
        Returns:
            ETLResult with execution statistics
        """
        start_time = datetime.now()
        
        result = ETLResult(
            run_id=self.run_id,
            status="RUNNING",
            start_time=start_time,
            end_time=None,
            duration=None,
            records_extracted=0,
            records_transformed=0,
            records_loaded=0,
            records_failed=0,
            error_count=0,
            warning_count=0
        )
        
        self.logger.info(f"ETL execution started - Run ID: {self.run_id}")
        
        try:
            # Step 1: Extract
            extractor = ETLExtractor(source_type=source_type, run_id=self.run_id, spark=self.spark)
            source_df = extractor.extract_data(
                filter_condition=filter_condition,
                max_records=max_records
            )
            
            result.records_extracted = source_df.count()
            
            if result.records_extracted == 0:
                self.logger.warning("No data extracted - ETL process stopping")
                result.status = "NO_DATA"
                result.end_time = datetime.now()
                return result
            
            # Step 2: Transform
            transformer = ETLTransformer(run_id=self.run_id, spark=self.spark)
            transformed_df = transformer.transform_data(source_df)
            
            # Validate
            is_valid, validation_errors = transformer.validate_data(transformed_df)
            
            if not is_valid:
                self.logger.error(f"Validation failed - {len(validation_errors)} errors")
                result.status = "VALIDATION_FAILED"
                result.error_count = len(validation_errors)
                result.end_time = datetime.now()
                return result
            
            result.records_transformed = transformed_df.count()
            
            # Step 3: Load
            loader = ETLLoader(
                target_type=target_type,
                batch_size=batch_size,
                run_id=self.run_id,
                spark=self.spark
            )
            
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
            
            self._log_run_completion(result)
            
            self.logger.info(f"ETL execution completed - Status: {result.status}")
            
        except Exception as e:
            self.logger.error(f"ETL execution error: {str(e)}")
            result.status = "ERROR"
            result.end_time = datetime.now()
            result.duration = int((result.end_time - result.start_time).total_seconds())
            self._log_run_completion(result)
        
        return result
    
    def _log_run_completion(self, result: ETLResult):
        """Log ETL run completion to database."""
        try:
            log_data = [{
                "run_id": result.run_id,
                "run_type": self.run_type,
                "status": result.status,
                "start_time": result.start_time,
                "end_time": result.end_time,
                "duration": result.duration,
                "records_extracted": result.records_extracted,
                "records_transformed": result.records_transformed,
                "records_loaded": result.records_loaded,
                "records_failed": result.records_failed,
                "error_count": result.error_count,
            }]
            
            log_df = self.spark.createDataFrame(log_data)
            
            log_df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://localhost:5432/etl_db") \
                .option("dbtable", "zetl_run_log") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
                
        except Exception as e:
            self.logger.error(f"Failed to log run completion: {str(e)}")