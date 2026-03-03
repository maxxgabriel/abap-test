"""
PySpark ETL Orchestrator Module
Migrated from ABAP zcl_etl_orchestrator
Coordinates the entire ETL process
"""
from pyspark.sql import SparkSession
from datetime import datetime
from dataclasses import dataclass
import uuid
from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.logger import ETLLogger


@dataclass
class ETLResult:
    """ETL execution result"""
    run_id: str
    status: str
    start_time: datetime
    end_time: datetime
    duration: int
    records_extracted: int = 0
    records_transformed: int = 0
    records_loaded: int = 0
    records_failed: int = 0
    error_count: int = 0
    warning_count: int = 0


class ETLOrchestrator:
    """Orchestrate complete ETL workflow"""
    
    def __init__(self, spark: SparkSession, config: dict, run_type: str = "MANUAL"):
        self.spark = spark
        self.config = config
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        self.logger = ETLLogger.get_instance()
    
    def _generate_run_id(self) -> str:
        """Generate unique run ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"RUN_{timestamp}_{unique_id}"
    
    def execute_etl(self, 
                    source_type: str = "DATABASE",
                    target_type: str = "DATABASE",
                    filter_expr: str = None,
                    batch_size: int = 1000,
                    max_records: int = 0) -> ETLResult:
        """
        Execute complete ETL workflow
        Converts ABAP orchestration logic to PySpark
        """
        result = ETLResult(
            run_id=self.run_id,
            status="RUNNING",
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration=0
        )
        
        self.logger.log_info(
            component="ORCHESTRATOR",
            message=f"ETL execution started - Run ID: {self.run_id}"
        )
        
        try:
            # Step 1: Extract
            extractor = ETLExtractor(self.spark, self.config, self.run_id, source_type)
            source_df = extractor.extract_data(filter_expr, max_records)
            result.records_extracted = source_df.count()
            
            if result.records_extracted == 0:
                self.logger.log_warning(
                    component="ORCHESTRATOR",
                    message="No data extracted - ETL process stopping"
                )
                result.status = "NO_DATA"
                return result
            
            # Step 2: Transform
            transformer = ETLTransformer(self.spark, self.config, self.run_id)
            transformed_df = transformer.transform_data(source_df)
            
            # Validate
            is_valid, validation_errors = transformer.validate_data(transformed_df)
            
            if not is_valid:
                self.logger.log_error(
                    component="ORCHESTRATOR",
                    message=f"Validation failed - {len(validation_errors)} errors"
                )
                result.status = "VALIDATION_FAILED"
                result.error_count = len(validation_errors)
                return result
            
            result.records_transformed = transformed_df.count()
            
            # Step 3: Load
            loader = ETLLoader(self.spark, self.config, self.run_id, target_type, batch_size)
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
            
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"ETL execution completed - Status: {result.status}"
            )
            
            return result
            
        except Exception as e:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message="ETL execution failed",
                details=str(e)
            )
            result.status = "ERROR"
            result.end_time = datetime.now()
            result.duration = int((result.end_time - result.start_time).total_seconds())
            return result