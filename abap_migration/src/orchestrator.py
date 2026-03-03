"""
ETL Orchestration Module
Coordinates extract, transform, and load operations
"""
from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict, Any, Optional
import logging
import uuid

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader


class ETLResult:
    """Result of ETL execution"""
    def __init__(self, run_id: str):
        self.run_id = run_id
        self.status = "RUNNING"
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.duration_seconds = 0
        self.records_extracted = 0
        self.records_transformed = 0
        self.records_loaded = 0
        self.records_failed = 0
        self.error_count = 0
        self.warning_count = 0
        self.error_message: Optional[str] = None


class ETLOrchestrator:
    """Orchestrates the entire ETL process"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], 
                 run_type: str = "MANUAL"):
        """
        Initialize orchestrator
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_type: Type of run (MANUAL, SCHEDULED, etc.)
        """
        self.spark = spark
        self.config = config
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        self.logger = logging.getLogger(__name__)
        
    def _generate_run_id(self) -> str:
        """Generate unique run ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"RUN_{timestamp}_{unique_id}"
    
    def execute_etl(self, source_type: str = "DATABASE", 
                    target_type: str = "DATABASE",
                    filter_value: Optional[str] = None,
                    batch_size: int = 1000,
                    max_records: int = 0) -> ETLResult:
        """
        Execute complete ETL pipeline
        
        Args:
            source_type: Source system type
            target_type: Target system type
            filter_value: Optional filter criteria
            batch_size: Batch size for loading
            max_records: Maximum records to process (0 = unlimited)
            
        Returns:
            ETLResult with execution statistics
        """
        result = ETLResult(self.run_id)
        result.start_time = datetime.now()
        
        self.logger.info(f"ETL execution started - Run ID: {self.run_id}")
        
        try:
            # Update config with runtime parameters
            config = self._prepare_config(source_type, target_type, batch_size)
            
            # Step 1: Extract
            self.logger.info("=== EXTRACTION PHASE ===")
            extractor = ETLExtractor(self.spark, config, self.run_id)
            df_source = extractor.extract_data(filter_value, max_records)
            result.records_extracted = df_source.count()
            
            if result.records_extracted == 0:
                self.logger.warning("No data extracted - stopping ETL process")
                result.status = "NO_DATA"
                result.end_time = datetime.now()
                result.duration_seconds = (result.end_time - result.start_time).total_seconds()
                return result
            
            # Step 2: Transform
            self.logger.info("=== TRANSFORMATION PHASE ===")
            transformer = ETLTransformer(self.spark, config, self.run_id)
            df_transformed = transformer.transform_data(df_source)
            result.records_transformed = df_transformed.count()
            
            # Validate
            is_valid, validation_errors = transformer.validate_data(df_transformed)
            if not is_valid:
                self.logger.error(f"Validation failed with {len(validation_errors)} errors")
                for error in validation_errors:
                    self.logger.error(f"  - {error}")
                result.status = "VALIDATION_FAILED"
                result.error_count = len(validation_errors)
                result.error_message = "; ".join(validation_errors)
                result.end_time = datetime.now()
                result.duration_seconds = (result.end_time - result.start_time).total_seconds()
                return result
            
            # Step 3: Load
            self.logger.info("=== LOADING PHASE ===")
            loader = ETLLoader(self.spark, config, self.run_id)
            load_result = loader.load_data(df_transformed, mode='UPSERT')
            
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
            result.duration_seconds = (result.end_time - result.start_time).total_seconds()
            
            # Log completion
            self._log_run_completion(result)
            
            self.logger.info(f"ETL execution completed - Status: {result.status}")
            
        except Exception as e:
            self.logger.error(f"ETL execution error: {str(e)}", exc_info=True)
            result.status = "ERROR"
            result.error_message = str(e)
            result.end_time = datetime.now()
            result.duration_seconds = (result.end_time - result.start_time).total_seconds()
            self._log_run_completion(result)
        
        return result
    
    def _prepare_config(self, source_type: str, target_type: str, 
                        batch_size: int) -> Dict[str, Any]:
        """Prepare configuration with runtime parameters"""
        config = self.config.copy()
        config['extraction']['source_type'] = source_type
        config['loading']['target_type'] = target_type
        config['loading']['batch_size'] = batch_size
        return config
    
    def _log_run_completion(self, result: ETLResult):
        """Log run completion to database"""
        try:
            jdbc_config = self.config['loading']['jdbc']
            
            # Create run log DataFrame
            log_data = [(
                self.run_id,
                self.run_type,
                result.start_time,
                result.end_time,
                result.duration_seconds,
                result.status,
                result.records_extracted,
                result.records_transformed,
                result.records_loaded,
                result.records_failed,
                result.error_count,
                result.error_message
            )]
            
            log_df = self.spark.createDataFrame(log_data, [
                "run_id", "run_type", "start_time", "end_time", "duration",
                "status", "records_extracted", "records_transformed",
                "records_loaded", "records_failed", "error_count", "error_message"
            ])
            
            log_df.write \
                .format("jdbc") \
                .option("url", jdbc_config['url']) \
                .option("dbtable", "zetl_run_log") \
                .option("user", jdbc_config['user']) \
                .option("password", jdbc_config['password']) \
                .option("driver", jdbc_config['driver']) \
                .mode("append") \
                .save()
                
            self.logger.info(f"Run log saved for {self.run_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to log run completion: {str(e)}")