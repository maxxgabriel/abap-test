"""
ETL Orchestrator - Coordinates the complete ETL pipeline
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
    """Container for ETL execution results"""
    
    def __init__(self, run_id: str):
        self.run_id = run_id
        self.status = "RUNNING"
        self.start_time = datetime.now()
        self.end_time = None
        self.duration_seconds = 0
        self.records_extracted = 0
        self.records_transformed = 0
        self.records_loaded = 0
        self.records_failed = 0
        self.error_count = 0
        self.warning_count = 0
        self.errors = []


class ETLOrchestrator:
    """Orchestrates the complete ETL pipeline execution"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the orchestrator
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def execute_etl(
        self,
        source_type: str = "database",
        target_type: str = "database",
        filter_condition: Optional[str] = None,
        max_records: int = 0,
        load_mode: str = "upsert"
    ) -> ETLResult:
        """
        Execute complete ETL pipeline
        
        Args:
            source_type: Type of data source
            target_type: Type of target destination
            filter_condition: Optional filter for extraction
            max_records: Maximum records to process
            load_mode: Mode for loading data
            
        Returns:
            ETLResult object with execution statistics
        """
        run_id = self._generate_run_id()
        result = ETLResult(run_id)
        
        self.logger.info(f"=== ETL Execution Started - Run ID: {run_id} ===")
        
        try:
            # Step 1: Extract
            self.logger.info("Step 1: Extracting data")
            extractor = ETLExtractor(self.spark, self.config, run_id)
            df_extracted = extractor.extract_data(
                source_type=source_type,
                filter_condition=filter_condition,
                max_records=max_records
            )
            result.records_extracted = df_extracted.count()
            
            if result.records_extracted == 0:
                self.logger.warning("No data extracted - stopping ETL process")
                result.status = "NO_DATA"
                result.end_time = datetime.now()
                result.duration_seconds = (result.end_time - result.start_time).total_seconds()
                return result
            
            # Step 2: Transform
            self.logger.info("Step 2: Transforming data")
            transformer = ETLTransformer(self.spark, self.config, run_id)
            df_transformed = transformer.transform_data(df_extracted)
            result.records_transformed = df_transformed.count()
            
            # Step 3: Validate
            self.logger.info("Step 3: Validating data")
            is_valid, validation_errors = transformer.validate_data(df_transformed)
            
            if not is_valid:
                self.logger.error(f"Validation failed with {len(validation_errors)} errors")
                result.status = "VALIDATION_FAILED"
                result.error_count = len(validation_errors)
                result.errors = validation_errors
                result.end_time = datetime.now()
                result.duration_seconds = (result.end_time - result.start_time).total_seconds()
                return result
            
            # Step 4: Load
            self.logger.info("Step 4: Loading data")
            loader = ETLLoader(self.spark, self.config, run_id)
            load_result = loader.load_data(
                df=df_transformed,
                mode=load_mode,
                target_type=target_type
            )
            
            result.records_loaded = load_result.success_count
            result.records_failed = load_result.error_count
            result.error_count = load_result.error_count
            result.errors = load_result.errors
            
            # Determine final status
            if load_result.error_count == 0:
                result.status = "SUCCESS"
            elif load_result.success_count > 0:
                result.status = "PARTIAL_SUCCESS"
            else:
                result.status = "FAILED"
            
            # Log to run log table
            self._log_run_completion(result)
            
        except Exception as e:
            self.logger.error(f"ETL execution error: {str(e)}", exc_info=True)
            result.status = "ERROR"
            result.error_count += 1
            result.errors.append(str(e))
            
        finally:
            result.end_time = datetime.now()
            result.duration_seconds = (result.end_time - result.start_time).total_seconds()
            
            self.logger.info(f"=== ETL Execution Completed - Status: {result.status} ===")
            self.logger.info(f"Records - Extracted: {result.records_extracted}, "
                           f"Transformed: {result.records_transformed}, "
                           f"Loaded: {result.records_loaded}, "
                           f"Failed: {result.records_failed}")
            self.logger.info(f"Duration: {result.duration_seconds:.2f} seconds")
        
        return result
    
    def _generate_run_id(self) -> str:
        """Generate unique run ID"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"RUN_{timestamp}_{unique_id}"
    
    def _log_run_completion(self, result: ETLResult):
        """Log run completion to database"""
        try:
            log_config = self.config['logging']['database']
            
            log_data = [(
                result.run_id,
                result.status,
                result.start_time,
                result.end_time,
                result.duration_seconds,
                result.records_extracted,
                result.records_transformed,
                result.records_loaded,
                result.records_failed,
                result.error_count
            )]
            
            columns = ["run_id", "status", "start_time", "end_time", "duration",
                      "records_extracted", "records_transformed", "records_loaded",
                      "records_failed", "error_count"]
            
            df_log = self.spark.createDataFrame(log_data, columns)
            
            df_log.write \
                .format("jdbc") \
                .option("url", log_config['jdbc_url']) \
                .option("dbtable", log_config['run_log_table']) \
                .option("user", log_config.get('user', '')) \
                .option("password", log_config.get('password', '')) \
                .mode("append") \
                .save()
            
            self.logger.info("Run log written successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to write run log: {str(e)}")