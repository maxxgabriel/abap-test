"""
PySpark ETL Orchestrator
Coordinates the complete ETL pipeline execution
"""
from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict, Any, Optional
from dataclasses import dataclass
import logging
import uuid

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.quality import DataQualityChecker


@dataclass
class ETLResult:
    """Result of ETL execution"""
    run_id: str
    status: str
    start_time: datetime
    end_time: datetime
    duration_seconds: int
    records_extracted: int
    records_transformed: int
    records_loaded: int
    records_failed: int
    error_count: int
    warning_count: int


class ETLOrchestrator:
    """Orchestrates the complete ETL pipeline"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_type: str = 'MANUAL'):
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
        
    def execute_etl(self,
                    source_type: str = 'DATABASE',
                    target_type: str = 'DATABASE',
                    filter_condition: Optional[str] = None,
                    max_records: int = 0) -> ETLResult:
        """
        Execute complete ETL pipeline
        
        Args:
            source_type: Source type (DATABASE, STAGING, INCREMENTAL)
            target_type: Target type
            filter_condition: Optional filter condition
            max_records: Maximum records to process
            
        Returns:
            ETLResult with execution statistics
        """
        start_time = datetime.now()
        self.logger.info(f"ETL execution started - Run ID: {self.run_id}")
        
        # Update config with runtime parameters
        runtime_config = self.config.copy()
        runtime_config['source_type'] = source_type
        runtime_config['target_type'] = target_type
        
        try:
            # Step 1: Extract
            self.logger.info("=== STEP 1: EXTRACTION ===")
            extractor = ETLExtractor(self.spark, runtime_config, self.run_id)
            source_df = extractor.extract_data(filter_condition, max_records)
            records_extracted = source_df.count()
            
            if records_extracted == 0:
                self.logger.warning("No data extracted - ETL process stopping")
                return self._create_result(
                    start_time=start_time,
                    status='NO_DATA',
                    records_extracted=0
                )
            
            # Step 2: Transform
            self.logger.info("=== STEP 2: TRANSFORMATION ===")
            transformer = ETLTransformer(self.spark, runtime_config, self.run_id)
            transformed_df = transformer.transform_data(source_df)
            
            # Step 3: Validate
            self.logger.info("=== STEP 3: VALIDATION ===")
            is_valid, validation_errors = transformer.validate_data(transformed_df)
            
            if not is_valid:
                self.logger.error(f"Validation failed - {len(validation_errors)} errors")
                for error in validation_errors:
                    self.logger.error(f"  - {error}")
                return self._create_result(
                    start_time=start_time,
                    status='VALIDATION_FAILED',
                    records_extracted=records_extracted,
                    error_count=len(validation_errors)
                )
            
            records_transformed = transformed_df.count()
            
            # Step 4: Data Quality Checks
            if runtime_config.get('enable_quality_checks', True):
                self.logger.info("=== STEP 4: DATA QUALITY CHECKS ===")
                quality_checker = DataQualityChecker(self.spark, runtime_config, self.run_id)
                quality_results = quality_checker.perform_quality_checks(transformed_df)
                
                failed_checks = [check for check in quality_results if not check.passed]
                if failed_checks:
                    self.logger.warning(f"{len(failed_checks)} quality checks failed")
            
            # Step 5: Load
            self.logger.info("=== STEP 5: LOADING ===")
            loader = ETLLoader(self.spark, runtime_config, self.run_id)
            load_result = loader.load_data(transformed_df, mode='UPSERT')
            
            # Determine final status
            if load_result.error_count == 0:
                status = 'SUCCESS'
            elif load_result.success_count > 0:
                status = 'PARTIAL_SUCCESS'
            else:
                status = 'FAILED'
            
            end_time = datetime.now()
            duration = int((end_time - start_time).total_seconds())
            
            result = ETLResult(
                run_id=self.run_id,
                status=status,
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration,
                records_extracted=records_extracted,
                records_transformed=records_transformed,
                records_loaded=load_result.success_count,
                records_failed=load_result.error_count,
                error_count=load_result.error_count,
                warning_count=0
            )
            
            # Log run to database
            self._log_run(result)
            
            self.logger.info(f"ETL execution completed - Status: {status}")
            return result
            
        except Exception as e:
            self.logger.error(f"ETL execution failed: {str(e)}")
            end_time = datetime.now()
            result = self._create_result(
                start_time=start_time,
                status='ERROR',
                error_count=1
            )
            self._log_run(result)
            raise
    
    def _generate_run_id(self) -> str:
        """Generate unique run ID"""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        unique_id = str(uuid.uuid4())[:8]
        return f"RUN_{timestamp}_{unique_id}"
    
    def _create_result(self,
                      start_time: datetime,
                      status: str,
                      records_extracted: int = 0,
                      records_transformed: int = 0,
                      records_loaded: int = 0,
                      records_failed: int = 0,
                      error_count: int = 0) -> ETLResult:
        """Create ETL result object"""
        end_time = datetime.now()
        duration = int((end_time - start_time).total_seconds())
        
        return ETLResult(
            run_id=self.run_id,
            status=status,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            records_extracted=records_extracted,
            records_transformed=records_transformed,
            records_loaded=records_loaded,
            records_failed=records_failed,
            error_count=error_count,
            warning_count=0
        )
    
    def _log_run(self, result: ETLResult):
        """Log run details to database"""
        try:
            log_data = [(
                result.run_id,
                self.run_type,
                result.status,
                result.start_time,
                result.end_time,
                result.duration_seconds,
                result.records_extracted,
                result.records_transformed,
                result.records_loaded,
                result.records_failed
            )]
            
            log_df = self.spark.createDataFrame(
                log_data,
                ["run_id", "run_type", "status", "start_time", "end_time",
                 "duration", "records_extracted", "records_transformed",
                 "records_loaded", "records_failed"]
            )
            
            jdbc_config = self.config['jdbc']
            log_df.write \
                .format("jdbc") \
                .option("url", jdbc_config['url']) \
                .option("dbtable", "etl_run_log") \
                .option("user", jdbc_config['user']) \
                .option("password", jdbc_config['password']) \
                .option("driver", jdbc_config['driver']) \
                .mode("append") \
                .save()
                
        except Exception as e:
            self.logger.error(f"Failed to log run: {str(e)}")