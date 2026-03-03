"""
ETL Orchestrator - Main workflow coordination
"""
from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict, Any, Optional
import logging
import uuid

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.data_quality import DataQualityChecker


class ETLOrchestrator:
    """Orchestrates the complete ETL workflow"""
    
    def __init__(self, spark: SparkSession, config: dict, run_type: str = "MANUAL"):
        self.spark = spark
        self.config = config
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.extractor = ETLExtractor(spark, config, self.run_id)
        self.transformer = ETLTransformer(spark, config, self.run_id)
        self.loader = ETLLoader(spark, config, self.run_id)
        self.quality_checker = DataQualityChecker(spark, config, self.run_id)
    
    def execute_etl(
        self,
        source_type: str = "DATABASE",
        target_type: str = "DATABASE",
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> Dict[str, Any]:
        """Execute complete ETL workflow"""
        
        result = {
            'run_id': self.run_id,
            'status': 'RUNNING',
            'start_time': datetime.now(),
            'end_time': None,
            'duration': 0,
            'records_extracted': 0,
            'records_transformed': 0,
            'records_loaded': 0,
            'records_failed': 0,
            'error_count': 0,
            'warning_count': 0
        }
        
        self.logger.info(f"ETL execution started - Run ID: {self.run_id}")
        
        try:
            # Step 1: Extract
            self.logger.info("Step 1: Extracting data...")
            source_df = self.extractor.extract_data(
                source_type=source_type,
                filter_condition=filter_condition,
                max_records=max_records
            )
            result['records_extracted'] = source_df.count()
            
            if result['records_extracted'] == 0:
                self.logger.warning("No data extracted - stopping ETL process")
                result['status'] = 'NO_DATA'
                return result
            
            # Step 2: Transform
            self.logger.info("Step 2: Transforming data...")
            transformed_df = self.transformer.transform_data(source_df)
            result['records_transformed'] = transformed_df.count()
            
            # Step 3: Validate
            self.logger.info("Step 3: Validating data...")
            is_valid, validation_errors = self.transformer.validate_data(transformed_df)
            
            if not is_valid:
                self.logger.error(f"Validation failed with {len(validation_errors)} errors")
                result['status'] = 'VALIDATION_FAILED'
                result['error_count'] = len(validation_errors)
                return result
            
            # Step 4: Quality Checks
            if self.config['data_quality'].get('enabled', True):
                self.logger.info("Step 4: Performing quality checks...")
                quality_results = self.quality_checker.perform_quality_checks(transformed_df)
                
                failed_checks = [qc for qc in quality_results if not qc['passed']]
                if failed_checks:
                    result['warning_count'] = len(failed_checks)
                    self.logger.warning(f"{len(failed_checks)} quality checks failed")
            
            # Step 5: Load
            self.logger.info("Step 5: Loading data...")
            load_result = self.loader.load_data(
                df=transformed_df,
                mode=self.config['load']['mode']
            )
            
            result['records_loaded'] = load_result['success_count']
            result['records_failed'] = load_result['error_count']
            result['error_count'] = load_result['error_count']
            
            # Determine final status
            if load_result['error_count'] == 0:
                result['status'] = 'SUCCESS'
            elif load_result['success_count'] > 0:
                result['status'] = 'PARTIAL_SUCCESS'
            else:
                result['status'] = 'FAILED'
            
        except Exception as e:
            self.logger.error(f"ETL execution failed: {str(e)}")
            result['status'] = 'ERROR'
            result['error_count'] += 1
        
        finally:
            result['end_time'] = datetime.now()
            result['duration'] = (result['end_time'] - result['start_time']).total_seconds()
            
            # Log run completion
            self._log_run_completion(result)
            
            self.logger.info(
                f"ETL execution completed - Status: {result['status']}, "
                f"Duration: {result['duration']:.2f}s"
            )
        
        return result
    
    def _generate_run_id(self) -> str:
        """Generate unique run ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"RUN_{timestamp}_{unique_id}"
    
    def _log_run_completion(self, result: Dict[str, Any]):
        """Log run completion to monitoring system"""
        try:
            log_path = self.config['monitoring']['run_log_path']
            
            log_df = self.spark.createDataFrame([result])
            
            log_df.write \
                .mode("append") \
                .parquet(log_path)
            
            self.logger.info(f"Run logged to {log_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to log run completion: {str(e)}")