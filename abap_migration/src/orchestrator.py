"""
ETL Orchestrator
Orchestrates the complete ETL pipeline execution.
Migrated from ABAP class zcl_etl_orchestrator
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
import uuid

from pyspark.sql import SparkSession, DataFrame

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.data_quality import DataQualityChecker
from src.logger import ETLLogger
from src.config import ETLConfig


class ETLOrchestrator:
    """Orchestrates the complete ETL pipeline execution."""
    
    def __init__(self, spark: SparkSession, config: ETLConfig, 
                 run_type: str = 'MANUAL', run_id: Optional[str] = None):
        """
        Initialize ETL orchestrator.
        
        Args:
            spark: SparkSession instance
            config: ETL configuration
            run_type: Type of run (MANUAL, SCHEDULED, TEST)
            run_id: Custom run ID (auto-generated if None)
        """
        self.spark = spark
        self.config = config
        self.run_type = run_type
        self.run_id = run_id or self._generate_run_id()
        self.logger = ETLLogger.get_instance()
        
    def _generate_run_id(self) -> str:
        """Generate unique run ID."""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        unique_id = str(uuid.uuid4())[:8]
        return f"RUN_{timestamp}_{unique_id}"
    
    def execute_etl(
        self,
        source_type: str = 'DATABASE',
        target_type: str = 'DATABASE',
        filter_clause: str = '',
        batch_size: int = 1000,
        max_records: int = 0,
        validate: bool = True,
        reconcile: bool = True,
        quality_checks: bool = True,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Execute the complete ETL pipeline.
        
        Args:
            source_type: Source type for extraction
            target_type: Target type for loading
            filter_clause: SQL filter for extraction
            batch_size: Batch size for processing
            max_records: Maximum records to process
            validate: Enable validation
            reconcile: Enable reconciliation
            quality_checks: Run quality checks
            dry_run: Dry run mode
            
        Returns:
            Dictionary with execution results
        """
        result = {
            'run_id': self.run_id,
            'status': 'RUNNING',
            'start_time': None,
            'end_time': None,
            'duration': 0,
            'records_extracted': 0,
            'records_transformed': 0,
            'records_loaded': 0,
            'records_failed': 0,
            'error_count': 0,
            'warning_count': 0,
            'quality_results': None
        }
        
        self.logger.log_info(
            component='ORCHESTRATOR',
            message=f"ETL execution started - Run ID: {self.run_id}"
        )
        
        start_time = datetime.now()
        result['start_time'] = start_time
        
        try:
            # Log run start
            self._log_run_start()
            
            # Step 1: Extract
            self.logger.log_info(
                component='ORCHESTRATOR',
                message='Starting extraction phase'
            )
            
            extractor = ETLExtractor(
                spark=self.spark,
                config=self.config,
                source_type=source_type,
                run_id=self.run_id
            )
            
            source_data = extractor.extract_data(
                filter_clause=filter_clause,
                max_records=max_records
            )
            
            result['records_extracted'] = source_data.count()
            
            if result['records_extracted'] == 0:
                self.logger.log_warning(
                    component='ORCHESTRATOR',
                    message='No data extracted - ETL process stopping'
                )
                result['status'] = 'NO_DATA'
                return result
            
            # Step 2: Transform
            self.logger.log_info(
                component='ORCHESTRATOR',
                message='Starting transformation phase'
            )
            
            transformer = ETLTransformer(
                spark=self.spark,
                config=self.config,
                run_id=self.run_id
            )
            
            transformed_data = transformer.transform_data(source_data)
            result['records_transformed'] = transformed_data.count()
            
            # Step 3: Validate
            if validate:
                self.logger.log_info(
                    component='ORCHESTRATOR',
                    message='Starting validation phase'
                )
                
                is_valid, validation_errors = transformer.validate_data(transformed_data)
                
                if not is_valid:
                    self.logger.log_error(
                        component='ORCHESTRATOR',
                        message=f'Validation failed - {len(validation_errors)} errors'
                    )
                    result['status'] = 'VALIDATION_FAILED'
                    result['error_count'] = len(validation_errors)
                    return result
            
            # Step 4: Quality Checks
            if quality_checks:
                self.logger.log_info(
                    component='ORCHESTRATOR',
                    message='Running data quality checks'
                )
                
                quality_checker = DataQualityChecker(
                    spark=self.spark,
                    config=self.config,
                    run_id=self.run_id
                )
                
                quality_results = quality_checker.perform_quality_checks(transformed_data)
                result['quality_results'] = quality_results
                
                # Check if quality checks passed
                failed_checks = [c for c in quality_results if not c['passed']]
                if failed_checks:
                    result['warning_count'] = len(failed_checks)
            
            # Step 5: Load
            if not dry_run:
                self.logger.log_info(
                    component='ORCHESTRATOR',
                    message='Starting load phase'
                )
                
                loader = ETLLoader(
                    spark=self.spark,
                    config=self.config,
                    target_type=target_type,
                    batch_size=batch_size,
                    run_id=self.run_id
                )
                
                load_result = loader.load_data(
                    data=transformed_data,
                    mode='upsert',
                    reconcile=reconcile
                )
                
                result['records_loaded'] = load_result['success_count']
                result['records_failed'] = load_result['error_count']
                result['error_count'] += load_result['error_count']
            else:
                self.logger.log_info(
                    component='ORCHESTRATOR',
                    message='Dry run mode - skipping load phase'
                )
                result['records_loaded'] = result['records_transformed']
            
            # Determine final status
            if result['error_count'] == 0:
                result['status'] = 'SUCCESS'
            elif result['records_loaded'] > 0:
                result['status'] = 'PARTIAL_SUCCESS'
            else:
                result['status'] = 'FAILED'
            
            # Calculate duration
            end_time = datetime.now()
            result['end_time'] = end_time
            result['duration'] = (end_time - start_time).total_seconds()
            
            # Log run end
            self._log_run_end(result)
            
            self.logger.log_info(
                component='ORCHESTRATOR',
                message=f"ETL execution completed - Status: {result['status']}"
            )
            
        except Exception as e:
            self.logger.log_error(
                component='ORCHESTRATOR',
                message='ETL execution failed',
                details=str(e)
            )
            result['status'] = 'ERROR'
            result['end_time'] = datetime.now()
            result['duration'] = (result['end_time'] - start_time).total_seconds()
            self._log_run_end(result)
            raise
        
        return result
    
    def _log_run_start(self):
        """Log run start to database."""
        try:
            run_log_data = [(
                self.run_id,
                self.run_type,
                'RUNNING',
                datetime.now(),
                None,
                0,
                0,
                0,
                0
            )]
            
            columns = [
                'run_id', 'run_type', 'status', 'start_time', 'end_time',
                'duration', 'records_extracted', 'records_loaded', 'error_count'
            ]
            
            df = self.spark.createDataFrame(run_log_data, columns)
            
            # Write to run log table
            table_name = self.config.get('tables.run_log', 'etl_run_log')
            df.write.mode('append').saveAsTable(table_name)
            
        except Exception as e:
            self.logger.log_warning(
                component='ORCHESTRATOR',
                message=f'Failed to log run start: {str(e)}'
            )
    
    def _log_run_end(self, result: Dict[str, Any]):
        """Log run end to database."""
        try:
            # Update run log
            table_name = self.config.get('tables.run_log', 'etl_run_log')
            
            update_query = f"""
                UPDATE {table_name}
                SET status = '{result['status']}',
                    end_time = '{result['end_time']}',
                    duration = {result['duration']},
                    records_extracted = {result['records_extracted']},
                    records_loaded = {result['records_loaded']},
                    error_count = {result['error_count']}
                WHERE run_id = '{self.run_id}'
            """
            
            self.spark.sql(update_query)
            
        except Exception as e:
            self.logger.log_warning(
                component='ORCHESTRATOR',
                message=f'Failed to log run end: {str(e)}'
            )
    
    def get_run_statistics(self, run_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get run statistics.
        
        Args:
            run_id: Specific run ID (None for all runs)
            
        Returns:
            List of run statistics
        """
        try:
            table_name = self.config.get('tables.run_log', 'etl_run_log')
            
            query = f"SELECT * FROM {table_name}"
            if run_id:
                query += f" WHERE run_id = '{run_id}'"
            query += " ORDER BY start_time DESC"
            
            df = self.spark.sql(query)
            return [row.asDict() for row in df.collect()]
            
        except Exception as e:
            self.logger.log_error(
                component='ORCHESTRATOR',
                message=f'Failed to get run statistics: {str(e)}'
            )
            return []