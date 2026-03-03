"""
ETL Orchestration Framework
Coordinates ETL workflow execution, generates unique run IDs, and tracks pipeline status.
"""

import uuid
from datetime import datetime
from typing import Dict, Optional, List
from pyspark.sql import SparkSession, DataFrame
import logging

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.data_quality import DataQualityChecker
from src.monitor import ETLMonitor
from src.config_manager import ConfigManager
from src.logger import ETLLogger


class ETLOrchestrator:
    """
    Main orchestrator for coordinating ETL workflow execution.
    Generates unique run IDs and tracks pipeline status across all stages.
    """
    
    def __init__(self, spark: SparkSession, run_type: str = "MANUAL"):
        """
        Initialize ETL orchestrator.
        
        Args:
            spark: Active Spark session
            run_type: Type of run (MANUAL, SCHEDULED, INCREMENTAL)
        """
        self.spark = spark
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        self.config = ConfigManager()
        self.logger = ETLLogger(component="ORCHESTRATOR", run_id=self.run_id)
        self.monitor = ETLMonitor(spark)
        
        # Initialize stage timestamps
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        
        # Initialize metrics
        self.metrics = {
            "records_extracted": 0,
            "records_transformed": 0,
            "records_loaded": 0,
            "records_failed": 0,
            "error_count": 0,
            "warning_count": 0
        }
        
    def _generate_run_id(self) -> str:
        """
        Generate unique run ID for tracking.
        
        Returns:
            Unique run ID string
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"RUN_{timestamp}_{unique_id}"
    
    def execute_etl(
        self,
        source_type: str = "DATABASE",
        target_type: str = "DATABASE",
        filter_condition: Optional[str] = None,
        batch_size: int = 1000,
        max_records: int = 0,
        enable_validation: bool = True,
        enable_reconciliation: bool = True
    ) -> Dict:
        """
        Execute complete ETL workflow.
        
        Args:
            source_type: Type of data source
            target_type: Type of data target
            filter_condition: Optional filter for extraction
            batch_size: Batch size for loading
            max_records: Maximum records to process (0 = unlimited)
            enable_validation: Whether to perform data validation
            enable_reconciliation: Whether to perform data reconciliation
            
        Returns:
            Dictionary with execution results and metrics
        """
        self.start_time = datetime.now()
        status = "RUNNING"
        
        self.logger.log_info(
            f"ETL execution started - Run ID: {self.run_id}, Type: {self.run_type}"
        )
        
        try:
            # Log run start to tracking table
            self._log_run_start()
            
            # Stage 1: Extract
            self.logger.log_info("Stage 1: Data Extraction")
            extracted_df = self._execute_extraction(
                source_type=source_type,
                filter_condition=filter_condition,
                max_records=max_records
            )
            
            if extracted_df is None or extracted_df.count() == 0:
                self.logger.log_warning("No data extracted - ETL process stopping")
                status = "NO_DATA"
                return self._create_result(status)
            
            self.metrics["records_extracted"] = extracted_df.count()
            
            # Stage 2: Transform
            self.logger.log_info("Stage 2: Data Transformation")
            transformed_df = self._execute_transformation(extracted_df)
            
            if transformed_df is None:
                raise Exception("Transformation failed")
                
            self.metrics["records_transformed"] = transformed_df.count()
            
            # Stage 3: Validate (if enabled)
            if enable_validation:
                self.logger.log_info("Stage 3: Data Validation")
                validation_result = self._execute_validation(transformed_df)
                
                if not validation_result["is_valid"]:
                    self.logger.log_error(
                        f"Validation failed - {len(validation_result['errors'])} errors"
                    )
                    status = "VALIDATION_FAILED"
                    self.metrics["error_count"] = len(validation_result["errors"])
                    return self._create_result(status)
            
            # Stage 4: Load
            self.logger.log_info("Stage 4: Data Loading")
            load_result = self._execute_load(
                transformed_df,
                target_type=target_type,
                batch_size=batch_size
            )
            
            self.metrics["records_loaded"] = load_result["success_count"]
            self.metrics["records_failed"] = load_result["error_count"]
            self.metrics["error_count"] = load_result["error_count"]
            
            # Stage 5: Reconciliation (if enabled)
            if enable_reconciliation and load_result["success_count"] > 0:
                self.logger.log_info("Stage 5: Data Reconciliation")
                reconciliation_result = self._execute_reconciliation(
                    transformed_df,
                    target_type
                )
                
                if not reconciliation_result:
                    self.logger.log_warning("Data reconciliation failed")
                    self.metrics["warning_count"] += 1
            
            # Determine final status
            if load_result["error_count"] == 0:
                status = "SUCCESS"
            elif load_result["success_count"] > 0:
                status = "PARTIAL_SUCCESS"
            else:
                status = "FAILED"
            
            self.logger.log_info(
                f"ETL execution completed - Status: {status}, "
                f"Loaded: {self.metrics['records_loaded']}, "
                f"Failed: {self.metrics['records_failed']}"
            )
            
        except Exception as e:
            self.logger.log_error(f"ETL execution error: {str(e)}")
            status = "ERROR"
            self.metrics["error_count"] += 1
            
        finally:
            self.end_time = datetime.now()
            self._log_run_end(status)
            
        return self._create_result(status)
    
    def _execute_extraction(
        self,
        source_type: str,
        filter_condition: Optional[str],
        max_records: int
    ) -> Optional[DataFrame]:
        """Execute data extraction stage."""
        try:
            extractor = ETLExtractor(
                spark=self.spark,
                source_type=source_type,
                run_id=self.run_id
            )
            
            df = extractor.extract_data(
                filter_condition=filter_condition,
                max_records=max_records
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(f"Extraction failed: {str(e)}")
            raise
    
    def _execute_transformation(self, source_df: DataFrame) -> Optional[DataFrame]:
        """Execute data transformation stage."""
        try:
            transformer = ETLTransformer(
                spark=self.spark,
                run_id=self.run_id
            )
            
            df = transformer.transform_data(source_df)
            return df
            
        except Exception as e:
            self.logger.log_error(f"Transformation failed: {str(e)}")
            raise
    
    def _execute_validation(self, transformed_df: DataFrame) -> Dict:
        """Execute data validation stage."""
        try:
            quality_checker = DataQualityChecker(
                spark=self.spark,
                run_id=self.run_id
            )
            
            result = quality_checker.perform_quality_checks(transformed_df)
            return result
            
        except Exception as e:
            self.logger.log_error(f"Validation failed: {str(e)}")
            raise
    
    def _execute_load(
        self,
        transformed_df: DataFrame,
        target_type: str,
        batch_size: int
    ) -> Dict:
        """Execute data loading stage."""
        try:
            loader = ETLLoader(
                spark=self.spark,
                target_type=target_type,
                batch_size=batch_size,
                run_id=self.run_id
            )
            
            result = loader.load_data(
                df=transformed_df,
                mode="upsert"
            )
            
            return result
            
        except Exception as e:
            self.logger.log_error(f"Loading failed: {str(e)}")
            raise
    
    def _execute_reconciliation(
        self,
        transformed_df: DataFrame,
        target_type: str
    ) -> bool:
        """Execute data reconciliation stage."""
        try:
            loader = ETLLoader(
                spark=self.spark,
                target_type=target_type,
                run_id=self.run_id
            )
            
            result = loader.reconcile_data(transformed_df)
            return result
            
        except Exception as e:
            self.logger.log_warning(f"Reconciliation failed: {str(e)}")
            return False
    
    def _log_run_start(self):
        """Log run start to tracking table."""
        try:
            run_data = [
                (
                    self.run_id,
                    self.run_type,
                    "RUNNING",
                    self.start_time,
                    None,
                    0,
                    0,
                    0,
                    0,
                    0
                )
            ]
            
            columns = [
                "run_id", "run_type", "status", "start_time", "end_time",
                "duration_seconds", "records_extracted", "records_transformed",
                "records_loaded", "records_failed"
            ]
            
            df = self.spark.createDataFrame(run_data, columns)
            
            # Write to run log table
            df.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(self.config.get("run_log_table", "etl_run_log"))
            
        except Exception as e:
            self.logger.log_warning(f"Failed to log run start: {str(e)}")
    
    def _log_run_end(self, status: str):
        """Log run end to tracking table."""
        try:
            duration_seconds = int((self.end_time - self.start_time).total_seconds())
            
            # Update run log with final metrics
            update_query = f"""
                UPDATE {self.config.get('run_log_table', 'etl_run_log')}
                SET 
                    status = '{status}',
                    end_time = '{self.end_time.isoformat()}',
                    duration_seconds = {duration_seconds},
                    records_extracted = {self.metrics['records_extracted']},
                    records_transformed = {self.metrics['records_transformed']},
                    records_loaded = {self.metrics['records_loaded']},
                    records_failed = {self.metrics['records_failed']}
                WHERE run_id = '{self.run_id}'
            """
            
            self.spark.sql(update_query)
            
        except Exception as e:
            self.logger.log_warning(f"Failed to log run end: {str(e)}")
    
    def _create_result(self, status: str) -> Dict:
        """
        Create result dictionary with execution metrics.
        
        Args:
            status: Final execution status
            
        Returns:
            Dictionary with execution results
        """
        duration_seconds = 0
        if self.start_time and self.end_time:
            duration_seconds = int((self.end_time - self.start_time).total_seconds())
        
        return {
            "run_id": self.run_id,
            "run_type": self.run_type,
            "status": status,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": duration_seconds,
            "metrics": self.metrics.copy()
        }
    
    def get_run_status(self, run_id: Optional[str] = None) -> Dict:
        """
        Get status of specific run or current run.
        
        Args:
            run_id: Optional run ID (defaults to current run)
            
        Returns:
            Dictionary with run status information
        """
        target_run_id = run_id or self.run_id
        
        try:
            df = self.spark.sql(f"""
                SELECT *
                FROM {self.config.get('run_log_table', 'etl_run_log')}
                WHERE run_id = '{target_run_id}'
            """)
            
            if df.count() == 0:
                return {"error": f"Run ID {target_run_id} not found"}
            
            return df.first().asDict()
            
        except Exception as e:
            self.logger.log_error(f"Failed to get run status: {str(e)}")
            return {"error": str(e)}
    
    def list_recent_runs(self, limit: int = 10) -> List[Dict]:
        """
        List recent ETL runs.
        
        Args:
            limit: Maximum number of runs to return
            
        Returns:
            List of run information dictionaries
        """
        try:
            df = self.spark.sql(f"""
                SELECT *
                FROM {self.config.get('run_log_table', 'etl_run_log')}
                ORDER BY start_time DESC
                LIMIT {limit}
            """)
            
            return [row.asDict() for row in df.collect()]
            
        except Exception as e:
            self.logger.log_error(f"Failed to list recent runs: {str(e)}")
            return []
    
    def schedule_etl(self, schedule_id: str) -> bool:
        """
        Execute ETL based on schedule configuration.
        
        Args:
            schedule_id: Schedule identifier
            
        Returns:
            True if scheduled successfully
        """
        try:
            # Get schedule configuration
            schedule_df = self.spark.sql(f"""
                SELECT *
                FROM {self.config.get('schedule_table', 'etl_schedule')}
                WHERE schedule_id = '{schedule_id}'
                  AND is_active = true
            """)
            
            if schedule_df.count() == 0:
                self.logger.log_error(f"Schedule {schedule_id} not found or inactive")
                return False
            
            schedule = schedule_df.first().asDict()
            
            # Execute ETL based on schedule type
            etl_type = schedule.get("etl_type", "FULL")
            
            result = self.execute_etl(
                source_type="INCREMENTAL" if etl_type == "INCREMENTAL" else "DATABASE",
                target_type="DATABASE"
            )
            
            self.logger.log_info(
                f"Scheduled ETL completed - Schedule: {schedule_id}, Status: {result['status']}"
            )
            
            return result["status"] in ["SUCCESS", "PARTIAL_SUCCESS"]
            
        except Exception as e:
            self.logger.log_error(f"Scheduled ETL failed: {str(e)}")
            return False