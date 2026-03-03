"""
ETL Orchestrator Module
Manages three-phase ETL execution with error handling and job scheduling
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import traceback
import uuid

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.utils.logger import ETLLogger
from src.utils.monitor import ETLMonitor
from src.utils.data_quality import DataQualityChecker


@dataclass
class ETLResult:
    """ETL execution result"""
    run_id: str
    status: str
    start_time: datetime
    end_time: Optional[datetime]
    duration_seconds: float
    records_extracted: int
    records_transformed: int
    records_loaded: int
    records_failed: int
    error_count: int
    warning_count: int
    error_message: Optional[str] = None


class ETLOrchestrator:
    """
    Orchestrates three-phase ETL execution with comprehensive error handling
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_type: str = "MANUAL"):
        """
        Initialize ETL orchestrator
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            run_type: Type of run (MANUAL, SCHEDULED, INCREMENTAL)
        """
        self.spark = spark
        self.config = config
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        
        self.logger = ETLLogger(run_id=self.run_id)
        self.monitor = ETLMonitor(spark=spark)
        
        # Initialize ETL components
        self.extractor = ETLExtractor(spark=spark, config=config, run_id=self.run_id)
        self.transformer = ETLTransformer(spark=spark, config=config, run_id=self.run_id)
        self.loader = ETLLoader(spark=spark, config=config, run_id=self.run_id)
        self.quality_checker = DataQualityChecker(spark=spark, run_id=self.run_id)
        
        self.logger.log_info(
            component="ORCHESTRATOR",
            message=f"Orchestrator initialized - Run ID: {self.run_id}, Type: {run_type}"
        )
    
    def _generate_run_id(self) -> str:
        """Generate unique run ID"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"RUN_{timestamp}_{unique_id}"
    
    def execute_etl(
        self,
        source_type: str = "DATABASE",
        target_type: str = "DATABASE",
        filter_condition: Optional[str] = None,
        batch_size: int = 1000,
        max_records: int = 0,
        validate_data: bool = True,
        enable_reconciliation: bool = True
    ) -> ETLResult:
        """
        Execute complete ETL pipeline
        
        Args:
            source_type: Source data type (DATABASE, STAGING, INCREMENTAL)
            target_type: Target data type (DATABASE, PARQUET, DELTA)
            filter_condition: Optional filter for extraction
            batch_size: Batch size for loading
            max_records: Maximum records to process (0 = unlimited)
            validate_data: Enable data validation
            enable_reconciliation: Enable reconciliation check
            
        Returns:
            ETLResult with execution statistics
        """
        start_time = datetime.now()
        result = ETLResult(
            run_id=self.run_id,
            status="RUNNING",
            start_time=start_time,
            end_time=None,
            duration_seconds=0.0,
            records_extracted=0,
            records_transformed=0,
            records_loaded=0,
            records_failed=0,
            error_count=0,
            warning_count=0
        )
        
        try:
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"Starting ETL execution - Source: {source_type}, Target: {target_type}"
            )
            
            # Log run start to database
            self._log_run_start(start_time)
            
            # Phase 1: Extract
            self.logger.log_info(component="ORCHESTRATOR", message="Phase 1: Extraction started")
            extracted_df = self._execute_extraction(
                source_type=source_type,
                filter_condition=filter_condition,
                max_records=max_records
            )
            
            if extracted_df is None or extracted_df.count() == 0:
                self.logger.log_warning(
                    component="ORCHESTRATOR",
                    message="No data extracted - ETL process stopping"
                )
                result.status = "NO_DATA"
                result.end_time = datetime.now()
                result.duration_seconds = (result.end_time - start_time).total_seconds()
                return result
            
            result.records_extracted = extracted_df.count()
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"Phase 1 complete: {result.records_extracted} records extracted"
            )
            
            # Phase 2: Transform
            self.logger.log_info(component="ORCHESTRATOR", message="Phase 2: Transformation started")
            transformed_df = self._execute_transformation(extracted_df)
            
            if transformed_df is None:
                raise Exception("Transformation returned None")
            
            result.records_transformed = transformed_df.count()
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"Phase 2 complete: {result.records_transformed} records transformed"
            )
            
            # Phase 2.5: Validation (if enabled)
            if validate_data:
                self.logger.log_info(component="ORCHESTRATOR", message="Data validation started")
                validation_result = self._execute_validation(transformed_df)
                
                if not validation_result["is_valid"]:
                    error_count = validation_result["error_count"]
                    self.logger.log_error(
                        component="ORCHESTRATOR",
                        message=f"Validation failed - {error_count} errors found"
                    )
                    result.status = "VALIDATION_FAILED"
                    result.error_count = error_count
                    result.end_time = datetime.now()
                    result.duration_seconds = (result.end_time - start_time).total_seconds()
                    return result
            
            # Phase 3: Load
            self.logger.log_info(component="ORCHESTRATOR", message="Phase 3: Loading started")
            load_result = self._execute_loading(
                df=transformed_df,
                target_type=target_type,
                batch_size=batch_size
            )
            
            result.records_loaded = load_result["success_count"]
            result.records_failed = load_result["error_count"]
            result.error_count = load_result["error_count"]
            
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"Phase 3 complete: {result.records_loaded} loaded, {result.records_failed} failed"
            )
            
            # Phase 4: Reconciliation (if enabled)
            if enable_reconciliation and result.records_loaded > 0:
                self.logger.log_info(component="ORCHESTRATOR", message="Reconciliation started")
                reconciliation_passed = self._execute_reconciliation(
                    source_df=transformed_df,
                    loaded_count=result.records_loaded
                )
                
                if not reconciliation_passed:
                    self.logger.log_warning(
                        component="ORCHESTRATOR",
                        message="Reconciliation detected discrepancies"
                    )
                    result.warning_count += 1
            
            # Determine final status
            if result.records_failed == 0:
                result.status = "SUCCESS"
            elif result.records_loaded > 0:
                result.status = "PARTIAL_SUCCESS"
            else:
                result.status = "FAILED"
            
            result.end_time = datetime.now()
            result.duration_seconds = (result.end_time - start_time).total_seconds()
            
            # Log completion
            self._log_run_end(result)
            
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"ETL execution completed - Status: {result.status}, Duration: {result.duration_seconds:.2f}s"
            )
            
            return result
            
        except Exception as e:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message="ETL execution failed",
                details=str(e),
                stack_trace=traceback.format_exc()
            )
            
            result.status = "ERROR"
            result.error_count += 1
            result.error_message = str(e)
            result.end_time = datetime.now()
            result.duration_seconds = (result.end_time - start_time).total_seconds()
            
            self._log_run_end(result)
            
            # Re-raise for Airflow/scheduler awareness
            raise
    
    def _execute_extraction(
        self,
        source_type: str,
        filter_condition: Optional[str],
        max_records: int
    ) -> Optional[DataFrame]:
        """Execute extraction phase"""
        try:
            return self.extractor.extract_data(
                source_type=source_type,
                filter_condition=filter_condition,
                max_records=max_records
            )
        except Exception as e:
            self.logger.log_error(
                component="EXTRACTION",
                message="Extraction phase failed",
                details=str(e)
            )
            raise
    
    def _execute_transformation(self, df: DataFrame) -> Optional[DataFrame]:
        """Execute transformation phase"""
        try:
            return self.transformer.transform_data(df)
        except Exception as e:
            self.logger.log_error(
                component="TRANSFORMATION",
                message="Transformation phase failed",
                details=str(e)
            )
            raise
    
    def _execute_validation(self, df: DataFrame) -> Dict[str, Any]:
        """Execute validation phase"""
        try:
            quality_checks = self.quality_checker.perform_quality_checks(df)
            
            is_valid = all(check["passed"] for check in quality_checks)
            error_count = sum(
                check["failed_count"] for check in quality_checks if not check["passed"]
            )
            
            return {
                "is_valid": is_valid,
                "error_count": error_count,
                "checks": quality_checks
            }
        except Exception as e:
            self.logger.log_error(
                component="VALIDATION",
                message="Validation phase failed",
                details=str(e)
            )
            raise
    
    def _execute_loading(
        self,
        df: DataFrame,
        target_type: str,
        batch_size: int
    ) -> Dict[str, int]:
        """Execute loading phase"""
        try:
            return self.loader.load_data(
                df=df,
                target_type=target_type,
                mode="upsert",
                batch_size=batch_size
            )
        except Exception as e:
            self.logger.log_error(
                component="LOADING",
                message="Loading phase failed",
                details=str(e)
            )
            raise
    
    def _execute_reconciliation(
        self,
        source_df: DataFrame,
        loaded_count: int
    ) -> bool:
        """Execute reconciliation check"""
        try:
            expected_count = source_df.count()
            
            if loaded_count != expected_count:
                self.logger.log_warning(
                    component="RECONCILIATION",
                    message=f"Count mismatch - Expected: {expected_count}, Loaded: {loaded_count}"
                )
                return False
            
            return True
        except Exception as e:
            self.logger.log_error(
                component="RECONCILIATION",
                message="Reconciliation failed",
                details=str(e)
            )
            return False
    
    def _log_run_start(self, start_time: datetime):
        """Log run start to database"""
        try:
            log_data = [
                (
                    self.run_id,
                    self.run_type,
                    "RUNNING",
                    start_time,
                    None,
                    0,
                    0,
                    0,
                    0
                )
            ]
            
            schema = StructType([
                StructField("run_id", StringType(), False),
                StructField("run_type", StringType(), False),
                StructField("status", StringType(), False),
                StructField("start_time", TimestampType(), False),
                StructField("end_time", TimestampType(), True),
                StructField("duration", IntegerType(), True),
                StructField("records_extracted", IntegerType(), True),
                StructField("records_loaded", IntegerType(), True),
                StructField("records_failed", IntegerType(), True)
            ])
            
            log_df = self.spark.createDataFrame(log_data, schema=schema)
            
            # Write to run log table
            log_df.write \
                .format("jdbc") \
                .option("url", self.config["database"]["url"]) \
                .option("dbtable", self.config["database"]["run_log_table"]) \
                .option("user", self.config["database"]["user"]) \
                .option("password", self.config["database"]["password"]) \
                .mode("append") \
                .save()
                
        except Exception as e:
            self.logger.log_warning(
                component="ORCHESTRATOR",
                message="Failed to log run start",
                details=str(e)
            )
    
    def _log_run_end(self, result: ETLResult):
        """Log run completion to database"""
        try:
            log_data = [
                (
                    result.run_id,
                    self.run_type,
                    result.status,
                    result.start_time,
                    result.end_time,
                    int(result.duration_seconds),
                    result.records_extracted,
                    result.records_loaded,
                    result.records_failed
                )
            ]
            
            schema = StructType([
                StructField("run_id", StringType(), False),
                StructField("run_type", StringType(), False),
                StructField("status", StringType(), False),
                StructField("start_time", TimestampType(), False),
                StructField("end_time", TimestampType(), True),
                StructField("duration", IntegerType(), True),
                StructField("records_extracted", IntegerType(), True),
                StructField("records_loaded", IntegerType(), True),
                StructField("records_failed", IntegerType(), True)
            ])
            
            log_df = self.spark.createDataFrame(log_data, schema=schema)
            
            # Update run log table
            log_df.write \
                .format("jdbc") \
                .option("url", self.config["database"]["url"]) \
                .option("dbtable", self.config["database"]["run_log_table"]) \
                .option("user", self.config["database"]["user"]) \
                .option("password", self.config["database"]["password"]) \
                .mode("append") \
                .save()
                
        except Exception as e:
            self.logger.log_warning(
                component="ORCHESTRATOR",
                message="Failed to log run end",
                details=str(e)
            )
    
    def get_run_statistics(self, days_back: int = 7) -> List[Dict[str, Any]]:
        """
        Get run statistics for specified period
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            List of run statistics
        """
        try:
            cutoff_date = datetime.now() - timedelta(days=days_back)
            
            stats_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.config["database"]["url"]) \
                .option("dbtable", self.config["database"]["run_log_table"]) \
                .option("user", self.config["database"]["user"]) \
                .option("password", self.config["database"]["password"]) \
                .load()
            
            # Filter by date
            from pyspark.sql.functions import col
            filtered_df = stats_df.filter(col("start_time") >= cutoff_date)
            
            return [row.asDict() for row in filtered_df.collect()]
            
        except Exception as e:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message="Failed to retrieve run statistics",
                details=str(e)
            )
            return []


def create_orchestrator(
    spark: SparkSession,
    config: Dict[str, Any],
    run_type: str = "MANUAL"
) -> ETLOrchestrator:
    """
    Factory function to create ETL orchestrator
    
    Args:
        spark: SparkSession instance
        config: Configuration dictionary
        run_type: Type of run
        
    Returns:
        ETLOrchestrator instance
    """
    return ETLOrchestrator(spark=spark, config=config, run_type=run_type)