"""
ETL Orchestration Module
Manages three-phase ETL execution with error handling and job scheduling
"""
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from dataclasses import dataclass, field
import uuid
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.utils.logger import ETLLogger
from src.utils.monitor import ETLMonitor
from src.utils.data_quality import DataQualityChecker


@dataclass
class ETLResult:
    """Container for ETL execution results"""
    run_id: str
    status: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration: Optional[int] = None
    records_extracted: int = 0
    records_transformed: int = 0
    records_loaded: int = 0
    records_failed: int = 0
    error_count: int = 0
    warning_count: int = 0
    errors: List[str] = field(default_factory=list)


class ETLOrchestrator:
    """
    Orchestrates the complete ETL pipeline execution
    Manages extraction, transformation, loading phases with error handling
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Dict[str, Any],
        run_type: str = "MANUAL"
    ):
        """
        Initialize ETL Orchestrator
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            run_type: Type of run (MANUAL, SCHEDULED, etc.)
        """
        self.spark = spark
        self.config = config
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        
        # Initialize components
        self.logger = ETLLogger(run_id=self.run_id)
        self.monitor = ETLMonitor(spark=spark, config=config)
        self.quality_checker = DataQualityChecker(spark=spark, run_id=self.run_id)
        
        self.logger.log_info(
            component="ORCHESTRATOR",
            message=f"Orchestrator initialized - Run ID: {self.run_id}"
        )
    
    def execute_etl(
        self,
        source_type: str = "DATABASE",
        target_type: str = "DATABASE",
        filter_condition: Optional[str] = None,
        batch_size: int = 1000,
        max_records: int = 0
    ) -> ETLResult:
        """
        Execute complete ETL pipeline
        
        Args:
            source_type: Type of data source
            target_type: Type of data target
            filter_condition: Optional filter for extraction
            batch_size: Batch size for loading
            max_records: Maximum records to process (0 for unlimited)
            
        Returns:
            ETLResult object with execution details
        """
        start_time = datetime.now()
        result = ETLResult(
            run_id=self.run_id,
            status="RUNNING",
            start_time=start_time
        )
        
        self.logger.log_info(
            component="ORCHESTRATOR",
            message=f"ETL execution started - Run ID: {self.run_id}"
        )
        
        self._log_run_start(start_time)
        
        try:
            # Phase 1: Extract
            result = self._execute_extract(
                result=result,
                source_type=source_type,
                filter_condition=filter_condition,
                max_records=max_records
            )
            
            if result.records_extracted == 0:
                result.status = "NO_DATA"
                self.logger.log_warning(
                    component="ORCHESTRATOR",
                    message="No data extracted - ETL process stopping"
                )
                return result
            
            # Phase 2: Transform
            result = self._execute_transform(result)
            
            if result.status == "VALIDATION_FAILED":
                return result
            
            # Phase 3: Load
            result = self._execute_load(
                result=result,
                target_type=target_type,
                batch_size=batch_size
            )
            
            # Finalize
            end_time = datetime.now()
            result.end_time = end_time
            result.duration = int((end_time - start_time).total_seconds())
            
            # Determine final status
            if result.error_count == 0:
                result.status = "SUCCESS"
            elif result.records_loaded > 0:
                result.status = "PARTIAL_SUCCESS"
            else:
                result.status = "FAILED"
            
            self._log_run_end(result)
            
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"ETL execution completed - Status: {result.status}"
            )
            
        except Exception as e:
            result.status = "ERROR"
            result.end_time = datetime.now()
            result.duration = int((result.end_time - start_time).total_seconds())
            result.errors.append(str(e))
            
            self._handle_orchestration_error(e)
            self._log_run_end(result)
        
        return result
    
    def _execute_extract(
        self,
        result: ETLResult,
        source_type: str,
        filter_condition: Optional[str],
        max_records: int
    ) -> ETLResult:
        """Execute extraction phase"""
        self.logger.log_info(
            component="ORCHESTRATOR",
            message="Starting extraction phase"
        )
        
        extractor = ETLExtractor(
            spark=self.spark,
            config=self.config,
            source_type=source_type,
            run_id=self.run_id
        )
        
        df_source = extractor.extract_data(
            filter_condition=filter_condition,
            max_records=max_records
        )
        
        result.records_extracted = df_source.count()
        
        # Cache for transformation
        df_source.cache()
        self._cached_extract_df = df_source
        
        self.logger.log_info(
            component="ORCHESTRATOR",
            message=f"Extraction complete - Records: {result.records_extracted}"
        )
        
        return result
    
    def _execute_transform(self, result: ETLResult) -> ETLResult:
        """Execute transformation phase"""
        self.logger.log_info(
            component="ORCHESTRATOR",
            message="Starting transformation phase"
        )
        
        transformer = ETLTransformer(
            spark=self.spark,
            config=self.config,
            run_id=self.run_id
        )
        
        df_transformed = transformer.transform_data(self._cached_extract_df)
        
        # Validate transformed data
        validation_result = transformer.validate_data(df_transformed)
        
        if not validation_result["is_valid"]:
            result.status = "VALIDATION_FAILED"
            result.error_count = len(validation_result["errors"])
            result.errors.extend(validation_result["errors"])
            
            self.logger.log_error(
                component="ORCHESTRATOR",
                message=f"Validation failed - {result.error_count} errors"
            )
            return result
        
        # Run quality checks
        quality_checks = self.quality_checker.perform_quality_checks(df_transformed)
        failed_checks = [check for check in quality_checks if not check["passed"]]
        
        if failed_checks:
            result.warning_count = len(failed_checks)
            self.logger.log_warning(
                component="ORCHESTRATOR",
                message=f"Quality checks: {len(failed_checks)} warnings"
            )
        
        result.records_transformed = df_transformed.count()
        
        # Cache for loading
        df_transformed.cache()
        self._cached_transform_df = df_transformed
        
        # Unpersist extraction cache
        self._cached_extract_df.unpersist()
        
        self.logger.log_info(
            component="ORCHESTRATOR",
            message=f"Transformation complete - Records: {result.records_transformed}"
        )
        
        return result
    
    def _execute_load(
        self,
        result: ETLResult,
        target_type: str,
        batch_size: int
    ) -> ETLResult:
        """Execute loading phase"""
        self.logger.log_info(
            component="ORCHESTRATOR",
            message="Starting loading phase"
        )
        
        loader = ETLLoader(
            spark=self.spark,
            config=self.config,
            target_type=target_type,
            batch_size=batch_size,
            run_id=self.run_id
        )
        
        load_result = loader.load_data(
            df=self._cached_transform_df,
            mode="upsert"
        )
        
        result.records_loaded = load_result["success_count"]
        result.records_failed = load_result["error_count"]
        result.error_count += load_result["error_count"]
        result.errors.extend(load_result["errors"])
        
        # Unpersist transformation cache
        self._cached_transform_df.unpersist()
        
        self.logger.log_info(
            component="ORCHESTRATOR",
            message=f"Loading complete - Loaded: {result.records_loaded}, Failed: {result.records_failed}"
        )
        
        return result
    
    def schedule_etl(self, schedule_id: str) -> bool:
        """
        Schedule ETL execution based on schedule configuration
        
        Args:
            schedule_id: Schedule identifier
            
        Returns:
            Success status
        """
        try:
            # Load schedule configuration
            schedule_config = self._get_schedule_config(schedule_id)
            
            if not schedule_config:
                self.logger.log_error(
                    component="ORCHESTRATOR",
                    message=f"Schedule not found: {schedule_id}"
                )
                return False
            
            if not schedule_config.get("is_active"):
                self.logger.log_warning(
                    component="ORCHESTRATOR",
                    message=f"Schedule is not active: {schedule_id}"
                )
                return False
            
            # Execute ETL with schedule parameters
            result = self.execute_etl(
                source_type=schedule_config.get("source_type", "DATABASE"),
                target_type=schedule_config.get("target_type", "DATABASE"),
                filter_condition=schedule_config.get("filter_condition"),
                batch_size=schedule_config.get("batch_size", 1000),
                max_records=schedule_config.get("max_records", 0)
            )
            
            return result.status in ["SUCCESS", "PARTIAL_SUCCESS"]
            
        except Exception as e:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message=f"Schedule execution failed: {str(e)}"
            )
            return False
    
    def get_run_statistics(self, run_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get run statistics
        
        Args:
            run_id: Specific run ID (None for all runs)
            
        Returns:
            List of run statistics
        """
        return self.monitor.get_performance_metrics(
            run_id=run_id,
            days_back=30
        )
    
    def _generate_run_id(self) -> str:
        """Generate unique run ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"RUN_{timestamp}_{unique_id}"
    
    def _log_run_start(self, start_time: datetime):
        """Log run start to database"""
        try:
            run_log_schema = StructType([
                StructField("run_id", StringType(), False),
                StructField("run_type", StringType(), True),
                StructField("status", StringType(), False),
                StructField("start_time", TimestampType(), False),
                StructField("created_by", StringType(), True)
            ])
            
            log_data = [(
                self.run_id,
                self.run_type,
                "RUNNING",
                start_time,
                self.config.get("user", "system")
            )]
            
            df_log = self.spark.createDataFrame(log_data, schema=run_log_schema)
            
            df_log.write \
                .format(self.config.get("db_format", "jdbc")) \
                .mode("append") \
                .option("url", self.config["db_url"]) \
                .option("dbtable", self.config.get("run_log_table", "etl_run_log")) \
                .option("user", self.config["db_user"]) \
                .option("password", self.config["db_password"]) \
                .save()
                
        except Exception as e:
            self.logger.log_warning(
                component="ORCHESTRATOR",
                message=f"Failed to log run start: {str(e)}"
            )
    
    def _log_run_end(self, result: ETLResult):
        """Log run completion to database"""
        try:
            # Read existing log entry
            df_log = self.spark.read \
                .format(self.config.get("db_format", "jdbc")) \
                .option("url", self.config["db_url"]) \
                .option("dbtable", self.config.get("run_log_table", "etl_run_log")) \
                .option("user", self.config["db_user"]) \
                .option("password", self.config["db_password"]) \
                .load() \
                .filter(f"run_id = '{self.run_id}'")
            
            # Update with completion details
            from pyspark.sql import functions as F
            
            df_updated = df_log.withColumn("status", F.lit(result.status)) \
                .withColumn("end_time", F.lit(result.end_time)) \
                .withColumn("duration", F.lit(result.duration)) \
                .withColumn("records_extracted", F.lit(result.records_extracted)) \
                .withColumn("records_transformed", F.lit(result.records_transformed)) \
                .withColumn("records_loaded", F.lit(result.records_loaded)) \
                .withColumn("records_failed", F.lit(result.records_failed)) \
                .withColumn("error_count", F.lit(result.error_count))
            
            # Write back (this is simplified - in production, use proper update mechanism)
            df_updated.write \
                .format(self.config.get("db_format", "jdbc")) \
                .mode("overwrite") \
                .option("url", self.config["db_url"]) \
                .option("dbtable", f"{self.config.get('run_log_table', 'etl_run_log')}_temp") \
                .option("user", self.config["db_user"]) \
                .option("password", self.config["db_password"]) \
                .save()
                
        except Exception as e:
            self.logger.log_warning(
                component="ORCHESTRATOR",
                message=f"Failed to log run end: {str(e)}"
            )
    
    def _handle_orchestration_error(self, error: Exception):
        """Handle orchestration errors"""
        self.logger.log_error(
            component="ORCHESTRATOR",
            message="Orchestration error occurred",
            details=str(error)
        )
        
        # Log to error table
        try:
            error_schema = StructType([
                StructField("run_id", StringType(), False),
                StructField("component", StringType(), False),
                StructField("error_message", StringType(), False),
                StructField("error_details", StringType(), True),
                StructField("created_at", TimestampType(), False),
                StructField("resolved", StringType(), False)
            ])
            
            error_data = [(
                self.run_id,
                "ORCHESTRATOR",
                "ETL execution failed",
                str(error),
                datetime.now(),
                "N"
            )]
            
            df_error = self.spark.createDataFrame(error_data, schema=error_schema)
            
            df_error.write \
                .format(self.config.get("db_format", "jdbc")) \
                .mode("append") \
                .option("url", self.config["db_url"]) \
                .option("dbtable", self.config.get("error_log_table", "etl_error_log")) \
                .option("user", self.config["db_user"]) \
                .option("password", self.config["db_password"]) \
                .save()
                
        except Exception as log_error:
            self.logger.log_warning(
                component="ORCHESTRATOR",
                message=f"Failed to log error: {str(log_error)}"
            )
    
    def _get_schedule_config(self, schedule_id: str) -> Optional[Dict[str, Any]]:
        """Get schedule configuration from database"""
        try:
            df_schedule = self.spark.read \
                .format(self.config.get("db_format", "jdbc")) \
                .option("url", self.config["db_url"]) \
                .option("dbtable", self.config.get("schedule_table", "etl_schedule")) \
                .option("user", self.config["db_user"]) \
                .option("password", self.config["db_password"]) \
                .load() \
                .filter(f"schedule_id = '{schedule_id}'")
            
            if df_schedule.count() == 0:
                return None
            
            schedule_row = df_schedule.first()
            return schedule_row.asDict()
            
        except Exception as e:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message=f"Failed to get schedule config: {str(e)}"
            )
            return None