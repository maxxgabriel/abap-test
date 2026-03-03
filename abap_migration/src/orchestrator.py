"""
ETL Orchestrator - Coordinates complete ETL workflow execution
"""
from typing import Dict, Optional, List, Any
from datetime import datetime
from dataclasses import dataclass, asdict
import uuid
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.logger import ETLLogger
from src.exceptions import ETLOrchestrationError


@dataclass
class ETLResult:
    """Result of ETL execution"""
    run_id: str
    status: str
    start_time: datetime
    end_time: Optional[datetime]
    duration_seconds: Optional[int]
    records_extracted: int
    records_transformed: int
    records_loaded: int
    records_failed: int
    error_count: int
    warning_count: int
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)


class ETLOrchestrator:
    """
    Orchestrates complete ETL workflow execution with status tracking
    """
    
    def __init__(
        self,
        spark: SparkSession,
        run_type: str = "MANUAL",
        logger: Optional[ETLLogger] = None
    ):
        """
        Initialize orchestrator
        
        Args:
            spark: SparkSession instance
            run_type: Type of run (MANUAL, SCHEDULED, TEST)
            logger: Optional logger instance
        """
        self.spark = spark
        self.run_type = run_type
        self.logger = logger or ETLLogger()
        self.run_id = self._generate_run_id()
        
    def _generate_run_id(self) -> str:
        """
        Generate unique run ID
        
        Returns:
            Unique run identifier
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_suffix = str(uuid.uuid4())[:8].upper()
        run_id = f"RUN_{timestamp}_{unique_suffix}"
        
        self.logger.log_info(
            component="ORCHESTRATOR",
            message=f"Generated run ID: {run_id}"
        )
        
        return run_id
    
    def execute_etl(
        self,
        source_type: str = "DATABASE",
        target_type: str = "DATABASE",
        filter_condition: Optional[str] = None,
        batch_size: int = 1000,
        max_records: int = 0
    ) -> ETLResult:
        """
        Execute complete ETL workflow
        
        Args:
            source_type: Source data type
            target_type: Target data type
            filter_condition: Optional filter for extraction
            batch_size: Batch size for loading
            max_records: Maximum records to process (0 = unlimited)
            
        Returns:
            ETLResult with execution details
        """
        start_time = datetime.now()
        
        result = ETLResult(
            run_id=self.run_id,
            status="RUNNING",
            start_time=start_time,
            end_time=None,
            duration_seconds=None,
            records_extracted=0,
            records_transformed=0,
            records_loaded=0,
            records_failed=0,
            error_count=0,
            warning_count=0
        )
        
        self.logger.log_info(
            component="ORCHESTRATOR",
            message=f"ETL execution started - Run ID: {self.run_id}, Type: {self.run_type}"
        )
        
        self._log_run_start()
        
        try:
            # Step 1: Extract
            extractor = ETLExtractor(
                spark=self.spark,
                source_type=source_type,
                run_id=self.run_id,
                logger=self.logger
            )
            
            source_df = extractor.extract_data(
                filter_condition=filter_condition,
                max_records=max_records
            )
            
            result.records_extracted = source_df.count()
            
            if result.records_extracted == 0:
                self.logger.log_warning(
                    component="ORCHESTRATOR",
                    message="No data extracted - ETL process stopping"
                )
                result.status = "NO_DATA"
                result.end_time = datetime.now()
                result.duration_seconds = int((result.end_time - start_time).total_seconds())
                self._log_run_end(result)
                return result
            
            # Step 2: Transform
            transformer = ETLTransformer(
                spark=self.spark,
                run_id=self.run_id,
                logger=self.logger
            )
            
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
                result.end_time = datetime.now()
                result.duration_seconds = int((result.end_time - start_time).total_seconds())
                self._log_run_end(result)
                return result
            
            result.records_transformed = transformed_df.count()
            
            # Step 3: Load
            loader = ETLLoader(
                spark=self.spark,
                target_type=target_type,
                batch_size=batch_size,
                run_id=self.run_id,
                logger=self.logger
            )
            
            load_result = loader.load_data(
                df=transformed_df,
                mode="UPSERT"
            )
            
            result.records_loaded = load_result["success_count"]
            result.records_failed = load_result["error_count"]
            result.error_count = load_result["error_count"]
            
            # Determine final status
            if result.error_count == 0:
                result.status = "SUCCESS"
            elif result.records_loaded > 0:
                result.status = "PARTIAL_SUCCESS"
            else:
                result.status = "FAILED"
            
            result.end_time = datetime.now()
            result.duration_seconds = int((result.end_time - start_time).total_seconds())
            
            self._log_run_end(result)
            
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"ETL execution completed - Status: {result.status}, Duration: {result.duration_seconds}s"
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
            result.duration_seconds = int((result.end_time - start_time).total_seconds())
            result.error_count += 1
            
            self._log_run_end(result)
            
            raise ETLOrchestrationError(f"ETL execution failed: {str(e)}") from e
    
    def _log_run_start(self) -> None:
        """Log run start to tracking table"""
        try:
            run_log_schema = StructType([
                StructField("run_id", StringType(), False),
                StructField("run_type", StringType(), False),
                StructField("status", StringType(), False),
                StructField("start_time", TimestampType(), False),
                StructField("created_by", StringType(), True)
            ])
            
            run_log_data = [(
                self.run_id,
                self.run_type,
                "RUNNING",
                datetime.now(),
                "system"
            )]
            
            run_log_df = self.spark.createDataFrame(run_log_data, run_log_schema)
            
            run_log_df.write.mode("append").saveAsTable("etl_run_log")
            
        except Exception as e:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message="Failed to log run start",
                details=str(e)
            )
    
    def _log_run_end(self, result: ETLResult) -> None:
        """
        Log run completion to tracking table
        
        Args:
            result: ETL execution result
        """
        try:
            # Read existing run log
            run_log_df = self.spark.table("etl_run_log")
            
            # Update with completion details
            from pyspark.sql.functions import when, col, lit
            
            updated_df = run_log_df.withColumn(
                "status",
                when(col("run_id") == self.run_id, lit(result.status))
                .otherwise(col("status"))
            ).withColumn(
                "end_time",
                when(col("run_id") == self.run_id, lit(result.end_time))
                .otherwise(col("end_time"))
            ).withColumn(
                "duration_seconds",
                when(col("run_id") == self.run_id, lit(result.duration_seconds))
                .otherwise(col("duration_seconds"))
            ).withColumn(
                "records_extracted",
                when(col("run_id") == self.run_id, lit(result.records_extracted))
                .otherwise(col("records_extracted"))
            ).withColumn(
                "records_transformed",
                when(col("run_id") == self.run_id, lit(result.records_transformed))
                .otherwise(col("records_transformed"))
            ).withColumn(
                "records_loaded",
                when(col("run_id") == self.run_id, lit(result.records_loaded))
                .otherwise(col("records_loaded"))
            ).withColumn(
                "records_failed",
                when(col("run_id") == self.run_id, lit(result.records_failed))
                .otherwise(col("records_failed"))
            ).withColumn(
                "error_count",
                when(col("run_id") == self.run_id, lit(result.error_count))
                .otherwise(col("error_count"))
            )
            
            updated_df.write.mode("overwrite").saveAsTable("etl_run_log")
            
        except Exception as e:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message="Failed to log run end",
                details=str(e)
            )
    
    def get_run_statistics(self, run_id: Optional[str] = None) -> DataFrame:
        """
        Get run statistics
        
        Args:
            run_id: Optional specific run ID, otherwise returns all runs
            
        Returns:
            DataFrame with run statistics
        """
        try:
            run_log_df = self.spark.table("etl_run_log")
            
            if run_id:
                return run_log_df.filter(run_log_df.run_id == run_id)
            else:
                return run_log_df
                
        except Exception as e:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message="Failed to get run statistics",
                details=str(e)
            )
            raise
    
    def schedule_etl(self, schedule_id: str) -> bool:
        """
        Schedule ETL execution based on schedule configuration
        
        Args:
            schedule_id: Schedule identifier
            
        Returns:
            Success status
        """
        try:
            # Read schedule configuration
            schedule_df = self.spark.table("etl_schedule").filter(
                f"schedule_id = '{schedule_id}' AND is_active = true"
            )
            
            if schedule_df.count() == 0:
                self.logger.log_warning(
                    component="ORCHESTRATOR",
                    message=f"Schedule {schedule_id} not found or inactive"
                )
                return False
            
            schedule_config = schedule_df.first()
            
            # Execute ETL based on schedule
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"Executing scheduled ETL: {schedule_config.schedule_name}"
            )
            
            result = self.execute_etl(
                source_type=schedule_config.etl_type or "DATABASE"
            )
            
            return result.status in ["SUCCESS", "PARTIAL_SUCCESS"]
            
        except Exception as e:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message=f"Scheduled execution failed for {schedule_id}",
                details=str(e)
            )
            return False