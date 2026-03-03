===FILE: src/extract.py===
"""
ETL Data Extraction Module
Extracts data from various sources with support for full and incremental loads.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from datetime import datetime
from typing import Optional, Dict, Any
import logging

from src.utils.logger import ETLLogger
from src.config_manager import ConfigManager


class ETLExtractor:
    """Handles data extraction from various sources"""
    
    SCHEMA = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("value", DecimalType(15, 2), True),
        StructField("status", StringType(), True),
        StructField("category", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("created_by", StringType(), True),
        StructField("changed_at", TimestampType(), True),
        StructField("changed_by", StringType(), True)
    ])
    
    def __init__(self, spark: SparkSession, run_id: str, config: ConfigManager):
        """
        Initialize extractor
        
        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
            config: Configuration manager
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = ETLLogger.get_instance()
        self.source_type = config.get("source_type", "DATABASE")
        
    def extract_data(
        self, 
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data based on source type
        
        Args:
            filter_condition: Optional SQL filter condition
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.log_info(
            component="EXTRACTOR",
            message=f"Starting extraction - Source: {self.source_type}"
        )
        
        try:
            if self.source_type == "DATABASE":
                df = self.extract_from_database(filter_condition)
            elif self.source_type == "STAGING":
                df = self.extract_from_staging()
            elif self.source_type == "INCREMENTAL":
                df = self.extract_incremental()
            else:
                df = self.extract_from_database(filter_condition)
            
            # Apply max records limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.log_info(
                component="EXTRACTOR",
                message=f"Extracted {record_count} records"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="EXTRACTOR",
                message="Extraction failed",
                details=str(e)
            )
            raise
    
    def extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """
        Extract from database source
        
        Args:
            filter_condition: Optional SQL WHERE clause
            
        Returns:
            DataFrame with extracted data
        """
        source_path = self.config.get("source_path", "data/source")
        
        try:
            # Read from parquet/delta table
            df = self.spark.read.schema(self.SCHEMA).parquet(source_path)
            
            # Apply filter if provided
            if filter_condition:
                df = df.filter(filter_condition)
            
            return df
            
        except Exception as e:
            self.logger.log_warning(
                component="EXTRACTOR",
                message=f"Database extraction warning: {str(e)}"
            )
            # Return empty DataFrame with schema
            return self.spark.createDataFrame([], self.SCHEMA)
    
    def extract_from_staging(self) -> DataFrame:
        """
        Extract from staging area
        
        Returns:
            DataFrame with staged data
        """
        staging_path = self.config.get("staging_path", "data/staging")
        
        try:
            df = self.spark.read.parquet(staging_path)
            df = df.filter(f"run_id = '{self.run_id}' AND status = 'READY'")
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="EXTRACTOR",
                message=f"Staging extraction failed: {str(e)}"
            )
            return self.spark.createDataFrame([], self.SCHEMA)
    
    def extract_incremental(self) -> DataFrame:
        """
        Extract only changed records since last successful run
        
        Returns:
            DataFrame with incremental data
        """
        try:
            # Get last successful run time from run log
            last_run_time = self._get_last_run_time()
            
            if last_run_time:
                source_path = self.config.get("source_path", "data/source")
                df = self.spark.read.schema(self.SCHEMA).parquet(source_path)
                df = df.filter(df.changed_at > last_run_time)
                
                self.logger.log_info(
                    component="EXTRACTOR",
                    message=f"Incremental extraction from {last_run_time}"
                )
                return df
            else:
                # No previous run, perform full extraction
                self.logger.log_warning(
                    component="EXTRACTOR",
                    message="No previous run found, performing full extraction"
                )
                return self.extract_from_database()
                
        except Exception as e:
            self.logger.log_error(
                component="EXTRACTOR",
                message=f"Incremental extraction failed: {str(e)}"
            )
            # Fallback to full extraction
            return self.extract_from_database()
    
    def _get_last_run_time(self) -> Optional[datetime]:
        """
        Get timestamp of last successful run
        
        Returns:
            Timestamp of last run or None
        """
        try:
            run_log_path = self.config.get("run_log_path", "data/run_log")
            df = self.spark.read.parquet(run_log_path)
            df = df.filter("status = 'SUCCESS'").orderBy("end_time", ascending=False)
            
            last_run = df.select("end_time").first()
            if last_run:
                return last_run["end_time"]
            return None
            
        except Exception:
            return None


===FILE: src/transform.py===
"""
ETL Data Transformation Module
Applies business rules, enrichment, and validation to extracted data.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, trim, when, lit, current_timestamp, 
    round as spark_round, regexp_replace, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from typing import Tuple, List
import logging

from src.utils.logger import ETLLogger
from src.config_manager import ConfigManager


class ETLTransformer:
    """Handles data transformation and business rules"""
    
    SCHEMA = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("value", DecimalType(15, 2), True),
        StructField("transformed_value", DecimalType(15, 2), True),
        StructField("status", StringType(), True),
        StructField("category", StringType(), True),
        StructField("priority", IntegerType(), True),
        StructField("etl_run_id", StringType(), True),
        StructField("processed_at", TimestampType(), True),
        StructField("processed_by", StringType(), True)
    ])
    
    def __init__(self, spark: SparkSession, run_id: str, config: ConfigManager):
        """
        Initialize transformer
        
        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
            config: Configuration manager
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.logger = ETLLogger.get_instance()
        
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply all transformations to source data
        
        Args:
            source_df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.log_info(
            component="TRANSFORMER",
            message="Starting transformation"
        )
        
        try:
            # Apply transformations
            df = self._apply_basic_transformations(source_df)
            df = self._calculate_derived_values(df)
            df = self._calculate_priority(df)
            df = self._apply_business_rules(df)
            df = self._enrich_data(df)
            
            record_count = df.count()
            self.logger.log_info(
                component="TRANSFORMER",
                message=f"Transformed {record_count} records"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_error(
                component="TRANSFORMER",
                message="Transformation failed",
                details=str(e)
            )
            raise
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic transformations like cleaning and formatting"""
        return df.select(
            col("id"),
            upper(trim(regexp_replace(col("name"), r'\s+', ' '))).alias("name"),
            col("value"),
            col("status"),
            coalesce(col("category"), lit("UNCATEGORIZED")).alias("category"),
            col("source_system"),
            col("created_at"),
            col("created_by"),
            col("changed_at"),
            col("changed_by"),
            lit(self.run_id).alias("etl_run_id"),
            current_timestamp().alias("processed_at"),
            lit(self.config.get("processed_by", "ETL_SYSTEM")).alias("processed_by")
        )
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """Calculate transformed values based on category"""
        premium_multiplier = float(self.config.get("premium_multiplier", "1.5"))
        
        return df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("value") * premium_multiplier)
            .when(col("category") == "VIP", col("value") * 1.3)
            .when(col("category") == "STANDARD", col("value") * 1.0)
            .when(col("category") == "BASIC", col("value") * 0.8)
            .when(col("category") == "TRIAL", col("value") * 0.5)
            .otherwise(col("value"))
        )
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """Calculate priority based on value and category"""
        return df.withColumn(
            "priority",
            when(col("value") >= 1000, lit(1))
            .when(col("value") >= 750, lit(2))
            .when(col("value") >= 500, lit(3))
            .when(col("value") >= 250, lit(4))
            .otherwise(lit(5))
        )
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to set status and validate data"""
        self.logger.log_info(
            component="TRANSFORMER",
            message="Applying business rules"
        )
        
        df = df.withColumn(
            "status",
            when(col("value").isNull(), lit("INVALID"))
            .when(col("transformed_value") >= 750, lit("HIGH_VALUE"))
            .when(col("transformed_value") >= 300, lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )
        
        # Override priority for high value items
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, lit(1))
            .otherwise(col("priority"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional calculated fields"""
        self.logger.log_info(
            component="TRANSFORMER",
            message="Enriching data"
        )
        
        # Round transformed values
        df = df.withColumn(
            "transformed_value",
            spark_round(col("transformed_value"), 2)
        )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, error_list)
        """
        errors = []
        
        # Check for required fields
        null_ids = df.filter(col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with missing ID")
        
        null_names = df.filter(col("name").isNull()).count()
        if null_names > 0:
            errors.append(f"{null_names} records with missing name")
        
        # Check for invalid values
        negative_values = df.filter(col("value") < 0).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative values")
        
        invalid_priority = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.log_info(
                component="TRANSFORMER",
                message="Validation passed"
            )
        else:
            self.logger.log_error(
                component="TRANSFORMER",
                message=f"Validation failed: {len(errors)} errors",
                details="; ".join(errors)
            )
        
        return is_valid, errors


===FILE: src/load.py===
"""
ETL Data Loading Module
Loads transformed data to target destination with batch processing and reconciliation.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from typing import Dict, Any
import logging

from src.utils.logger import ETLLogger
from src.config_manager import ConfigManager


class ETLLoader:
    """Handles data loading to target destination"""
    
    def __init__(
        self, 
        spark: SparkSession, 
        run_id: str, 
        config: ConfigManager,
        batch_size: int = 1000
    ):
        """
        Initialize loader
        
        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
            config: Configuration manager
            batch_size: Batch size for loading
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.batch_size = batch_size
        self.logger = ETLLogger.get_instance()
        self.target_type = config.get("target_type", "DATABASE")
        
    def load_data(
        self, 
        df: DataFrame, 
        mode: str = "INSERT"
    ) -> Dict[str, Any]:
        """
        Load data to target
        
        Args:
            df: DataFrame to load
            mode: Load mode (INSERT, UPDATE, UPSERT)
            
        Returns:
            Dictionary with load results
        """
        self.logger.log_info(
            component="LOADER",
            message=f"Starting load - Mode: {mode}, Batch size: {self.batch_size}"
        )
        
        success_count = 0
        error_count = 0
        errors = []
        
        try:
            total_count = df.count()
            
            if mode.upper() == "UPSERT":
                success_count = self._load_upsert(df)
            elif mode.upper() == "UPDATE":
                success_count = self._load_update(df)
            else:  # INSERT
                success_count = self._load_insert(df)
            
            error_count = total_count - success_count
            
            # Reconcile data
            if self.config.get("enable_reconciliation", "true").lower() == "true":
                reconciled = self._reconcile_data(df)
                if not reconciled:
                    self.logger.log_warning(
                        component="LOADER",
                        message="Data reconciliation failed"
                    )
                    errors.append("Reconciliation check failed")
            
            self.logger.log_info(
                component="LOADER",
                message=f"Load complete - Success: {success_count}, Errors: {error_count}"
            )
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Load failed",
                details=str(e)
            )
            error_count = df.count() if df else 0
            errors.append(str(e))
        
        return {
            "success_count": success_count,
            "error_count": error_count,
            "total_count": df.count() if df else 0,
            "errors": errors
        }
    
    def _load_insert(self, df: DataFrame) -> int:
        """
        Insert new records
        
        Args:
            df: DataFrame to insert
            
        Returns:
            Number of successfully loaded records
        """
        target_path = self.config.get("target_path", "data/target")
        
        try:
            df.write.mode("append").parquet(target_path)
            return df.count()
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message=f"Insert failed: {str(e)}"
            )
            return 0
    
    def _load_update(self, df: DataFrame) -> int:
        """
        Update existing records
        
        Args:
            df: DataFrame to update
            
        Returns:
            Number of successfully loaded records
        """
        target_path = self.config.get("target_path", "data/target")
        
        try:
            # Read existing data
            existing_df = self.spark.read.parquet(target_path)
            
            # Remove old records with matching IDs
            updated_df = existing_df.join(
                df.select("id"), 
                on="id", 
                how="left_anti"
            )
            
            # Add new records
            updated_df = updated_df.union(df)
            
            # Write back
            updated_df.write.mode("overwrite").parquet(target_path)
            return df.count()
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message=f"Update failed: {str(e)}"
            )
            return 0
    
    def _load_upsert(self, df: DataFrame) -> int:
        """
        Insert or update records (upsert)
        
        Args:
            df: DataFrame to upsert
            
        Returns:
            Number of successfully loaded records
        """
        target_path = self.config.get("target_path", "data/target")
        
        try:
            # Try to read existing data
            try:
                existing_df = self.spark.read.parquet(target_path)
                
                # Remove records with matching IDs
                merged_df = existing_df.join(
                    df.select("id"),
                    on="id",
                    how="left_anti"
                ).union(df)
                
                # Write merged data
                merged_df.write.mode("overwrite").parquet(target_path)
                
            except Exception:
                # Target doesn't exist, perform insert
                df.write.mode("append").parquet(target_path)
            
            return df.count()
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message=f"Upsert failed: {str(e)}"
            )
            return 0
    
    def _reconcile_data(self, df: DataFrame) -> bool:
        """
        Reconcile loaded data with source
        
        Args:
            df: Source DataFrame
            
        Returns:
            True if reconciliation passes
        """
        try:
            target_path = self.config.get("target_path", "data/target")
            loaded_df = self.spark.read.parquet(target_path)
            
            # Filter to current run
            loaded_df = loaded_df.filter(col("etl_run_id") == self.run_id)
            
            source_count = df.count()
            loaded_count = loaded_df.count()
            
            matches = source_count == loaded_count
            
            if matches:
                self.logger.log_info(
                    component="LOADER",
                    message=f"Reconciliation passed: {loaded_count} records"
                )
            else:
                self.logger.log_warning(
                    component="LOADER",
                    message=f"Reconciliation mismatch: Source={source_count}, Loaded={loaded_count}"
                )
            
            return matches
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message=f"Reconciliation error: {str(e)}"
            )
            return False


===FILE: src/orchestrator.py===
"""
ETL Orchestrator
Coordinates the complete ETL pipeline execution.
"""

from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict, Any, Optional
import time

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.utils.logger import ETLLogger
from src.utils.data_quality import DataQualityChecker
from src.config_manager import ConfigManager


class ETLOrchestrator:
    """Orchestrates the complete ETL process"""
    
    def __init__(self, spark: SparkSession, run_type: str = "MANUAL"):
        """
        Initialize orchestrator
        
        Args:
            spark: SparkSession instance
            run_type: Type of run (MANUAL, SCHEDULED, TEST)
        """
        self.spark = spark
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        self.logger = ETLLogger.get_instance()
        self.config = ConfigManager()
        
    def execute_etl(
        self,
        source_type: str = "DATABASE",
        target_type: str = "DATABASE",
        filter_condition: Optional[str] = None,
        batch_size: int = 1000,
        max_records: int = 0
    ) -> Dict[str, Any]:
        """
        Execute complete ETL pipeline
        
        Args:
            source_type: Source type (DATABASE, STAGING, INCREMENTAL)
            target_type: Target type (DATABASE, etc.)
            filter_condition: Optional filter
            batch_size: Batch size for loading
            max_records: Maximum records to process
            
        Returns:
            Dictionary with execution results
        """
        start_time = datetime.now()
        
        result = {
            "run_id": self.run_id,
            "status": "RUNNING",
            "start_time": start_time,
            "end_time": None,
            "duration": 0,
            "records_extracted": 0,
            "records_transformed": 0,
            "records_loaded": 0,
            "records_failed": 0,
            "error_count": 0,
            "warning_count": 0
        }
        
        self.logger.log_info(
            component="ORCHESTRATOR",
            message=f"ETL execution started - Run ID: {self.run_id}"
        )
        
        try:
            # Update config with runtime parameters
            self.config.config["source_type"] = source_type
            self.config.config["target_type"] = target_type
            
            # Step 1: Extract
            extractor = ETLExtractor(self.spark, self.run_id, self.config)
            source_df = extractor.extract_data(filter_condition, max_records)
            
            result["records_extracted"] = source_df.count()
            
            if result["records_extracted"] == 0:
                self.logger.log_warning(
                    component="ORCHESTRATOR",
                    message="No data extracted - ETL process stopping"
                )
                result["status"] = "NO_DATA"
                return result
            
            # Step 2: Transform
            transformer = ETLTransformer(self.spark, self.run_id, self.config)
            transformed_df = transformer.transform_data(source_df)
            
            result["records_transformed"] = transformed_df.count()
            
            # Step 3: Validate
            is_valid, errors = transformer.validate_data(transformed_df)
            
            if not is_valid:
                self.logger.log_error(
                    component="ORCHESTRATOR",
                    message=f"Validation failed - {len(errors)} errors"
                )
                result["status"] = "VALIDATION_FAILED"
                result["error_count"] = len(errors)
                return result
            
            # Step 4: Quality Checks
            quality_checker = DataQualityChecker(self.spark, self.run_id, self.config)
            quality_results = quality_checker.perform_quality_checks(transformed_df)
            
            failed_checks = [c for c in quality_results if not c["passed"]]
            if failed_checks:
                result["warning_count"] = len(failed_checks)
                self.logger.log_warning(
                    component="ORCHESTRATOR",
                    message=f"{len(failed_checks)} quality checks failed"
                )
            
            # Step 5: Load
            loader = ETLLoader(self.spark, self.run_id, self.config, batch_size)
            load_result = loader.load_data(transformed_df, mode="UPSERT")
            
            result["records_loaded"] = load_result["success_count"]
            result["records_failed"] = load_result["error_count"]
            result["error_count"] += load_result["error_count"]
            
            # Determine final status
            if load_result["error_count"] == 0:
                result["status"] = "SUCCESS"
            elif load_result["success_count"] > 0:
                result["status"] = "PARTIAL_SUCCESS"
            else:
                result["status"] = "FAILED"
            
        except Exception as e:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message="ETL execution failed",
                details=str(e)
            )
            result["status"] = "ERROR"
            result["error_count"] += 1
        
        finally:
            end_time = datetime.now()
            result["end_time"] = end_time
            result["duration"] = int((end_time - start_time).total_seconds())
            
            self._log_run_completion(result)
            
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"ETL execution completed - Status: {result['status']}"
            )
        
        return result
    
    def _generate_run_id(self) -> str:
        """Generate unique run ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"RUN_{timestamp}"
    
    def _log_run_completion(self, result: Dict[str, Any]) -> None:
        """
        Log run completion to run log
        
        Args:
            result: Execution result dictionary
        """
        try:
            run_log_path = self.config.get("run_log_path", "data/run_log")
            
            # Create run log record
            log_df = self.spark.createDataFrame([{
                "run_id": result["run_id"],
                "run_type": self.run_type,
                "status": result["status"],
                "start_time": result["start_time"],
                "end_time": result["end_time"],
                "duration": result["duration"],
                "records_extracted": result["records_extracted"],
                "records_transformed": result["records_transformed"],
                "records_loaded": result["records_loaded"],
                "records_failed": result["records_failed"],
                "error_count": result["error_count"],
                "warning_count": result["warning_count"]
            }])
            
            # Append to run log
            log_df.write.mode("append").parquet(run_log_path)
            
        except Exception as e:
            self.logger.log_warning(
                component="ORCHESTRATOR",
                message=f"Failed to write run log: {str(e)}"
            )


===FILE: src/config_manager.py===
"""
Configuration Manager
Manages ETL configuration from YAML files and provides runtime access.
"""

import yaml
from typing import Any, Dict, Optional
import os


class ConfigManager:
    """Manages ETL configuration"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize configuration manager
        
        Args:
            config_path: Path to configuration file
        """
        self.config_path = config_path
        self.config = self._load_config()
        
    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from YAML file
        
        Returns:
            Configuration dictionary
        """
        if os.path.exists(self.config_path):
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        else:
            return self._default_config()
    
    def _default_config(self) -> Dict[str, Any]:
        """Return default configuration"""
        return {
            "source_type": "DATABASE",
            "target_type": "DATABASE",
            "source_path": "data/source",
            "target_path": "data/target",
            "staging_path": "data/staging",
            "run_log_path": "data/run_log",
            "batch_size": 1000,
            "max_retries": 3,
            "enable_reconciliation": "true",
            "premium_multiplier": "1.5",
            "log_retention_days": 90,
            "alert_email": "admin@example.com",
            "processed_by": "ETL_SYSTEM"
        }
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        return self.config.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """
        Set configuration value
        
        Args:
            key: Configuration key
            value: Configuration value
        """
        self.config[key] = value
    
    def save(self) -> None:
        """Save configuration to file"""
        with open(self.config_path, 'w') as f:
            yaml.dump(self.config, f, default_flow_style=False)


===FILE: src/utils/logger.py===
"""
ETL Logger
Centralized logging utility for ETL processes.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
import logging


class ETLLogger:
    """Singleton logger for ETL processes"""
    
    _instance = None
    
    def __init__(self):