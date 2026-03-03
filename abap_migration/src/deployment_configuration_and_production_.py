===FILE: src/extract.py===
"""
ETL Data Extraction Module
Extracts data from various sources with incremental load support
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional, Dict, Any
import logging
from src.utils.logger import ETLLogger
from src.utils.config import ConfigManager


class DataExtractor:
    """
    Handles data extraction from multiple sources with support for
    full, incremental, and staged extraction modes.
    """
    
    def __init__(self, spark: SparkSession, run_id: str, source_type: str = "DATABASE"):
        self.spark = spark
        self.run_id = run_id
        self.source_type = source_type
        self.logger = ETLLogger.get_instance()
        self.config = ConfigManager.get_instance()
        
    def get_schema(self) -> StructType:
        """Define source data schema"""
        return StructType([
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
    
    def extract_data(
        self,
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Main extraction method that routes to appropriate source
        
        Args:
            filter_condition: SQL filter condition
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        self.logger.log_info(
            "EXTRACTOR",
            f"Starting extraction - Source: {self.source_type}, Run ID: {self.run_id}"
        )
        
        try:
            if self.source_type == "DATABASE":
                df = self._extract_from_database(filter_condition)
            elif self.source_type == "STAGING":
                df = self._extract_from_staging()
            elif self.source_type == "INCREMENTAL":
                df = self._extract_incremental()
            else:
                df = self._extract_from_database(filter_condition)
            
            # Apply max records limit
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            self.logger.log_info("EXTRACTOR", f"Extracted {record_count} records")
            
            return df
            
        except Exception as e:
            self.logger.log_error("EXTRACTOR", "Extraction failed", str(e))
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract from source database/table"""
        source_table = self.config.get("source_table", "etl_source_data")
        
        query = f"SELECT * FROM {source_table}"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", self.config.get("source_jdbc_url")) \
            .option("dbtable", f"({query}) as src") \
            .option("user", self.config.get("source_user")) \
            .option("password", self.config.get("source_password")) \
            .option("driver", self.config.get("jdbc_driver")) \
            .load()
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract from staging area"""
        staging_path = self.config.get("staging_path", "data/staging")
        
        df = self.spark.read \
            .schema(self.get_schema()) \
            .parquet(f"{staging_path}/run_id={self.run_id}")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last run"""
        # Get last successful run timestamp
        run_log_table = self.config.get("run_log_table", "etl_run_log")
        
        last_run_query = f"""
        SELECT MAX(end_time) as last_run_time 
        FROM {run_log_table}
        WHERE status = 'SUCCESS'
        """
        
        last_run_df = self.spark.read \
            .format("jdbc") \
            .option("url", self.config.get("source_jdbc_url")) \
            .option("dbtable", f"({last_run_query}) as lr") \
            .option("user", self.config.get("source_user")) \
            .option("password", self.config.get("source_password")) \
            .load()
        
        last_run_time = last_run_df.first()["last_run_time"]
        
        if last_run_time:
            filter_condition = f"changed_at > '{last_run_time}'"
        else:
            filter_condition = None
            self.logger.log_warning(
                "EXTRACTOR",
                "No previous run found, performing full extraction"
            )
        
        return self._extract_from_database(filter_condition)


===FILE: src/transform.py===
"""
ETL Data Transformation Module
Applies business rules, enrichment, and validation
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, upper, trim, when, lit, current_timestamp, 
    regexp_replace, coalesce, udf
)
from pyspark.sql.types import IntegerType, DecimalType
from typing import Tuple, List
from src.utils.logger import ETLLogger
from src.utils.config import ConfigManager


class DataTransformer:
    """
    Handles data transformation including business rules,
    enrichment, and validation.
    """
    
    def __init__(self, run_id: str):
        self.run_id = run_id
        self.logger = ETLLogger.get_instance()
        self.config = ConfigManager.get_instance()
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Main transformation pipeline
        
        Args:
            df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.log_info("TRANSFORMER", "Starting transformation")
        
        # Clean and normalize
        df = self._clean_data(df)
        
        # Calculate derived values
        df = self._calculate_derived_values(df)
        
        # Calculate priority
        df = self._calculate_priority(df)
        
        # Apply business rules
        df = self._apply_business_rules(df)
        
        # Enrich data
        df = self._enrich_data(df)
        
        # Add ETL metadata
        df = df.withColumn("etl_run_id", lit(self.run_id)) \
               .withColumn("processed_at", current_timestamp()) \
               .withColumn("processed_by", lit("etl_system"))
        
        record_count = df.count()
        self.logger.log_info("TRANSFORMER", f"Transformed {record_count} records")
        
        return df
    
    def _clean_data(self, df: DataFrame) -> DataFrame:
        """Clean and normalize data"""
        # Normalize name: uppercase, trim, remove extra spaces
        df = df.withColumn(
            "name",
            upper(trim(regexp_replace(col("name"), "\\s+", " ")))
        )
        
        # Set default category if missing
        df = df.withColumn(
            "category",
            coalesce(col("category"), lit("UNCATEGORIZED"))
        )
        
        return df
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """Calculate transformed values based on category"""
        premium_multiplier = float(self.config.get("premium_multiplier", "1.5"))
        
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", col("value") * premium_multiplier)
            .when(col("category") == "VIP", col("value") * 1.75)
            .when(col("category") == "STANDARD", col("value") * 1.2)
            .otherwise(col("value"))
        )
        
        return df
    
    def _calculate_priority(self, df: DataFrame) -> DataFrame:
        """Calculate priority based on value and category"""
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, lit(1))
            .when(col("transformed_value") >= 750, lit(2))
            .when(col("transformed_value") >= 500, lit(3))
            .when(col("transformed_value") >= 250, lit(4))
            .otherwise(lit(5))
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business validation rules"""
        # Rule 1: Set status based on value
        df = df.withColumn(
            "status",
            when(col("value").isNull(), lit("INVALID"))
            .when(col("transformed_value") >= 750, lit("HIGH_VALUE"))
            .when(col("transformed_value") >= 300, lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )
        
        # Rule 2: Priority override for high value
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= 1000, lit(1))
            .otherwise(col("priority"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich with additional calculated fields"""
        # Add value tier
        df = df.withColumn(
            "value_tier",
            when(col("transformed_value") >= 1000, lit("PLATINUM"))
            .when(col("transformed_value") >= 500, lit("GOLD"))
            .when(col("transformed_value") >= 250, lit("SILVER"))
            .otherwise(lit("BRONZE"))
        )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data
        
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        errors = []
        
        # Check for null IDs
        null_ids = df.filter(col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null ID")
        
        # Check for null names
        null_names = df.filter(col("name").isNull()).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null name")
        
        # Check for negative values
        negative_values = df.filter(col("value") < 0).count()
        if negative_values > 0:
            errors.append(f"{negative_values} records with negative values")
        
        # Check priority range
        invalid_priority = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.logger.log_info("TRANSFORMER", "All validation checks passed")
        else:
            self.logger.log_error("TRANSFORMER", "Validation failed", str(errors))
        
        return is_valid, errors


===FILE: src/load.py===
"""
ETL Data Loading Module
Loads transformed data to target with batch processing and reconciliation
"""

from pyspark.sql import DataFrame
from typing import Dict, Any, List
from src.utils.logger import ETLLogger
from src.utils.config import ConfigManager


class DataLoader:
    """
    Handles data loading to target systems with batch processing,
    error handling, and data reconciliation.
    """
    
    def __init__(self, run_id: str, target_type: str = "DATABASE", batch_size: int = 1000):
        self.run_id = run_id
        self.target_type = target_type
        self.batch_size = batch_size
        self.logger = ETLLogger.get_instance()
        self.config = ConfigManager.get_instance()
    
    def load_data(self, df: DataFrame, mode: str = "upsert") -> Dict[str, Any]:
        """
        Main loading method with batch processing
        
        Args:
            df: Transformed DataFrame to load
            mode: Load mode - 'insert', 'update', 'upsert', 'overwrite'
            
        Returns:
            Dictionary with load results
        """
        self.logger.log_info(
            "LOADER",
            f"Starting load - Mode: {mode}, Batch size: {self.batch_size}"
        )
        
        total_count = df.count()
        success_count = 0
        error_count = 0
        errors = []
        
        try:
            if self.target_type == "DATABASE":
                success_count = self._load_to_database(df, mode)
            elif self.target_type == "PARQUET":
                success_count = self._load_to_parquet(df, mode)
            elif self.target_type == "DELTA":
                success_count = self._load_to_delta(df, mode)
            else:
                success_count = self._load_to_database(df, mode)
            
            error_count = total_count - success_count
            
            # Perform reconciliation
            if self.config.get("enable_reconciliation", "true").lower() == "true":
                reconciled = self._reconcile_data(df, success_count)
                if not reconciled:
                    self.logger.log_warning("LOADER", "Data reconciliation failed")
            
            self.logger.log_info(
                "LOADER",
                f"Load complete - Success: {success_count}, Errors: {error_count}"
            )
            
        except Exception as e:
            self.logger.log_error("LOADER", "Load failed", str(e))
            error_count = total_count
            errors.append(str(e))
        
        return {
            "success_count": success_count,
            "error_count": error_count,
            "total_count": total_count,
            "errors": errors
        }
    
    def _load_to_database(self, df: DataFrame, mode: str) -> int:
        """Load to target database"""
        target_table = self.config.get("target_table", "etl_target_data")
        
        # Map modes
        jdbc_mode = {
            "insert": "append",
            "update": "overwrite",
            "upsert": "append",
            "overwrite": "overwrite"
        }.get(mode, "append")
        
        df.write \
            .format("jdbc") \
            .option("url", self.config.get("target_jdbc_url")) \
            .option("dbtable", target_table) \
            .option("user", self.config.get("target_user")) \
            .option("password", self.config.get("target_password")) \
            .option("driver", self.config.get("jdbc_driver")) \
            .option("batchsize", str(self.batch_size)) \
            .mode(jdbc_mode) \
            .save()
        
        return df.count()
    
    def _load_to_parquet(self, df: DataFrame, mode: str) -> int:
        """Load to Parquet files"""
        target_path = self.config.get("target_path", "data/output")
        
        df.write \
            .mode(mode if mode != "upsert" else "append") \
            .partitionBy("category") \
            .parquet(f"{target_path}/run_id={self.run_id}")
        
        return df.count()
    
    def _load_to_delta(self, df: DataFrame, mode: str) -> int:
        """Load to Delta Lake"""
        target_path = self.config.get("delta_path", "data/delta")
        
        if mode == "upsert":
            # Implement Delta merge/upsert logic
            df.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(target_path)
        else:
            df.write \
                .format("delta") \
                .mode(mode) \
                .save(target_path)
        
        return df.count()
    
    def _reconcile_data(self, df: DataFrame, expected_count: int) -> bool:
        """
        Reconcile loaded data with source
        
        Args:
            df: Source DataFrame
            expected_count: Expected record count
            
        Returns:
            True if reconciliation passes
        """
        try:
            target_table = self.config.get("target_table", "etl_target_data")
            
            # Count records in target for this run
            target_df = df._jdf.sparkSession().read \
                .format("jdbc") \
                .option("url", self.config.get("target_jdbc_url")) \
                .option("dbtable", target_table) \
                .option("user", self.config.get("target_user")) \
                .option("password", self.config.get("target_password")) \
                .load()
            
            actual_count = target_df.filter(f"etl_run_id = '{self.run_id}'").count()
            
            matches = actual_count == expected_count
            
            if matches:
                self.logger.log_info("LOADER", "Data reconciliation passed")
            else:
                self.logger.log_warning(
                    "LOADER",
                    f"Reconciliation mismatch: Expected {expected_count}, Found {actual_count}"
                )
            
            return matches
            
        except Exception as e:
            self.logger.log_error("LOADER", "Reconciliation error", str(e))
            return False


===FILE: src/orchestrator.py===
"""
ETL Orchestrator
Coordinates the complete ETL workflow with monitoring and error handling
"""

from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict, Any, Optional
from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.utils.logger import ETLLogger
from src.utils.config import ConfigManager
from src.utils.monitor import ETLMonitor
from src.utils.health_check import HealthChecker


class ETLOrchestrator:
    """
    Orchestrates the complete ETL workflow with health checks,
    monitoring, and error recovery.
    """
    
    def __init__(self, spark: SparkSession, run_type: str = "MANUAL"):
        self.spark = spark
        self.run_type = run_type
        self.run_id = self._generate_run_id()
        self.logger = ETLLogger.get_instance()
        self.config = ConfigManager.get_instance()
        self.monitor = ETLMonitor.get_instance(spark)
        self.health_checker = HealthChecker(spark)
    
    def _generate_run_id(self) -> str:
        """Generate unique run ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"RUN_{timestamp}"
    
    def execute_etl(
        self,
        source_type: str = "DATABASE",
        target_type: str = "DATABASE",
        filter_condition: Optional[str] = None,
        batch_size: int = 1000,
        max_records: int = 0
    ) -> Dict[str, Any]:
        """
        Execute complete ETL workflow
        
        Args:
            source_type: Source type (DATABASE, STAGING, INCREMENTAL)
            target_type: Target type (DATABASE, PARQUET, DELTA)
            filter_condition: Optional filter condition
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
            "duration_seconds": 0,
            "records_extracted": 0,
            "records_transformed": 0,
            "records_loaded": 0,
            "records_failed": 0,
            "error_count": 0,
            "warning_count": 0
        }
        
        self.logger.log_info(
            "ORCHESTRATOR",
            f"ETL execution started - Run ID: {self.run_id}"
        )
        
        # Pre-execution health check
        health_status = self.health_checker.check_health()
        if health_status != "HEALTHY":
            self.logger.log_warning(
                "ORCHESTRATOR",
                f"Health check warning: {health_status}"
            )
        
        try:
            # Step 1: Extract
            extractor = DataExtractor(self.spark, self.run_id, source_type)
            df_source = extractor.extract_data(filter_condition, max_records)
            result["records_extracted"] = df_source.count()
            
            if result["records_extracted"] == 0:
                self.logger.log_warning("ORCHESTRATOR", "No data extracted")
                result["status"] = "NO_DATA"
                return result
            
            # Step 2: Transform
            transformer = DataTransformer(self.run_id)
            df_transformed = transformer.transform_data(df_source)
            result["records_transformed"] = df_transformed.count()
            
            # Step 3: Validate
            is_valid, validation_errors = transformer.validate_data(df_transformed)
            if not is_valid:
                self.logger.log_error(
                    "ORCHESTRATOR",
                    f"Validation failed: {len(validation_errors)} errors"
                )
                result["status"] = "VALIDATION_FAILED"
                result["error_count"] = len(validation_errors)
                return result
            
            # Step 4: Load
            loader = DataLoader(self.run_id, target_type, batch_size)
            load_result = loader.load_data(df_transformed, mode="upsert")
            
            result["records_loaded"] = load_result["success_count"]
            result["records_failed"] = load_result["error_count"]
            result["error_count"] = load_result["error_count"]
            
            # Determine final status
            if load_result["error_count"] == 0:
                result["status"] = "SUCCESS"
            elif load_result["success_count"] > 0:
                result["status"] = "PARTIAL_SUCCESS"
            else:
                result["status"] = "FAILED"
            
        except Exception as e:
            self.logger.log_error("ORCHESTRATOR", "ETL execution failed", str(e))
            result["status"] = "ERROR"
            result["error_count"] += 1
        
        finally:
            end_time = datetime.now()
            result["end_time"] = end_time
            result["duration_seconds"] = int((end_time - start_time).total_seconds())
            
            # Log run completion
            self._log_run_completion(result)
            
            # Update monitoring metrics
            self.monitor.record_run(result)
            
            self.logger.log_info(
                "ORCHESTRATOR",
                f"ETL execution completed - Status: {result['status']}"
            )
        
        return result
    
    def _log_run_completion(self, result: Dict[str, Any]):
        """Log run completion to database"""
        run_log_table = self.config.get("run_log_table", "etl_run_log")
        
        log_df = self.spark.createDataFrame([{
            "run_id": result["run_id"],
            "run_type": self.run_type,
            "status": result["status"],
            "start_time": result["start_time"],
            "end_time": result["end_time"],
            "duration": result["duration_seconds"],
            "records_extracted": result["records_extracted"],
            "records_transformed": result["records_transformed"],
            "records_loaded": result["records_loaded"],
            "records_failed": result["records_failed"],
            "error_count": result["error_count"]
        }])
        
        try:
            log_df.write \
                .format("jdbc") \
                .option("url", self.config.get("target_jdbc_url")) \
                .option("dbtable", run_log_table) \
                .option("user", self.config.get("target_user")) \
                .option("password", self.config.get("target_password")) \
                .mode("append") \
                .save()
        except Exception as e:
            self.logger.log_error("ORCHESTRATOR", "Failed to log run", str(e))


===FILE: src/utils/logger.py===
"""
ETL Logging Utility
Centralized logging with multiple levels and persistence
"""

import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from threading import Lock


class ETLLogger:
    """Singleton logger for ETL operations"""
    
    _instance = None
    _lock = Lock()
    
    def __init__(self):
        self.logs: List[Dict[str, Any]] = []
        self._setup_logger()
    
    @classmethod
    def get_instance(cls) -> 'ETLLogger':
        """Get singleton instance"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance
    
    def _setup_logger(self):
        """Setup Python logger"""
        self.logger = logging.getLogger("ETL")
        self.logger.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # File handler
        file_handler = logging.FileHandler('logs/etl.log')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
    
    def log_info(self, component: str, message: str, details: str = ""):
        """Log info level message"""
        self._add_log("INFO", component, message, details)
        self.logger.info(f"[{component}] {message}")
    
    def log_error(self, component: str, message: str, details: str = ""):
        """Log error level message"""
        self._add_log("ERROR", component, message, details)
        self.logger.error(f"[{component}] {message} - {details}")
    
    def log_warning(self, component: str, message: str, details: str = ""):
        """Log warning level message"""
        self._add_log("WARNING", component, message, details)
        self.logger.warning(f"[{component}] {message}")
    
    def _add_log(self, level: str, component: str, message: str, details: str):
        """Add log entry to in-memory store"""
        log_entry = {
            "timestamp": datetime.now(),
            "level": level,
            "component": component,
            "message": message,
            "details": details
        }
        self.logs.append(log_entry)
    
    def get_logs(self) -> List[Dict[str, Any]]:
        """Get all logs"""
        return self.logs
    
    def clear_logs(self):
        """Clear all logs"""
        self.logs.clear()


===FILE: src/utils/config.py===
"""
Configuration Management
Loads and manages ETL configuration from YAML
"""

import yaml
from typing import Any, Dict, Optional
from pathlib import Path
from threading import Lock


class ConfigManager:
    """Singleton configuration manager"""
    
    _instance = None
    _lock = Lock()
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = config_path
        self.config: Dict[str, Any] = {}
        self._load_config()
    
    @classmethod
    def get_instance(cls, config_path: str = "config.yaml") -> 'ConfigManager':
        """Get singleton instance"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls(config_path)
        return cls._instance
    
    def _load_config(self):
        """Load configuration from YAML file"""
        config_file = Path(self.config_path)
        if config_file.exists():
            with open(config_file, 'r') as f:
                self.config = yaml.safe_load(f)
        else:
            # Default configuration
            self.config = self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration"""
        return {
            "spark": {
                "app_name": "ETL_Pipeline",
                "master": "local[*]",
                "executor_memory": "4g",
                "driver_memory": "2g"
            },
            "source": {
                "type": "DATABASE",
                "jdbc_url": "jdbc:postgresql://localhost:5432/source_db",
                "table": "etl_source_data",
                "user": "etl_user",
                "password": "etl_password"
            },
            "target": {
                "type": "DATABASE",
                "jdbc_url": "jdbc:postgresql://localhost:5432/target_db",
                "table": "etl_target_data",
                "user": "etl_user",
                "password": "etl_password"
            },
            "processing": {
                "batch_size": 1000,
                "max_records": 0,
                "enable_reconciliation": True,
                "premium_multiplier": 1.5
            },
            "monitoring": {
                "enable_health_checks": True,
                "dashboard_retention_days": 30,
                "alert_threshold_error_rate": 20
            },
            "deployment": {
                "environment": "development",
                "log_level": "INFO",
                "enable_rollback": True,
                "health_check_interval_seconds": 60
            }
        }
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key (supports dot notation)"""
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any):
        """Set configuration value"""
        keys = key.split('.')
        config = self.config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
    
    def reload(self):
        """Reload configuration from file"""
        self._load_config()


===FILE: src/utils/monitor.py===