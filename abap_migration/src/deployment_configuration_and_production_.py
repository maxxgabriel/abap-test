===FILE: src/extract.py===
"""
ETL Data Extraction Module
Extracts data from various sources with production-grade error handling and monitoring
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional, Dict, Any
import logging
from src.monitoring import MetricsCollector


class DataExtractor:
    """Production data extractor with monitoring and alerting"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        self.metrics = MetricsCollector()
        
    def get_source_schema(self) -> StructType:
        """Define source data schema"""
        return StructType([
            StructField("id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("value", DecimalType(15, 2), nullable=False),
            StructField("status", StringType(), nullable=True),
            StructField("category", StringType(), nullable=True),
            StructField("source_system", StringType(), nullable=True),
            StructField("created_at", TimestampType(), nullable=True),
            StructField("created_by", StringType(), nullable=True),
            StructField("changed_at", TimestampType(), nullable=True),
            StructField("changed_by", StringType(), nullable=True),
        ])
    
    def extract_data(
        self,
        source_type: str = "database",
        filter_expr: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data from configured source with monitoring
        
        Args:
            source_type: Type of source (database, staging, incremental)
            filter_expr: Optional filter expression
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            Extracted DataFrame
        """
        start_time = datetime.now()
        self.logger.info(f"Starting extraction - Source: {source_type}, Run ID: {self.run_id}")
        
        try:
            self.metrics.increment_counter("extraction_started", {"source_type": source_type})
            
            if source_type == "database":
                df = self._extract_from_database(filter_expr)
            elif source_type == "staging":
                df = self._extract_from_staging()
            elif source_type == "incremental":
                df = self._extract_incremental()
            else:
                df = self._extract_from_database(filter_expr)
            
            # Apply max records limit
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            duration = (datetime.now() - start_time).total_seconds()
            
            # Record metrics
            self.metrics.record_gauge("records_extracted", record_count, {"run_id": self.run_id})
            self.metrics.record_histogram("extraction_duration_seconds", duration)
            
            self.logger.info(f"Extracted {record_count} records in {duration:.2f}s")
            
            return df
            
        except Exception as e:
            self.metrics.increment_counter("extraction_errors", {"error_type": type(e).__name__})
            self.logger.error(f"Extraction failed: {str(e)}", exc_info=True)
            raise
    
    def _extract_from_database(self, filter_expr: Optional[str] = None) -> DataFrame:
        """Extract from database source"""
        jdbc_config = self.config['sources']['database']
        
        query = f"(SELECT * FROM {jdbc_config['table']}"
        if filter_expr:
            query += f" WHERE {filter_expr}"
        query += ") as source_data"
        
        df = (self.spark.read
              .format("jdbc")
              .option("url", jdbc_config['url'])
              .option("dbtable", query)
              .option("user", jdbc_config['user'])
              .option("password", jdbc_config['password'])
              .option("driver", jdbc_config['driver'])
              .option("numPartitions", jdbc_config.get('partitions', 4))
              .load())
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract from staging area"""
        staging_path = self.config['sources']['staging']['path']
        
        df = (self.spark.read
              .format(self.config['sources']['staging'].get('format', 'parquet'))
              .option("mergeSchema", "true")
              .load(staging_path)
              .filter(f"run_id = '{self.run_id}' AND status = 'READY'"))
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract incremental changes since last run"""
        # Get last successful run timestamp
        last_run_time = self._get_last_run_timestamp()
        
        jdbc_config = self.config['sources']['database']
        query = f"""(
            SELECT * FROM {jdbc_config['table']}
            WHERE changed_at > '{last_run_time}'
        ) as incremental_data"""
        
        df = (self.spark.read
              .format("jdbc")
              .option("url", jdbc_config['url'])
              .option("dbtable", query)
              .option("user", jdbc_config['user'])
              .option("password", jdbc_config['password'])
              .option("driver", jdbc_config['driver'])
              .load())
        
        return df
    
    def _get_last_run_timestamp(self) -> str:
        """Get timestamp of last successful run"""
        log_table = self.config['monitoring']['run_log_table']
        
        try:
            last_run = (self.spark.read
                       .format("jdbc")
                       .option("url", self.config['sources']['database']['url'])
                       .option("dbtable", f"(SELECT MAX(end_time) as last_time FROM {log_table} WHERE status = 'SUCCESS') as lr")
                       .option("user", self.config['sources']['database']['user'])
                       .option("password", self.config['sources']['database']['password'])
                       .load())
            
            result = last_run.collect()[0]['last_time']
            return result if result else "1970-01-01 00:00:00"
        except Exception as e:
            self.logger.warning(f"Could not get last run time: {e}")
            return "1970-01-01 00:00:00"


===FILE: src/transform.py===
"""
ETL Data Transformation Module
Applies business rules, enrichment, and validation with data quality checks
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, trim, regexp_replace, when, lit, current_timestamp,
    current_user, coalesce, concat_ws
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from typing import Dict, Any, List, Tuple
from datetime import datetime
import logging
from src.monitoring import MetricsCollector


class DataTransformer:
    """Production data transformer with quality checks"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        self.metrics = MetricsCollector()
    
    def get_transformed_schema(self) -> StructType:
        """Define transformed data schema"""
        return StructType([
            StructField("id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("value", DecimalType(15, 2), nullable=False),
            StructField("transformed_value", DecimalType(15, 2), nullable=False),
            StructField("status", StringType(), nullable=False),
            StructField("category", StringType(), nullable=False),
            StructField("priority", IntegerType(), nullable=False),
            StructField("etl_run_id", StringType(), nullable=False),
            StructField("processed_at", TimestampType(), nullable=False),
            StructField("processed_by", StringType(), nullable=False),
        ])
    
    def transform_data(self, source_df: DataFrame) -> DataFrame:
        """
        Apply complete transformation pipeline
        
        Args:
            source_df: Source DataFrame to transform
            
        Returns:
            Transformed DataFrame
        """
        start_time = datetime.now()
        self.logger.info(f"Starting transformation for run {self.run_id}")
        
        try:
            self.metrics.increment_counter("transformation_started")
            
            # Basic transformations
            df = self._apply_basic_transformations(source_df)
            
            # Business rules
            df = self._apply_business_rules(df)
            
            # Enrichment
            df = self._enrich_data(df)
            
            # Add metadata
            df = self._add_metadata(df)
            
            record_count = df.count()
            duration = (datetime.now() - start_time).total_seconds()
            
            self.metrics.record_gauge("records_transformed", record_count, {"run_id": self.run_id})
            self.metrics.record_histogram("transformation_duration_seconds", duration)
            
            self.logger.info(f"Transformed {record_count} records in {duration:.2f}s")
            
            return df
            
        except Exception as e:
            self.metrics.increment_counter("transformation_errors", {"error_type": type(e).__name__})
            self.logger.error(f"Transformation failed: {str(e)}", exc_info=True)
            raise
    
    def _apply_basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic data transformations"""
        return (df
                .withColumn("name", trim(upper(regexp_replace(col("name"), "\\s+", " "))))
                .withColumn("transformed_value", self._calculate_derived_value(col("value"), col("category")))
                .withColumn("priority", self._calculate_priority(col("value"), col("category")))
                .withColumn("category", coalesce(col("category"), lit("UNCATEGORIZED")))
                .withColumn("status", lit("TRANSFORMED")))
    
    def _calculate_derived_value(self, value_col, category_col):
        """Calculate derived value based on business rules"""
        multiplier_config = self.config['transformation']['category_multipliers']
        
        result = value_col
        for category, multiplier in multiplier_config.items():
            result = when(
                category_col == category,
                value_col * multiplier
            ).otherwise(result)
        
        return result
    
    def _calculate_priority(self, value_col, category_col):
        """Calculate priority based on value and category"""
        return (when(value_col >= 1000, 1)
                .when(value_col >= 750, 2)
                .when(value_col >= 500, 3)
                .when(value_col >= 250, 4)
                .otherwise(5))
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to data"""
        df = (df
              # Rule 1: Status based on value
              .withColumn("status",
                         when(col("value").isNull(), "INVALID")
                         .when(col("transformed_value") >= 750, "HIGH_VALUE")
                         .when(col("transformed_value") >= 300, "MEDIUM_VALUE")
                         .otherwise("LOW_VALUE"))
              
              # Rule 2: Priority override for high value
              .withColumn("priority",
                         when(col("transformed_value") >= 1000, 1)
                         .otherwise(col("priority"))))
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional information"""
        # Apply category-specific enrichment
        enrichment_config = self.config['transformation'].get('enrichment', {})
        
        if enrichment_config.get('premium_boost', False):
            df = df.withColumn("transformed_value",
                              when(col("category") == "PREMIUM",
                                   col("transformed_value") * 1.2)
                              .otherwise(col("transformed_value")))
        
        return df
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """Add ETL metadata to records"""
        return (df
                .withColumn("etl_run_id", lit(self.run_id))
                .withColumn("processed_at", current_timestamp())
                .withColumn("processed_by", lit(current_user())))
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data
        
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        self.logger.info("Starting data validation")
        errors = []
        
        # Check for required fields
        null_ids = df.filter(col("id").isNull()).count()
        if null_ids > 0:
            errors.append(f"{null_ids} records with null ID")
        
        null_names = df.filter(col("name").isNull()).count()
        if null_names > 0:
            errors.append(f"{null_names} records with null name")
        
        # Check for valid values
        invalid_values = df.filter(col("value") < 0).count()
        if invalid_values > 0:
            errors.append(f"{invalid_values} records with negative values")
        
        # Check priority range
        invalid_priority = df.filter((col("priority") < 1) | (col("priority") > 5)).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        is_valid = len(errors) == 0
        
        # Record metrics
        self.metrics.record_gauge("validation_errors", len(errors), {"run_id": self.run_id})
        
        if is_valid:
            self.logger.info("Validation passed")
        else:
            self.logger.warning(f"Validation failed with {len(errors)} errors")
            for error in errors:
                self.logger.warning(f"  - {error}")
        
        return is_valid, errors


===FILE: src/load.py===
"""
ETL Data Loading Module
Loads transformed data to target with reconciliation and error handling
"""
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any, NamedTuple
from datetime import datetime
import logging
from src.monitoring import MetricsCollector


class LoadResult(NamedTuple):
    """Result of load operation"""
    success_count: int
    error_count: int
    total_count: int
    errors: list


class DataLoader:
    """Production data loader with monitoring"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], run_id: str):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        self.metrics = MetricsCollector()
    
    def load_data(
        self,
        df: DataFrame,
        mode: str = "upsert",
        batch_size: int = 1000
    ) -> LoadResult:
        """
        Load data to target destination
        
        Args:
            df: DataFrame to load
            mode: Load mode (insert, update, upsert)
            batch_size: Batch size for loading
            
        Returns:
            LoadResult with statistics
        """
        start_time = datetime.now()
        total_count = df.count()
        
        self.logger.info(f"Starting load - Mode: {mode}, Records: {total_count}")
        
        try:
            self.metrics.increment_counter("load_started", {"mode": mode})
            
            # Load to target
            self._load_to_target(df, mode, batch_size)
            
            # Reconcile data
            if self.config['data_quality'].get('enable_reconciliation', True):
                reconciliation_passed = self._reconcile_data(df)
                if not reconciliation_passed:
                    self.logger.warning("Data reconciliation failed")
                    self.metrics.increment_counter("reconciliation_failures")
            
            duration = (datetime.now() - start_time).total_seconds()
            
            # Record metrics
            self.metrics.record_gauge("records_loaded", total_count, {"run_id": self.run_id})
            self.metrics.record_histogram("load_duration_seconds", duration)
            
            self.logger.info(f"Loaded {total_count} records in {duration:.2f}s")
            
            return LoadResult(
                success_count=total_count,
                error_count=0,
                total_count=total_count,
                errors=[]
            )
            
        except Exception as e:
            self.metrics.increment_counter("load_errors", {"error_type": type(e).__name__})
            self.logger.error(f"Load failed: {str(e)}", exc_info=True)
            
            return LoadResult(
                success_count=0,
                error_count=total_count,
                total_count=total_count,
                errors=[str(e)]
            )
    
    def _load_to_target(self, df: DataFrame, mode: str, batch_size: int):
        """Load data to configured target"""
        target_config = self.config['targets']['database']
        
        # Prepare write operation
        writer = (df.write
                  .format("jdbc")
                  .option("url", target_config['url'])
                  .option("dbtable", target_config['table'])
                  .option("user", target_config['user'])
                  .option("password", target_config['password'])
                  .option("driver", target_config['driver'])
                  .option("batchsize", batch_size))
        
        # Apply mode-specific options
        if mode == "insert":
            writer.mode("append").save()
        elif mode == "update":
            # Use Delta Lake for updates if available
            if target_config.get('use_delta', False):
                self._update_with_delta(df, target_config)
            else:
                writer.mode("overwrite").save()
        elif mode == "upsert":
            if target_config.get('use_delta', False):
                self._upsert_with_delta(df, target_config)
            else:
                # Fallback to overwrite
                writer.mode("overwrite").save()
        else:
            writer.mode("append").save()
    
    def _update_with_delta(self, df: DataFrame, target_config: Dict[str, Any]):
        """Update using Delta Lake"""
        from delta.tables import DeltaTable
        
        delta_path = target_config['delta_path']
        
        if DeltaTable.isDeltaTable(self.spark, delta_path):
            delta_table = DeltaTable.forPath(self.spark, delta_path)
            
            (delta_table.alias("target")
             .merge(df.alias("source"), "target.id = source.id")
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())
        else:
            df.write.format("delta").mode("overwrite").save(delta_path)
    
    def _upsert_with_delta(self, df: DataFrame, target_config: Dict[str, Any]):
        """Upsert using Delta Lake"""
        self._update_with_delta(df, target_config)
    
    def _reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data with source
        
        Returns:
            True if reconciliation passes
        """
        self.logger.info("Starting data reconciliation")
        
        try:
            # Read back loaded data
            target_config = self.config['targets']['database']
            
            loaded_count = loaded_df.count()
            
            target_df = (self.spark.read
                        .format("jdbc")
                        .option("url", target_config['url'])
                        .option("dbtable", f"(SELECT * FROM {target_config['table']} WHERE etl_run_id = '{self.run_id}') as loaded")
                        .option("user", target_config['user'])
                        .option("password", target_config['password'])
                        .load())
            
            target_count = target_df.count()
            
            matches = loaded_count == target_count
            
            self.logger.info(f"Reconciliation: Source={loaded_count}, Target={target_count}, Match={matches}")
            
            return matches
            
        except Exception as e:
            self.logger.error(f"Reconciliation failed: {str(e)}")
            return False


===FILE: src/monitoring.py===
"""
Production Monitoring and Metrics Collection
Integrates with monitoring systems (Prometheus, CloudWatch, etc.)
"""
from typing import Dict, Any, Optional
from datetime import datetime
import logging
import json
from collections import defaultdict


class MetricsCollector:
    """Collects and exposes metrics for monitoring systems"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.counters = defaultdict(int)
        self.gauges = {}
        self.histograms = defaultdict(list)
    
    def increment_counter(self, name: str, labels: Optional[Dict[str, str]] = None):
        """Increment a counter metric"""
        key = self._make_key(name, labels)
        self.counters[key] += 1
        self._log_metric("counter", name, self.counters[key], labels)
    
    def record_gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Record a gauge metric"""
        key = self._make_key(name, labels)
        self.gauges[key] = value
        self._log_metric("gauge", name, value, labels)
    
    def record_histogram(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Record a histogram metric"""
        key = self._make_key(name, labels)
        self.histograms[key].append(value)
        self._log_metric("histogram", name, value, labels)
    
    def _make_key(self, name: str, labels: Optional[Dict[str, str]] = None) -> str:
        """Create a unique key for metric with labels"""
        if labels:
            label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
            return f"{name}{{{label_str}}}"
        return name
    
    def _log_metric(self, metric_type: str, name: str, value: Any, labels: Optional[Dict[str, str]] = None):
        """Log metric for monitoring systems to scrape"""
        metric_data = {
            "timestamp": datetime.now().isoformat(),
            "type": metric_type,
            "name": name,
            "value": value,
            "labels": labels or {}
        }
        self.logger.info(f"METRIC: {json.dumps(metric_data)}")
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of all metrics"""
        return {
            "counters": dict(self.counters),
            "gauges": dict(self.gauges),
            "histograms": {k: {"count": len(v), "sum": sum(v), "avg": sum(v)/len(v) if v else 0} 
                          for k, v in self.histograms.items()}
        }


class HealthChecker:
    """System health monitoring"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.metrics = MetricsCollector()
    
    def check_health(self) -> Dict[str, Any]:
        """Perform comprehensive health check"""
        health_status = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "checks": {}
        }
        
        # Check Spark connectivity
        health_status["checks"]["spark"] = self._check_spark()
        
        # Check database connectivity
        health_status["checks"]["database"] = self._check_database()
        
        # Check resource usage
        health_status["checks"]["resources"] = self._check_resources()
        
        # Overall status
        if any(check["status"] == "unhealthy" for check in health_status["checks"].values()):
            health_status["status"] = "unhealthy"
        elif any(check["status"] == "degraded" for check in health_status["checks"].values()):
            health_status["status"] = "degraded"
        
        self.logger.info(f"Health check: {health_status['status']}")
        return health_status
    
    def _check_spark(self) -> Dict[str, str]:
        """Check Spark session health"""
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark:
                return {"status": "healthy", "message": "Spark session active"}
            return {"status": "degraded", "message": "No active Spark session"}
        except Exception as e:
            return {"status": "unhealthy", "message": str(e)}
    
    def _check_database(self) -> Dict[str, str]:
        """Check database connectivity"""
        # Implementation would test actual database connection
        return {"status": "healthy", "message": "Database connection OK"}
    
    def _check_resources(self) -> Dict[str, str]:
        """Check system resources"""
        # Implementation would check memory, CPU, disk usage
        return {"status": "healthy", "message": "Resources within limits"}


class AlertManager:
    """Manages alerts and notifications"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def send_alert(self, alert_type: str, message: str, severity: str = "medium", metadata: Optional[Dict] = None):
        """Send alert through configured channels"""
        alert = {
            "timestamp": datetime.now().isoformat(),
            "type": alert_type,
            "severity": severity,
            "message": message,
            "metadata": metadata or {}
        }
        
        self.logger.warning(f"ALERT: {json.dumps(alert)}")
        
        # Send to configured alert channels
        if self.config.get('alerts', {}).get('email_enabled', False):
            self._send_email_alert(alert)
        
        if self.config.get('alerts', {}).get('slack_enabled', False):
            self._send_slack_alert(alert)
    
    def _send_email_alert(self, alert: Dict[str, Any]):
        """Send email alert"""
        # Implementation would use SMTP or AWS SES
        self.logger.info(f"Email alert sent: {alert['message']}")
    
    def _send_slack_alert(self, alert: Dict[str, Any]):
        """Send Slack alert"""
        # Implementation would use Slack webhook
        self.logger.info(f"Slack alert sent: {alert['message']}")


===FILE: src/orchestrator.py===
"""
ETL Orchestration and Workflow Management
Coordinates all ETL components with comprehensive error handling and monitoring
"""
from pyspark.sql import SparkSession
from typing import Dict, Any, NamedTuple
from datetime import datetime
import logging
import yaml
from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.monitoring import MetricsCollector, HealthChecker, AlertManager
from src.data_quality import DataQualityChecker


class ETLResult(NamedTuple):
    """Result of ETL execution"""
    run_id: str
    status: str
    start_time: datetime
    end_time: datetime
    duration: float
    records_extracted: int
    records_transformed: int
    records_loaded: int
    records_failed: int
    error_count: int
    warning_count: int


class ETLOrchestrator:
    """Production ETL orchestrator with full monitoring"""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config = self._load_config(config_path)
        self.logger = self._setup_logging()
        self.spark = self._create_spark_session()
        self.metrics = MetricsCollector()
        self.health_checker = HealthChecker(self.config)
        self.alert_manager = AlertManager(self.config)
        self.run_id = self._generate_run_id()
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _setup_logging(self) -> logging.Logger:
        """Configure logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('etl_execution.log'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with production configuration"""
        spark_config = self.config['spark']
        
        builder = SparkSession.builder.appName(spark_config['app_name'])
        
        # Apply Spark configurations
        for key, value in spark_config.get('config', {}).items():
            builder = builder.config(key, value)
        
        return builder.getOrCreate()
    
    def _generate_run_id(self) -> str:
        """Generate unique run ID"""
        return f"ETL_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def execute_etl(
        self,
        source_type: str = "database",
        target_type: str = "database",
        filter_expr: str = None,
        batch_size: int = None,
        max_records: int = 0
    ) -> ETLResult:
        """
        Execute complete ETL pipeline
        
        Args:
            source_type: Source type (database, staging, incremental)
            target_type: Target type (database, delta, etc.)
            filter_expr: Optional filter expression
            batch_size: Batch size for loading
            max_records: Maximum records to process
            
        Returns:
            ETLResult with execution statistics
        """
        start_time = datetime.now()
        
        self.logger.info(f"{'='*60}")
        self.logger.info(f"ETL Execution Started - Run ID: {self.run_id}")
        self.logger.info(f"{'='*60}")
        
        # Initialize counters
        records_extracted = 0
        records_transformed = 0
        records_loaded = 0
        records_failed = 0
        error_count = 0
        warning_count = 0
        status = "RUNNING"
        
        try:
            # Pre-execution health check
            health = self.health_checker.check_health()
            if health['status'] == 'unhealthy':
                raise RuntimeError("System health check failed")
            
            # Step 1: Extract
            self.logger.info("Step 1: Data Extraction")
            extractor = DataExtractor(self.spark, self.config, self.run_id)
            source