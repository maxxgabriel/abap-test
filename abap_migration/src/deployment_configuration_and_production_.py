===FILE: src/extract.py===
"""
ETL Data Extraction Module
Extracts data from various source systems with health monitoring.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from datetime import datetime
from typing import Optional, Dict, Any
import logging

from src.monitoring import MetricsCollector, HealthCheck
from src.config import ConfigManager


class DataExtractor:
    """Handles data extraction from multiple source types with monitoring."""
    
    def __init__(self, spark: SparkSession, config: ConfigManager, run_id: str):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        self.metrics = MetricsCollector()
        
    def get_source_schema(self) -> StructType:
        """Define source data schema."""
        return StructType([
            StructField("id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("value", DecimalType(15, 2), nullable=True),
            StructField("status", StringType(), nullable=True),
            StructField("category", StringType(), nullable=True),
            StructField("source_system", StringType(), nullable=True),
            StructField("created_at", TimestampType(), nullable=True),
            StructField("created_by", StringType(), nullable=True),
            StructField("changed_at", TimestampType(), nullable=True),
            StructField("changed_by", StringType(), nullable=True)
        ])
    
    def extract_data(
        self,
        source_type: str = "database",
        filter_condition: Optional[str] = None,
        max_records: int = 0
    ) -> DataFrame:
        """
        Extract data from specified source with monitoring.
        
        Args:
            source_type: Type of source (database, staging, incremental)
            filter_condition: Optional SQL filter condition
            max_records: Maximum records to extract (0 = unlimited)
            
        Returns:
            DataFrame with extracted data
        """
        start_time = datetime.now()
        self.logger.info(f"Starting extraction - Source: {source_type}, Run ID: {self.run_id}")
        
        try:
            if source_type.lower() == "database":
                df = self._extract_from_database(filter_condition)
            elif source_type.lower() == "staging":
                df = self._extract_from_staging()
            elif source_type.lower() == "incremental":
                df = self._extract_incremental()
            else:
                df = self._extract_from_database(filter_condition)
            
            # Apply record limit if specified
            if max_records > 0:
                df = df.limit(max_records)
            
            record_count = df.count()
            duration = (datetime.now() - start_time).total_seconds()
            
            # Record metrics
            self.metrics.record_extraction(
                run_id=self.run_id,
                record_count=record_count,
                duration=duration,
                source_type=source_type
            )
            
            self.logger.info(f"Extracted {record_count} records in {duration:.2f}s")
            return df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            self.metrics.record_error("EXTRACTION", str(e))
            raise
    
    def _extract_from_database(self, filter_condition: Optional[str] = None) -> DataFrame:
        """Extract from database source."""
        jdbc_config = self.config.get("jdbc")
        
        query = f"(SELECT * FROM {jdbc_config['source_table']}"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        query += ") AS source_data"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config["url"]) \
            .option("dbtable", query) \
            .option("user", jdbc_config["user"]) \
            .option("password", jdbc_config["password"]) \
            .option("driver", jdbc_config["driver"]) \
            .load()
        
        return df
    
    def _extract_from_staging(self) -> DataFrame:
        """Extract from staging area."""
        staging_path = self.config.get("paths")["staging"]
        
        df = self.spark.read \
            .format("parquet") \
            .schema(self.get_source_schema()) \
            .load(f"{staging_path}/{self.run_id}")
        
        return df
    
    def _extract_incremental(self) -> DataFrame:
        """Extract only changed records since last run."""
        jdbc_config = self.config.get("jdbc")
        
        # Get last successful run time
        last_run_query = """
            (SELECT MAX(end_time) as last_run 
             FROM etl_run_log 
             WHERE status = 'SUCCESS') AS last_run_time
        """
        
        last_run_df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config["url"]) \
            .option("dbtable", last_run_query) \
            .option("user", jdbc_config["user"]) \
            .option("password", jdbc_config["password"]) \
            .load()
        
        last_run_time = last_run_df.collect()[0]["last_run"]
        
        if last_run_time:
            # Extract changed records
            query = f"""
                (SELECT * FROM {jdbc_config['source_table']}
                 WHERE changed_at > '{last_run_time}') AS changed_data
            """
        else:
            # Full extract if no previous run
            query = f"(SELECT * FROM {jdbc_config['source_table']}) AS full_data"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config["url"]) \
            .option("dbtable", query) \
            .option("user", jdbc_config["user"]) \
            .option("password", jdbc_config["password"]) \
            .load()
        
        return df


===FILE: src/transform.py===
"""
ETL Data Transformation Module
Applies business rules and transformations with validation.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, upper, trim, when, lit, current_timestamp, 
    regexp_replace, coalesce, round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
from typing import Tuple, List
import logging

from src.config import ConfigManager
from src.monitoring import MetricsCollector


class DataTransformer:
    """Handles data transformation with business rules and validation."""
    
    def __init__(self, spark: SparkSession, config: ConfigManager, run_id: str):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        self.metrics = MetricsCollector()
    
    def get_target_schema(self) -> StructType:
        """Define transformed data schema."""
        return StructType([
            StructField("id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("value", DecimalType(15, 2), nullable=True),
            StructField("transformed_value", DecimalType(15, 2), nullable=True),
            StructField("status", StringType(), nullable=True),
            StructField("category", StringType(), nullable=True),
            StructField("priority", IntegerType(), nullable=True),
            StructField("etl_run_id", StringType(), nullable=False),
            StructField("processed_at", TimestampType(), nullable=False),
            StructField("processed_by", StringType(), nullable=False)
        ])
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply all transformations to the dataset.
        
        Args:
            df: Source DataFrame
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting transformation")
        
        # Basic transformations
        df_transformed = df.select(
            col("id"),
            upper(trim(col("name"))).alias("name"),
            col("value"),
            col("status"),
            col("category")
        )
        
        # Calculate derived values
        df_transformed = self._calculate_derived_values(df_transformed)
        
        # Apply business rules
        df_transformed = self._apply_business_rules(df_transformed)
        
        # Apply category-specific rules
        df_transformed = self._apply_category_rules(df_transformed)
        
        # Enrich data
        df_transformed = self._enrich_data(df_transformed)
        
        # Add ETL metadata
        df_transformed = df_transformed.withColumn("etl_run_id", lit(self.run_id)) \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("processed_by", lit("etl_system"))
        
        record_count = df_transformed.count()
        self.logger.info(f"Transformed {record_count} records")
        
        return df_transformed
    
    def _calculate_derived_values(self, df: DataFrame) -> DataFrame:
        """Calculate transformed values based on category."""
        category_rules = self.config.get("transformation")["category_multipliers"]
        
        # Create case expression for category multipliers
        multiplier_expr = when(col("category") == "PREMIUM", lit(category_rules["PREMIUM"]))
        for category, multiplier in category_rules.items():
            if category != "PREMIUM":
                multiplier_expr = multiplier_expr.when(
                    col("category") == category, 
                    lit(multiplier)
                )
        multiplier_expr = multiplier_expr.otherwise(lit(1.0))
        
        df = df.withColumn(
            "transformed_value",
            spark_round(col("value") * multiplier_expr, 2)
        )
        
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules to the data."""
        rules = self.config.get("transformation")["business_rules"]
        
        # Rule 1: Set status based on transformed value
        df = df.withColumn(
            "status",
            when(col("transformed_value").isNull(), lit("INVALID"))
            .when(col("transformed_value") >= rules["high_value_threshold"], lit("HIGH_VALUE"))
            .when(col("transformed_value") >= rules["medium_value_threshold"], lit("MEDIUM_VALUE"))
            .otherwise(lit("LOW_VALUE"))
        )
        
        # Rule 2: Calculate priority
        df = df.withColumn(
            "priority",
            when(col("transformed_value") >= rules["priority_1_threshold"], lit(1))
            .when(col("transformed_value") >= rules["priority_2_threshold"], lit(2))
            .when(col("transformed_value") >= rules["priority_3_threshold"], lit(3))
            .otherwise(lit(5))
        )
        
        # Rule 3: Name normalization
        df = df.withColumn(
            "name",
            trim(regexp_replace(col("name"), "\\s+", " "))
        )
        
        # Rule 4: Category validation
        df = df.withColumn(
            "category",
            coalesce(col("category"), lit("UNCATEGORIZED"))
        )
        
        return df
    
    def _apply_category_rules(self, df: DataFrame) -> DataFrame:
        """Apply category-specific rules."""
        # Premium category adjustments
        df = df.withColumn(
            "transformed_value",
            when(col("category") == "PREMIUM", 
                 spark_round(col("transformed_value") * 1.2, 2))
            .otherwise(col("transformed_value"))
        )
        
        return df
    
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional information."""
        # Add enrichment logic based on configuration
        enrichment_config = self.config.get("transformation").get("enrichment", {})
        
        if enrichment_config.get("enabled", False):
            # Example enrichment: add flags, derived fields, etc.
            df = df.withColumn(
                "is_high_priority",
                when(col("priority") <= 2, lit(True)).otherwise(lit(False))
            )
        
        return df
    
    def validate_data(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate transformed data.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Tuple of (is_valid, list of validation errors)
        """
        errors = []
        validation_rules = self.config.get("validation")
        
        # Rule 1: Check for null IDs
        null_id_count = df.filter(col("id").isNull()).count()
        if null_id_count > 0:
            errors.append(f"{null_id_count} records with null ID")
        
        # Rule 2: Check for null names
        null_name_count = df.filter(col("name").isNull()).count()
        if null_name_count > 0:
            errors.append(f"{null_name_count} records with null name")
        
        # Rule 3: Check for negative values
        if validation_rules.get("allow_negative_values", False) is False:
            negative_count = df.filter(col("value") < 0).count()
            if negative_count > 0:
                errors.append(f"{negative_count} records with negative values")
        
        # Rule 4: Check priority range
        invalid_priority = df.filter(
            (col("priority") < 1) | (col("priority") > 5)
        ).count()
        if invalid_priority > 0:
            errors.append(f"{invalid_priority} records with invalid priority")
        
        # Rule 5: Check for duplicates
        total_count = df.count()
        unique_count = df.select("id").distinct().count()
        if total_count != unique_count:
            errors.append(f"{total_count - unique_count} duplicate IDs found")
        
        is_valid = len(errors) == 0
        
        if not is_valid:
            self.logger.warning(f"Validation failed: {', '.join(errors)}")
            self.metrics.record_validation_errors(self.run_id, errors)
        
        return is_valid, errors


===FILE: src/load.py===
"""
ETL Data Loading Module
Loads transformed data to target with reconciliation.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from datetime import datetime
from typing import Dict, Any
import logging

from src.config import ConfigManager
from src.monitoring import MetricsCollector


class DataLoader:
    """Handles data loading to target systems with reconciliation."""
    
    def __init__(self, spark: SparkSession, config: ConfigManager, run_id: str):
        self.spark = spark
        self.config = config
        self.run_id = run_id
        self.logger = logging.getLogger(__name__)
        self.metrics = MetricsCollector()
    
    def load_data(
        self, 
        df: DataFrame, 
        mode: str = "upsert"
    ) -> Dict[str, Any]:
        """
        Load data to target with specified mode.
        
        Args:
            df: Transformed DataFrame to load
            mode: Load mode (insert, update, upsert)
            
        Returns:
            Dictionary with load results
        """
        start_time = datetime.now()
        batch_size = self.config.get("loading")["batch_size"]
        
        self.logger.info(f"Starting load - Mode: {mode}, Batch size: {batch_size}")
        
        try:
            total_count = df.count()
            success_count = 0
            error_count = 0
            errors = []
            
            # Load based on mode
            if mode.lower() == "insert":
                success_count = self._insert_data(df)
            elif mode.lower() == "update":
                success_count = self._update_data(df)
            elif mode.lower() == "upsert":
                success_count = self._upsert_data(df)
            else:
                success_count = self._insert_data(df)
            
            error_count = total_count - success_count
            
            # Reconcile data
            if self.config.get("loading").get("enable_reconciliation", True):
                reconcile_result = self._reconcile_data(df)
                if not reconcile_result:
                    self.logger.warning("Data reconciliation failed")
                    errors.append("Reconciliation mismatch detected")
            
            duration = (datetime.now() - start_time).total_seconds()
            
            # Record metrics
            self.metrics.record_loading(
                run_id=self.run_id,
                success_count=success_count,
                error_count=error_count,
                duration=duration
            )
            
            result = {
                "success_count": success_count,
                "error_count": error_count,
                "total_count": total_count,
                "errors": errors,
                "duration": duration
            }
            
            self.logger.info(
                f"Load complete - Success: {success_count}, "
                f"Errors: {error_count}, Duration: {duration:.2f}s"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            self.metrics.record_error("LOADING", str(e))
            raise
    
    def _insert_data(self, df: DataFrame) -> int:
        """Insert new records."""
        jdbc_config = self.config.get("jdbc")
        
        df.write \
            .format("jdbc") \
            .option("url", jdbc_config["url"]) \
            .option("dbtable", jdbc_config["target_table"]) \
            .option("user", jdbc_config["user"]) \
            .option("password", jdbc_config["password"]) \
            .option("driver", jdbc_config["driver"]) \
            .mode("append") \
            .save()
        
        return df.count()
    
    def _update_data(self, df: DataFrame) -> int:
        """Update existing records."""
        jdbc_config = self.config.get("jdbc")
        
        # Read existing data
        existing_df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config["url"]) \
            .option("dbtable", jdbc_config["target_table"]) \
            .option("user", jdbc_config["user"]) \
            .option("password", jdbc_config["password"]) \
            .load()
        
        # Join and update
        updated_df = existing_df.alias("existing").join(
            df.alias("new"),
            col("existing.id") == col("new.id"),
            "inner"
        ).select("new.*")
        
        # Write updates
        updated_df.write \
            .format("jdbc") \
            .option("url", jdbc_config["url"]) \
            .option("dbtable", jdbc_config["target_table"]) \
            .option("user", jdbc_config["user"]) \
            .option("password", jdbc_config["password"]) \
            .mode("overwrite") \
            .save()
        
        return updated_df.count()
    
    def _upsert_data(self, df: DataFrame) -> int:
        """Perform upsert (merge) operation."""
        jdbc_config = self.config.get("jdbc")
        
        # For databases supporting MERGE
        if jdbc_config.get("supports_merge", False):
            # Use native MERGE statement
            df.write \
                .format("jdbc") \
                .option("url", jdbc_config["url"]) \
                .option("dbtable", jdbc_config["target_table"]) \
                .option("user", jdbc_config["user"]) \
                .option("password", jdbc_config["password"]) \
                .option("mergeSchema", "true") \
                .mode("append") \
                .save()
        else:
            # Fallback: delete and insert
            temp_table = f"{jdbc_config['target_table']}_temp"
            
            # Write to temp table
            df.write \
                .format("jdbc") \
                .option("url", jdbc_config["url"]) \
                .option("dbtable", temp_table) \
                .option("user", jdbc_config["user"]) \
                .option("password", jdbc_config["password"]) \
                .mode("overwrite") \
                .save()
            
            # Execute merge logic via SQL
            # This is database-specific
        
        return df.count()
    
    def _reconcile_data(self, df: DataFrame) -> bool:
        """
        Reconcile loaded data against source.
        
        Args:
            df: Loaded DataFrame
            
        Returns:
            True if reconciliation passes
        """
        self.logger.info("Starting data reconciliation")
        
        jdbc_config = self.config.get("jdbc")
        
        # Read loaded data from target
        loaded_df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config["url"]) \
            .option("dbtable", jdbc_config["target_table"]) \
            .option("user", jdbc_config["user"]) \
            .option("password", jdbc_config["password"]) \
            .load() \
            .filter(col("etl_run_id") == self.run_id)
        
        # Compare counts
        source_count = df.count()
        target_count = loaded_df.count()
        
        if source_count != target_count:
            self.logger.warning(
                f"Reconciliation count mismatch: "
                f"Source={source_count}, Target={target_count}"
            )
            return False
        
        # Compare checksums (optional, for more thorough validation)
        reconciliation_config = self.config.get("loading").get("reconciliation", {})
        if reconciliation_config.get("check_checksums", False):
            # Implement checksum comparison
            pass
        
        self.logger.info("Data reconciliation passed")
        return True


===FILE: src/monitoring.py===
"""
ETL Monitoring and Health Check Module
Provides real-time observability and alerting.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
from dataclasses import dataclass, field
from enum import Enum


class AlertSeverity(Enum):
    """Alert severity levels."""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class HealthStatus(Enum):
    """System health status."""
    HEALTHY = "HEALTHY"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    OVERLOADED = "OVERLOADED"


@dataclass
class Metric:
    """Represents a single metric."""
    timestamp: datetime
    component: str
    metric_name: str
    value: float
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class Alert:
    """Represents a system alert."""
    alert_id: str
    timestamp: datetime
    alert_type: str
    severity: AlertSeverity
    message: str
    details: Dict[str, Any] = field(default_factory=dict)


class MetricsCollector:
    """Collects and stores ETL metrics."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.metrics: List[Metric] = []
        self.errors: List[Dict[str, Any]] = []
    
    def record_extraction(
        self, 
        run_id: str, 
        record_count: int, 
        duration: float,
        source_type: str
    ):
        """Record extraction metrics."""
        self.metrics.append(Metric(
            timestamp=datetime.now(),
            component="EXTRACTION",
            metric_name="records_extracted",
            value=record_count,
            tags={"run_id": run_id, "source_type": source_type}
        ))
        
        self.metrics.append(Metric(
            timestamp=datetime.now(),
            component="EXTRACTION",
            metric_name="duration_seconds",
            value=duration,
            tags={"run_id": run_id}
        ))
        
        throughput = record_count / duration if duration > 0 else 0
        self.metrics.append(Metric(
            timestamp=datetime.now(),
            component="EXTRACTION",
            metric_name="throughput_records_per_sec",
            value=throughput,
            tags={"run_id": run_id}
        ))
    
    def record_loading(
        self,
        run_id: str,
        success_count: int,
        error_count: int,
        duration: float
    ):
        """Record loading metrics."""
        self.metrics.append(Metric(
            timestamp=datetime.now(),
            component="LOADING",
            metric_name="records_loaded",
            value=success_count,
            tags={"run_id": run_id}
        ))
        
        self.metrics.append(Metric(
            timestamp=datetime.now(),
            component="LOADING",
            metric_name="errors",
            value=error_count,
            tags={"run_id": run_id}
        ))
        
        self.metrics.append(Metric(
            timestamp=datetime.now(),
            component="LOADING",
            metric_name="duration_seconds",
            value=duration,
            tags={"run_id": run_id}
        ))
    
    def record_error(self, component: str, error_message: str):
        """Record an error."""
        self.errors.append({
            "timestamp": datetime.now(),
            "component": component,
            "error": error_message
        })
        
        self.logger.error(f"[{component}] {error_message}")
    
    def record_validation_errors(self, run_id: str, errors: List[str]):
        """Record validation errors."""
        for error in errors:
            self.record_error("VALIDATION", error)
    
    def get_metrics(
        self, 
        component: Optional[str] = None,
        since: Optional[datetime] = None
    ) -> List[Metric]:
        """Retrieve metrics with optional filtering."""
        filtered = self.metrics
        
        if component:
            filtered = [m for m in filtered if m.component == component]
        
        if since:
            filtered = [m for m in filtered if m.timestamp >= since]
        
        return filtered


class HealthCheck:
    """Performs system health checks."""
    
    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics = metrics_collector
        self.logger = logging.getLogger(__name__)
        self.alerts: List[Alert] = []
    
    def check_health(self) -> HealthStatus:
        """
        Perform comprehensive health check.
        
        Returns:
            Current health status
        """
        # Check error rate
        recent_errors = self._get_recent_error_count(minutes=60)
        if recent_errors > 50:
            self._create_alert(
                alert_type="ERROR_RATE",
                severity=AlertSeverity.CRITICAL,
                message=f"Critical error rate: {recent_errors} errors in last hour"
            )
            return HealthStatus.CRITICAL
        elif recent_errors > 20:
            self._create_alert(
                alert_type="ERROR_RATE",
                severity=AlertSeverity.HIGH,
                message=f"High error rate: {recent_errors} errors in last hour"
            )
            return HealthStatus.WARNING
        
        # Check throughput
        avg_throughput = self._calculate_avg_throughput()
        if avg_throughput < 100:  # Records per second
            self._create_alert(
                alert_type="PERFORMANCE",
                severity=AlertSeverity.MEDIUM,
                message=f"Low throughput: {avg_throughput:.2f} records/sec"
            )
            return HealthStatus.WARNING
        
        return HealthStatus.HEALTHY
    
    def _get_recent_error_count(self, minutes: int) -> int:
        """Count errors in recent time window."""
        cutoff = datetime.now() - timedelta(minutes=minutes)
        return len([e for e in self.metrics.errors if e["timestamp"] >= cutoff])
    
    def _calculate_avg_throughput(self) -> float:
        """Calculate average throughput."""
        throughput_metrics = [
            m for m in self.metrics.get_metrics()
            if m.metric_name == "throughput_records_per_sec"
        ]
        
        if not throughput_metrics:
            return 0.0
        
        return sum(m.value for m in throughput_metrics) / len(throughput_metrics)
    
    def _create_alert(
        self,
        alert_type: str,
        severity: AlertSeverity,
        message: str,
        details: Optional[Dict[str, Any]] = None
    ):
        """Create and store an alert."""
        alert = Alert(
            alert_id=f"ALERT_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            timestamp=datetime.now(),
            alert_type=alert_type,
            severity=severity,
            message=message,
            details=details or {}
        )
        
        self.alerts.append(alert)
        self.logger.warning(f"[{severity.value}] {alert_type}: {message}")
    
    def get_active_alerts(self) -> List[Alert]:
        """Retrieve all active alerts."""
        return self.alerts


class DashboardDataProvider:
    """Provides data for monitoring dashboards."""
    
    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics = metrics_collector
    
    def get_dashboard_summary(self, days_back: int = 7) -> Dict[str, Any]:
        """
        Get dashboard summary data.
        
        Args:
            days_back: Number of days to look back
            
        Returns:
            Dictionary with dashboard metrics
        """
        cutoff = datetime.now() - timedelta(days=days_back)
        recent_metrics = self.metrics.get_metrics(since=cutoff)
        
        # Calculate aggregates
        total_records = sum(
            m.value for m in recent_metrics 
            if m.metric_name == "records_extracted"
        )
        
        error_count = len([
            e for e in self.metrics.errors 
            if e["timestamp"] >= cutoff
        ])
        
        avg_duration = self._calculate_average_duration(recent_metrics)
        
        return {
            "period_days": days_back,
            "total_records_processed": int(total_records),
            "total_errors": error_count,
            "average_duration_seconds": avg_duration,
            "health_status": "HEALTHY" if error_count < 10 else "WARNING",
            "last_updated": datetime.now().isoformat()
        }
    
    def _calculate_average_duration(self, metrics: List[Metric]) -> float:
        """Calculate average job duration."""
        duration_metrics = [
            m for m in metrics 
            if m.metric_name == "duration_seconds"
        ]
        
        if not duration_metrics:
            return 0.0
        
        return sum(m.value for m in duration_metrics) / len(duration_metrics)


===FILE: src/config.py===
"""
Configuration Management Module
Handles all ETL