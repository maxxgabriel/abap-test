"""
PySpark Data Initialization Utility
Generates sample records with categories, configuration key-value pairs, 
and ETL schedule definitions using PySpark DataFrames.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, 
    IntegerType, TimestampType, BooleanType
)
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import random
import yaml
import logging
from pathlib import Path


class DataInitializer:
    """Initialize sample data for ETL testing."""
    
    def __init__(self, spark: SparkSession, config_path: str = "config.yaml"):
        """
        Initialize the data generator.
        
        Args:
            spark: Active SparkSession
            config_path: Path to configuration file
        """
        self.spark = spark
        self.config = self._load_config(config_path)
        self.logger = self._setup_logger()
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            self.logger.warning(f"Config file not found: {config_path}. Using defaults.")
            return self._get_default_config()
    
    def _get_default_config(self) -> dict:
        """Return default configuration."""
        return {
            'data_init': {
                'num_source_records': 100,
                'categories': ['PREMIUM', 'STANDARD', 'BASIC', 'VIP', 'TRIAL'],
                'value_range': {'min': 50, 'max': 1000},
                'output_format': 'parquet',
                'output_paths': {
                    'source_data': 'data/source',
                    'config_data': 'data/config',
                    'schedule_data': 'data/schedule'
                }
            },
            'logging': {
                'level': 'INFO',
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            }
        }
    
    def _setup_logger(self) -> logging.Logger:
        """Configure logging."""
        log_config = self.config.get('logging', {})
        logging.basicConfig(
            level=getattr(logging, log_config.get('level', 'INFO')),
            format=log_config.get('format', '%(asctime)s - %(levelname)s - %(message)s')
        )
        return logging.getLogger(__name__)
    
    def create_sample_source_data(self, num_records: int = None) -> None:
        """
        Generate sample source data records.
        
        Args:
            num_records: Number of records to generate (overrides config)
        """
        config = self.config['data_init']
        num_records = num_records or config['num_source_records']
        
        self.logger.info(f"Creating {num_records} sample source records...")
        
        # Define schema
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("value", DecimalType(15, 2), False),
            StructField("status", StringType(), False),
            StructField("category", StringType(), False),
            StructField("source_system", StringType(), False),
            StructField("created_at", TimestampType(), False),
            StructField("created_by", StringType(), False),
            StructField("changed_at", TimestampType(), False),
            StructField("changed_by", StringType(), False)
        ])
        
        # Generate data
        current_time = datetime.now()
        categories = config['categories']
        value_min = config['value_range']['min']
        value_max = config['value_range']['max']
        
        data = []
        for i in range(1, num_records + 1):
            record = (
                f"{i:010d}",  # id with leading zeros
                f"Product {i}",
                float(random.randint(value_min, value_max)),
                "ACTIVE",
                categories[i % len(categories)],
                "SAP_ERP",
                current_time,
                "SYSTEM",
                current_time,
                "SYSTEM"
            )
            data.append(record)
        
        # Create DataFrame
        df = self.spark.createDataFrame(data, schema)
        
        # Write to output
        output_path = config['output_paths']['source_data']
        output_format = config['output_format']
        
        self._write_dataframe(df, output_path, output_format, "source_data")
        
        self.logger.info(f"✓ Created {num_records} source records")
    
    def create_config_data(self) -> None:
        """Generate configuration key-value pairs."""
        self.logger.info("Creating configuration data...")
        
        # Define schema
        schema = StructType([
            StructField("config_key", StringType(), False),
            StructField("config_value", StringType(), False),
            StructField("description", StringType(), True),
            StructField("config_type", StringType(), False),
            StructField("is_active", BooleanType(), False),
            StructField("changed_at", TimestampType(), False),
            StructField("changed_by", StringType(), False)
        ])
        
        current_time = datetime.now()
        
        # Configuration entries
        config_entries = [
            ("BATCH_SIZE", "1000", "Default batch size for data loading", 
             "PERFORMANCE", True),
            ("MAX_RETRIES", "3", "Maximum number of retries on error", 
             "ERROR_HANDLING", True),
            ("ALERT_EMAIL", "admin@example.com", "Email address for alerts", 
             "NOTIFICATION", True),
            ("LOG_RETENTION_DAYS", "90", "Number of days to retain logs", 
             "MAINTENANCE", True),
            ("ENABLE_RECONCILIATION", "true", "Enable data reconciliation after load", 
             "DATA_QUALITY", True),
            ("PREMIUM_MULTIPLIER", "1.5", "Value multiplier for premium category", 
             "BUSINESS_RULE", True),
            ("STANDARD_MULTIPLIER", "1.2", "Value multiplier for standard category", 
             "BUSINESS_RULE", True),
            ("BASIC_MULTIPLIER", "1.0", "Value multiplier for basic category", 
             "BUSINESS_RULE", True),
            ("VIP_MULTIPLIER", "2.0", "Value multiplier for VIP category", 
             "BUSINESS_RULE", True),
            ("TRIAL_MULTIPLIER", "0.8", "Value multiplier for trial category", 
             "BUSINESS_RULE", True),
            ("HIGH_VALUE_THRESHOLD", "750", "Threshold for high value classification", 
             "BUSINESS_RULE", True),
            ("MEDIUM_VALUE_THRESHOLD", "300", "Threshold for medium value classification", 
             "BUSINESS_RULE", True),
            ("ENABLE_DATA_PROFILING", "true", "Enable automatic data profiling", 
             "DATA_QUALITY", True),
            ("ENABLE_ANOMALY_DETECTION", "true", "Enable anomaly detection", 
             "DATA_QUALITY", True),
            ("PARALLEL_JOBS", "4", "Number of parallel ETL jobs", 
             "PERFORMANCE", True),
            ("CHECKPOINT_INTERVAL", "100", "Checkpoint interval in records", 
             "PERFORMANCE", True),
        ]
        
        data = [
            (key, value, desc, ctype, active, current_time, "SYSTEM")
            for key, value, desc, ctype, active in config_entries
        ]
        
        df = self.spark.createDataFrame(data, schema)
        
        # Write to output
        config = self.config['data_init']
        output_path = config['output_paths']['config_data']
        output_format = config['output_format']
        
        self._write_dataframe(df, output_path, output_format, "config_data")
        
        self.logger.info(f"✓ Created {len(config_entries)} configuration entries")
    
    def create_schedule_data(self) -> None:
        """Generate ETL schedule definitions."""
        self.logger.info("Creating schedule data...")
        
        # Define schema
        schema = StructType([
            StructField("schedule_id", StringType(), False),
            StructField("schedule_name", StringType(), False),
            StructField("etl_type", StringType(), False),
            StructField("frequency", StringType(), False),
            StructField("start_date", StringType(), False),
            StructField("start_time", StringType(), False),
            StructField("end_date", StringType(), True),
            StructField("is_active", BooleanType(), False),
            StructField("cron_expression", StringType(), True),
            StructField("retry_count", IntegerType(), False),
            StructField("timeout_minutes", IntegerType(), False),
            StructField("created_by", StringType(), False),
            StructField("created_at", TimestampType(), False)
        ])
        
        current_time = datetime.now()
        current_date = current_time.strftime("%Y-%m-%d")
        end_date = (current_time + timedelta(days=365)).strftime("%Y-%m-%d")
        
        # Schedule entries
        schedule_entries = [
            ("SCHED001", "Daily Full Load", "FULL", "DAILY", 
             current_date, "02:00:00", end_date, True, 
             "0 2 * * *", 3, 120),
            ("SCHED002", "Hourly Incremental", "INCREMENTAL", "HOURLY", 
             current_date, "00:00:00", end_date, True, 
             "0 * * * *", 2, 30),
            ("SCHED003", "Weekly Reconciliation", "RECONCILIATION", "WEEKLY", 
             current_date, "18:00:00", end_date, False, 
             "0 18 * * 0", 1, 240),
            ("SCHED004", "Monthly Archive", "ARCHIVE", "MONTHLY", 
             current_date, "03:00:00", end_date, True, 
             "0 3 1 * *", 1, 480),
            ("SCHED005", "Real-time Stream", "STREAMING", "CONTINUOUS", 
             current_date, "00:00:00", end_date, True, 
             None, 0, 0),
        ]
        
        data = [
            (sid, name, etype, freq, sdate, stime, edate, active, 
             cron, retry, timeout, "SYSTEM", current_time)
            for sid, name, etype, freq, sdate, stime, edate, active, 
            cron, retry, timeout in schedule_entries
        ]
        
        df = self.spark.createDataFrame(data, schema)
        
        # Write to output
        config = self.config['data_init']
        output_path = config['output_paths']['schedule_data']
        output_format = config['output_format']
        
        self._write_dataframe(df, output_path, output_format, "schedule_data")
        
        self.logger.info(f"✓ Created {len(schedule_entries)} schedule entries")
    
    def create_category_metadata(self) -> None:
        """Generate category metadata with business rules."""
        self.logger.info("Creating category metadata...")
        
        schema = StructType([
            StructField("category", StringType(), False),
            StructField("priority_level", IntegerType(), False),
            StructField("multiplier", DecimalType(3, 2), False),
            StructField("min_value", DecimalType(15, 2), False),
            StructField("max_value", DecimalType(15, 2), False),
            StructField("description", StringType(), True),
            StructField("is_active", BooleanType(), False)
        ])
        
        categories = [
            ("PREMIUM", 1, 1.5, 500.0, 10000.0, 
             "Premium tier with highest priority and 50% value boost", True),
            ("VIP", 1, 2.0, 1000.0, 20000.0, 
             "VIP tier with double value multiplier", True),
            ("STANDARD", 2, 1.2, 100.0, 5000.0, 
             "Standard tier with 20% value boost", True),
            ("BASIC", 3, 1.0, 50.0, 1000.0, 
             "Basic tier with no multiplier", True),
            ("TRIAL", 4, 0.8, 10.0, 500.0, 
             "Trial tier with reduced multiplier", True),
        ]
        
        df = self.spark.createDataFrame(categories, schema)
        
        output_path = "data/category_metadata"
        output_format = self.config['data_init']['output_format']
        
        self._write_dataframe(df, output_path, output_format, "category_metadata")
        
        self.logger.info(f"✓ Created {len(categories)} category metadata entries")
    
    def _write_dataframe(self, df, path: str, format: str, name: str) -> None:
        """
        Write DataFrame to storage.
        
        Args:
            df: DataFrame to write
            path: Output path
            format: Output format (parquet, csv, etc.)
            name: Dataset name for logging
        """
        try:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            
            df.write.mode("overwrite").format(format).save(path)
            
            record_count = df.count()
            self.logger.info(f"Wrote {record_count} records to {path}")
            
        except Exception as e:
            self.logger.error(f"Failed to write {name}: {str(e)}")
            raise
    
    def initialize_all(self) -> None:
        """Initialize all sample data."""
        self.logger.info("=" * 60)
        self.logger.info("Starting data initialization...")
        self.logger.info("=" * 60)
        
        try:
            self.create_sample_source_data()
            self.create_config_data()
            self.create_schedule_data()
            self.create_category_metadata()
            
            self.logger.info("=" * 60)
            self.logger.info("✓ Data initialization completed successfully")
            self.logger.info("=" * 60)
            
        except Exception as e:
            self.logger.error(f"Data initialization failed: {str(e)}")
            raise
    
    def show_summary(self) -> None:
        """Display summary of generated data."""
        config = self.config['data_init']
        
        print("\n" + "=" * 60)
        print("DATA INITIALIZATION SUMMARY")
        print("=" * 60)
        
        paths = config['output_paths']
        for name, path in paths.items():
            try:
                df = self.spark.read.format(config['output_format']).load(path)
                count = df.count()
                print(f"  {name:20s}: {count:6d} records -> {path}")
            except Exception:
                print(f"  {name:20s}: Not found")
        
        print("=" * 60 + "\n")


def main():
    """Main execution function."""
    # Create Spark session
    spark = SparkSession.builder \
        .appName("ETL Data Initialization") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        # Initialize data
        initializer = DataInitializer(spark)
        initializer.initialize_all()
        initializer.show_summary()
        
    except Exception as e:
        logging.error(f"Initialization failed: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()