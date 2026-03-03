"""
PySpark Data Initialization Utility
Generates sample records with categories, configuration key-value pairs, and ETL schedule definitions.
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
    """Initializes sample data for ETL testing"""
    
    def __init__(self, spark: SparkSession, config_path: str = "config.yaml"):
        self.spark = spark
        self.config = self._load_config(config_path)
        self.logger = self._setup_logger()
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def create_sample_source_data(self, num_rows: int = 100):
        """
        Create sample source data with various categories
        
        Args:
            num_rows: Number of sample records to generate
        """
        self.logger.info(f"Creating {num_rows} sample source records...")
        
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
        
        # Generate sample data
        categories = self.config['data_generation']['categories']
        statuses = self.config['data_generation']['statuses']
        value_range = self.config['data_generation']['value_range']
        
        current_time = datetime.now()
        
        data = []
        for i in range(1, num_rows + 1):
            record = {
                "id": f"{i:010d}",
                "name": f"Product {i}",
                "value": float(random.randint(value_range['min'], value_range['max'])),
                "status": random.choice(statuses),
                "category": categories[i % len(categories)],
                "source_system": "SAP_ERP",
                "created_at": current_time,
                "created_by": "SYSTEM",
                "changed_at": current_time,
                "changed_by": "SYSTEM"
            }
            data.append(record)
        
        # Create DataFrame
        df = self.spark.createDataFrame(data, schema)
        
        # Write to table/file
        output_path = self.config['paths']['source_data']
        df.write.mode("overwrite").parquet(output_path)
        
        self.logger.info(f"✓ Created {df.count()} source records at {output_path}")
        
        return df
    
    def create_config_data(self):
        """Create configuration key-value pairs"""
        self.logger.info("Creating configuration data...")
        
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
        
        config_entries = [
            {
                "config_key": "BATCH_SIZE",
                "config_value": "1000",
                "description": "Default batch size for data loading",
                "config_type": "PERFORMANCE",
                "is_active": True,
                "changed_at": current_time,
                "changed_by": "SYSTEM"
            },
            {
                "config_key": "MAX_RETRIES",
                "config_value": "3",
                "description": "Maximum number of retries on error",
                "config_type": "ERROR_HANDLING",
                "is_active": True,
                "changed_at": current_time,
                "changed_by": "SYSTEM"
            },
            {
                "config_key": "ALERT_EMAIL",
                "config_value": "admin@example.com",
                "description": "Email address for alerts",
                "config_type": "NOTIFICATION",
                "is_active": True,
                "changed_at": current_time,
                "changed_by": "SYSTEM"
            },
            {
                "config_key": "LOG_RETENTION_DAYS",
                "config_value": "90",
                "description": "Number of days to retain logs",
                "config_type": "MAINTENANCE",
                "is_active": True,
                "changed_at": current_time,
                "changed_by": "SYSTEM"
            },
            {
                "config_key": "ENABLE_RECONCILIATION",
                "config_value": "true",
                "description": "Enable data reconciliation after load",
                "config_type": "DATA_QUALITY",
                "is_active": True,
                "changed_at": current_time,
                "changed_by": "SYSTEM"
            },
            {
                "config_key": "PREMIUM_MULTIPLIER",
                "config_value": "1.5",
                "description": "Value multiplier for premium category",
                "config_type": "BUSINESS_RULE",
                "is_active": True,
                "changed_at": current_time,
                "changed_by": "SYSTEM"
            },
            {
                "config_key": "STANDARD_MULTIPLIER",
                "config_value": "1.2",
                "description": "Value multiplier for standard category",
                "config_type": "BUSINESS_RULE",
                "is_active": True,
                "changed_at": current_time,
                "changed_by": "SYSTEM"
            },
            {
                "config_key": "BASIC_MULTIPLIER",
                "config_value": "1.0",
                "description": "Value multiplier for basic category",
                "config_type": "BUSINESS_RULE",
                "is_active": True,
                "changed_at": current_time,
                "changed_by": "SYSTEM"
            },
            {
                "config_key": "VIP_MULTIPLIER",
                "config_value": "2.0",
                "description": "Value multiplier for VIP category",
                "config_type": "BUSINESS_RULE",
                "is_active": True,
                "changed_at": current_time,
                "changed_by": "SYSTEM"
            },
            {
                "config_key": "TRIAL_MULTIPLIER",
                "config_value": "0.8",
                "description": "Value multiplier for trial category",
                "config_type": "BUSINESS_RULE",
                "is_active": True,
                "changed_at": current_time,
                "changed_by": "SYSTEM"
            },
            {
                "config_key": "HIGH_VALUE_THRESHOLD",
                "config_value": "750",
                "description": "Threshold for high value classification",
                "config_type": "BUSINESS_RULE",
                "is_active": True,
                "changed_at": current_time,
                "changed_by": "SYSTEM"
            },
            {
                "config_key": "MEDIUM_VALUE_THRESHOLD",
                "config_value": "300",
                "description": "Threshold for medium value classification",
                "config_type": "BUSINESS_RULE",
                "is_active": True,
                "changed_at": current_time,
                "changed_by": "SYSTEM"
            }
        ]
        
        df = self.spark.createDataFrame(config_entries, schema)
        
        output_path = self.config['paths']['config_data']
        df.write.mode("overwrite").parquet(output_path)
        
        self.logger.info(f"✓ Created {df.count()} configuration entries at {output_path}")
        
        return df
    
    def create_schedule_data(self):
        """Create ETL schedule definitions"""
        self.logger.info("Creating schedule data...")
        
        schema = StructType([
            StructField("schedule_id", StringType(), False),
            StructField("schedule_name", StringType(), False),
            StructField("etl_type", StringType(), False),
            StructField("frequency", StringType(), False),
            StructField("start_date", StringType(), False),
            StructField("start_time", StringType(), False),
            StructField("is_active", BooleanType(), False),
            StructField("created_by", StringType(), False),
            StructField("created_at", TimestampType(), False),
            StructField("parameters", StringType(), True)
        ])
        
        current_time = datetime.now()
        current_date = current_time.strftime("%Y-%m-%d")
        
        schedules = [
            {
                "schedule_id": "SCHED001",
                "schedule_name": "Daily Full Load",
                "etl_type": "FULL",
                "frequency": "DAILY",
                "start_date": current_date,
                "start_time": "02:00:00",
                "is_active": True,
                "created_by": "SYSTEM",
                "created_at": current_time,
                "parameters": '{"source_type": "DATABASE", "target_type": "DATABASE", "batch_size": 1000}'
            },
            {
                "schedule_id": "SCHED002",
                "schedule_name": "Hourly Incremental",
                "etl_type": "INCREMENTAL",
                "frequency": "HOURLY",
                "start_date": current_date,
                "start_time": "00:00:00",
                "is_active": True,
                "created_by": "SYSTEM",
                "created_at": current_time,
                "parameters": '{"source_type": "INCREMENTAL", "target_type": "DATABASE", "batch_size": 500}'
            },
            {
                "schedule_id": "SCHED003",
                "schedule_name": "Weekly Reconciliation",
                "etl_type": "RECONCILIATION",
                "frequency": "WEEKLY",
                "start_date": current_date,
                "start_time": "18:00:00",
                "is_active": False,
                "created_by": "SYSTEM",
                "created_at": current_time,
                "parameters": '{"reconciliation_days": 7, "alert_on_mismatch": true}'
            },
            {
                "schedule_id": "SCHED004",
                "schedule_name": "Monthly Archive",
                "etl_type": "ARCHIVE",
                "frequency": "MONTHLY",
                "start_date": current_date,
                "start_time": "01:00:00",
                "is_active": True,
                "created_by": "SYSTEM",
                "created_at": current_time,
                "parameters": '{"retention_months": 12, "compression": "gzip"}'
            },
            {
                "schedule_id": "SCHED005",
                "schedule_name": "Quality Check - Daily",
                "etl_type": "QUALITY_CHECK",
                "frequency": "DAILY",
                "start_date": current_date,
                "start_time": "06:00:00",
                "is_active": True,
                "created_by": "SYSTEM",
                "created_at": current_time,
                "parameters": '{"check_types": ["completeness", "uniqueness", "validity"], "alert_threshold": 0.95}'
            }
        ]
        
        df = self.spark.createDataFrame(schedules, schema)
        
        output_path = self.config['paths']['schedule_data']
        df.write.mode("overwrite").parquet(output_path)
        
        self.logger.info(f"✓ Created {df.count()} schedule entries at {output_path}")
        
        return df
    
    def create_staging_data(self, num_rows: int = 50):
        """Create sample staging data"""
        self.logger.info(f"Creating {num_rows} staging records...")
        
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("run_id", StringType(), False),
            StructField("status", StringType(), False),
            StructField("raw_data", StringType(), True),
            StructField("parsed_data", StringType(), True),
            StructField("created_at", TimestampType(), False),
            StructField("processed_at", TimestampType(), True)
        ])
        
        current_time = datetime.now()
        
        data = []
        for i in range(1, num_rows + 1):
            record = {
                "id": f"STG{i:07d}",
                "run_id": "RUN001",
                "status": "READY" if i % 4 != 0 else "PROCESSED",
                "raw_data": f'{{"product_id": "{i}", "value": {random.randint(50, 1000)}}}',
                "parsed_data": None if i % 4 != 0 else f'{{"id": "{i}", "validated": true}}',
                "created_at": current_time - timedelta(hours=random.randint(1, 24)),
                "processed_at": current_time if i % 4 == 0 else None
            }
            data.append(record)
        
        df = self.spark.createDataFrame(data, schema)
        
        output_path = self.config['paths']['staging_data']
        df.write.mode("overwrite").parquet(output_path)
        
        self.logger.info(f"✓ Created {df.count()} staging records at {output_path}")
        
        return df
    
    def initialize_all(self, num_source_rows: int = 100, num_staging_rows: int = 50):
        """Initialize all data tables"""
        self.logger.info("=" * 60)
        self.logger.info("Starting Data Initialization")
        self.logger.info("=" * 60)
        
        try:
            # Create all data
            source_df = self.create_sample_source_data(num_source_rows)
            config_df = self.create_config_data()
            schedule_df = self.create_schedule_data()
            staging_df = self.create_staging_data(num_staging_rows)
            
            # Summary
            self.logger.info("=" * 60)
            self.logger.info("Data Initialization Complete")
            self.logger.info("=" * 60)
            self.logger.info(f"✓ Source records: {source_df.count()}")
            self.logger.info(f"✓ Config entries: {config_df.count()}")
            self.logger.info(f"✓ Schedule entries: {schedule_df.count()}")
            self.logger.info(f"✓ Staging records: {staging_df.count()}")
            self.logger.info("=" * 60)
            
            return {
                'source': source_df,
                'config': config_df,
                'schedule': schedule_df,
                'staging': staging_df
            }
            
        except Exception as e:
            self.logger.error(f"✗ Failed to initialize data: {str(e)}")
            raise


def main():
    """Main execution function"""
    # Create Spark session
    spark = SparkSession.builder \
        .appName("ETL Data Initialization") \
        .config("spark.sql.warehouse.dir", "./spark-warehouse") \
        .getOrCreate()
    
    try:
        # Initialize data
        initializer = DataInitializer(spark, "config.yaml")
        results = initializer.initialize_all(num_source_rows=100, num_staging_rows=50)
        
        # Display sample data
        print("\n" + "=" * 60)
        print("Sample Source Data (first 5 rows):")
        print("=" * 60)
        results['source'].show(5, truncate=False)
        
        print("\n" + "=" * 60)
        print("Configuration Data:")
        print("=" * 60)
        results['config'].show(truncate=False)
        
        print("\n" + "=" * 60)
        print("Schedule Data:")
        print("=" * 60)
        results['schedule'].show(truncate=False)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()