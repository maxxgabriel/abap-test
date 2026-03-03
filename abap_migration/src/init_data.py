"""
ETL Data Initialization Module
Creates sample data for testing ETL processes
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType, IntegerType
from datetime import datetime
import random
from typing import List, Tuple
import logging

logger = logging.getLogger(__name__)


class ETLDataInitializer:
    """Initialize sample data for ETL testing"""
    
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.categories = ['PREMIUM', 'STANDARD', 'BASIC', 'VIP', 'TRIAL']
        
    def create_sample_data(self, num_rows: int = 100) -> None:
        """Create sample source data"""
        logger.info(f"Creating {num_rows} sample source records...")
        
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
        
        data = []
        current_time = datetime.now()
        
        for i in range(1, num_rows + 1):
            value = random.uniform(50, 1000)
            category = self.categories[i % len(self.categories)]
            
            data.append((
                f"{i:010d}",
                f"Product {i}",
                round(value, 2),
                "ACTIVE",
                category,
                "SAP_ERP",
                current_time,
                "SYSTEM",
                current_time,
                "SYSTEM"
            ))
        
        df = self.spark.createDataFrame(data, schema)
        
        # Write to source table
        source_path = self.config.get('paths', {}).get('source_data', '/data/source')
        df.write.mode("overwrite").parquet(source_path)
        
        logger.info(f"✓ Created {num_rows} source records at {source_path}")
        
    def create_config_data(self) -> None:
        """Create configuration data"""
        logger.info("Creating configuration data...")
        
        schema = StructType([
            StructField("config_key", StringType(), False),
            StructField("config_value", StringType(), False),
            StructField("description", StringType(), False),
            StructField("config_type", StringType(), False),
            StructField("is_active", StringType(), False),
            StructField("changed_at", TimestampType(), False),
            StructField("changed_by", StringType(), False)
        ])
        
        current_time = datetime.now()
        
        config_data = [
            ("BATCH_SIZE", "1000", "Default batch size for data loading", "PERFORMANCE", "Y", current_time, "SYSTEM"),
            ("MAX_RETRIES", "3", "Maximum number of retries on error", "ERROR_HANDLING", "Y", current_time, "SYSTEM"),
            ("ALERT_EMAIL", "admin@example.com", "Email address for alerts", "NOTIFICATION", "Y", current_time, "SYSTEM"),
            ("LOG_RETENTION_DAYS", "90", "Number of days to retain logs", "MAINTENANCE", "Y", current_time, "SYSTEM"),
            ("ENABLE_RECONCILIATION", "Y", "Enable data reconciliation after load", "DATA_QUALITY", "Y", current_time, "SYSTEM"),
            ("PREMIUM_MULTIPLIER", "1.5", "Value multiplier for premium category", "BUSINESS_RULE", "Y", current_time, "SYSTEM"),
            ("HIGH_VALUE_THRESHOLD", "750", "Threshold for high value classification", "BUSINESS_RULE", "Y", current_time, "SYSTEM"),
            ("MEDIUM_VALUE_THRESHOLD", "300", "Threshold for medium value classification", "BUSINESS_RULE", "Y", current_time, "SYSTEM")
        ]
        
        df = self.spark.createDataFrame(config_data, schema)
        
        config_path = self.config.get('paths', {}).get('config_data', '/data/config')
        df.write.mode("overwrite").parquet(config_path)
        
        logger.info(f"✓ Created {len(config_data)} configuration entries at {config_path}")
        
    def create_schedule_data(self) -> None:
        """Create schedule data"""
        logger.info("Creating schedule data...")
        
        schema = StructType([
            StructField("schedule_id", StringType(), False),
            StructField("schedule_name", StringType(), False),
            StructField("etl_type", StringType(), False),
            StructField("frequency", StringType(), False),
            StructField("start_date", StringType(), False),
            StructField("start_time", StringType(), False),
            StructField("is_active", StringType(), False),
            StructField("created_by", StringType(), False),
            StructField("created_at", TimestampType(), False)
        ])
        
        current_time = datetime.now()
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        schedule_data = [
            ("SCHED001", "Daily Full Load", "FULL", "DAILY", current_date, "02:00:00", "Y", "SYSTEM", current_time),
            ("SCHED002", "Hourly Incremental", "INCREMENTAL", "HOURLY", current_date, "00:00:00", "Y", "SYSTEM", current_time),
            ("SCHED003", "Weekly Reconciliation", "RECONCILIATION", "WEEKLY", current_date, "18:00:00", "N", "SYSTEM", current_time)
        ]
        
        df = self.spark.createDataFrame(schedule_data, schema)
        
        schedule_path = self.config.get('paths', {}).get('schedule_data', '/data/schedule')
        df.write.mode("overwrite").parquet(schedule_path)
        
        logger.info(f"✓ Created {len(schedule_data)} schedule entries at {schedule_path}")
        
    def initialize_all(self, num_rows: int = 100) -> None:
        """Initialize all sample data"""
        logger.info("Starting full data initialization...")
        self.create_sample_data(num_rows)
        self.create_config_data()
        self.create_schedule_data()
        logger.info("✓ Data initialization complete")


def main():
    """Main execution function"""
    import yaml
    
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    spark = SparkSession.builder \
        .appName("ETL Data Initialization") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        initializer = ETLDataInitializer(spark, config)
        initializer.initialize_all(num_rows=100)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()