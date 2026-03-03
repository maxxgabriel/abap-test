"""
PySpark Data Initialization Utility
Generates sample records with categories, configuration key-value pairs, and ETL schedule definitions.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, TimestampType, BooleanType
)
from pyspark.sql.functions import current_timestamp, col, lit
from datetime import datetime, timedelta
import random
import yaml
import logging
from typing import Dict, List


class DataInitializer:
    """Initialize sample data for ETL testing using PySpark DataFrames"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize DataInitializer with configuration
        
        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.spark = self._create_spark_session()
        self.logger = self._setup_logger()
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            # Return default config if file not found
            return {
                'init_data': {
                    'sample_rows': 100,
                    'categories': ['PREMIUM', 'STANDARD', 'BASIC', 'VIP', 'TRIAL'],
                    'value_range': {'min': 50, 'max': 1000}
                },
                'spark': {
                    'app_name': 'ETL_Data_Initializer',
                    'master': 'local[*]'
                },
                'database': {
                    'format': 'delta',
                    'checkpoint_location': '/tmp/etl_checkpoints'
                }
            }
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        spark_config = self.config.get('spark', {})
        
        builder = SparkSession.builder \
            .appName(spark_config.get('app_name', 'ETL_Data_Initializer')) \
            .master(spark_config.get('master', 'local[*]'))
        
        # Add additional Spark configurations
        for key, value in spark_config.get('configs', {}).items():
            builder = builder.config(key, value)
        
        return builder.getOrCreate()
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def create_sample_data(self, num_rows: int = None) -> None:
        """
        Generate sample source data records
        
        Args:
            num_rows: Number of rows to generate (defaults to config value)
        """
        self.logger.info("Creating sample source data...")
        
        num_rows = num_rows or self.config['init_data']['sample_rows']
        categories = self.config['init_data']['categories']
        value_range = self.config['init_data']['value_range']
        
        # Define schema for source data
        source_schema = StructType([
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
        current_time = datetime.now()
        data = []
        
        for i in range(1, num_rows + 1):
            # Generate random value
            value = round(random.uniform(
                value_range['min'], 
                value_range['max']
            ), 2)
            
            # Assign category based on ID modulo
            category = categories[i % len(categories)]
            
            # Create record
            record = (
                f"{i:010d}",  # Zero-padded ID
                f"Product {i}",
                float(value),
                "ACTIVE",
                category,
                "SAP_ERP",
                current_time,
                "SYSTEM",
                current_time,
                "SYSTEM"
            )
            data.append(record)
        
        # Create DataFrame
        df = self.spark.createDataFrame(data, schema=source_schema)
        
        # Write to storage
        output_path = self.config.get('database', {}).get('source_data_path', 
                                                           'data/source_data')
        df.write \
            .mode("overwrite") \
            .format(self.config.get('database', {}).get('format', 'parquet')) \
            .save(output_path)
        
        self.logger.info(f"✓ Created {num_rows} source records at {output_path}")
        
        # Show sample
        df.show(5, truncate=False)
    
    def create_config_data(self) -> None:
        """Generate configuration key-value pairs"""
        self.logger.info("Creating configuration data...")
        
        # Define schema for config data
        config_schema = StructType([
            StructField("config_key", StringType(), False),
            StructField("config_value", StringType(), False),
            StructField("description", StringType(), False),
            StructField("config_type", StringType(), False),
            StructField("is_active", BooleanType(), False),
            StructField("changed_at", TimestampType(), False),
            StructField("changed_by", StringType(), False)
        ])
        
        # Configuration entries
        current_time = datetime.now()
        config_data = [
            (
                "BATCH_SIZE",
                "1000",
                "Default batch size for data loading",
                "PERFORMANCE",
                True,
                current_time,
                "SYSTEM"
            ),
            (
                "MAX_RETRIES",
                "3",
                "Maximum number of retries on error",
                "ERROR_HANDLING",
                True,
                current_time,
                "SYSTEM"
            ),
            (
                "ALERT_EMAIL",
                "admin@example.com",
                "Email address for alerts",
                "NOTIFICATION",
                True,
                current_time,
                "SYSTEM"
            ),
            (
                "LOG_RETENTION_DAYS",
                "90",
                "Number of days to retain logs",
                "MAINTENANCE",
                True,
                current_time,
                "SYSTEM"
            ),
            (
                "ENABLE_RECONCILIATION",
                "true",
                "Enable data reconciliation after load",
                "DATA_QUALITY",
                True,
                current_time,
                "SYSTEM"
            ),
            (
                "PREMIUM_MULTIPLIER",
                "1.5",
                "Value multiplier for premium category",
                "BUSINESS_RULE",
                True,
                current_time,
                "SYSTEM"
            ),
            (
                "STANDARD_MULTIPLIER",
                "1.2",
                "Value multiplier for standard category",
                "BUSINESS_RULE",
                True,
                current_time,
                "SYSTEM"
            ),
            (
                "VIP_MULTIPLIER",
                "2.0",
                "Value multiplier for VIP category",
                "BUSINESS_RULE",
                True,
                current_time,
                "SYSTEM"
            ),
            (
                "ERROR_THRESHOLD",
                "0.05",
                "Maximum acceptable error rate (5%)",
                "DATA_QUALITY",
                True,
                current_time,
                "SYSTEM"
            ),
            (
                "PARALLEL_JOBS",
                "4",
                "Number of parallel ETL jobs",
                "PERFORMANCE",
                True,
                current_time,
                "SYSTEM"
            )
        ]
        
        # Create DataFrame
        df = self.spark.createDataFrame(config_data, schema=config_schema)
        
        # Write to storage
        output_path = self.config.get('database', {}).get('config_data_path', 
                                                           'data/config_data')
        df.write \
            .mode("overwrite") \
            .format(self.config.get('database', {}).get('format', 'parquet')) \
            .save(output_path)
        
        self.logger.info(f"✓ Created {len(config_data)} configuration entries at {output_path}")
        
        # Show all config
        df.show(truncate=False)
    
    def create_schedule_data(self) -> None:
        """Generate ETL schedule definitions"""
        self.logger.info("Creating schedule data...")
        
        # Define schema for schedule data
        schedule_schema = StructType([
            StructField("schedule_id", StringType(), False),
            StructField("schedule_name", StringType(), False),
            StructField("etl_type", StringType(), False),
            StructField("frequency", StringType(), False),
            StructField("start_date", StringType(), False),
            StructField("start_time", StringType(), False),
            StructField("end_date", StringType(), True),
            StructField("is_active", BooleanType(), False),
            StructField("cron_expression", StringType(), True),
            StructField("timezone", StringType(), False),
            StructField("retry_count", IntegerType(), False),
            StructField("timeout_minutes", IntegerType(), False),
            StructField("created_by", StringType(), False),
            StructField("created_at", TimestampType(), False)
        ])
        
        # Schedule entries
        current_time = datetime.now()
        today = datetime.now().strftime("%Y-%m-%d")
        
        schedule_data = [
            (
                "SCHED001",
                "Daily Full Load",
                "FULL",
                "DAILY",
                today,
                "02:00:00",
                None,
                True,
                "0 2 * * *",  # Daily at 2 AM
                "UTC",
                3,
                120,
                "SYSTEM",
                current_time
            ),
            (
                "SCHED002",
                "Hourly Incremental",
                "INCREMENTAL",
                "HOURLY",
                today,
                "00:00:00",
                None,
                True,
                "0 * * * *",  # Every hour
                "UTC",
                2,
                30,
                "SYSTEM",
                current_time
            ),
            (
                "SCHED003",
                "Weekly Reconciliation",
                "RECONCILIATION",
                "WEEKLY",
                today,
                "18:00:00",
                None,
                False,
                "0 18 * * 0",  # Sunday at 6 PM
                "UTC",
                1,
                240,
                "SYSTEM",
                current_time
            ),
            (
                "SCHED004",
                "Monthly Archive",
                "ARCHIVE",
                "MONTHLY",
                today,
                "00:00:00",
                None,
                True,
                "0 0 1 * *",  # First day of month at midnight
                "UTC",
                1,
                480,
                "SYSTEM",
                current_time
            ),
            (
                "SCHED005",
                "Real-time Stream Processing",
                "STREAMING",
                "CONTINUOUS",
                today,
                "00:00:00",
                None,
                True,
                None,
                "UTC",
                0,
                0,
                "SYSTEM",
                current_time
            ),
            (
                "SCHED006",
                "Quality Check Batch",
                "QUALITY_CHECK",
                "DAILY",
                today,
                "04:00:00",
                None,
                True,
                "0 4 * * *",  # Daily at 4 AM
                "UTC",
                2,
                60,
                "SYSTEM",
                current_time
            )
        ]
        
        # Create DataFrame
        df = self.spark.createDataFrame(schedule_data, schema=schedule_schema)
        
        # Write to storage
        output_path = self.config.get('database', {}).get('schedule_data_path', 
                                                           'data/schedule_data')
        df.write \
            .mode("overwrite") \
            .format(self.config.get('database', {}).get('format', 'parquet')) \
            .save(output_path)
        
        self.logger.info(f"✓ Created {len(schedule_data)} schedule entries at {output_path}")
        
        # Show all schedules
        df.show(truncate=False)
    
    def create_category_metadata(self) -> None:
        """Generate category metadata with business rules"""
        self.logger.info("Creating category metadata...")
        
        # Define schema
        category_schema = StructType([
            StructField("category", StringType(), False),
            StructField("description", StringType(), False),
            StructField("priority_base", IntegerType(), False),
            StructField("value_multiplier", DecimalType(5, 2), False),
            StructField("discount_eligible", BooleanType(), False),
            StructField("validation_rules", StringType(), True),
            StructField("is_active", BooleanType(), False),
            StructField("created_at", TimestampType(), False)
        ])
        
        # Category data
        current_time = datetime.now()
        category_data = [
            (
                "PREMIUM",
                "Premium tier customers with enhanced benefits",
                1,
                1.5,
                True,
                "value >= 500",
                True,
                current_time
            ),
            (
                "VIP",
                "VIP customers with maximum benefits",
                1,
                2.0,
                True,
                "value >= 800",
                True,
                current_time
            ),
            (
                "STANDARD",
                "Standard tier customers",
                3,
                1.2,
                True,
                "value >= 200",
                True,
                current_time
            ),
            (
                "BASIC",
                "Basic tier customers",
                4,
                1.0,
                False,
                "value >= 50",
                True,
                current_time
            ),
            (
                "TRIAL",
                "Trial customers with limited access",
                5,
                0.9,
                False,
                "value >= 0",
                True,
                current_time
            )
        ]
        
        # Create DataFrame
        df = self.spark.createDataFrame(category_data, schema=category_schema)
        
        # Write to storage
        output_path = self.config.get('database', {}).get('category_metadata_path', 
                                                           'data/category_metadata')
        df.write \
            .mode("overwrite") \
            .format(self.config.get('database', {}).get('format', 'parquet')) \
            .save(output_path)
        
        self.logger.info(f"✓ Created {len(category_data)} category metadata entries at {output_path}")
        
        df.show(truncate=False)
    
    def initialize_all(self, num_rows: int = None) -> None:
        """
        Initialize all data sets
        
        Args:
            num_rows: Number of sample rows to generate
        """
        self.logger.info("="*70)
        self.logger.info("ETL Data Initialization Started")
        self.logger.info("="*70)
        
        try:
            # Create all datasets
            self.create_sample_data(num_rows)
            self.create_config_data()
            self.create_schedule_data()
            self.create_category_metadata()
            
            self.logger.info("="*70)
            self.logger.info("✓ All data initialization completed successfully")
            self.logger.info("="*70)
            
        except Exception as e:
            self.logger.error(f"✗ Data initialization failed: {str(e)}")
            raise
    
    def verify_data(self) -> None:
        """Verify that all data was created successfully"""
        self.logger.info("Verifying initialized data...")
        
        datasets = {
            'source_data': self.config.get('database', {}).get('source_data_path', 'data/source_data'),
            'config_data': self.config.get('database', {}).get('config_data_path', 'data/config_data'),
            'schedule_data': self.config.get('database', {}).get('schedule_data_path', 'data/schedule_data'),
            'category_metadata': self.config.get('database', {}).get('category_metadata_path', 'data/category_metadata')
        }
        
        for name, path in datasets.items():
            try:
                df = self.spark.read \
                    .format(self.config.get('database', {}).get('format', 'parquet')) \
                    .load(path)
                count = df.count()
                self.logger.info(f"  ✓ {name}: {count} records")
            except Exception as e:
                self.logger.error(f"  ✗ {name}: Failed to read - {str(e)}")
    
    def cleanup(self) -> None:
        """Cleanup resources"""
        if self.spark:
            self.spark.stop()
            self.logger.info("Spark session stopped")


def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Initialize ETL test data')
    parser.add_argument('--rows', type=int, default=100, 
                       help='Number of sample rows to generate')
    parser.add_argument('--config', type=str, default='config.yaml',
                       help='Path to configuration file')
    parser.add_argument('--verify', action='store_true',
                       help='Verify data after initialization')
    
    args = parser.parse_args()
    
    # Initialize data
    initializer = DataInitializer(config_path=args.config)
    
    try:
        initializer.initialize_all(num_rows=args.rows)
        
        if args.verify:
            initializer.verify_data()
            
    finally:
        initializer.cleanup()


if __name__ == "__main__":
    main()