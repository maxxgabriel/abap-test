"""
PySpark Data Initialization Utility
Generates sample records with categories, configuration key-value pairs, and ETL schedule definitions.
"""

import sys
from datetime import datetime, timedelta
from typing import List, Tuple
import random

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, 
    IntegerType, TimestampType, BooleanType
)
from pyspark.sql.functions import lit, current_timestamp
import yaml


class DataInitializer:
    """Initializes sample data for ETL testing and operations."""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize the data initializer.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = self._setup_logging()
        
    def _setup_logging(self):
        """Configure logging for the initializer."""
        log4j = self.spark._jvm.org.apache.log4j
        return log4j.LogManager.getLogger(self.__class__.__name__)
    
    def create_sample_data(self, num_rows: int = 100) -> DataFrame:
        """
        Generate sample source data records.
        
        Args:
            num_rows: Number of sample records to generate
            
        Returns:
            DataFrame with sample source data
        """
        self.logger.info(f"Creating {num_rows} sample source data records...")
        
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
        
        # Category definitions
        categories = ['PREMIUM', 'STANDARD', 'BASIC', 'VIP', 'TRIAL']
        
        # Generate sample data
        sample_data = []
        current_time = datetime.now()
        
        for i in range(1, num_rows + 1):
            # Generate random value between 50 and 1000
            value = round(random.uniform(50, 1000), 2)
            
            # Assign category based on index
            category = categories[i % len(categories)]
            
            record = (
                str(i).zfill(10),  # id: zero-padded to 10 digits
                f"Product {i}",    # name
                float(value),      # value
                "ACTIVE",          # status
                category,          # category
                "SAP_ERP",        # source_system
                current_time,     # created_at
                "SYSTEM",         # created_by
                current_time,     # changed_at
                "SYSTEM"          # changed_by
            )
            sample_data.append(record)
        
        # Create DataFrame
        df = self.spark.createDataFrame(sample_data, schema)
        
        self.logger.info(f"✓ Created {df.count()} source records")
        return df
    
    def create_config_data(self) -> DataFrame:
        """
        Generate configuration key-value pairs.
        
        Returns:
            DataFrame with configuration data
        """
        self.logger.info("Creating configuration data...")
        
        # Define schema
        schema = StructType([
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
        config_entries = [
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
                "BASIC_MULTIPLIER",
                "1.0",
                "Value multiplier for basic category",
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
                "TRIAL_MULTIPLIER",
                "0.8",
                "Value multiplier for trial category",
                "BUSINESS_RULE",
                True,
                current_time,
                "SYSTEM"
            ),
            (
                "HIGH_VALUE_THRESHOLD",
                "750.00",
                "Threshold for high value classification",
                "BUSINESS_RULE",
                True,
                current_time,
                "SYSTEM"
            ),
            (
                "MEDIUM_VALUE_THRESHOLD",
                "300.00",
                "Threshold for medium value classification",
                "BUSINESS_RULE",
                True,
                current_time,
                "SYSTEM"
            )
        ]
        
        # Create DataFrame
        df = self.spark.createDataFrame(config_entries, schema)
        
        self.logger.info(f"✓ Created {df.count()} configuration entries")
        return df
    
    def create_schedule_data(self) -> DataFrame:
        """
        Generate ETL schedule definitions.
        
        Returns:
            DataFrame with schedule data
        """
        self.logger.info("Creating schedule data...")
        
        # Define schema
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
            StructField("description", StringType(), True)
        ])
        
        # Schedule entries
        current_time = datetime.now()
        today = current_time.strftime("%Y-%m-%d")
        
        schedule_entries = [
            (
                "SCHED001",
                "Daily Full Load",
                "FULL",
                "DAILY",
                today,
                "02:00:00",
                True,
                "SYSTEM",
                current_time,
                "Full data load executed daily at 2 AM"
            ),
            (
                "SCHED002",
                "Hourly Incremental",
                "INCREMENTAL",
                "HOURLY",
                today,
                "00:00:00",
                True,
                "SYSTEM",
                current_time,
                "Incremental load executed every hour"
            ),
            (
                "SCHED003",
                "Weekly Reconciliation",
                "RECONCILIATION",
                "WEEKLY",
                today,
                "18:00:00",
                False,
                "SYSTEM",
                current_time,
                "Data reconciliation executed weekly on Sunday at 6 PM"
            ),
            (
                "SCHED004",
                "Monthly Archive",
                "ARCHIVE",
                "MONTHLY",
                today,
                "03:00:00",
                True,
                "SYSTEM",
                current_time,
                "Archive old data monthly on the 1st at 3 AM"
            ),
            (
                "SCHED005",
                "Real-time Sync",
                "STREAMING",
                "CONTINUOUS",
                today,
                "00:00:00",
                False,
                "SYSTEM",
                current_time,
                "Continuous real-time data synchronization"
            )
        ]
        
        # Create DataFrame
        df = self.spark.createDataFrame(schedule_entries, schema)
        
        self.logger.info(f"✓ Created {df.count()} schedule entries")
        return df
    
    def save_to_table(self, df: DataFrame, table_name: str, mode: str = "overwrite"):
        """
        Save DataFrame to a table.
        
        Args:
            df: DataFrame to save
            table_name: Name of the target table
            mode: Write mode (overwrite, append, etc.)
        """
        try:
            self.logger.info(f"Saving data to table: {table_name}")
            
            # Write to table
            df.write \
                .mode(mode) \
                .format(self.config.get('output_format', 'parquet')) \
                .saveAsTable(table_name)
            
            self.logger.info(f"✓ Successfully saved {df.count()} records to {table_name}")
            
        except Exception as e:
            self.logger.error(f"✗ Failed to save data to {table_name}: {str(e)}")
            raise
    
    def save_to_path(self, df: DataFrame, path: str, mode: str = "overwrite"):
        """
        Save DataFrame to a file path.
        
        Args:
            df: DataFrame to save
            path: Output file path
            mode: Write mode (overwrite, append, etc.)
        """
        try:
            self.logger.info(f"Saving data to path: {path}")
            
            # Write to path
            df.write \
                .mode(mode) \
                .format(self.config.get('output_format', 'parquet')) \
                .save(path)
            
            self.logger.info(f"✓ Successfully saved {df.count()} records to {path}")
            
        except Exception as e:
            self.logger.error(f"✗ Failed to save data to {path}: {str(e)}")
            raise
    
    def initialize_all_data(self, num_rows: int = 100) -> dict:
        """
        Initialize all sample data (source, config, and schedule).
        
        Args:
            num_rows: Number of sample source records to generate
            
        Returns:
            Dictionary with all generated DataFrames
        """
        self.logger.info("Starting data initialization process...")
        
        results = {}
        
        try:
            # Create sample source data
            source_df = self.create_sample_data(num_rows)
            results['source_data'] = source_df
            
            # Save source data
            if self.config.get('save_to_tables', True):
                self.save_to_table(source_df, self.config['tables']['source_data'])
            if self.config.get('save_to_files', False):
                self.save_to_path(source_df, f"{self.config['output_path']}/source_data")
            
            # Create configuration data
            config_df = self.create_config_data()
            results['config_data'] = config_df
            
            # Save config data
            if self.config.get('save_to_tables', True):
                self.save_to_table(config_df, self.config['tables']['config'])
            if self.config.get('save_to_files', False):
                self.save_to_path(config_df, f"{self.config['output_path']}/config_data")
            
            # Create schedule data
            schedule_df = self.create_schedule_data()
            results['schedule_data'] = schedule_df
            
            # Save schedule data
            if self.config.get('save_to_tables', True):
                self.save_to_table(schedule_df, self.config['tables']['schedule'])
            if self.config.get('save_to_files', False):
                self.save_to_path(schedule_df, f"{self.config['output_path']}/schedule_data")
            
            self.logger.info("✓ Data initialization completed successfully")
            
            return results
            
        except Exception as e:
            self.logger.error(f"✗ Data initialization failed: {str(e)}")
            raise


def load_config(config_path: str) -> dict:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Configuration dictionary
    """
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def main():
    """Main execution function."""
    
    # Parse command line arguments
    if len(sys.argv) < 2:
        print("Usage: spark-submit init_data.py <config_path> [num_rows]")
        sys.exit(1)
    
    config_path = sys.argv[1]
    num_rows = int(sys.argv[2]) if len(sys.argv) > 2 else 100
    
    # Load configuration
    config = load_config(config_path)
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName(config['app_name']) \
        .config("spark.sql.warehouse.dir", config.get('warehouse_dir', '/user/hive/warehouse')) \
        .enableHiveSupport() \
        .getOrCreate()
    
    try:
        # Initialize data
        initializer = DataInitializer(spark, config)
        results = initializer.initialize_all_data(num_rows)
        
        # Display summary
        print("\n" + "="*60)
        print("DATA INITIALIZATION SUMMARY")
        print("="*60)
        print(f"Source Data Records:    {results['source_data'].count()}")
        print(f"Configuration Entries:  {results['config_data'].count()}")
        print(f"Schedule Definitions:   {results['schedule_data'].count()}")
        print("="*60)
        
        # Show sample data
        if config.get('show_samples', True):
            print("\nSample Source Data:")
            results['source_data'].show(5, truncate=False)
            
            print("\nConfiguration Data:")
            results['config_data'].show(5, truncate=False)
            
            print("\nSchedule Data:")
            results['schedule_data'].show(5, truncate=False)
        
    except Exception as e:
        print(f"Error during data initialization: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()