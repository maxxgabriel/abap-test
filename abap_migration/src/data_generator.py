"""
Data generator module for creating test datasets with random categorized records.
"""

import random
import string
from datetime import datetime, timedelta
from typing import List, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, 
    IntegerType, TimestampType
)


class DataGenerator:
    """Generate test ETL datasets with configurable parameters."""
    
    CATEGORIES = ["PREMIUM", "STANDARD", "BASIC", "VIP", "TRIAL"]
    STATUSES = ["ACTIVE", "INACTIVE", "PENDING", "ARCHIVED"]
    SOURCE_SYSTEMS = ["SAP_ERP", "CRM", "WAREHOUSE", "WEB"]
    
    def __init__(self, spark: SparkSession):
        """
        Initialize data generator.
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        
    def generate_source_data(
        self, 
        num_records: int = 100,
        seed: int = 42
    ) -> DataFrame:
        """
        Generate source data records.
        
        Args:
            num_records: Number of records to generate
            seed: Random seed for reproducibility
            
        Returns:
            DataFrame with source data
        """
        random.seed(seed)
        
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
        base_time = datetime.now() - timedelta(days=30)
        
        for i in range(1, num_records + 1):
            record_id = f"{i:010d}"
            name = f"Product {i}"
            value = round(random.uniform(50.0, 1000.0), 2)
            status = random.choice(self.STATUSES)
            category = random.choice(self.CATEGORIES)
            source_system = random.choice(self.SOURCE_SYSTEMS)
            
            created_at = base_time + timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            changed_at = created_at + timedelta(
                days=random.randint(0, 10),
                hours=random.randint(0, 23)
            )
            
            created_by = self._generate_username()
            changed_by = self._generate_username()
            
            data.append((
                record_id, name, value, status, category, source_system,
                created_at, created_by, changed_at, changed_by
            ))
        
        return self.spark.createDataFrame(data, schema)
    
    def generate_staging_data(
        self,
        num_records: int = 50,
        run_id: str = "RUN001",
        seed: int = 42
    ) -> DataFrame:
        """
        Generate staging area data.
        
        Args:
            num_records: Number of records to generate
            run_id: ETL run identifier
            seed: Random seed
            
        Returns:
            DataFrame with staging data
        """
        random.seed(seed)
        
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("run_id", StringType(), False),
            StructField("status", StringType(), False),
            StructField("raw_data", StringType(), True),
            StructField("created_at", TimestampType(), False)
        ])
        
        data = []
        base_time = datetime.now()
        
        for i in range(1, num_records + 1):
            record_id = f"STG{i:07d}"
            status = random.choice(["READY", "PROCESSING", "COMPLETED"])
            raw_data = f"{{\"id\":\"{i}\",\"value\":{random.randint(100, 999)}}}"
            created_at = base_time + timedelta(minutes=i)
            
            data.append((record_id, run_id, status, raw_data, created_at))
        
        return self.spark.createDataFrame(data, schema)
    
    def _generate_username(self) -> str:
        """Generate random username."""
        return f"USER{random.randint(1000, 9999)}"


class ConfigGenerator:
    """Generate configuration key-value stores for ETL."""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize config generator.
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        
    def generate_config(self) -> DataFrame:
        """
        Generate ETL configuration entries.
        
        Returns:
            DataFrame with configuration data
        """
        schema = StructType([
            StructField("config_key", StringType(), False),
            StructField("config_value", StringType(), False),
            StructField("description", StringType(), True),
            StructField("config_type", StringType(), False),
            StructField("is_active", StringType(), False),
            StructField("changed_at", TimestampType(), False),
            StructField("changed_by", StringType(), False)
        ])
        
        now = datetime.now()
        
        data = [
            ("BATCH_SIZE", "1000", "Default batch size for data loading", 
             "PERFORMANCE", "X", now, "ADMIN"),
            ("MAX_RETRIES", "3", "Maximum number of retries on error",
             "ERROR_HANDLING", "X", now, "ADMIN"),
            ("ALERT_EMAIL", "admin@example.com", "Email address for alerts",
             "NOTIFICATION", "X", now, "ADMIN"),
            ("LOG_RETENTION_DAYS", "90", "Number of days to retain logs",
             "MAINTENANCE", "X", now, "ADMIN"),
            ("ENABLE_RECONCILIATION", "X", "Enable data reconciliation after load",
             "DATA_QUALITY", "X", now, "ADMIN"),
            ("PREMIUM_MULTIPLIER", "1.5", "Value multiplier for premium category",
             "BUSINESS_RULE", "X", now, "ADMIN"),
            ("MIN_VALUE_THRESHOLD", "50.0", "Minimum acceptable value",
             "VALIDATION", "X", now, "ADMIN"),
            ("MAX_VALUE_THRESHOLD", "10000.0", "Maximum acceptable value",
             "VALIDATION", "X", now, "ADMIN"),
            ("ENABLE_DATA_QUALITY", "X", "Enable data quality checks",
             "DATA_QUALITY", "X", now, "ADMIN"),
            ("QUALITY_THRESHOLD", "95.0", "Minimum quality score threshold",
             "DATA_QUALITY", "X", now, "ADMIN")
        ]
        
        return self.spark.createDataFrame(data, schema)


class ScheduleGenerator:
    """Generate ETL schedule definitions."""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize schedule generator.
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        
    def generate_schedules(self) -> DataFrame:
        """
        Generate ETL schedule entries.
        
        Returns:
            DataFrame with schedule data
        """
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
        
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        
        data = [
            ("SCHED001", "Daily Full Load", "FULL", "DAILY",
             today, "02:00:00", "X", "ADMIN", now),
            ("SCHED002", "Hourly Incremental", "INCREMENTAL", "HOURLY",
             today, "00:00:00", "X", "ADMIN", now),
            ("SCHED003", "Weekly Reconciliation", "RECONCILIATION", "WEEKLY",
             today, "18:00:00", "", "ADMIN", now),
            ("SCHED004", "Monthly Archive", "ARCHIVE", "MONTHLY",
             today, "23:00:00", "X", "ADMIN", now),
            ("SCHED005", "Real-time Sync", "STREAMING", "CONTINUOUS",
             today, "00:00:00", "X", "ADMIN", now)
        ]
        
        return self.spark.createDataFrame(data, schema)


def main():
    """Main execution function."""
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("ETL Data Generator") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        print("=" * 60)
        print("ETL Data Generator - Initializing Test Data")
        print("=" * 60)
        
        # Generate source data
        print("\n✓ Generating source data...")
        data_gen = DataGenerator(spark)
        source_df = data_gen.generate_source_data(num_records=100)
        print(f"  Generated {source_df.count()} source records")
        source_df.show(5, truncate=False)
        
        # Generate staging data
        print("\n✓ Generating staging data...")
        staging_df = data_gen.generate_staging_data(num_records=50)
        print(f"  Generated {staging_df.count()} staging records")
        staging_df.show(5, truncate=False)
        
        # Generate configuration
        print("\n✓ Generating configuration...")
        config_gen = ConfigGenerator(spark)
        config_df = config_gen.generate_config()
        print(f"  Generated {config_df.count()} configuration entries")
        config_df.show(truncate=False)
        
        # Generate schedules
        print("\n✓ Generating schedules...")
        schedule_gen = ScheduleGenerator(spark)
        schedule_df = schedule_gen.generate_schedules()
        print(f"  Generated {schedule_df.count()} schedule definitions")
        schedule_df.show(truncate=False)
        
        # Save to Parquet for persistence
        print("\n✓ Saving generated data...")
        source_df.write.mode("overwrite").parquet("data/source_data.parquet")
        staging_df.write.mode("overwrite").parquet("data/staging_data.parquet")
        config_df.write.mode("overwrite").parquet("data/config_data.parquet")
        schedule_df.write.mode("overwrite").parquet("data/schedule_data.parquet")
        
        print("\n" + "=" * 60)
        print("Data generation completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ Error during data generation: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()