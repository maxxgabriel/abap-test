"""
Data generator for ETL testing - creates sample datasets with random categorized records
"""
import random
import string
from datetime import datetime, timedelta
from typing import List, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, 
    TimestampType, IntegerType
)


class ETLDataGenerator:
    """Generate test datasets for ETL pipeline testing"""
    
    CATEGORIES = ['PREMIUM', 'STANDARD', 'BASIC', 'VIP', 'TRIAL']
    STATUSES = ['ACTIVE', 'INACTIVE', 'PENDING', 'SUSPENDED']
    SOURCE_SYSTEMS = ['SAP_ERP', 'SAP_S4', 'LEGACY_DB', 'CLOUD_APP']
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def generate_source_data(self, num_records: int = 100) -> DataFrame:
        """Generate sample source data records"""
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("value", DecimalType(15, 2), False),
            StructField("status", StringType(), True),
            StructField("category", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", StringType(), True),
            StructField("changed_at", TimestampType(), True),
            StructField("changed_by", StringType(), True)
        ])
        
        records = []
        base_time = datetime.now()
        
        for i in range(1, num_records + 1):
            record = {
                "id": f"{i:010d}",
                "name": f"Product {i}",
                "value": round(random.uniform(50, 1000), 2),
                "status": random.choice(self.STATUSES),
                "category": self.CATEGORIES[i % len(self.CATEGORIES)],
                "source_system": random.choice(self.SOURCE_SYSTEMS),
                "created_at": base_time - timedelta(days=random.randint(1, 365)),
                "created_by": self._generate_username(),
                "changed_at": base_time - timedelta(hours=random.randint(1, 24)),
                "changed_by": self._generate_username()
            }
            records.append(record)
        
        return self.spark.createDataFrame(records, schema)
    
    def generate_config_data(self) -> DataFrame:
        """Generate configuration key-value store"""
        schema = StructType([
            StructField("config_key", StringType(), False),
            StructField("config_value", StringType(), False),
            StructField("description", StringType(), True),
            StructField("config_type", StringType(), True),
            StructField("is_active", StringType(), True),
            StructField("changed_at", TimestampType(), True),
            StructField("changed_by", StringType(), True)
        ])
        
        configs = [
            {
                "config_key": "BATCH_SIZE",
                "config_value": "1000",
                "description": "Default batch size for data loading",
                "config_type": "PERFORMANCE",
                "is_active": "X",
                "changed_at": datetime.now(),
                "changed_by": "ADMIN"
            },
            {
                "config_key": "MAX_RETRIES",
                "config_value": "3",
                "description": "Maximum number of retries on error",
                "config_type": "ERROR_HANDLING",
                "is_active": "X",
                "changed_at": datetime.now(),
                "changed_by": "ADMIN"
            },
            {
                "config_key": "ALERT_EMAIL",
                "config_value": "admin@example.com",
                "description": "Email address for alerts",
                "config_type": "NOTIFICATION",
                "is_active": "X",
                "changed_at": datetime.now(),
                "changed_by": "ADMIN"
            },
            {
                "config_key": "LOG_RETENTION_DAYS",
                "config_value": "90",
                "description": "Number of days to retain logs",
                "config_type": "MAINTENANCE",
                "is_active": "X",
                "changed_at": datetime.now(),
                "changed_by": "ADMIN"
            },
            {
                "config_key": "ENABLE_RECONCILIATION",
                "config_value": "X",
                "description": "Enable data reconciliation after load",
                "config_type": "DATA_QUALITY",
                "is_active": "X",
                "changed_at": datetime.now(),
                "changed_by": "ADMIN"
            },
            {
                "config_key": "PREMIUM_MULTIPLIER",
                "config_value": "1.5",
                "description": "Value multiplier for premium category",
                "config_type": "BUSINESS_RULE",
                "is_active": "X",
                "changed_at": datetime.now(),
                "changed_by": "ADMIN"
            },
            {
                "config_key": "INCREMENTAL_LOOKBACK_HOURS",
                "config_value": "24",
                "description": "Hours to look back for incremental loads",
                "config_type": "EXTRACTION",
                "is_active": "X",
                "changed_at": datetime.now(),
                "changed_by": "ADMIN"
            },
            {
                "config_key": "VALIDATION_THRESHOLD",
                "config_value": "95",
                "description": "Minimum validation pass rate percentage",
                "config_type": "DATA_QUALITY",
                "is_active": "X",
                "changed_at": datetime.now(),
                "changed_by": "ADMIN"
            }
        ]
        
        return self.spark.createDataFrame(configs, schema)
    
    def generate_schedule_data(self) -> DataFrame:
        """Generate ETL schedule definitions"""
        schema = StructType([
            StructField("schedule_id", StringType(), False),
            StructField("schedule_name", StringType(), False),
            StructField("etl_type", StringType(), True),
            StructField("frequency", StringType(), True),
            StructField("start_date", StringType(), True),
            StructField("start_time", StringType(), True),
            StructField("is_active", StringType(), True),
            StructField("created_by", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("last_run_at", TimestampType(), True),
            StructField("next_run_at", TimestampType(), True)
        ])
        
        schedules = [
            {
                "schedule_id": "SCHED001",
                "schedule_name": "Daily Full Load",
                "etl_type": "FULL",
                "frequency": "DAILY",
                "start_date": datetime.now().strftime("%Y%m%d"),
                "start_time": "020000",
                "is_active": "X",
                "created_by": "ADMIN",
                "created_at": datetime.now(),
                "last_run_at": datetime.now() - timedelta(days=1),
                "next_run_at": datetime.now() + timedelta(days=1)
            },
            {
                "schedule_id": "SCHED002",
                "schedule_name": "Hourly Incremental",
                "etl_type": "INCREMENTAL",
                "frequency": "HOURLY",
                "start_date": datetime.now().strftime("%Y%m%d"),
                "start_time": "000000",
                "is_active": "X",
                "created_by": "ADMIN",
                "created_at": datetime.now(),
                "last_run_at": datetime.now() - timedelta(hours=1),
                "next_run_at": datetime.now() + timedelta(hours=1)
            },
            {
                "schedule_id": "SCHED003",
                "schedule_name": "Weekly Reconciliation",
                "etl_type": "RECONCILIATION",
                "frequency": "WEEKLY",
                "start_date": datetime.now().strftime("%Y%m%d"),
                "start_time": "180000",
                "is_active": "",
                "created_by": "ADMIN",
                "created_at": datetime.now(),
                "last_run_at": datetime.now() - timedelta(days=7),
                "next_run_at": datetime.now() + timedelta(days=7)
            },
            {
                "schedule_id": "SCHED004",
                "schedule_name": "Real-time Stream",
                "etl_type": "STREAMING",
                "frequency": "CONTINUOUS",
                "start_date": datetime.now().strftime("%Y%m%d"),
                "start_time": "000000",
                "is_active": "",
                "created_by": "ADMIN",
                "created_at": datetime.now(),
                "last_run_at": None,
                "next_run_at": None
            }
        ]
        
        return self.spark.createDataFrame(schedules, schema)
    
    def generate_staging_data(self, run_id: str, num_records: int = 50) -> DataFrame:
        """Generate staging area records"""
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("run_id", StringType(), False),
            StructField("raw_data", StringType(), True),
            StructField("status", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("processed_at", TimestampType(), True)
        ])
        
        records = []
        base_time = datetime.now()
        
        for i in range(1, num_records + 1):
            record = {
                "id": f"STAGE{i:06d}",
                "run_id": run_id,
                "raw_data": f'{{"product_id": "{i}", "amount": {random.uniform(100, 1000):.2f}}}',
                "status": random.choice(["READY", "PROCESSING", "COMPLETED"]),
                "created_at": base_time - timedelta(minutes=random.randint(1, 60)),
                "processed_at": base_time if random.random() > 0.3 else None
            }
            records.append(record)
        
        return self.spark.createDataFrame(records, schema)
    
    @staticmethod
    def _generate_username() -> str:
        """Generate random username"""
        return ''.join(random.choices(string.ascii_uppercase, k=6))


def main():
    """Main execution for standalone testing"""
    spark = SparkSession.builder \
        .appName("ETL Data Generator") \
        .master("local[*]") \
        .getOrCreate()
    
    generator = ETLDataGenerator(spark)
    
    # Generate and display sample data
    print("=" * 80)
    print("Generating Source Data...")
    print("=" * 80)
    source_df = generator.generate_source_data(100)
    source_df.show(10, truncate=False)
    print(f"Generated {source_df.count()} source records\n")
    
    print("=" * 80)
    print("Generating Config Data...")
    print("=" * 80)
    config_df = generator.generate_config_data()
    config_df.show(truncate=False)
    print(f"Generated {config_df.count()} config entries\n")
    
    print("=" * 80)
    print("Generating Schedule Data...")
    print("=" * 80)
    schedule_df = generator.generate_schedule_data()
    schedule_df.show(truncate=False)
    print(f"Generated {schedule_df.count()} schedule entries\n")
    
    print("=" * 80)
    print("Generating Staging Data...")
    print("=" * 80)
    staging_df = generator.generate_staging_data("RUN001", 20)
    staging_df.show(10, truncate=False)
    print(f"Generated {staging_df.count()} staging records\n")
    
    # Save to files for testing
    output_path = "data/generated"
    source_df.write.mode("overwrite").parquet(f"{output_path}/source_data")
    config_df.write.mode("overwrite").parquet(f"{output_path}/config_data")
    schedule_df.write.mode("overwrite").parquet(f"{output_path}/schedule_data")
    staging_df.write.mode("overwrite").parquet(f"{output_path}/staging_data")
    
    print("=" * 80)
    print(f"All data saved to {output_path}/")
    print("=" * 80)
    
    spark.stop()


if __name__ == "__main__":
    main()