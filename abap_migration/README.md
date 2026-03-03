# PySpark ETL Logger Utility

A singleton logger class with in-memory storage for ETL process logging, providing structured logging with timestamp, level, component, and message fields using Python logging standards.

## Features

- **Singleton Pattern**: Thread-safe singleton implementation ensures single logger instance across application
- **In-Memory Storage**: All log entries stored in memory for quick retrieval and analysis
- **Structured Logging**: Standardized log format with timestamp, level, component, and message
- **Multiple Log Levels**: Support for INFO, WARNING, ERROR, and DEBUG levels
- **Component Tracking**: Track logs by ETL component (EXTRACTOR, TRANSFORMER, LOADER, etc.)
- **Filtering & Retrieval**: Filter logs by level, component, or retrieve all logs
- **Summary Statistics**: Get counts and summaries of log entries
- **Python Logging Integration**: Built on Python's standard logging module

## Installation

```bash
# Install dependencies
pip install pyspark pytest pyyaml
```

## Usage

### Basic Logging

```python
from src.logger import get_logger

# Get logger instance
logger = get_logger()

# Log messages
logger.log_info("EXTRACTOR", "Starting data extraction")
logger.log_warning("TRANSFORMER", "Missing values detected")
logger.log_error("LOADER", "Failed to load batch", "Connection timeout")
logger.log_debug("ORCHESTRATOR", "Processing batch 1 of 10")
```

### Retrieving Logs

```python
# Get all logs
all_logs = logger.get_logs()

# Filter by level
error_logs = logger.get_logs_by_level('ERROR')
warning_logs = logger.get_logs_by_level('WARNING')

# Filter by component
extractor_logs = logger.get_logs_by_component('EXTRACTOR')

# Get counts
error_count = logger.get_error_count()
warning_count = logger.get_warning_count()

# Get summary
summary = logger.get_log_summary()
print(f"Total logs: {summary['total']}")
print(f"Errors: {summary['error']}")
```

### Exporting Logs

```python
# Export all logs with summary
export_data = logger.export_logs_to_dict()
print(export_data['summary'])
for log in export_data['logs']:
    print(log)

# Clear logs
logger.clear_logs()
```

## Configuration

Edit `config.yaml` to customize logger behavior:

```yaml
logging:
  level: INFO
  format: "[%(asctime)s] %(levelname)s: %(component)s - %(message)s"
  
  storage:
    max_entries: 10000
    auto_clear: false

components:
  extractor: EXTRACTOR
  transformer: TRANSFORMER
  loader: LOADER
  orchestrator: ORCHESTRATOR
```

## Running Tests

```bash
# Run all tests
pytest tests/test_logger.py -v

# Run specific test class
pytest tests/test_logger.py::TestETLLoggerSingleton -v

# Run with coverage
pytest tests/test_logger.py --cov=src.logger --cov-report=html
```

## Log Entry Structure

Each log entry contains:

```python
{
    'timestamp': '2024-01-01 12:00:00.123',
    'level': 'INFO',
    'component': 'EXTRACTOR',
    'message': 'Extraction started',
    'details': 'Processing 1000 records'  # Optional
}
```

## Thread Safety

The logger implements double-checked locking for thread-safe singleton instantiation:

```python
# Safe to call from multiple threads
from concurrent.futures import ThreadPoolExecutor

def log_from_thread(thread_id):
    logger = get_logger()
    logger.log_info("THREAD", f"Thread {thread_id} logging")

with ThreadPoolExecutor(max_workers=10) as executor:
    executor.map(log_from_thread, range(10))
```

## Integration with PySpark ETL

```python
from pyspark.sql import SparkSession
from src.logger import get_logger

# Initialize Spark
spark = SparkSession.builder.appName("ETL").getOrCreate()
logger = get_logger()

try:
    logger.log_info("EXTRACTOR", "Starting extraction")
    df = spark.read.parquet("input/data.parquet")
    logger.log_info("EXTRACTOR", f"Extracted {df.count()} records")
    
    logger.log_info("TRANSFORMER", "Starting transformation")
    transformed_df = df.filter(df.value > 0)
    logger.log_info("TRANSFORMER", f"Transformed {transformed_df.count()} records")
    
    logger.log_info("LOADER", "Starting load")
    transformed_df.write.mode("overwrite").parquet("output/data.parquet")
    logger.log_info("LOADER", "Load completed successfully")
    
except Exception as e:
    logger.log_error("ETL", "ETL process failed", str(e))
    raise
finally:
    # Export logs for analysis
    log_export = logger.export_logs_to_dict()
    print(f"ETL completed with {log_export['summary']['error']} errors")
```

## Best Practices

1. **Component Naming**: Use consistent uppercase component names (EXTRACTOR, TRANSFORMER, LOADER)
2. **Message Format**: Keep messages concise, use details for additional information
3. **Error Logging**: Always include details when logging errors
4. **Log Levels**:
   - INFO: Normal operations, milestones
   - WARNING: Recoverable issues, data quality concerns
   - ERROR: Failures, exceptions
   - DEBUG: Detailed troubleshooting information
5. **Memory Management**: Clear logs periodically for long-running processes
6. **Summary Checks**: Review log summary after ETL runs for quick health check

## License

MIT License