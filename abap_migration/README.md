# PySpark ETL Logger Utility

A singleton logger class with in-memory storage for ETL process logging, implementing Python logging standards with timestamp, level, component, and message fields.

## Features

- **Singleton Pattern**: Thread-safe singleton implementation ensures single logger instance
- **In-Memory Storage**: Stores all log entries in memory for easy retrieval and analysis
- **Standard Log Levels**: Supports INFO, WARNING, ERROR, and DEBUG levels
- **Component Tracking**: Associates each log entry with its source component
- **Filtering**: Filter logs by level and/or component
- **Export**: Export logs in JSON or CSV format
- **Python Logging Integration**: Integrates with Python's standard logging module

## Installation

```bash
pip install pyspark pytest pyyaml
```

## Usage

### Basic Logging

```python
from src.logger import ETLLogger

# Get logger instance (singleton)
logger = ETLLogger.get_instance()

# Log messages at different levels
logger.log_info('EXTRACTOR', 'Starting data extraction')
logger.log_warning('TRANSFORMER', 'Missing values detected', '10 records affected')
logger.log_error('LOADER', 'Load failed', 'Database connection timeout')
logger.log_debug('ORCHESTRATOR', 'Debug information')
```

### Retrieving Logs

```python
# Get all logs
all_logs = logger.get_logs()

# Filter by level
error_logs = logger.get_logs(level='ERROR')

# Filter by component
extractor_logs = logger.get_logs(component='EXTRACTOR')

# Filter by both
extractor_errors = logger.get_logs(level='ERROR', component='EXTRACTOR')
```

### Log Summary

```python
# Get count summary by level
summary = logger.get_log_summary()
print(f"Info: {summary['INFO']}, Errors: {summary['ERROR']}")

# Get total log count
total = logger.get_log_count()
```

### Exporting Logs

```python
# Export as JSON
json_logs = logger.export_logs(format='json')

# Export as CSV
csv_logs = logger.export_logs(format='csv')
```

### Clearing Logs

```python
# Clear all logs from memory
logger.clear_logs()
```

## Configuration

Edit `config.yaml` to customize logging behavior:

```yaml
logging:
  level: INFO
  max_log_entries: 10000
  export_format: json
  
  components:
    EXTRACTOR:
      level: INFO
      enabled: true
```

## Testing

Run the test suite:

```bash
# Run all tests
pytest tests/test_logger.py -v

# Run with coverage
pytest tests/test_logger.py -v --cov=src.logger --cov-report=html

# Run specific test
pytest tests/test_logger.py::TestETLLogger::test_singleton_pattern -v
```

## Architecture

### LogEntry

Dataclass representing a single log entry:
- `timestamp`: ISO format timestamp
- `level`: Log level (INFO, WARNING, ERROR, DEBUG)
- `component`: Source component name
- `message`: Log message
- `details`: Optional additional information

### ETLLogger

Singleton class managing log entries:
- Thread-safe singleton instantiation
- In-memory list storage
- Python logging integration
- Export capabilities

## Best Practices

1. **Use Appropriate Levels**:
   - INFO: Normal operational messages
   - WARNING: Warning messages for potentially harmful situations
   - ERROR: Error messages for failures
   - DEBUG: Detailed debugging information

2. **Component Naming**: Use consistent component names across your ETL pipeline

3. **Include Details**: Provide detailed information in the `details` parameter for errors

4. **Regular Clearing**: Clear logs periodically to manage memory usage

5. **Export Before Clearing**: Export logs before clearing if persistence is needed

## License

MIT License