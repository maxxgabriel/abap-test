"""
Pytest configuration and fixtures for ETL Logger tests.
"""
import pytest
from src.logger import ETLLogger


@pytest.fixture(scope='function')
def logger():
    """Provide a clean logger instance for each test."""
    logger_instance = ETLLogger.get_instance()
    logger_instance.clear_logs()
    yield logger_instance
    logger_instance.clear_logs()


@pytest.fixture(scope='session')
def sample_log_entries():
    """Provide sample log entries for testing."""
    return [
        {
            'level': 'INFO',
            'component': 'EXTRACTOR',
            'message': 'Data extraction started',
            'details': None
        },
        {
            'level': 'WARNING',
            'component': 'TRANSFORMER',
            'message': 'Missing values detected',
            'details': '10 records affected'
        },
        {
            'level': 'ERROR',
            'component': 'LOADER',
            'message': 'Load failed',
            'details': 'Database connection timeout'
        }
    ]