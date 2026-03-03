"""
Pytest configuration and shared fixtures
"""

import pytest
from src.logger import get_logger


@pytest.fixture(scope='function')
def logger():
    """
    Fixture that provides a clean logger instance for each test.
    Automatically clears logs before and after each test.
    """
    logger_instance = get_logger()
    logger_instance.clear_logs()
    yield logger_instance
    logger_instance.clear_logs()


@pytest.fixture(scope='function')
def sample_logs():
    """
    Fixture that provides pre-populated sample logs for testing.
    """
    logger_instance = get_logger()
    logger_instance.clear_logs()
    
    # Create sample logs
    logger_instance.log_info('EXTRACTOR', 'Extraction started')
    logger_instance.log_info('EXTRACTOR', 'Connected to source', 'Connection established')
    logger_instance.log_info('EXTRACTOR', 'Extracted 1000 records')
    logger_instance.log_warning('TRANSFORMER', 'Missing values in 10 records')
    logger_instance.log_info('TRANSFORMER', 'Transformation complete')
    logger_instance.log_error('LOADER', 'Failed to load batch 1', 'Timeout error')
    logger_instance.log_info('LOADER', 'Retry successful')
    
    yield logger_instance
    
    logger_instance.clear_logs()


@pytest.fixture(scope='session')
def test_config():
    """
    Fixture that provides test configuration.
    """
    return {
        'test_mode': True,
        'max_logs': 1000,
        'components': ['EXTRACTOR', 'TRANSFORMER', 'LOADER', 'VALIDATOR'],
        'log_levels': ['INFO', 'WARNING', 'ERROR']
    }