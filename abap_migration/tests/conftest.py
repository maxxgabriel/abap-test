"""
Pytest configuration and fixtures for ETL Logger tests
"""

import pytest
from src.logger import ETLLogger


@pytest.fixture(scope="function")
def logger():
    """Provide a fresh logger instance for each test"""
    logger = ETLLogger.get_instance()
    logger.clear_logs()
    yield logger
    logger.clear_logs()


@pytest.fixture(scope="function")
def sample_logs(logger):
    """Provide sample logs for testing"""
    logger.log_info("EXTRACTOR", "Extraction started")
    logger.log_info("EXTRACTOR", "Extracted 1000 records")
    logger.log_warning("TRANSFORMER", "Missing values detected")
    logger.log_error("LOADER", "Connection failed", "Timeout after 30s")
    logger.log_debug("ORCHESTRATOR", "Processing batch 1")
    return logger


@pytest.fixture(scope="session")
def test_config():
    """Provide test configuration"""
    return {
        'max_log_entries': 10000,
        'test_component': 'TEST',
        'test_batch_size': 100
    }