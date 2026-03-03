"""
Unit tests for ConfigManager
"""

import pytest
import tempfile
import os
from pathlib import Path

from src.config_manager import ConfigManager, ConfigurationError


class TestConfigManager:
    
    @pytest.fixture
    def temp_config_file(self):
        """Create temporary config file for testing."""
        config_content = """
spark:
  app_name: "TestApp"
  master: "local[1]"
  config:
    spark.sql.shuffle.partitions: 10

database:
  source:
    jdbc_url: "jdbc:test://localhost"
    user: "test_user"
    password_env: "TEST_DB_PASSWORD"

logging:
  level: "DEBUG"
  console:
    enabled: true

etl:
  batch_size: 500
"""
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.yaml') as f:
            f.write(config_content)
            temp_path = f.name
        
        yield temp_path
        
        # Cleanup
        os.unlink(temp_path)
    
    def test_singleton_pattern(self):
        """Test ConfigManager implements singleton."""
        config1 = ConfigManager()
        config2 = ConfigManager()
        
        assert config1 is config2
    
    def test_load_config(self, temp_config_file):
        """Test configuration loading."""
        config = ConfigManager()
        config._load_config(temp_config_file)
        
        assert config.get('spark.app_name') == "TestApp"
        assert config.get('etl.batch_size') == 500
    
    def test_get_with_dot_notation(self, temp_config_file):
        """Test getting values with dot notation."""
        config = ConfigManager()
        config._load_config(temp_config_file)
        
        assert config.get('spark.master') == "local[1]"
        assert config.get('spark.config.spark.sql.shuffle.partitions') == 10
    
    def test_get_with_default(self, temp_config_file):
        """Test default value when key not found."""
        config = ConfigManager()
        config._load_config(temp_config_file)
        
        assert config.get('nonexistent.key', 'default_value') == 'default_value'
    
    def test_get_spark_config(self, temp_config_file):
        """Test getting Spark configuration."""
        config = ConfigManager()
        config._load_config(temp_config_file)
        
        spark_config = config.get_spark_config()
        
        assert spark_config['app_name'] == "TestApp"
        assert spark_config['master'] == "local[1]"
    
    def test_get_database_config(self, temp_config_file):
        """Test getting database configuration."""
        os.environ['TEST_DB_PASSWORD'] = 'test_password'
        
        config = ConfigManager()
        config._load_config(temp_config_file)
        
        db_config = config.get_database_config('source')
        
        assert db_config['jdbc_url'] == "jdbc:test://localhost"
        assert db_config['user'] == "test_user"
        assert db_config['password'] == 'test_password'
        
        del os.environ['TEST_DB_PASSWORD']
    
    def test_set_config_value(self, temp_config_file):
        """Test setting configuration value."""
        config = ConfigManager()
        config._load_config(temp_config_file)
        
        config.set('test.key', 'test_value')
        
        assert config.get('test.key') == 'test_value'
    
    def test_missing_config_file(self):
        """Test handling of missing config file."""
        config = ConfigManager()
        
        with pytest.raises(ConfigurationError):
            config._load_config('/nonexistent/config.yaml')
    
    def test_to_dict(self, temp_config_file):
        """Test converting config to dictionary."""
        config = ConfigManager()
        config._load_config(temp_config_file)
        
        config_dict = config.to_dict()
        
        assert isinstance(config_dict, dict)
        assert 'spark' in config_dict
        assert 'database' in config_dict