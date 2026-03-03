"""
Unit tests for Configuration Manager
"""

import pytest
import os
import tempfile
from pathlib import Path

from src.config_manager import ConfigManager, config


class TestConfigManager:
    """Test suite for ConfigManager"""
    
    @pytest.fixture
    def sample_config_file(self):
        """Create a temporary config file for testing"""
        config_content = """
database:
  jdbc_url: "jdbc:postgresql://localhost:5432/test_db"
  driver: "org.postgresql.Driver"
  user: "test_user"
  password: "${TEST_DB_PASSWORD}"

source:
  type: "database"
  table_name: "test_source"
  max_records: 100

target:
  batch_size: 500
  mode: "insert"

logging:
  level: "DEBUG"
  console_output: true

features:
  test_feature: true
  disabled_feature: false
"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.yaml') as f:
            f.write(config_content)
            temp_path = f.name
        
        yield temp_path
        
        # Cleanup
        os.unlink(temp_path)
    
    def test_config_singleton(self):
        """Test that ConfigManager is a singleton"""
        config1 = ConfigManager()
        config2 = ConfigManager()
        
        assert config1 is config2
    
    def test_load_config(self, sample_config_file):
        """Test loading configuration from file"""
        manager = ConfigManager()
        manager.load_config(sample_config_file)
        
        assert manager.get('database.jdbc_url') == "jdbc:postgresql://localhost:5432/test_db"
        assert manager.get('source.table_name') == "test_source"
    
    def test_get_with_dot_notation(self, sample_config_file):
        """Test getting config values with dot notation"""
        manager = ConfigManager()
        manager.load_config(sample_config_file)
        
        assert manager.get('database.user') == "test_user"
        assert manager.get('target.batch_size') == 500
    
    def test_get_with_default(self, sample_config_file):
        """Test getting config with default value"""
        manager = ConfigManager()
        manager.load_config(sample_config_file)
        
        assert manager.get('nonexistent.key', 'default_value') == 'default_value'
    
    def test_environment_variable_resolution(self, sample_config_file):
        """Test environment variable resolution"""
        os.environ['TEST_DB_PASSWORD'] = 'secret_password'
        
        manager = ConfigManager()
        manager.load_config(sample_config_file)
        
        assert manager.get('database.password') == 'secret_password'
        
        # Cleanup
        del os.environ['TEST_DB_PASSWORD']
    
    def test_get_section(self, sample_config_file):
        """Test getting entire configuration section"""
        manager = ConfigManager()
        manager.load_config(sample_config_file)
        
        db_config = manager.get_section('database')
        
        assert 'jdbc_url' in db_config
        assert 'user' in db_config
        assert db_config['user'] == 'test_user'
    
    def test_set_runtime_value(self, sample_config_file):
        """Test setting configuration value at runtime"""
        manager = ConfigManager()
        manager.load_config(sample_config_file)
        
        manager.set('runtime.test_value', 'test')
        
        assert manager.get('runtime.test_value') == 'test'
    
    def test_is_feature_enabled(self, sample_config_file):
        """Test feature flag checking"""
        manager = ConfigManager()
        manager.load_config(sample_config_file)
        
        assert manager.is_feature_enabled('test_feature') is True
        assert manager.is_feature_enabled('disabled_feature') is False
        assert manager.is_feature_enabled('nonexistent_feature') is False
    
    def test_get_batch_size(self, sample_config_file):
        """Test getting batch size"""
        manager = ConfigManager()
        manager.load_config(sample_config_file)
        
        assert manager.get_batch_size() == 500
    
    def test_get_log_level(self, sample_config_file):
        """Test getting log level"""
        manager = ConfigManager()
        manager.load_config(sample_config_file)
        
        assert manager.get_log_level() == "DEBUG"
    
    def test_get_spark_config(self, sample_config_file):
        """Test getting Spark configuration"""
        manager = ConfigManager()
        manager.load_config(sample_config_file)
        
        spark_config = manager.get_spark_config()
        
        assert 'spark.app.name' in spark_config
        assert 'spark.sql.adaptive.enabled' in spark_config
    
    def test_config_file_not_found(self):
        """Test handling of missing config file"""
        manager = ConfigManager()
        
        with pytest.raises(FileNotFoundError):
            manager.load_config('nonexistent_config.yaml')
    
    def test_reload_config(self, sample_config_file):
        """Test reloading configuration"""
        manager = ConfigManager()
        manager.load_config(sample_config_file)
        
        original_value = manager.get('source.max_records')
        
        # Modify config in memory
        manager.set('source.max_records', 999)
        assert manager.get('source.max_records') == 999
        
        # Reload should restore original
        manager.reload()
        assert manager.get('source.max_records') == original_value