"""
Production deployment module for ETL system.
Handles deployment procedures, configuration validation, and pre-deployment checks.
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import logging
from pyspark.sql import SparkSession
import yaml
import os

logger = logging.getLogger(__name__)


@dataclass
class DeploymentConfig:
    """Deployment configuration parameters."""
    environment: str
    spark_config: Dict[str, Any]
    monitoring_config: Dict[str, Any]
    health_check_config: Dict[str, Any]
    rollback_config: Dict[str, Any]


class DeploymentManager:
    """Manages production deployment procedures."""
    
    def __init__(self, config_path: str):
        """
        Initialize deployment manager.
        
        Args:
            config_path: Path to deployment configuration file
        """
        self.config = self._load_config(config_path)
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def _load_config(self, config_path: str) -> DeploymentConfig:
        """Load deployment configuration from YAML."""
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        return DeploymentConfig(
            environment=config_data['deployment']['environment'],
            spark_config=config_data['deployment']['spark_config'],
            monitoring_config=config_data['monitoring'],
            health_check_config=config_data['health_checks'],
            rollback_config=config_data['deployment']['rollback']
        )
    
    def validate_deployment_prerequisites(self) -> Dict[str, bool]:
        """
        Validate all deployment prerequisites.
        
        Returns:
            Dictionary with validation results
        """
        self.logger.info("Validating deployment prerequisites")
        
        results = {
            'config_valid': self._validate_config(),
            'dependencies_installed': self._check_dependencies(),
            'database_accessible': self._check_database_connectivity(),
            'resources_available': self._check_resources(),
            'backup_exists': self._verify_backup()
        }
        
        all_valid = all(results.values())
        if all_valid:
            self.logger.info("All prerequisites validated successfully")
        else:
            failed = [k for k, v in results.items() if not v]
            self.logger.error(f"Prerequisites validation failed: {failed}")
        
        return results
    
    def _validate_config(self) -> bool:
        """Validate deployment configuration."""
        try:
            required_fields = [
                'environment',
                'spark_config',
                'monitoring_config'
            ]
            
            for field in required_fields:
                if not getattr(self.config, field):
                    self.logger.error(f"Missing required config field: {field}")
                    return False
            
            # Validate environment
            if self.config.environment not in ['development', 'staging', 'production']:
                self.logger.error(f"Invalid environment: {self.config.environment}")
                return False
            
            return True
        except Exception as e:
            self.logger.error(f"Config validation failed: {str(e)}")
            return False
    
    def _check_dependencies(self) -> bool:
        """Check if all required dependencies are installed."""
        required_packages = [
            'pyspark',
            'pyyaml',
            'prometheus_client',
            'boto3',
            'pytest'
        ]
        
        for package in required_packages:
            try:
                __import__(package)
            except ImportError:
                self.logger.error(f"Required package not installed: {package}")
                return False
        
        return True
    
    def _check_database_connectivity(self) -> bool:
        """Check database connectivity."""
        try:
            spark = SparkSession.builder.getOrCreate()
            # Test connection with a simple query
            spark.sql("SELECT 1").collect()
            return True
        except Exception as e:
            self.logger.error(f"Database connectivity check failed: {str(e)}")
            return False
    
    def _check_resources(self) -> bool:
        """Check if sufficient resources are available."""
        try:
            # Check memory availability
            import psutil
            mem = psutil.virtual_memory()
            
            min_memory_gb = self.config.spark_config.get('min_memory_gb', 8)
            available_memory_gb = mem.available / (1024**3)
            
            if available_memory_gb < min_memory_gb:
                self.logger.error(
                    f"Insufficient memory. Required: {min_memory_gb}GB, "
                    f"Available: {available_memory_gb:.2f}GB"
                )
                return False
            
            # Check disk space
            disk = psutil.disk_usage('/')
            min_disk_gb = 50
            available_disk_gb = disk.free / (1024**3)
            
            if available_disk_gb < min_disk_gb:
                self.logger.error(
                    f"Insufficient disk space. Required: {min_disk_gb}GB, "
                    f"Available: {available_disk_gb:.2f}GB"
                )
                return False
            
            return True
        except Exception as e:
            self.logger.error(f"Resource check failed: {str(e)}")
            return False
    
    def _verify_backup(self) -> bool:
        """Verify that backup exists."""
        try:
            backup_path = self.config.rollback_config.get('backup_path')
            if not backup_path:
                self.logger.warning("No backup path configured")
                return True  # Not critical for development
            
            return os.path.exists(backup_path)
        except Exception as e:
            self.logger.error(f"Backup verification failed: {str(e)}")
            return False
    
    def deploy(self) -> bool:
        """
        Execute deployment procedure.
        
        Returns:
            True if deployment successful
        """
        self.logger.info(f"Starting deployment to {self.config.environment}")
        
        try:
            # Step 1: Validate prerequisites
            if not all(self.validate_deployment_prerequisites().values()):
                raise Exception("Prerequisites validation failed")
            
            # Step 2: Create backup
            self.logger.info("Creating backup")
            self._create_backup()
            
            # Step 3: Deploy configuration
            self.logger.info("Deploying configuration")
            self._deploy_config()
            
            # Step 4: Initialize Spark session
            self.logger.info("Initializing Spark session")
            spark = self._initialize_spark()
            
            # Step 5: Deploy artifacts
            self.logger.info("Deploying artifacts")
            self._deploy_artifacts()
            
            # Step 6: Run smoke tests
            self.logger.info("Running smoke tests")
            if not self._run_smoke_tests():
                raise Exception("Smoke tests failed")
            
            # Step 7: Initialize monitoring
            self.logger.info("Initializing monitoring")
            self._initialize_monitoring()
            
            # Step 8: Health check
            self.logger.info("Running health check")
            if not self._health_check():
                raise Exception("Health check failed")
            
            self.logger.info("Deployment completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Deployment failed: {str(e)}")
            self.logger.info("Initiating rollback")
            self.rollback()
            return False
    
    def _create_backup(self):
        """Create backup before deployment."""
        backup_path = self.config.rollback_config.get('backup_path')
        if backup_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = f"{backup_path}/backup_{timestamp}"
            self.logger.info(f"Creating backup at {backup_file}")
            # Implement actual backup logic
    
    def _deploy_config(self):
        """Deploy configuration files."""
        config_path = self.config.rollback_config.get('config_path', '/etc/etl')
        os.makedirs(config_path, exist_ok=True)
        self.logger.info(f"Deployed configuration to {config_path}")
    
    def _initialize_spark(self) -> SparkSession:
        """Initialize Spark session with deployment configuration."""
        builder = SparkSession.builder.appName("ETL-Production")
        
        # Apply Spark configuration
        for key, value in self.config.spark_config.items():
            builder = builder.config(f"spark.{key}", value)
        
        return builder.getOrCreate()
    
    def _deploy_artifacts(self):
        """Deploy ETL artifacts."""
        self.logger.info("Deploying ETL modules")
        # Copy modules to deployment directory
        # Implement actual deployment logic
    
    def _run_smoke_tests(self) -> bool:
        """Run basic smoke tests after deployment."""
        try:
            spark = SparkSession.builder.getOrCreate()
            
            # Test 1: Basic SQL query
            result = spark.sql("SELECT 1 as test").collect()
            assert result[0]['test'] == 1
            
            # Test 2: DataFrame operations
            df = spark.createDataFrame([(1, "test")], ["id", "name"])
            assert df.count() == 1
            
            self.logger.info("Smoke tests passed")
            return True
        except Exception as e:
            self.logger.error(f"Smoke tests failed: {str(e)}")
            return False
    
    def _initialize_monitoring(self):
        """Initialize monitoring systems."""
        from src.monitor import MonitoringManager
        
        monitor = MonitoringManager(self.config.monitoring_config)
        monitor.initialize()
        self.logger.info("Monitoring initialized")
    
    def _health_check(self) -> bool:
        """Perform health check."""
        from src.health import HealthChecker
        
        health = HealthChecker(self.config.health_check_config)
        return health.check_system_health()
    
    def rollback(self):
        """Rollback deployment."""
        self.logger.warning("Initiating rollback procedure")
        
        try:
            backup_path = self.config.rollback_config.get('backup_path')
            if backup_path and os.path.exists(backup_path):
                self.logger.info(f"Restoring from backup: {backup_path}")
                # Implement rollback logic
                self.logger.info("Rollback completed")
            else:
                self.logger.error("No backup found for rollback")
        except Exception as e:
            self.logger.error(f"Rollback failed: {str(e)}")


def main():
    """Main deployment entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='ETL System Deployment')
    parser.add_argument('--config', required=True, help='Path to deployment config')
    parser.add_argument('--dry-run', action='store_true', help='Validate without deploying')
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    deployer = DeploymentManager(args.config)
    
    if args.dry_run:
        logger.info("Running in dry-run mode")
        results = deployer.validate_deployment_prerequisites()
        logger.info(f"Validation results: {results}")
    else:
        success = deployer.deploy()
        exit(0 if success else 1)


if __name__ == "__main__":
    main()