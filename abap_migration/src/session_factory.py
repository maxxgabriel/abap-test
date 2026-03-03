"""
Session Factory for ETL Framework
Provides centralized Spark session management and database connections.
"""

from pyspark.sql import SparkSession
from typing import Optional, Dict, Any
import os
from .config_manager import config
from .logger import logger


class SessionFactory:
    """
    Factory for creating and managing Spark sessions and database connections.
    
    Features:
    - Singleton Spark session
    - Configuration-based setup
    - Database connection management
    - Resource cleanup
    """
    
    _spark_session: Optional[SparkSession] = None
    _instance: Optional['SessionFactory'] = None
    
    def __new__(cls):
        """Singleton pattern implementation."""
        if cls._instance is None:
            cls._instance = super(SessionFactory, cls).__new__(cls)
        return cls._instance
    
    def get_spark_session(self, app_name: Optional[str] = None) -> SparkSession:
        """
        Get or create Spark session.
        
        Args:
            app_name: Optional application name override
            
        Returns:
            SparkSession instance
        """
        if self._spark_session is None:
            self._spark_session = self._create_spark_session(app_name)
        
        return self._spark_session
    
    def _create_spark_session(self, app_name: Optional[str] = None) -> SparkSession:
        """
        Create new Spark session with configuration.
        
        Args:
            app_name: Optional application name override
            
        Returns:
            Configured SparkSession
        """
        spark_config = config.get_spark_config()
        
        if app_name is None:
            app_name = spark_config.get('app_name', 'ETL_Framework')
        
        master = spark_config.get('master', 'local[*]')
        
        logger.log_info(
            'SESSION_FACTORY',
            f'Creating Spark session: {app_name}'
        )
        
        # Build Spark session
        builder = SparkSession.builder \
            .appName(app_name) \
            .master(master)
        
        # Apply configuration from config file
        spark_configs = spark_config.get('config', {})
        for key, value in spark_configs.items():
            builder = builder.config(key, value)
        
        # Create session
        spark = builder.getOrCreate()
        
        # Set log level
        log_level = spark_config.get('log_level', 'WARN')
        spark.sparkContext.setLogLevel(log_level)
        
        logger.log_info(
            'SESSION_FACTORY',
            f'Spark session created successfully',
            f'Version: {spark.version}, Master: {master}'
        )
        
        return spark
    
    def get_jdbc_url(self, db_type: str = 'source') -> str:
        """
        Get JDBC connection URL for database.
        
        Args:
            db_type: 'source' or 'target'
            
        Returns:
            JDBC URL string
        """
        db_config = config.get_database_config(db_type)
        return db_config.get('url', '')
    
    def get_jdbc_properties(self, db_type: str = 'source') -> Dict[str, str]:
        """
        Get JDBC connection properties.
        
        Args:
            db_type: 'source' or 'target'
            
        Returns:
            Properties dictionary
        """
        db_config = config.get_database_config(db_type)
        
        properties = {
            'user': db_config.get('user', ''),
            'password': db_config.get('password', ''),
            'driver': db_config.get('driver', 'org.postgresql.Driver')
        }
        
        # Add additional properties
        extra_props = db_config.get('properties', {})
        properties.update(extra_props)
        
        return properties
    
    def read_jdbc(self, table: str, db_type: str = 'source', 
                  conditions: Optional[str] = None) -> 'DataFrame':
        """
        Read data from database table using JDBC.
        
        Args:
            table: Table name
            db_type: 'source' or 'target'
            conditions: Optional WHERE clause
            
        Returns:
            Spark DataFrame
        """
        spark = self.get_spark_session()
        url = self.get_jdbc_url(db_type)
        properties = self.get_jdbc_properties(db_type)
        
        if conditions:
            table = f"(SELECT * FROM {table} WHERE {conditions}) as subquery"
        
        logger.log_info(
            'SESSION_FACTORY',
            f'Reading from database: {table}',
            f'Database: {db_type}'
        )
        
        try:
            df = spark.read.jdbc(url=url, table=table, properties=properties)
            
            logger.log_info(
                'SESSION_FACTORY',
                f'Successfully read {df.count()} records from {table}'
            )
            
            return df
            
        except Exception as e:
            logger.log_error(
                'SESSION_FACTORY',
                f'Error reading from database: {table}',
                str(e),
                e
            )
            raise
    
    def write_jdbc(self, df: 'DataFrame', table: str, db_type: str = 'target',
                   mode: str = 'overwrite') -> None:
        """
        Write DataFrame to database table using JDBC.
        
        Args:
            df: Spark DataFrame to write
            table: Target table name
            db_type: 'source' or 'target'
            mode: Write mode ('overwrite', 'append', etc.)
        """
        url = self.get_jdbc_url(db_type)
        properties = self.get_jdbc_properties(db_type)
        
        logger.log_info(
            'SESSION_FACTORY',
            f'Writing to database: {table}',
            f'Database: {db_type}, Mode: {mode}, Records: {df.count()}'
        )
        
        try:
            df.write.jdbc(url=url, table=table, mode=mode, properties=properties)
            
            logger.log_info(
                'SESSION_FACTORY',
                f'Successfully wrote data to {table}'
            )
            
        except Exception as e:
            logger.log_error(
                'SESSION_FACTORY',
                f'Error writing to database: {table}',
                str(e),
                e
            )
            raise
    
    def stop_spark_session(self) -> None:
        """Stop and cleanup Spark session."""
        if self._spark_session is not None:
            logger.log_info('SESSION_FACTORY', 'Stopping Spark session')
            self._spark_session.stop()
            self._spark_session = None
    
    def __enter__(self):
        """Context manager entry."""
        return self.get_spark_session()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop_spark_session()


# Singleton instance
session_factory = SessionFactory()