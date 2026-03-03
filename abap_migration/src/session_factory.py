"""
Session Factory Module
Database and Spark session management
"""

from typing import Optional, Dict, Any
from contextlib import contextmanager
from pyspark.sql import SparkSession

from src.config_manager import config
from src.logger import ETLLogger


class SessionFactory:
    """Factory for creating and managing database and Spark sessions"""
    
    _spark_session: Optional[SparkSession] = None
    _logger = ETLLogger.get_logger('SESSION_FACTORY')
    
    @classmethod
    def get_spark_session(cls, app_name: Optional[str] = None) -> SparkSession:
        """
        Get or create Spark session with configured settings
        
        Args:
            app_name: Optional application name override
            
        Returns:
            Configured SparkSession
        """
        if cls._spark_session is not None:
            return cls._spark_session
        
        cls._logger.info("Creating new Spark session")
        
        # Get Spark configuration
        spark_config = config.get_spark_config()
        db_config = config.get_database_config()
        
        if app_name:
            spark_config['spark.app.name'] = app_name
        
        # Build Spark session
        builder = SparkSession.builder
        
        for key, value in spark_config.items():
            builder = builder.config(key, value)
        
        # Add JDBC driver
        jdbc_driver = db_config.get('driver', 'org.postgresql.Driver')
        builder = builder.config('spark.jars.packages',
                                'org.postgresql:postgresql:42.5.0')
        
        cls._spark_session = builder.getOrCreate()
        
        # Set log level
        cls._spark_session.sparkContext.setLogLevel(config.get_log_level())
        
        cls._logger.info(f"Spark session created: {cls._spark_session.version}")
        
        return cls._spark_session
    
    @classmethod
    def stop_spark_session(cls) -> None:
        """Stop the Spark session"""
        if cls._spark_session is not None:
            cls._logger.info("Stopping Spark session")
            cls._spark_session.stop()
            cls._spark_session = None
    
    @classmethod
    def get_jdbc_url(cls) -> str:
        """Get JDBC connection URL"""
        db_config = config.get_database_config()
        return db_config['jdbc_url']
    
    @classmethod
    def get_jdbc_properties(cls) -> Dict[str, str]:
        """Get JDBC connection properties"""
        db_config = config.get_database_config()
        
        return {
            'user': db_config['user'],
            'password': db_config['password'],
            'driver': db_config['driver']
        }
    
    @classmethod
    @contextmanager
    def spark_context(cls, app_name: Optional[str] = None):
        """
        Context manager for Spark session
        
        Usage:
            with SessionFactory.spark_context() as spark:
                df = spark.read.csv('data.csv')
        """
        spark = cls.get_spark_session(app_name)
        try:
            yield spark
        finally:
            # Don't stop session in context manager - allow reuse
            pass
    
    @classmethod
    def create_database_connection(cls):
        """
        Create database connection using configured settings
        
        Returns:
            Database connection object
        """
        db_config = config.get_database_config()
        
        # Import appropriate driver
        if 'postgresql' in db_config['jdbc_url']:
            import psycopg2
            conn = psycopg2.connect(
                host=cls._extract_host(db_config['jdbc_url']),
                database=cls._extract_database(db_config['jdbc_url']),
                user=db_config['user'],
                password=db_config['password']
            )
        else:
            raise ValueError(f"Unsupported database: {db_config['jdbc_url']}")
        
        cls._logger.info("Database connection established")
        return conn
    
    @staticmethod
    def _extract_host(jdbc_url: str) -> str:
        """Extract host from JDBC URL"""
        # jdbc:postgresql://localhost:5432/etl_db -> localhost
        parts = jdbc_url.split('//')[1].split(':')
        return parts[0]
    
    @staticmethod
    def _extract_database(jdbc_url: str) -> str:
        """Extract database name from JDBC URL"""
        # jdbc:postgresql://localhost:5432/etl_db -> etl_db
        return jdbc_url.split('/')[-1]
    
    @classmethod
    def get_connection_pool_config(cls) -> Dict[str, Any]:
        """Get connection pool configuration"""
        pool_config = config.get('database.connection_pool', {})
        
        return {
            'min_connections': pool_config.get('min_connections', 2),
            'max_connections': pool_config.get('max_connections', 10),
            'connection_timeout': pool_config.get('connection_timeout', 30000)
        }
    
    @classmethod
    def test_connection(cls) -> bool:
        """
        Test database connection
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            spark = cls.get_spark_session()
            jdbc_url = cls.get_jdbc_url()
            properties = cls.get_jdbc_properties()
            
            # Try to read from database
            test_query = "(SELECT 1 as test) as test_table"
            df = spark.read.jdbc(
                url=jdbc_url,
                table=test_query,
                properties=properties
            )
            
            result = df.count()
            
            cls._logger.info("Database connection test successful")
            return result == 1
            
        except Exception as e:
            cls._logger.error("Database connection test failed", exception=e)
            return False


# Convenience function for getting Spark session
def get_spark() -> SparkSession:
    """Get Spark session - convenience function"""
    return SessionFactory.get_spark_session()