"""
Database and Spark session factory for consistent resource handling.
Manages connection pooling and session lifecycle.
"""

from typing import Optional, Dict, Any
from contextlib import contextmanager
from pyspark.sql import SparkSession

from src.config_manager import config
from src.logger import ComponentLogger


class SessionFactory:
    """
    Factory for creating and managing Spark sessions and database connections.
    Implements singleton pattern for SparkSession.
    """
    
    _spark_session: Optional[SparkSession] = None
    
    @classmethod
    def get_spark_session(cls, app_name: Optional[str] = None) -> SparkSession:
        """
        Get or create SparkSession singleton.
        
        Args:
            app_name: Optional application name override
            
        Returns:
            SparkSession instance
        """
        if cls._spark_session is None:
            cls._spark_session = cls._create_spark_session(app_name)
        
        return cls._spark_session
    
    @classmethod
    def _create_spark_session(cls, app_name: Optional[str] = None) -> SparkSession:
        """
        Create new SparkSession with configuration.
        
        Args:
            app_name: Optional application name
            
        Returns:
            Configured SparkSession
        """
        logger = ComponentLogger("SessionFactory")
        
        spark_config = config.get_spark_config()
        
        if app_name is None:
            app_name = spark_config.get('app_name', 'ETL_Framework')
        
        logger.info(f"Creating SparkSession: {app_name}")
        
        # Create session builder
        builder = SparkSession.builder.appName(app_name)
        
        # Set master
        master = spark_config.get('master', 'local[*]')
        builder = builder.master(master)
        
        # Apply Spark configurations
        spark_configs = spark_config.get('config', {})
        for key, value in spark_configs.items():
            builder = builder.config(key, value)
        
        # Add JDBC drivers
        builder = builder.config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.5.0"
        )
        
        session = builder.getOrCreate()
        
        # Set log level
        log_level = config.get('logging.level', 'INFO')
        session.sparkContext.setLogLevel(log_level)
        
        logger.info(f"SparkSession created successfully: {session.version}")
        
        return session
    
    @classmethod
    def stop_spark_session(cls) -> None:
        """Stop active SparkSession."""
        if cls._spark_session is not None:
            logger = ComponentLogger("SessionFactory")
            logger.info("Stopping SparkSession")
            cls._spark_session.stop()
            cls._spark_session = None
    
    @classmethod
    @contextmanager
    def spark_session_context(cls, app_name: Optional[str] = None):
        """
        Context manager for SparkSession.
        
        Args:
            app_name: Optional application name
            
        Yields:
            SparkSession instance
        """
        session = cls.get_spark_session(app_name)
        try:
            yield session
        finally:
            # Note: We don't stop the session here as it's singleton
            # Call stop_spark_session() explicitly when truly done
            pass
    
    @staticmethod
    def get_jdbc_connection_properties(db_type: str = 'source') -> Dict[str, str]:
        """
        Get JDBC connection properties for database.
        
        Args:
            db_type: 'source' or 'target'
            
        Returns:
            Dictionary of JDBC connection properties
        """
        db_config = config.get_database_config(db_type)
        
        properties = {
            'user': db_config['user'],
            'password': db_config.get('password', ''),
            'driver': db_config['driver']
        }
        
        # Add additional connection properties
        conn_props = db_config.get('connection_properties', {})
        properties.update({k: str(v) for k, v in conn_props.items()})
        
        return properties
    
    @staticmethod
    def read_from_jdbc(
        spark: SparkSession,
        table: str,
        db_type: str = 'source',
        query: Optional[str] = None
    ):
        """
        Read data from JDBC source.
        
        Args:
            spark: SparkSession instance
            table: Table name
            db_type: 'source' or 'target'
            query: Optional SQL query instead of table
            
        Returns:
            DataFrame
        """
        db_config = config.get_database_config(db_type)
        properties = SessionFactory.get_jdbc_connection_properties(db_type)
        
        if query:
            # Use query as dbtable
            dbtable = f"({query}) as subquery"
        else:
            dbtable = table
        
        return spark.read \
            .format("jdbc") \
            .option("url", db_config['jdbc_url']) \
            .option("dbtable", dbtable) \
            .options(**properties) \
            .load()
    
    @staticmethod
    def write_to_jdbc(
        df,
        table: str,
        mode: str = "append",
        db_type: str = 'target'
    ) -> None:
        """
        Write DataFrame to JDBC target.
        
        Args:
            df: DataFrame to write
            table: Target table name
            mode: Write mode (append, overwrite, etc.)
            db_type: 'source' or 'target'
        """
        db_config = config.get_database_config(db_type)
        properties = SessionFactory.get_jdbc_connection_properties(db_type)
        
        df.write \
            .format("jdbc") \
            .option("url", db_config['jdbc_url']) \
            .option("dbtable", table) \
            .options(**properties) \
            .mode(mode) \
            .save()


class DatabaseConnectionManager:
    """
    Manager for database connection pooling and health checks.
    """
    
    def __init__(self, db_type: str = 'source'):
        """
        Initialize connection manager.
        
        Args:
            db_type: 'source' or 'target'
        """
        self.db_type = db_type
        self.logger = ComponentLogger(f"DatabaseManager-{db_type}")
    
    def test_connection(self, spark: SparkSession) -> bool:
        """
        Test database connectivity.
        
        Args:
            spark: SparkSession instance
            
        Returns:
            True if connection successful
        """
        try:
            self.logger.info(f"Testing {self.db_type} database connection")
            
            # Try simple query
            test_df = SessionFactory.read_from_jdbc(
                spark,
                table="(SELECT 1 as test_col) as test",
                db_type=self.db_type
            )
            
            count = test_df.count()
            
            if count == 1:
                self.logger.info(f"{self.db_type} database connection successful")
                return True
            else:
                self.logger.error(f"{self.db_type} database connection test failed")
                return False
                
        except Exception as e:
            self.logger.error(
                f"{self.db_type} database connection failed",
                exception=e
            )
            return False