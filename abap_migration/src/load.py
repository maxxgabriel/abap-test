"""
ETL Loader - Handles data loading to target systems
"""
from typing import Dict, Optional
from pyspark.sql import SparkSession, DataFrame

from src.logger import ETLLogger
from src.exceptions import ETLLoadError


class ETLLoader:
    """Loads data to target systems"""
    
    def __init__(
        self,
        spark: SparkSession,
        target_type: str = "DATABASE",
        batch_size: int = 1000,
        run_id: str = "",
        logger: Optional[ETLLogger] = None
    ):
        self.spark = spark
        self.target_type = target_type
        self.batch_size = batch_size
        self.run_id = run_id
        self.logger = logger or ETLLogger()
    
    def load_data(self, df: DataFrame, mode: str = "INSERT") -> Dict[str, int]:
        """Load data to target"""
        self.logger.log_info(
            component="LOADER",
            message=f"Starting load - Mode: {mode}, Batch size: {self.batch_size}"
        )
        
        try:
            total_count = df.count()
            
            # Perform load based on mode
            if mode == "INSERT":
                df.write.mode("append").saveAsTable("etl_target_data")
                success_count = total_count
                error_count = 0
            elif mode == "UPSERT":
                # Merge logic would go here
                df.write.mode("overwrite").saveAsTable("etl_target_data")
                success_count = total_count
                error_count = 0
            else:
                df.write.mode("append").saveAsTable("etl_target_data")
                success_count = total_count
                error_count = 0
            
            self.logger.log_info(
                component="LOADER",
                message=f"Load complete - Success: {success_count}, Errors: {error_count}"
            )
            
            return {
                "success_count": success_count,
                "error_count": error_count,
                "total_count": total_count,
                "errors": []
            }
            
        except Exception as e:
            self.logger.log_error(
                component="LOADER",
                message="Load failed",
                details=str(e)
            )
            raise ETLLoadError(f"Load failed: {str(e)}") from e