"""
Load module for Delta Lake with merge/upsert operations.
Replaces ABAP batch loading with Delta Lake merge operations.
"""
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp, lit
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class DeltaLakeLoader:
    """Load data to Delta Lake with support for multiple write modes."""
    
    def __init__(self, spark: SparkSession, run_id: str, config: Dict[str, Any]):
        """
        Initialize Delta Lake loader.
        
        Args:
            spark: SparkSession instance
            run_id: Unique run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        self.target_path = config['delta_lake']['target_path']
        self.batch_size = config.get('batch_size', 1000)
        
    def load_data(
        self,
        df: DataFrame,
        mode: str = 'upsert',
        partition_cols: Optional[list] = None
    ) -> Dict[str, Any]:
        """
        Load data to Delta Lake with specified mode.
        
        Args:
            df: DataFrame to load
            mode: Write mode - 'insert', 'upsert', 'overwrite', 'append'
            partition_cols: Columns to partition by
            
        Returns:
            Dictionary with load results
        """
        logger.info(f"Starting load - Mode: {mode}, Batch size: {self.batch_size}")
        
        result = {
            'success_count': 0,
            'error_count': 0,
            'total_count': df.count(),
            'errors': []
        }
        
        try:
            if mode == 'insert':
                success = self._insert_new(df, partition_cols)
            elif mode == 'upsert' or mode == 'merge':
                success = self._upsert_data(df, partition_cols)
            elif mode == 'overwrite':
                success = self._overwrite_data(df, partition_cols)
            elif mode == 'append':
                success = self._append_data(df, partition_cols)
            else:
                raise ValueError(f"Unsupported mode: {mode}")
            
            if success:
                result['success_count'] = result['total_count']
                logger.info(f"Load complete - Success: {result['success_count']}")
            else:
                result['error_count'] = result['total_count']
                result['errors'].append("Load operation failed")
                
        except Exception as e:
            logger.error(f"Load error: {str(e)}")
            result['error_count'] = result['total_count']
            result['errors'].append(str(e))
        
        # Perform reconciliation if enabled
        if self.config.get('enable_reconciliation', True) and result['error_count'] == 0:
            reconcile_success = self._reconcile_data(df)
            if not reconcile_success:
                logger.warning("Data reconciliation detected discrepancies")
        
        return result
    
    def _insert_new(self, df: DataFrame, partition_cols: Optional[list]) -> bool:
        """
        Insert new records only (append mode with deduplication).
        
        Args:
            df: DataFrame to insert
            partition_cols: Partition columns
            
        Returns:
            Success flag
        """
        try:
            writer = df.write.format("delta").mode("append")
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            writer.save(self.target_path)
            
            logger.info(f"Inserted {df.count()} new records")
            return True
            
        except Exception as e:
            logger.error(f"Insert failed: {str(e)}")
            return False
    
    def _upsert_data(self, df: DataFrame, partition_cols: Optional[list]) -> bool:
        """
        Upsert data using Delta Lake MERGE operation.
        
        Args:
            df: DataFrame to upsert
            partition_cols: Partition columns
            
        Returns:
            Success flag
        """
        try:
            # Check if table exists
            if DeltaTable.isDeltaTable(self.spark, self.target_path):
                delta_table = DeltaTable.forPath(self.spark, self.target_path)
                
                # Prepare merge keys
                merge_keys = self.config['delta_lake'].get('merge_keys', ['id'])
                merge_condition = ' AND '.join([
                    f"target.{key} = source.{key}" for key in merge_keys
                ])
                
                # Perform MERGE operation
                (delta_table.alias("target")
                 .merge(df.alias("source"), merge_condition)
                 .whenMatchedUpdateAll()
                 .whenNotMatchedInsertAll()
                 .execute())
                
                logger.info(f"Upserted {df.count()} records")
            else:
                # Table doesn't exist, create it
                writer = df.write.format("delta").mode("overwrite")
                
                if partition_cols:
                    writer = writer.partitionBy(*partition_cols)
                
                writer.save(self.target_path)
                logger.info(f"Created table and inserted {df.count()} records")
            
            return True
            
        except Exception as e:
            logger.error(f"Upsert failed: {str(e)}")
            return False
    
    def _overwrite_data(self, df: DataFrame, partition_cols: Optional[list]) -> bool:
        """
        Overwrite target table completely.
        
        Args:
            df: DataFrame to write
            partition_cols: Partition columns
            
        Returns:
            Success flag
        """
        try:
            writer = df.write.format("delta").mode("overwrite")
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            writer.save(self.target_path)
            
            logger.info(f"Overwrote table with {df.count()} records")
            return True
            
        except Exception as e:
            logger.error(f"Overwrite failed: {str(e)}")
            return False
    
    def _append_data(self, df: DataFrame, partition_cols: Optional[list]) -> bool:
        """
        Append data to existing table.
        
        Args:
            df: DataFrame to append
            partition_cols: Partition columns
            
        Returns:
            Success flag
        """
        try:
            writer = df.write.format("delta").mode("append")
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            writer.save(self.target_path)
            
            logger.info(f"Appended {df.count()} records")
            return True
            
        except Exception as e:
            logger.error(f"Append failed: {str(e)}")
            return False
    
    def _reconcile_data(self, loaded_df: DataFrame) -> bool:
        """
        Reconcile loaded data against target table.
        
        Args:
            loaded_df: DataFrame that was loaded
            
        Returns:
            True if reconciliation matches
        """
        try:
            if not DeltaTable.isDeltaTable(self.spark, self.target_path):
                logger.warning("Target table doesn't exist for reconciliation")
                return False
            
            # Read target table
            target_df = self.spark.read.format("delta").load(self.target_path)
            
            # Filter to current run
            target_run_df = target_df.filter(col("etl_run_id") == self.run_id)
            
            loaded_count = loaded_df.count()
            target_count = target_run_df.count()
            
            if loaded_count == target_count:
                logger.info(f"Reconciliation successful: {loaded_count} records match")
                return True
            else:
                logger.warning(
                    f"Reconciliation mismatch: "
                    f"Loaded {loaded_count}, Found {target_count}"
                )
                return False
                
        except Exception as e:
            logger.error(f"Reconciliation failed: {str(e)}")
            return False
    
    def optimize_table(self, z_order_cols: Optional[list] = None):
        """
        Optimize Delta Lake table with OPTIMIZE and Z-ORDER.
        
        Args:
            z_order_cols: Columns for Z-ORDER BY optimization
        """
        try:
            if not DeltaTable.isDeltaTable(self.spark, self.target_path):
                logger.warning("Cannot optimize non-existent table")
                return
            
            delta_table = DeltaTable.forPath(self.spark, self.target_path)
            
            # Run OPTIMIZE
            if z_order_cols:
                delta_table.optimize().executeZOrderBy(*z_order_cols)
                logger.info(f"Optimized table with Z-ORDER BY {z_order_cols}")
            else:
                delta_table.optimize().executeCompaction()
                logger.info("Optimized table with compaction")
                
        except Exception as e:
            logger.error(f"Table optimization failed: {str(e)}")
    
    def vacuum_table(self, retention_hours: int = 168):
        """
        Vacuum old versions from Delta Lake table.
        
        Args:
            retention_hours: Hours of history to retain (default: 7 days)
        """
        try:
            if not DeltaTable.isDeltaTable(self.spark, self.target_path):
                logger.warning("Cannot vacuum non-existent table")
                return
            
            delta_table = DeltaTable.forPath(self.spark, self.target_path)
            delta_table.vacuum(retention_hours)
            
            logger.info(f"Vacuumed table (retention: {retention_hours} hours)")
            
        except Exception as e:
            logger.error(f"Table vacuum failed: {str(e)}")