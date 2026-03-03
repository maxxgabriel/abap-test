"""
Main ETL execution program with CLI interface.
Orchestrates extract, transform, and load operations with comprehensive error handling.
"""

import argparse
import sys
from datetime import datetime
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.utils.logger import ETLLogger
from src.utils.monitor import ETLMonitor
from src.utils.config import ConfigManager


class ETLContext:
    """
    Context object to replace ABAP memory exports.
    Holds shared state across ETL operations.
    """
    def __init__(self, run_id: str, run_type: str = "MANUAL"):
        self.run_id = run_id
        self.run_type = run_type
        self.start_time = datetime.now()
        self.end_time: Optional[datetime] = None
        self.status = "RUNNING"
        self.records_extracted = 0
        self.records_transformed = 0
        self.records_loaded = 0
        self.records_failed = 0
        self.error_count = 0
        self.warning_count = 0
        self.duration_seconds = 0
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary for logging."""
        return {
            "run_id": self.run_id,
            "run_type": self.run_type,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "status": self.status,
            "records_extracted": self.records_extracted,
            "records_transformed": self.records_transformed,
            "records_loaded": self.records_loaded,
            "records_failed": self.records_failed,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "duration_seconds": self.duration_seconds
        }


class ETLOrchestrator:
    """
    Main ETL orchestrator - replaces FORM routines with methods.
    Coordinates the entire ETL pipeline execution.
    """
    
    def __init__(self, spark: SparkSession, config: ConfigManager):
        self.spark = spark
        self.config = config
        self.logger = ETLLogger.get_instance()
        self.monitor = ETLMonitor.get_instance()
        
    def execute_etl(
        self,
        context: ETLContext,
        source_type: str = "DATABASE",
        target_type: str = "DATABASE",
        filter_expr: Optional[str] = None,
        batch_size: int = 1000,
        max_records: int = 0,
        validate: bool = True,
        reconcile: bool = True
    ) -> ETLContext:
        """
        Execute complete ETL pipeline.
        Replaces execute_etl FORM routine.
        
        Args:
            context: ETL execution context
            source_type: Source data type (DATABASE, STAGING, INCREMENTAL)
            target_type: Target data type (DATABASE, WAREHOUSE)
            filter_expr: Optional filter expression
            batch_size: Records per batch for loading
            max_records: Maximum records to process (0 = unlimited)
            validate: Whether to validate transformed data
            reconcile: Whether to reconcile loaded data
            
        Returns:
            Updated ETL context with results
        """
        self.logger.log_info(
            "ORCHESTRATOR",
            f"ETL execution started - Run ID: {context.run_id}"
        )
        
        try:
            # Step 1: Extract
            context = self._extract_phase(
                context, source_type, filter_expr, max_records
            )
            
            if context.records_extracted == 0:
                self.logger.log_warning(
                    "ORCHESTRATOR",
                    "No data extracted - ETL process stopping"
                )
                context.status = "NO_DATA"
                return context
            
            # Step 2: Transform
            context = self._transform_phase(context, validate)
            
            if context.status == "VALIDATION_FAILED":
                return context
            
            # Step 3: Load
            context = self._load_phase(
                context, target_type, batch_size, reconcile
            )
            
            # Finalize
            context.end_time = datetime.now()
            context.duration_seconds = int(
                (context.end_time - context.start_time).total_seconds()
            )
            
            self._log_run_completion(context)
            
            self.logger.log_info(
                "ORCHESTRATOR",
                f"ETL execution completed - Status: {context.status}"
            )
            
        except Exception as e:
            self._handle_orchestration_error(context, e)
            
        return context
    
    def _extract_phase(
        self,
        context: ETLContext,
        source_type: str,
        filter_expr: Optional[str],
        max_records: int
    ) -> ETLContext:
        """Extract phase - replaces PERFORM extract_data."""
        self.logger.log_info("ORCHESTRATOR", "Starting extraction phase")
        
        extractor = ETLExtractor(
            self.spark,
            self.config,
            source_type=source_type,
            run_id=context.run_id
        )
        
        df_extracted = extractor.extract_data(
            filter_expr=filter_expr,
            max_records=max_records
        )
        
        context.records_extracted = df_extracted.count()
        context.extracted_df = df_extracted  # Store in context
        
        self.logger.log_info(
            "ORCHESTRATOR",
            f"Extracted {context.records_extracted} records"
        )
        
        return context
    
    def _transform_phase(
        self,
        context: ETLContext,
        validate: bool
    ) -> ETLContext:
        """Transform phase - replaces PERFORM transform_data."""
        self.logger.log_info("ORCHESTRATOR", "Starting transformation phase")
        
        transformer = ETLTransformer(
            self.spark,
            self.config,
            run_id=context.run_id
        )
        
        df_transformed = transformer.transform_data(context.extracted_df)
        
        # Validate if requested
        if validate:
            validation_result = transformer.validate_data(df_transformed)
            
            if not validation_result["is_valid"]:
                self.logger.log_error(
                    "ORCHESTRATOR",
                    f"Validation failed - {validation_result['error_count']} errors"
                )
                context.status = "VALIDATION_FAILED"
                context.error_count = validation_result["error_count"]
                return context
        
        context.records_transformed = df_transformed.count()
        context.transformed_df = df_transformed  # Store in context
        
        self.logger.log_info(
            "ORCHESTRATOR",
            f"Transformed {context.records_transformed} records"
        )
        
        return context
    
    def _load_phase(
        self,
        context: ETLContext,
        target_type: str,
        batch_size: int,
        reconcile: bool
    ) -> ETLContext:
        """Load phase - replaces PERFORM load_data."""
        self.logger.log_info("ORCHESTRATOR", "Starting load phase")
        
        loader = ETLLoader(
            self.spark,
            self.config,
            target_type=target_type,
            batch_size=batch_size,
            run_id=context.run_id
        )
        
        load_result = loader.load_data(
            context.transformed_df,
            mode="UPSERT",
            reconcile=reconcile
        )
        
        context.records_loaded = load_result["success_count"]
        context.records_failed = load_result["error_count"]
        context.error_count += load_result["error_count"]
        
        # Determine final status
        if load_result["error_count"] == 0:
            context.status = "SUCCESS"
        elif load_result["success_count"] > 0:
            context.status = "PARTIAL_SUCCESS"
        else:
            context.status = "FAILED"
        
        self.logger.log_info(
            "ORCHESTRATOR",
            f"Loaded {context.records_loaded} records, "
            f"Failed: {context.records_failed}"
        )
        
        return context
    
    def _log_run_completion(self, context: ETLContext):
        """Log run completion to monitoring tables."""
        self.monitor.log_run_completion(context.to_dict())
    
    def _handle_orchestration_error(self, context: ETLContext, error: Exception):
        """Handle errors during orchestration."""
        context.status = "ERROR"
        context.end_time = datetime.now()
        context.error_count += 1
        
        self.logger.log_error(
            "ORCHESTRATOR",
            f"ETL execution failed: {str(error)}",
            details=str(error.__class__.__name__)
        )
        
        self._log_run_completion(context)


def parse_arguments():
    """
    Parse command-line arguments.
    Replaces ABAP selection screen parameters.
    """
    parser = argparse.ArgumentParser(
        description="ETL Main Execution Program",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Data source/target parameters (Block b1)
    parser.add_argument(
        "--source-type",
        type=str,
        default="DATABASE",
        choices=["DATABASE", "STAGING", "INCREMENTAL"],
        help="Source data type"
    )
    
    parser.add_argument(
        "--target-type",
        type=str,
        default="DATABASE",
        choices=["DATABASE", "WAREHOUSE"],
        help="Target data type"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for data loading"
    )
    
    parser.add_argument(
        "--filter",
        type=str,
        default=None,
        help="Filter expression for source data"
    )
    
    parser.add_argument(
        "--max-records",
        type=int,
        default=0,
        help="Maximum records to process (0 = unlimited)"
    )
    
    # Processing options (Block b2)
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Use incremental load mode"
    )
    
    parser.add_argument(
        "--validate",
        action="store_true",
        default=True,
        help="Validate transformed data"
    )
    
    parser.add_argument(
        "--no-validate",
        dest="validate",
        action="store_false",
        help="Skip data validation"
    )
    
    parser.add_argument(
        "--reconcile",
        action="store_true",
        default=True,
        help="Reconcile loaded data"
    )
    
    parser.add_argument(
        "--no-reconcile",
        dest="reconcile",
        action="store_false",
        help="Skip data reconciliation"
    )
    
    # Execution options (Block b3)
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Test mode - no data will be committed"
    )
    
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output"
    )
    
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to configuration file"
    )
    
    parser.add_argument(
        "--run-type",
        type=str,
        default="MANUAL",
        choices=["MANUAL", "SCHEDULED", "TEST"],
        help="Type of ETL run"
    )
    
    return parser.parse_args()


def display_banner():
    """Display startup banner - replaces ABAP header output."""
    print("╔════════════════════════════════════════════════════════════╗")
    print("║          ETL Process Execution Started                     ║")
    print("╚════════════════════════════════════════════════════════════╝")
    print()


def display_summary(context: ETLContext, debug: bool = False):
    """
    Display execution summary.
    Replaces FORM display_results.
    """
    print()
    print("┌────────────────────────────────────────────────────────┐")
    print("│ ETL Execution Summary                                  │")
    print("├────────────────────────────────────────────────────────┤")
    print(f"│ Run ID:           {context.run_id:<30} │")
    
    # Status with emoji
    status_display = {
        "SUCCESS": "✓ SUCCESS",
        "PARTIAL_SUCCESS": "⚠ PARTIAL SUCCESS",
        "FAILED": "✗ FAILED",
        "ERROR": "✗ ERROR",
        "NO_DATA": "⚠ NO DATA"
    }
    print(f"│ Status:           {status_display.get(context.status, context.status):<30} │")
    
    print("├────────────────────────────────────────────────────────┤")
    print("│ Records:                                               │")
    print(f"│   Extracted:      {context.records_extracted:<30} │")
    print(f"│   Transformed:    {context.records_transformed:<30} │")
    print(f"│   Loaded:         {context.records_loaded:<30} │")
    print(f"│   Failed:         {context.records_failed:<30} │")
    print("├────────────────────────────────────────────────────────┤")
    print("│ Performance:                                           │")
    print(f"│   Duration:       {context.duration_seconds:<20} seconds │")
    print(f"│   Errors:         {context.error_count:<30} │")
    print(f"│   Warnings:       {context.warning_count:<30} │")
    print("└────────────────────────────────────────────────────────┘")
    print()
    
    if debug:
        display_debug_info(context)


def display_debug_info(context: ETLContext):
    """Display debug information - replaces FORM show_debug_info."""
    print("╔════════════════════════════════════════════════════════════╗")
    print("║          Debug Information                                 ║")
    print("╚════════════════════════════════════════════════════════════╝")
    print()
    
    monitor = ETLMonitor.get_instance()
    dashboard = monitor.get_dashboard_data(days_back=7)
    
    print("7-Day Dashboard Summary:")
    print(f"  Total Runs:        {dashboard['total_runs']}")
    print(f"  Successful:        {dashboard['successful_runs']}")
    print(f"  Failed:            {dashboard['failed_runs']}")
    print(f"  Running:           {dashboard['running_jobs']}")
    print(f"  Avg Duration:      {dashboard['avg_duration']} sec")
    print(f"  Total Records:     {dashboard['total_records']}")
    print(f"  Error Rate:        {dashboard['error_rate']:.2f}%")
    print()
    
    # Health check
    health_status = monitor.check_health()
    print(f"Health Status: {health_status}")
    print()


def generate_run_id() -> str:
    """Generate unique run ID."""
    return datetime.now().strftime("RUN_%Y%m%d_%H%M%S")


def main():
    """
    Main entry point for ETL execution.
    Replaces START-OF-SELECTION and END-OF-SELECTION sections.
    """
    # Parse arguments
    args = parse_arguments()
    
    # Display banner
    display_banner()
    
    # Test mode warning
    if args.test_mode:
        print("⚠ TEST MODE - No data will be committed")
        print()
    
    # Initialize Spark
    spark = (SparkSession.builder
             .appName("ETL_Main_Execution")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    
    # Set log level
    if args.debug:
        spark.sparkContext.setLogLevel("INFO")
    else:
        spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Load configuration
        config = ConfigManager(args.config)
        
        # Generate run ID
        run_id = generate_run_id()
        
        # Create context
        context = ETLContext(run_id=run_id, run_type=args.run_type)
        
        # Override source type if incremental
        source_type = "INCREMENTAL" if args.incremental else args.source_type
        
        # Create orchestrator
        orchestrator = ETLOrchestrator(spark, config)
        
        # Execute ETL
        context = orchestrator.execute_etl(
            context=context,
            source_type=source_type,
            target_type=args.target_type,
            filter_expr=args.filter,
            batch_size=args.batch_size,
            max_records=args.max_records,
            validate=args.validate,
            reconcile=args.reconcile
        )
        
        # Display results
        display_summary(context, debug=args.debug)
        
        # Exit with appropriate code
        if context.status in ["SUCCESS", "NO_DATA"]:
            sys.exit(0)
        elif context.status == "PARTIAL_SUCCESS":
            sys.exit(1)
        else:
            sys.exit(2)
            
    except Exception as e:
        print(f"✗ FATAL ERROR: {str(e)}")
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(3)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()