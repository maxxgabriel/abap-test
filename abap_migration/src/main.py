"""
Main ETL execution program with CLI interface.
Orchestrates the complete ETL pipeline with monitoring and error handling.
"""
import argparse
import sys
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.utils.logger import ETLLogger
from src.utils.monitor import ETLMonitor
from src.utils.data_quality import DataQualityChecker
from src.config_manager import ConfigManager


class ETLOrchestrator:
    """
    Main orchestrator for ETL pipeline execution.
    Coordinates extraction, transformation, loading, and monitoring.
    """
    
    def __init__(self, run_type: str = "MANUAL", config_path: str = "config.yaml"):
        self.run_id = self._generate_run_id()
        self.run_type = run_type
        self.config = ConfigManager(config_path)
        self.logger = ETLLogger(run_id=self.run_id)
        self.monitor = ETLMonitor(run_id=self.run_id)
        self.spark: Optional[SparkSession] = None
        
    def _generate_run_id(self) -> str:
        """Generate unique run ID."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"RUN_{timestamp}"
    
    def initialize_spark(self) -> SparkSession:
        """Initialize and configure Spark session."""
        spark_config = self.config.get("spark", {})
        
        builder = SparkSession.builder \
            .appName(f"ETL_Pipeline_{self.run_id}")
        
        # Apply Spark configurations
        for key, value in spark_config.items():
            builder = builder.config(f"spark.{key}", value)
        
        self.spark = builder.getOrCreate()
        self.logger.log_info(
            component="ORCHESTRATOR",
            message=f"Spark session initialized - Run ID: {self.run_id}"
        )
        return self.spark
    
    def execute_etl(
        self,
        source_type: str = "DATABASE",
        target_type: str = "DATABASE",
        filter_expr: Optional[str] = None,
        batch_size: int = 1000,
        max_records: int = 0,
        validate: bool = True,
        reconcile: bool = True
    ) -> Dict[str, Any]:
        """
        Execute complete ETL pipeline.
        
        Args:
            source_type: Type of data source (DATABASE, STAGING, INCREMENTAL)
            target_type: Type of data target (DATABASE, PARQUET, DELTA)
            filter_expr: SQL filter expression for extraction
            batch_size: Batch size for loading
            max_records: Maximum records to process (0 = unlimited)
            validate: Enable data validation
            reconcile: Enable data reconciliation
            
        Returns:
            Dictionary containing execution results and metrics
        """
        start_time = datetime.now()
        result = {
            "run_id": self.run_id,
            "status": "RUNNING",
            "start_time": start_time,
            "end_time": None,
            "duration_seconds": 0,
            "records_extracted": 0,
            "records_transformed": 0,
            "records_loaded": 0,
            "records_failed": 0,
            "error_count": 0,
            "warning_count": 0,
            "errors": []
        }
        
        try:
            # Initialize Spark if not already initialized
            if not self.spark:
                self.initialize_spark()
            
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"Starting ETL execution - Source: {source_type}, Target: {target_type}"
            )
            
            # Step 1: Extract
            self.logger.log_info(component="ORCHESTRATOR", message="Starting extraction phase")
            extractor = ETLExtractor(
                spark=self.spark,
                source_type=source_type,
                run_id=self.run_id,
                config=self.config
            )
            
            source_df = extractor.extract_data(
                filter_expr=filter_expr,
                max_records=max_records
            )
            
            if source_df is None or source_df.count() == 0:
                self.logger.log_warning(
                    component="ORCHESTRATOR",
                    message="No data extracted - ETL process stopping"
                )
                result["status"] = "NO_DATA"
                result["end_time"] = datetime.now()
                return result
            
            result["records_extracted"] = source_df.count()
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"Extracted {result['records_extracted']} records"
            )
            
            # Step 2: Transform
            self.logger.log_info(component="ORCHESTRATOR", message="Starting transformation phase")
            transformer = ETLTransformer(
                spark=self.spark,
                run_id=self.run_id,
                config=self.config
            )
            
            transformed_df = transformer.transform_data(source_df)
            result["records_transformed"] = transformed_df.count()
            
            # Step 3: Validate (if enabled)
            if validate:
                self.logger.log_info(component="ORCHESTRATOR", message="Running data validation")
                quality_checker = DataQualityChecker(
                    spark=self.spark,
                    run_id=self.run_id,
                    config=self.config
                )
                
                validation_result = quality_checker.perform_quality_checks(transformed_df)
                
                if not validation_result["is_valid"]:
                    result["error_count"] = validation_result["failed_checks"]
                    result["errors"] = validation_result["errors"]
                    result["status"] = "VALIDATION_FAILED"
                    self.logger.log_error(
                        component="ORCHESTRATOR",
                        message=f"Validation failed with {validation_result['failed_checks']} errors"
                    )
                    result["end_time"] = datetime.now()
                    return result
            
            # Step 4: Load
            self.logger.log_info(component="ORCHESTRATOR", message="Starting load phase")
            loader = ETLLoader(
                spark=self.spark,
                target_type=target_type,
                batch_size=batch_size,
                run_id=self.run_id,
                config=self.config
            )
            
            load_result = loader.load_data(
                df=transformed_df,
                mode="UPSERT"
            )
            
            result["records_loaded"] = load_result["success_count"]
            result["records_failed"] = load_result["error_count"]
            result["error_count"] += load_result["error_count"]
            
            # Step 5: Reconcile (if enabled)
            if reconcile and result["records_loaded"] > 0:
                self.logger.log_info(component="ORCHESTRATOR", message="Running reconciliation")
                reconcile_result = loader.reconcile_data(transformed_df)
                if not reconcile_result:
                    result["warning_count"] += 1
                    self.logger.log_warning(
                        component="ORCHESTRATOR",
                        message="Data reconciliation detected mismatches"
                    )
            
            # Determine final status
            if result["error_count"] == 0 and result["records_failed"] == 0:
                result["status"] = "SUCCESS"
            elif result["records_loaded"] > 0:
                result["status"] = "PARTIAL_SUCCESS"
            else:
                result["status"] = "FAILED"
            
            end_time = datetime.now()
            result["end_time"] = end_time
            result["duration_seconds"] = (end_time - start_time).total_seconds()
            
            # Log run statistics
            self._log_run_end(result)
            
            self.logger.log_info(
                component="ORCHESTRATOR",
                message=f"ETL execution completed - Status: {result['status']}, "
                       f"Duration: {result['duration_seconds']:.2f}s"
            )
            
        except Exception as e:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message="ETL execution failed",
                details=str(e)
            )
            result["status"] = "ERROR"
            result["error_count"] += 1
            result["errors"].append(str(e))
            result["end_time"] = datetime.now()
            result["duration_seconds"] = (result["end_time"] - start_time).total_seconds()
            
        finally:
            # Update monitoring metrics
            self.monitor.update_metrics(result)
            
        return result
    
    def _log_run_end(self, result: Dict[str, Any]) -> None:
        """Log execution results to run log table."""
        try:
            run_log_data = [{
                "run_id": result["run_id"],
                "run_type": self.run_type,
                "status": result["status"],
                "start_time": result["start_time"].isoformat(),
                "end_time": result["end_time"].isoformat() if result["end_time"] else None,
                "duration_seconds": result["duration_seconds"],
                "records_extracted": result["records_extracted"],
                "records_transformed": result["records_transformed"],
                "records_loaded": result["records_loaded"],
                "records_failed": result["records_failed"],
                "error_count": result["error_count"],
                "warning_count": result["warning_count"]
            }]
            
            run_log_df = self.spark.createDataFrame(run_log_data)
            
            # Write to run log table
            db_config = self.config.get("database", {})
            table_name = db_config.get("run_log_table", "etl_run_log")
            
            run_log_df.write \
                .format("jdbc") \
                .option("url", db_config.get("url")) \
                .option("dbtable", table_name) \
                .option("user", db_config.get("user")) \
                .option("password", db_config.get("password")) \
                .mode("append") \
                .save()
            
        except Exception as e:
            self.logger.log_error(
                component="ORCHESTRATOR",
                message="Failed to log run statistics",
                details=str(e)
            )
    
    def get_run_statistics(self, run_id: Optional[str] = None) -> Dict[str, Any]:
        """Get execution statistics for a specific run or all runs."""
        return self.monitor.get_run_statistics(run_id or self.run_id)
    
    def shutdown(self) -> None:
        """Clean shutdown of resources."""
        if self.spark:
            self.spark.stop()
            self.logger.log_info(
                component="ORCHESTRATOR",
                message="Spark session stopped"
            )


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure argument parser for CLI."""
    parser = argparse.ArgumentParser(
        description="ETL Pipeline Execution System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full load from database
  python src/main.py --source DATABASE --target DATABASE
  
  # Incremental load with filter
  python src/main.py --source INCREMENTAL --filter "status='ACTIVE'"
  
  # Test mode with limited records
  python src/main.py --test --max-records 100
  
  # Load to Delta Lake with custom batch size
  python src/main.py --target DELTA --batch-size 5000
        """
    )
    
    # Data source options
    parser.add_argument(
        "--source",
        type=str,
        default="DATABASE",
        choices=["DATABASE", "STAGING", "INCREMENTAL"],
        help="Source data type (default: DATABASE)"
    )
    
    parser.add_argument(
        "--target",
        type=str,
        default="DATABASE",
        choices=["DATABASE", "PARQUET", "DELTA", "CSV"],
        help="Target data type (default: DATABASE)"
    )
    
    # Processing options
    parser.add_argument(
        "--filter",
        type=str,
        help="SQL filter expression for extraction (e.g., 'status=\"ACTIVE\"')"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for data loading (default: 1000)"
    )
    
    parser.add_argument(
        "--max-records",
        type=int,
        default=0,
        help="Maximum number of records to process (0 = unlimited, default: 0)"
    )
    
    # Feature flags
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Disable data validation"
    )
    
    parser.add_argument(
        "--no-reconcile",
        action="store_true",
        help="Disable data reconciliation"
    )
    
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run in test mode (no data commits)"
    )
    
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )
    
    # Configuration
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)"
    )
    
    # Statistics
    parser.add_argument(
        "--show-stats",
        action="store_true",
        help="Display dashboard statistics after execution"
    )
    
    parser.add_argument(
        "--stats-days",
        type=int,
        default=7,
        help="Number of days for statistics display (default: 7)"
    )
    
    return parser


def display_results(result: Dict[str, Any], monitor: ETLMonitor, show_stats: bool, stats_days: int) -> None:
    """Display execution results and optional statistics."""
    print("\n" + "="*80)
    print("ETL EXECUTION SUMMARY".center(80))
    print("="*80)
    
    # Status indicator
    status_icon = {
        "SUCCESS": "✓",
        "PARTIAL_SUCCESS": "⚠",
        "FAILED": "✗",
        "ERROR": "✗",
        "NO_DATA": "○"
    }.get(result["status"], "?")
    
    print(f"\n{status_icon} Status: {result['status']}")
    print(f"  Run ID: {result['run_id']}")
    print(f"  Duration: {result['duration_seconds']:.2f} seconds")
    
    print("\n" + "-"*80)
    print("RECORDS PROCESSED")
    print("-"*80)
    print(f"  Extracted:    {result['records_extracted']:>10,}")
    print(f"  Transformed:  {result['records_transformed']:>10,}")
    print(f"  Loaded:       {result['records_loaded']:>10,}")
    print(f"  Failed:       {result['records_failed']:>10,}")
    
    if result["error_count"] > 0 or result["warning_count"] > 0:
        print("\n" + "-"*80)
        print("ISSUES")
        print("-"*80)
        print(f"  Errors:   {result['error_count']:>10}")
        print(f"  Warnings: {result['warning_count']:>10}")
        
        if result.get("errors"):
            print("\n  Error Details:")
            for i, error in enumerate(result["errors"][:5], 1):
                print(f"    {i}. {error}")
    
    # Display statistics dashboard
    if show_stats:
        print("\n" + "="*80)
        print(f"{stats_days}-DAY DASHBOARD SUMMARY".center(80))
        print("="*80)
        
        dashboard = monitor.get_dashboard_data(days_back=stats_days)
        
        print(f"\n  Total Runs:       {dashboard['total_runs']:>10,}")
        print(f"  Successful:       {dashboard['successful_runs']:>10,}")
        print(f"  Failed:           {dashboard['failed_runs']:>10,}")
        print(f"  Running:          {dashboard['running_jobs']:>10,}")
        print(f"  Avg Duration:     {dashboard['avg_duration_seconds']:>10.2f}s")
        print(f"  Total Records:    {dashboard['total_records']:>10,}")
        print(f"  Error Rate:       {dashboard['error_rate']:>10.2f}%")
        
        # Health status
        health_status = monitor.check_health()
        health_icon = "✓" if health_status == "HEALTHY" else "⚠"
        print(f"\n  {health_icon} System Health: {health_status}")
    
    print("\n" + "="*80 + "\n")


def main():
    """Main entry point for CLI execution."""
    parser = create_argument_parser()
    args = parser.parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Display startup banner
    print("\n" + "="*80)
    print("ETL PIPELINE EXECUTION SYSTEM".center(80))
    print("="*80 + "\n")
    
    if args.test:
        print("⚠ TEST MODE - No data will be committed\n")
    
    try:
        # Initialize orchestrator
        orchestrator = ETLOrchestrator(
            run_type="TEST" if args.test else "MANUAL",
            config_path=args.config
        )
        
        # Execute ETL pipeline
        result = orchestrator.execute_etl(
            source_type=args.source,
            target_type=args.target,
            filter_expr=args.filter,
            batch_size=args.batch_size,
            max_records=args.max_records,
            validate=not args.no_validate,
            reconcile=not args.no_reconcile
        )
        
        # Display results
        display_results(
            result=result,
            monitor=orchestrator.monitor,
            show_stats=args.show_stats,
            stats_days=args.stats_days
        )
        
        # Exit with appropriate code
        exit_code = 0 if result["status"] in ["SUCCESS", "NO_DATA"] else 1
        
    except KeyboardInterrupt:
        print("\n\n⚠ Execution interrupted by user")
        exit_code = 130
        
    except Exception as e:
        print(f"\n✗ Fatal error: {str(e)}")
        if args.debug:
            import traceback
            traceback.print_exc()
        exit_code = 1
        
    finally:
        if 'orchestrator' in locals():
            orchestrator.shutdown()
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()