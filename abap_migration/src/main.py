#!/usr/bin/env python3
"""
ETL Main Execution Program
Main orchestration script with CLI interface for ETL pipeline execution.
Migrated from ABAP Report Z_ETL_MAIN
"""

import argparse
import sys
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path

from pyspark.sql import SparkSession

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.orchestrator import ETLOrchestrator
from src.monitor import ETLMonitor
from src.data_quality import DataQualityChecker
from src.logger import ETLLogger
from src.config import ETLConfig


def setup_logging(debug: bool = False) -> logging.Logger:
    """Configure logging for the application."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format='[%(asctime)s] %(levelname)s: %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure the command-line argument parser."""
    parser = argparse.ArgumentParser(
        description='ETL Process Execution - Extract, Transform, Load pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full ETL with default settings
  python src/main.py --source DATABASE --target DATABASE
  
  # Run incremental load with custom batch size
  python src/main.py --incremental --batch-size 5000
  
  # Test mode with debug logging
  python src/main.py --test --debug --max-records 100
  
  # Run with filter and reconciliation
  python src/main.py --filter "status='ACTIVE'" --reconcile
        """
    )
    
    # ETL Configuration
    etl_group = parser.add_argument_group('ETL Configuration')
    etl_group.add_argument(
        '--source',
        type=str,
        default='DATABASE',
        choices=['DATABASE', 'STAGING', 'INCREMENTAL', 'FILE'],
        help='Source type for data extraction (default: DATABASE)'
    )
    etl_group.add_argument(
        '--target',
        type=str,
        default='DATABASE',
        choices=['DATABASE', 'PARQUET', 'DELTA', 'HIVE'],
        help='Target type for data loading (default: DATABASE)'
    )
    etl_group.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Batch size for data processing (default: 1000)'
    )
    etl_group.add_argument(
        '--filter',
        type=str,
        default='',
        help='SQL filter clause for data extraction'
    )
    etl_group.add_argument(
        '--max-records',
        type=int,
        default=0,
        help='Maximum records to process (0 = unlimited)'
    )
    
    # Processing Options
    process_group = parser.add_argument_group('Processing Options')
    process_group.add_argument(
        '--incremental',
        action='store_true',
        help='Run incremental load (only changed records)'
    )
    process_group.add_argument(
        '--validate',
        action='store_true',
        default=True,
        help='Enable data validation (default: enabled)'
    )
    process_group.add_argument(
        '--no-validate',
        dest='validate',
        action='store_false',
        help='Disable data validation'
    )
    process_group.add_argument(
        '--reconcile',
        action='store_true',
        default=True,
        help='Enable data reconciliation (default: enabled)'
    )
    process_group.add_argument(
        '--no-reconcile',
        dest='reconcile',
        action='store_false',
        help='Disable data reconciliation'
    )
    process_group.add_argument(
        '--quality-checks',
        action='store_true',
        default=True,
        help='Run data quality checks'
    )
    
    # Runtime Options
    runtime_group = parser.add_argument_group('Runtime Options')
    runtime_group.add_argument(
        '--test',
        action='store_true',
        help='Test mode - no data will be committed'
    )
    runtime_group.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    runtime_group.add_argument(
        '--config',
        type=str,
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    runtime_group.add_argument(
        '--run-id',
        type=str,
        help='Custom run ID (auto-generated if not provided)'
    )
    runtime_group.add_argument(
        '--dry-run',
        action='store_true',
        help='Perform dry run without actual data changes'
    )
    
    # Monitoring Options
    monitor_group = parser.add_argument_group('Monitoring Options')
    monitor_group.add_argument(
        '--show-dashboard',
        action='store_true',
        help='Display monitoring dashboard after execution'
    )
    monitor_group.add_argument(
        '--show-stats',
        action='store_true',
        help='Display detailed statistics'
    )
    
    return parser


def display_header():
    """Display application header."""
    print("╔" + "═" * 60 + "╗")
    print("║" + " " * 15 + "ETL Process Execution Started" + " " * 16 + "║")
    print("╚" + "═" * 60 + "╝")
    print()


def display_summary(result: Dict[str, Any]):
    """Display execution summary."""
    print()
    print("┌" + "─" * 60 + "┐")
    print("│ ETL Execution Summary" + " " * 39 + "│")
    print("├" + "─" * 60 + "┤")
    
    # Status with icon
    status = result['status']
    if status == 'SUCCESS':
        status_display = "✓ SUCCESS"
    elif status == 'PARTIAL_SUCCESS':
        status_display = "⚠ PARTIAL SUCCESS"
    elif status == 'FAILED':
        status_display = "✗ FAILED"
    else:
        status_display = "✗ ERROR"
    
    print(f"│ Run ID:           {result['run_id']:<41} │")
    print(f"│ Status:           {status_display:<41} │")
    print("├" + "─" * 60 + "┤")
    print("│ Records:" + " " * 52 + "│")
    print(f"│   Extracted:      {result['records_extracted']:<41} │")
    print(f"│   Transformed:    {result['records_transformed']:<41} │")
    print(f"│   Loaded:         {result['records_loaded']:<41} │")
    print(f"│   Failed:         {result['records_failed']:<41} │")
    print("├" + "─" * 60 + "┤")
    print("│ Performance:" + " " * 48 + "│")
    print(f"│   Duration:       {result['duration']:<32} seconds │")
    print(f"│   Errors:         {result['error_count']:<41} │")
    print(f"│   Warnings:       {result['warning_count']:<41} │")
    print("└" + "─" * 60 + "┘")
    print()


def display_dashboard(monitor: ETLMonitor, days_back: int = 7):
    """Display monitoring dashboard."""
    dashboard = monitor.get_dashboard_data(days_back=days_back)
    
    print()
    print("╔" + "═" * 60 + "╗")
    print(f"║          {days_back}-Day Dashboard Summary" + " " * 31 + "║")
    print("╚" + "═" * 60 + "╝")
    print()
    
    print(f"  Total Runs:        {dashboard['total_runs']}")
    print(f"  Successful:        {dashboard['successful_runs']}")
    print(f"  Failed:            {dashboard['failed_runs']}")
    print(f"  Running:           {dashboard['running_jobs']}")
    print(f"  Avg Duration:      {dashboard['avg_duration']} sec")
    print(f"  Total Records:     {dashboard['total_records']}")
    print(f"  Error Rate:        {dashboard['error_rate']:.2f}%")
    print()
    
    # Health check
    health = monitor.check_health()
    health_icon = "✓" if health == "HEALTHY" else "⚠" if health == "WARNING" else "✗"
    print(f"  System Health:     {health_icon} {health}")
    print()


def execute_etl(args: argparse.Namespace, config: ETLConfig, 
                spark: SparkSession, logger: logging.Logger) -> Dict[str, Any]:
    """Execute the ETL process."""
    
    # Display configuration
    if args.test:
        print("⚠ TEST MODE - No data will be committed")
        print()
    
    if args.dry_run:
        print("⚠ DRY RUN MODE - Execution will be simulated")
        print()
    
    # Determine source type
    source_type = 'INCREMENTAL' if args.incremental else args.source
    
    logger.info(f"Starting ETL execution - Source: {source_type}, Target: {args.target}")
    
    # Create orchestrator
    orchestrator = ETLOrchestrator(
        spark=spark,
        config=config,
        run_type='TEST' if args.test else 'MANUAL',
        run_id=args.run_id
    )
    
    # Execute ETL pipeline
    result = orchestrator.execute_etl(
        source_type=source_type,
        target_type=args.target,
        filter_clause=args.filter,
        batch_size=args.batch_size,
        max_records=args.max_records,
        validate=args.validate,
        reconcile=args.reconcile,
        quality_checks=args.quality_checks,
        dry_run=args.dry_run
    )
    
    return result


def main():
    """Main entry point for the ETL application."""
    parser = create_argument_parser()
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(debug=args.debug)
    
    # Display header
    display_header()
    
    try:
        # Load configuration
        config = ETLConfig(config_path=args.config)
        logger.info(f"Loaded configuration from {args.config}")
        
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName(config.get('spark.app_name', 'ETL_Pipeline')) \
            .config('spark.sql.adaptive.enabled', 'true') \
            .config('spark.sql.adaptive.coalescePartitions.enabled', 'true') \
            .getOrCreate()
        
        logger.info("Spark session initialized")
        
        # Execute ETL
        result = execute_etl(args, config, spark, logger)
        
        # Display summary
        display_summary(result)
        
        # Show dashboard if requested
        if args.show_dashboard:
            monitor = ETLMonitor(spark=spark, config=config)
            display_dashboard(monitor)
        
        # Show detailed statistics if requested
        if args.show_stats:
            orchestrator = ETLOrchestrator(spark=spark, config=config)
            stats = orchestrator.get_run_statistics(run_id=result['run_id'])
            print("Detailed Statistics:")
            for stat in stats:
                print(f"  {stat}")
        
        # Determine exit code
        if result['status'] in ['SUCCESS', 'PARTIAL_SUCCESS']:
            exit_code = 0
        else:
            exit_code = 1
        
        # Cleanup
        spark.stop()
        logger.info("ETL execution completed")
        
        sys.exit(exit_code)
        
    except KeyboardInterrupt:
        logger.warning("ETL execution interrupted by user")
        print("\n⚠ ETL execution interrupted")
        sys.exit(130)
        
    except Exception as e:
        logger.error(f"ETL execution failed: {str(e)}", exc_info=True)
        print(f"\n✗ ERROR: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()