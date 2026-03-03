*&---------------------------------------------------------------------*
*& Report Z_ETL_MAIN
*&---------------------------------------------------------------------*
*& Main ETL execution program with enhanced features
*&---------------------------------------------------------------------*
REPORT z_etl_main.

* Selection screen parameters
SELECTION-SCREEN BEGIN OF BLOCK b1 WITH FRAME TITLE TEXT-001.
PARAMETERS: p_source TYPE char20 DEFAULT 'DATABASE',
            p_target TYPE char20 DEFAULT 'DATABASE',
            p_batch  TYPE i DEFAULT 1000,
            p_filter TYPE char50,
            p_maxrec TYPE i DEFAULT 0.
SELECTION-SCREEN END OF BLOCK b1.

SELECTION-SCREEN BEGIN OF BLOCK b2 WITH FRAME TITLE TEXT-002.
PARAMETERS: p_incr   AS CHECKBOX DEFAULT '',
            p_valid  AS CHECKBOX DEFAULT 'X',
            p_recon  AS CHECKBOX DEFAULT 'X'.
SELECTION-SCREEN END OF BLOCK b2.

SELECTION-SCREEN BEGIN OF BLOCK b3 WITH FRAME TITLE TEXT-003.
PARAMETERS: p_test   AS CHECKBOX DEFAULT '',
            p_debug  AS CHECKBOX DEFAULT ''.
SELECTION-SCREEN END OF BLOCK b3.

* Text symbols
SELECTION-SCREEN BEGIN OF LINE.
SELECTION-SCREEN COMMENT 1(30) TEXT-001 FOR FIELD p_source.
SELECTION-SCREEN END OF LINE.

START-OF-SELECTION.
  PERFORM execute_etl.

END-OF-SELECTION.
  PERFORM display_results.

*&---------------------------------------------------------------------*
*& Form execute_etl
*&---------------------------------------------------------------------*
FORM execute_etl.
  DATA: lo_orchestrator TYPE REF TO zcl_etl_orchestrator,
        ls_result       TYPE zcl_etl_orchestrator=>ty_etl_result,
        lv_source_type  TYPE string,
        lv_target_type  TYPE string.

  WRITE: / '╔════════════════════════════════════════════════════════════╗',
         / '║          ETL Process Execution Started                     ║',
         / '╚════════════════════════════════════════════════════════════╝', /.

  " Determine source type
  IF p_incr = 'X'.
    lv_source_type = 'INCREMENTAL'.
  ELSE.
    lv_source_type = p_source.
  ENDIF.

  lv_target_type = p_target.

  " Test mode
  IF p_test = 'X'.
    WRITE: / '⚠ TEST MODE - No data will be committed', /.
  ENDIF.

  TRY.
      " Create orchestrator
      lo_orchestrator = NEW zcl_etl_orchestrator( iv_run_type = 'MANUAL' ).

      " Execute ETL
      ls_result = lo_orchestrator->execute_etl(
        iv_source_type = lv_source_type
        iv_target_type = lv_target_type
        iv_filter      = p_filter
        iv_batch_size  = p_batch
        iv_max_records = p_maxrec
      ).

      " Store result for display
      EXPORT ls_result TO MEMORY ID 'ETL_RESULT'.

      " Display summary
      WRITE: / '┌────────────────────────────────────────────────────────┐',
             / '│ ETL Execution Summary                                  │',
             / '├────────────────────────────────────────────────────────┤'.

      WRITE: / '│ Run ID:           ', ls_result-run_id,
             / '│ Status:           ', ls_result-status.

      CASE ls_result-status.
        WHEN 'SUCCESS'.
          WRITE: / '│ ✓ Status:         SUCCESS                              │'.
        WHEN 'PARTIAL_SUCCESS'.
          WRITE: / '│ ⚠ Status:         PARTIAL SUCCESS                      │'.
        WHEN 'FAILED'.
          WRITE: / '│ ✗ Status:         FAILED                               │'.
        WHEN 'ERROR'.
          WRITE: / '│ ✗ Status:         ERROR                                │'.
      ENDCASE.

      WRITE: / '├────────────────────────────────────────────────────────┤',
             / '│ Records:                                               │',
             / '│   Extracted:      ', ls_result-records_extracted,
             / '│   Transformed:    ', ls_result-records_transformed,
             / '│   Loaded:         ', ls_result-records_loaded,
             / '│   Failed:         ', ls_result-records_failed,
             / '├────────────────────────────────────────────────────────┤',
             / '│ Performance:                                           │',
             / '│   Duration:       ', ls_result-duration, ' seconds',
             / '│   Errors:         ', ls_result-error_count,
             / '│   Warnings:       ', ls_result-warning_count,
             / '└────────────────────────────────────────────────────────┘', /.

      " Show monitoring dashboard
      IF p_debug = 'X'.
        PERFORM show_debug_info( ls_result ).
      ENDIF.

    CATCH cx_root INTO DATA(lx_error).
      WRITE: / '✗ ERROR:', lx_error->get_text( ),
             / '  Details:', lx_error->get_longtext( ).
  ENDTRY.
ENDFORM.

*&---------------------------------------------------------------------*
*& Form display_results
*&---------------------------------------------------------------------*
FORM display_results.
  DATA: ls_result TYPE zcl_etl_orchestrator=>ty_etl_result,
        lo_monitor TYPE REF TO zcl_etl_monitor,
        ls_dashboard TYPE zcl_etl_monitor=>ty_dashboard_data.

  " Import result from memory
  IMPORT ls_result FROM MEMORY ID 'ETL_RESULT'.
  IF sy-subrc <> 0.
    RETURN.
  ENDIF.

  " Get monitoring data
  lo_monitor = zcl_etl_monitor=>get_instance( ).
  ls_dashboard = lo_monitor->get_dashboard_data( iv_days_back = 7 ).

  WRITE: / '╔════════════════════════════════════════════════════════════╗',
         / '║          7-Day Dashboard Summary                           ║',
         / '╚════════════════════════════════════════════════════════════╝', /.

  WRITE: / '  Total Runs:        ', ls_dashboard-total_runs,
         / '  Successful:        ', ls_dashboard-successful_runs,
         / '  Failed:            ', ls_dashboard-failed_runs,
         / '  Running:           ', ls_dashboard-running_jobs,
         / '  Avg Duration:      ', ls_dashboard-avg_duration, ' sec',
         / '  Total Records:     ', ls_dashboard-total_records,
         / '  Error Rate:        ', ls_dashboard-error_rate, '%', /.

  " Health check
  DATA(lv_health) = lo_monitor->check_health( ).
  WRITE: / '  System Health:     ', lv_health, /.

  " Show recent errors
  DATA(lt_errors) = lo_monitor->get_error_summary( iv_days_back = 1 ).
  IF lt_errors IS NOT INITIAL.
    WRITE: / '  ⚠ Recent Errors:', /.
    LOOP AT lt_errors INTO DATA(ls_error).
      WRITE: / '    -', ls_error-error_message.
    ENDLOOP.
  ENDIF.

  WRITE: / '═══════════════════════════════════════════════════════════', /.
ENDFORM.

*&---------------------------------------------------------------------*
*& Form show_debug_info
*&---------------------------------------------------------------------*
FORM show_debug_info USING ps_result TYPE zcl_etl_orchestrator=>ty_etl_result.
  DATA: lo_logger TYPE REF TO zcl_etl_logger,
        lt_logs   TYPE zcl_etl_logger=>tt_log_entries.

  WRITE: / '╔════════════════════════════════════════════════════════════╗',
         / '║          Debug Information                                 ║',
         / '╚════════════════════════════════════════════════════════════╝', /.

  " Show detailed logs
  lo_logger = zcl_etl_logger=>get_instance( ).
  lt_logs = lo_logger->get_logs( ).

  WRITE: / '  Log Entries:', /.
  LOOP AT lt_logs INTO DATA(ls_log).
    WRITE: / '  [', ls_log-level+0(4), ']',
              ls_log-component+0(15),
              '-', ls_log-message.
  ENDLOOP.

  WRITE: / '═══════════════════════════════════════════════════════════', /.
ENDFORM.
