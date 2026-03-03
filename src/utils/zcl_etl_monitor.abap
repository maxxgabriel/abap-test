CLASS zcl_etl_monitor DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    TYPES:
      BEGIN OF ty_dashboard_data,
        total_runs        TYPE i,
        successful_runs   TYPE i,
        failed_runs       TYPE i,
        running_jobs      TYPE i,
        avg_duration      TYPE i,
        total_records     TYPE i,
        error_rate        TYPE p DECIMALS 2,
        last_run_time     TYPE timestampl,
        last_run_status   TYPE char20,
      END OF ty_dashboard_data,

      BEGIN OF ty_performance_metric,
        run_id            TYPE char20,
        start_time        TYPE timestampl,
        duration          TYPE i,
        records_processed TYPE i,
        throughput        TYPE p DECIMALS 2,
        status            TYPE char20,
      END OF ty_performance_metric,
      tt_performance_metrics TYPE STANDARD TABLE OF ty_performance_metric WITH DEFAULT KEY.

    CLASS-METHODS get_instance
      RETURNING
        VALUE(ro_monitor) TYPE REF TO zcl_etl_monitor.

    METHODS get_dashboard_data
      IMPORTING
        iv_days_back      TYPE i DEFAULT 7
      RETURNING
        VALUE(rs_data)    TYPE ty_dashboard_data.

    METHODS get_performance_metrics
      IMPORTING
        iv_days_back      TYPE i DEFAULT 30
      RETURNING
        VALUE(rt_metrics) TYPE tt_performance_metrics.

    METHODS get_error_summary
      IMPORTING
        iv_days_back      TYPE i DEFAULT 7
      RETURNING
        VALUE(rt_errors)  TYPE STANDARD TABLE OF zetl_error_log.

    METHODS check_health
      RETURNING
        VALUE(rv_health_status) TYPE char20.

    METHODS send_alert
      IMPORTING
        iv_alert_type TYPE char20
        iv_message    TYPE string
        iv_severity   TYPE char10.

  PROTECTED SECTION.
  PRIVATE SECTION.
    CLASS-DATA go_instance TYPE REF TO zcl_etl_monitor.
    DATA mo_logger TYPE REF TO zcl_etl_logger.

    METHODS calculate_throughput
      IMPORTING
        iv_records  TYPE i
        iv_duration TYPE i
      RETURNING
        VALUE(rv_throughput) TYPE p.
ENDCLASS.

CLASS zcl_etl_monitor IMPLEMENTATION.

  METHOD get_instance.
    IF go_instance IS NOT BOUND.
      go_instance = NEW zcl_etl_monitor( ).
      go_instance->mo_logger = zcl_etl_logger=>get_instance( ).
    ENDIF.
    ro_monitor = go_instance.
  ENDMETHOD.

  METHOD get_dashboard_data.
    DATA: lv_cutoff_time TYPE timestampl.

    " Calculate cutoff time
    GET TIME STAMP FIELD lv_cutoff_time.
    lv_cutoff_time = cl_abap_tstmp=>subtractsecs(
      tstmp = lv_cutoff_time
      secs  = iv_days_back * 86400
    ).

    " Get statistics
    SELECT
      COUNT( * ) AS total_runs,
      SUM( CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END ) AS successful,
      SUM( CASE WHEN status = 'FAILED' OR status = 'ERROR' THEN 1 ELSE 0 END ) AS failed,
      SUM( CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END ) AS running,
      AVG( duration ) AS avg_duration,
      SUM( records_loaded ) AS total_records
    FROM zetl_run_log
    WHERE start_time >= @lv_cutoff_time
    INTO (@rs_data-total_runs,
          @rs_data-successful_runs,
          @rs_data-failed_runs,
          @rs_data-running_jobs,
          @rs_data-avg_duration,
          @rs_data-total_records).

    " Calculate error rate
    IF rs_data-total_runs > 0.
      rs_data-error_rate = ( rs_data-failed_runs / rs_data-total_runs ) * 100.
    ENDIF.

    " Get last run info
    SELECT SINGLE end_time, status
      FROM zetl_run_log
      ORDER BY end_time DESCENDING
      INTO (@rs_data-last_run_time, @rs_data-last_run_status).
  ENDMETHOD.

  METHOD get_performance_metrics.
    DATA: lv_cutoff_time TYPE timestampl.

    GET TIME STAMP FIELD lv_cutoff_time.
    lv_cutoff_time = cl_abap_tstmp=>subtractsecs(
      tstmp = lv_cutoff_time
      secs  = iv_days_back * 86400
    ).

    SELECT
      run_id,
      start_time,
      duration,
      records_loaded AS records_processed,
      status
    FROM zetl_run_log
    WHERE start_time >= @lv_cutoff_time
    ORDER BY start_time DESCENDING
    INTO TABLE @DATA(lt_runs).

    LOOP AT lt_runs INTO DATA(ls_run).
      APPEND VALUE #(
        run_id            = ls_run-run_id
        start_time        = ls_run-start_time
        duration          = ls_run-duration
        records_processed = ls_run-records_processed
        throughput        = calculate_throughput(
                              iv_records  = ls_run-records_processed
                              iv_duration = ls_run-duration
                            )
        status            = ls_run-status
      ) TO rt_metrics.
    ENDLOOP.
  ENDMETHOD.

  METHOD get_error_summary.
    DATA: lv_cutoff_time TYPE timestampl.

    GET TIME STAMP FIELD lv_cutoff_time.
    lv_cutoff_time = cl_abap_tstmp=>subtractsecs(
      tstmp = lv_cutoff_time
      secs  = iv_days_back * 86400
    ).

    SELECT * FROM zetl_error_log
      WHERE created_at >= @lv_cutoff_time
        AND resolved = ''
      ORDER BY created_at DESCENDING
      INTO TABLE @rt_errors.
  ENDMETHOD.

  METHOD check_health.
    DATA: ls_dashboard TYPE ty_dashboard_data.

    ls_dashboard = get_dashboard_data( iv_days_back = 1 ).

    " Health check logic
    IF ls_dashboard-running_jobs > 5.
      rv_health_status = 'OVERLOADED'.
      send_alert(
        iv_alert_type = 'PERFORMANCE'
        iv_message    = |Too many running jobs: { ls_dashboard-running_jobs }|
        iv_severity   = 'HIGH'
      ).
    ELSEIF ls_dashboard-error_rate > 50.
      rv_health_status = 'CRITICAL'.
      send_alert(
        iv_alert_type = 'ERROR_RATE'
        iv_message    = |High error rate: { ls_dashboard-error_rate }%|
        iv_severity   = 'CRITICAL'
      ).
    ELSEIF ls_dashboard-error_rate > 20.
      rv_health_status = 'WARNING'.
      send_alert(
        iv_alert_type = 'ERROR_RATE'
        iv_message    = |Elevated error rate: { ls_dashboard-error_rate }%|
        iv_severity   = 'MEDIUM'
      ).
    ELSEIF ls_dashboard-failed_runs = 0 AND ls_dashboard-successful_runs > 0.
      rv_health_status = 'HEALTHY'.
    ELSE.
      rv_health_status = 'UNKNOWN'.
    ENDIF.
  ENDMETHOD.

  METHOD send_alert.
    " In real implementation, this would send emails, SMS, or system messages
    mo_logger->log_warning(
      iv_component = 'MONITOR'
      iv_message   = |ALERT [{ iv_severity }] { iv_alert_type }: { iv_message }|
    ).

    " Example: Send email via SAP standard function
    " CALL FUNCTION 'SO_NEW_DOCUMENT_SEND_API1'
    "   ...

    " Example: Create application log entry
    " CALL FUNCTION 'BAL_LOG_CREATE'
    "   ...
  ENDMETHOD.

  METHOD calculate_throughput.
    IF iv_duration > 0.
      rv_throughput = iv_records / iv_duration.  " records per second
    ELSE.
      rv_throughput = 0.
    ENDIF.
  ENDMETHOD.

ENDCLASS.
