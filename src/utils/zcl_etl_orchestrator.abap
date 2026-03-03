CLASS zcl_etl_orchestrator DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    TYPES:
      BEGIN OF ty_etl_result,
        run_id            TYPE char20,
        status            TYPE char20,
        start_time        TYPE timestampl,
        end_time          TYPE timestampl,
        duration          TYPE i,
        records_extracted TYPE i,
        records_transformed TYPE i,
        records_loaded    TYPE i,
        records_failed    TYPE i,
        error_count       TYPE i,
        warning_count     TYPE i,
      END OF ty_etl_result.

    METHODS constructor
      IMPORTING
        iv_run_type TYPE char20 DEFAULT 'MANUAL'.

    METHODS execute_etl
      IMPORTING
        iv_source_type  TYPE string DEFAULT 'DATABASE'
        iv_target_type  TYPE string DEFAULT 'DATABASE'
        iv_filter       TYPE string OPTIONAL
        iv_batch_size   TYPE i DEFAULT 1000
        iv_max_records  TYPE i DEFAULT 0
      RETURNING
        VALUE(rs_result) TYPE ty_etl_result.

    METHODS schedule_etl
      IMPORTING
        iv_schedule_id TYPE char20
      RETURNING
        VALUE(rv_success) TYPE abap_bool.

    METHODS get_run_statistics
      IMPORTING
        iv_run_id TYPE char20 OPTIONAL
      RETURNING
        VALUE(rt_stats) TYPE STANDARD TABLE OF zetl_run_log.

  PROTECTED SECTION.
  PRIVATE SECTION.
    DATA: mv_run_id   TYPE char20,
          mv_run_type TYPE char20,
          mo_logger   TYPE REF TO zcl_etl_logger.

    METHODS generate_run_id
      RETURNING
        VALUE(rv_run_id) TYPE char20.

    METHODS log_run_start
      RETURNING
        VALUE(rv_timestamp) TYPE timestampl.

    METHODS log_run_end
      IMPORTING
        iv_start_time       TYPE timestampl
        iv_status           TYPE char20
        iv_records_extracted TYPE i
        iv_records_transformed TYPE i
        iv_records_loaded   TYPE i
        iv_records_failed   TYPE i.

    METHODS handle_orchestration_error
      IMPORTING
        ix_error TYPE REF TO cx_root.
ENDCLASS.

CLASS zcl_etl_orchestrator IMPLEMENTATION.

  METHOD constructor.
    mv_run_id = generate_run_id( ).
    mv_run_type = iv_run_type.
    mo_logger = zcl_etl_logger=>get_instance( ).
  ENDMETHOD.

  METHOD execute_etl.
    DATA: lv_start_time TYPE timestampl,
          lv_end_time   TYPE timestampl,
          lv_duration   TYPE i.

    " Initialize result
    rs_result-run_id = mv_run_id.
    rs_result-status = 'RUNNING'.

    mo_logger->log_info(
      iv_component = 'ORCHESTRATOR'
      iv_message   = |ETL execution started - Run ID: { mv_run_id }|
    ).

    lv_start_time = log_run_start( ).

    TRY.
        " Step 1: Extract
        DATA(lo_extractor) = NEW zcl_etl_extractor(
          iv_source_type = iv_source_type
          iv_run_id      = mv_run_id
        ).

        DATA(lt_source_data) = lo_extractor->extract_data(
          iv_filter      = iv_filter
          iv_max_records = iv_max_records
        ).

        rs_result-records_extracted = lines( lt_source_data ).

        IF lt_source_data IS INITIAL.
          mo_logger->log_warning(
            iv_component = 'ORCHESTRATOR'
            iv_message   = 'No data extracted - ETL process stopping'
          ).
          rs_result-status = 'NO_DATA'.
          RETURN.
        ENDIF.

        " Step 2: Transform
        DATA(lo_transformer) = NEW zcl_etl_transformer( iv_run_id = mv_run_id ).
        DATA(lt_transformed_data) = lo_transformer->transform_data( lt_source_data ).

        " Validate
        DATA(lv_valid) = lo_transformer->validate_data(
          EXPORTING
            it_data              = lt_transformed_data
          IMPORTING
            et_validation_errors = DATA(lt_errors)
        ).

        IF lv_valid = abap_false.
          mo_logger->log_error(
            iv_component = 'ORCHESTRATOR'
            iv_message   = |Validation failed - { lines( lt_errors ) } errors|
          ).
          rs_result-status = 'VALIDATION_FAILED'.
          rs_result-error_count = lines( lt_errors ).
          RETURN.
        ENDIF.

        rs_result-records_transformed = lines( lt_transformed_data ).

        " Step 3: Load
        DATA(lo_loader) = NEW zcl_etl_loader(
          iv_target_type = iv_target_type
          iv_batch_size  = iv_batch_size
          iv_run_id      = mv_run_id
        ).

        DATA(ls_load_result) = lo_loader->load_data(
          it_data = lt_transformed_data
          iv_mode = 'UPSERT'
        ).

        rs_result-records_loaded = ls_load_result-success_count.
        rs_result-records_failed = ls_load_result-error_count.
        rs_result-error_count    = ls_load_result-error_count.

        " Determine final status
        IF ls_load_result-error_count = 0.
          rs_result-status = 'SUCCESS'.
        ELSEIF ls_load_result-success_count > 0.
          rs_result-status = 'PARTIAL_SUCCESS'.
        ELSE.
          rs_result-status = 'FAILED'.
        ENDIF.

        GET TIME STAMP FIELD lv_end_time.
        rs_result-end_time = lv_end_time.
        rs_result-duration = cl_abap_tstmp=>subtract(
          tstmp1 = lv_end_time
          tstmp2 = lv_start_time
        ).

        " Log completion
        log_run_end(
          iv_start_time          = lv_start_time
          iv_status              = rs_result-status
          iv_records_extracted   = rs_result-records_extracted
          iv_records_transformed = rs_result-records_transformed
          iv_records_loaded      = rs_result-records_loaded
          iv_records_failed      = rs_result-records_failed
        ).

        mo_logger->log_info(
          iv_component = 'ORCHESTRATOR'
          iv_message   = |ETL execution completed - Status: { rs_result-status }|
        ).

      CATCH cx_root INTO DATA(lx_error).
        handle_orchestration_error( lx_error ).
        rs_result-status = 'ERROR'.
        GET TIME STAMP FIELD lv_end_time.
        rs_result-end_time = lv_end_time.

        log_run_end(
          iv_start_time          = lv_start_time
          iv_status              = 'ERROR'
          iv_records_extracted   = rs_result-records_extracted
          iv_records_transformed = rs_result-records_transformed
          iv_records_loaded      = rs_result-records_loaded
          iv_records_failed      = rs_result-records_failed
        ).
    ENDTRY.
  ENDMETHOD.

  METHOD schedule_etl.
    DATA: ls_schedule TYPE zetl_schedule.

    " Load schedule configuration
    SELECT SINGLE * FROM zetl_schedule
      WHERE schedule_id = @iv_schedule_id
        AND is_active = 'X'
      INTO @ls_schedule.

    IF sy-subrc <> 0.
      mo_logger->log_error(
        iv_component = 'ORCHESTRATOR'
        iv_message   = |Schedule { iv_schedule_id } not found or inactive|
      ).
      rv_success = abap_false.
      RETURN.
    ENDIF.

    " Submit background job
    TRY.
        DATA: lv_jobname  TYPE btcjob,
              lv_jobcount TYPE btcjobcnt.

        lv_jobname = |ETL_{ ls_schedule-schedule_name }|.

        CALL FUNCTION 'JOB_OPEN'
          EXPORTING
            jobname          = lv_jobname
          IMPORTING
            jobcount         = lv_jobcount
          EXCEPTIONS
            cant_create_job  = 1
            invalid_job_data = 2
            jobname_missing  = 3
            OTHERS           = 4.

        IF sy-subrc = 0.
          SUBMIT z_etl_main
            VIA JOB lv_jobname NUMBER lv_jobcount
            AND RETURN.

          CALL FUNCTION 'JOB_CLOSE'
            EXPORTING
              jobcount             = lv_jobcount
              jobname              = lv_jobname
              strtimmed            = 'X'
            EXCEPTIONS
              cant_start_immediate = 1
              invalid_startdate    = 2
              jobname_missing      = 3
              job_close_failed     = 4
              job_nosteps          = 5
              OTHERS               = 6.

          IF sy-subrc = 0.
            rv_success = abap_true.
            mo_logger->log_info(
              iv_component = 'ORCHESTRATOR'
              iv_message   = |Job scheduled: { lv_jobname }|
            ).
          ELSE.
            rv_success = abap_false.
          ENDIF.
        ENDIF.

      CATCH cx_root INTO DATA(lx_error).
        mo_logger->log_error(
          iv_component = 'ORCHESTRATOR'
          iv_message   = 'Job scheduling failed'
          iv_details   = lx_error->get_text( )
        ).
        rv_success = abap_false.
    ENDTRY.
  ENDMETHOD.

  METHOD get_run_statistics.
    IF iv_run_id IS NOT INITIAL.
      SELECT * FROM zetl_run_log
        WHERE run_id = @iv_run_id
        INTO TABLE @rt_stats.
    ELSE.
      SELECT * FROM zetl_run_log
        ORDER BY start_time DESCENDING
        INTO TABLE @rt_stats
        UP TO 100 ROWS.
    ENDIF.
  ENDMETHOD.

  METHOD generate_run_id.
    DATA: lv_timestamp TYPE timestampl.

    GET TIME STAMP FIELD lv_timestamp.
    rv_run_id = |RUN{ lv_timestamp }|.
    CONDENSE rv_run_id NO-GAPS.
  ENDMETHOD.

  METHOD log_run_start.
    GET TIME STAMP FIELD rv_timestamp.

    INSERT zetl_run_log FROM @( VALUE #(
      client      = sy-mandt
      run_id      = mv_run_id
      run_type    = mv_run_type
      status      = 'RUNNING'
      start_time  = rv_timestamp
      started_by  = sy-uname
    ) ).

    COMMIT WORK.
  ENDMETHOD.

  METHOD log_run_end.
    DATA: lv_end_time TYPE timestampl,
          lv_duration TYPE i.

    GET TIME STAMP FIELD lv_end_time.

    lv_duration = cl_abap_tstmp=>subtract(
      tstmp1 = lv_end_time
      tstmp2 = iv_start_time
    ).

    UPDATE zetl_run_log
      SET status              = @iv_status
          end_time            = @lv_end_time
          duration            = @lv_duration
          records_extracted   = @iv_records_extracted
          records_transformed = @iv_records_transformed
          records_loaded      = @iv_records_loaded
          records_failed      = @iv_records_failed
      WHERE run_id = @mv_run_id.

    COMMIT WORK.
  ENDMETHOD.

  METHOD handle_orchestration_error.
    DATA: lv_error_id   TYPE char20,
          lv_timestamp  TYPE timestampl.

    GET TIME STAMP FIELD lv_timestamp.
    lv_error_id = |ORCERR{ lv_timestamp }|.

    INSERT zetl_error_log FROM @( VALUE #(
      client        = sy-mandt
      error_id      = lv_error_id
      run_id        = mv_run_id
      error_type    = 'ORCHESTRATION'
      severity      = 'CRITICAL'
      component     = 'ORCHESTRATOR'
      error_message = ix_error->get_text( )
      error_details = ix_error->get_longtext( )
      created_at    = lv_timestamp
      resolved      = ''
    ) ).

    COMMIT WORK.

    mo_logger->log_error(
      iv_component = 'ORCHESTRATOR'
      iv_message   = 'Orchestration error'
      iv_details   = ix_error->get_text( )
    ).
  ENDMETHOD.

ENDCLASS.
