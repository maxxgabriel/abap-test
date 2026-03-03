CLASS zcl_etl_loader DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    TYPES:
      BEGIN OF ty_load_result,
        success_count TYPE i,
        error_count   TYPE i,
        total_count   TYPE i,
        errors        TYPE string_table,
      END OF ty_load_result.

    METHODS constructor
      IMPORTING
        iv_target_type TYPE string OPTIONAL
        iv_batch_size  TYPE i DEFAULT 1000
        iv_run_id      TYPE char20.

    METHODS load_data
      IMPORTING
        it_data             TYPE zcl_etl_transformer=>tt_transformed_data
        iv_mode             TYPE char10 DEFAULT 'INSERT'
      RETURNING
        VALUE(rs_result)    TYPE ty_load_result.

    METHODS load_to_database
      IMPORTING
        it_data             TYPE zcl_etl_transformer=>tt_transformed_data
        iv_mode             TYPE char10
      RETURNING
        VALUE(rv_success)   TYPE abap_bool.

    METHODS reconcile_data
      IMPORTING
        it_loaded_data      TYPE zcl_etl_transformer=>tt_transformed_data
      RETURNING
        VALUE(rv_matches)   TYPE abap_bool.

  PROTECTED SECTION.
  PRIVATE SECTION.
    DATA: mv_target_type TYPE string,
          mv_batch_size  TYPE i,
          mv_run_id      TYPE char20,
          mo_logger      TYPE REF TO zcl_etl_logger.

    METHODS commit_batch
      IMPORTING
        it_batch            TYPE zcl_etl_transformer=>tt_transformed_data
        iv_mode             TYPE char10
      RETURNING
        VALUE(rv_success)   TYPE abap_bool.

    METHODS handle_load_error
      IMPORTING
        iv_record_id   TYPE char10
        ix_error       TYPE REF TO cx_root.

    METHODS update_existing
      IMPORTING
        it_data            TYPE zcl_etl_transformer=>tt_transformed_data
      RETURNING
        VALUE(rv_success)  TYPE abap_bool.

    METHODS insert_new
      IMPORTING
        it_data            TYPE zcl_etl_transformer=>tt_transformed_data
      RETURNING
        VALUE(rv_success)  TYPE abap_bool.
ENDCLASS.

CLASS zcl_etl_loader IMPLEMENTATION.

  METHOD constructor.
    mv_target_type = COND #( WHEN iv_target_type IS NOT INITIAL
                             THEN iv_target_type
                             ELSE 'DATABASE' ).
    mv_batch_size = iv_batch_size.
    mv_run_id = iv_run_id.
    mo_logger = zcl_etl_logger=>get_instance( ).
  ENDMETHOD.

  METHOD load_data.
    DATA: lv_success_count TYPE i,
          lv_error_count   TYPE i,
          lt_errors        TYPE string_table,
          lt_batch         TYPE zcl_etl_transformer=>tt_transformed_data.

    mo_logger->log_info(
      iv_component = 'LOADER'
      iv_message   = |Starting load - Mode: { iv_mode }, Batch size: { mv_batch_size }|
    ).

    " Process data in batches
    LOOP AT it_data INTO DATA(ls_data).
      APPEND ls_data TO lt_batch.

      " When batch size is reached, commit
      IF lines( lt_batch ) >= mv_batch_size OR sy-tabix = lines( it_data ).
        IF commit_batch(
             it_batch = lt_batch
             iv_mode  = iv_mode
           ) = abap_true.
          lv_success_count = lv_success_count + lines( lt_batch ).
        ELSE.
          lv_error_count = lv_error_count + lines( lt_batch ).
          APPEND |Batch failed at row { sy-tabix }| TO lt_errors.
        ENDIF.
        CLEAR lt_batch.
      ENDIF.
    ENDLOOP.

    " Reconcile loaded data
    DATA(lv_reconciled) = reconcile_data( it_data ).
    IF lv_reconciled = abap_false.
      mo_logger->log_warning(
        iv_component = 'LOADER'
        iv_message   = 'Data reconciliation failed'
      ).
    ENDIF.

    " Return results
    rs_result = VALUE #(
      success_count = lv_success_count
      error_count   = lv_error_count
      total_count   = lines( it_data )
      errors        = lt_errors
    ).

    mo_logger->log_info(
      iv_component = 'LOADER'
      iv_message   = |Load complete - Success: { lv_success_count }, Errors: { lv_error_count }|
    ).
  ENDMETHOD.

  METHOD load_to_database.
    TRY.
        CASE iv_mode.
          WHEN 'INSERT'.
            rv_success = insert_new( it_data ).

          WHEN 'UPDATE'.
            rv_success = update_existing( it_data ).

          WHEN 'UPSERT'.
            " Try update first, then insert
            rv_success = update_existing( it_data ).
            IF rv_success = abap_false.
              rv_success = insert_new( it_data ).
            ENDIF.

          WHEN OTHERS.
            rv_success = insert_new( it_data ).
        ENDCASE.

      CATCH cx_root INTO DATA(lx_error).
        mo_logger->log_error(
          iv_component = 'LOADER'
          iv_message   = 'Database load error'
          iv_details   = lx_error->get_text( )
        ).
        rv_success = abap_false.
    ENDTRY.
  ENDMETHOD.

  METHOD commit_batch.
    rv_success = load_to_database(
      it_data = it_batch
      iv_mode = iv_mode
    ).
  ENDMETHOD.

  METHOD insert_new.
    DATA: lt_target_tab TYPE STANDARD TABLE OF zetl_target_data.

    " Convert to database format
    LOOP AT it_data INTO DATA(ls_data).
      APPEND VALUE #(
        client            = sy-mandt
        id                = ls_data-id
        name              = ls_data-name
        value             = ls_data-value
        transformed_value = ls_data-transformed_value
        status            = ls_data-status
        category          = ls_data-category
        priority          = ls_data-priority
        etl_run_id        = ls_data-etl_run_id
        processed_at      = ls_data-processed_at
        processed_by      = ls_data-processed_by
      ) TO lt_target_tab.
    ENDLOOP.

    " Insert into target table
    TRY.
        INSERT zetl_target_data FROM TABLE lt_target_tab.
        IF sy-subrc = 0.
          COMMIT WORK.
          rv_success = abap_true.
        ELSE.
          ROLLBACK WORK.
          rv_success = abap_false.
        ENDIF.
      CATCH cx_root INTO DATA(lx_error).
        ROLLBACK WORK.
        handle_load_error(
          iv_record_id = 'BATCH'
          ix_error     = lx_error
        ).
        rv_success = abap_false.
    ENDTRY.
  ENDMETHOD.

  METHOD update_existing.
    TRY.
        LOOP AT it_data INTO DATA(ls_data).
          UPDATE zetl_target_data
            SET name              = @ls_data-name
                value             = @ls_data-value
                transformed_value = @ls_data-transformed_value
                status            = @ls_data-status
                category          = @ls_data-category
                priority          = @ls_data-priority
                etl_run_id        = @ls_data-etl_run_id
                processed_at      = @ls_data-processed_at
                processed_by      = @ls_data-processed_by
            WHERE id = @ls_data-id.

          IF sy-subrc <> 0.
            handle_load_error(
              iv_record_id = ls_data-id
              ix_error     = NEW cx_sy_sql_error( )
            ).
          ENDIF.
        ENDLOOP.

        COMMIT WORK.
        rv_success = abap_true.

      CATCH cx_root INTO DATA(lx_error).
        ROLLBACK WORK.
        handle_load_error(
          iv_record_id = 'UPDATE_BATCH'
          ix_error     = lx_error
        ).
        rv_success = abap_false.
    ENDTRY.
  ENDMETHOD.

  METHOD reconcile_data.
    DATA: lv_loaded_count TYPE i,
          lv_source_count TYPE i.

    " Count records in target table for this run
    SELECT COUNT( * ) FROM zetl_target_data
      WHERE etl_run_id = @mv_run_id
      INTO @lv_loaded_count.

    lv_source_count = lines( it_loaded_data ).

    IF lv_loaded_count = lv_source_count.
      rv_matches = abap_true.
      mo_logger->log_info(
        iv_component = 'LOADER'
        iv_message   = |Reconciliation successful: { lv_loaded_count } records|
      ).
    ELSE.
      rv_matches = abap_false.
      mo_logger->log_error(
        iv_component = 'LOADER'
        iv_message   = |Reconciliation failed: Expected { lv_source_count }, Found { lv_loaded_count }|
      ).
    ENDIF.
  ENDMETHOD.

  METHOD handle_load_error.
    DATA: lv_error_id TYPE char20,
          lv_timestamp TYPE timestampl.

    GET TIME STAMP FIELD lv_timestamp.

    " Generate unique error ID
    lv_error_id = |ERR{ lv_timestamp }|.

    " Log to error table
    INSERT zetl_error_log FROM @( VALUE #(
      client        = sy-mandt
      error_id      = lv_error_id
      run_id        = mv_run_id
      error_type    = 'LOAD_ERROR'
      severity      = 'HIGH'
      component     = 'LOADER'
      record_id     = iv_record_id
      error_message = ix_error->get_text( )
      error_details = ix_error->get_longtext( )
      created_at    = lv_timestamp
      resolved      = ''
    ) ).

    COMMIT WORK.

    mo_logger->log_error(
      iv_component = 'LOADER'
      iv_message   = |Load error for record { iv_record_id }|
      iv_details   = ix_error->get_text( )
    ).
  ENDMETHOD.

ENDCLASS.
