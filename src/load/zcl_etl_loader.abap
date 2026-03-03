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
        iv_batch_size  TYPE i DEFAULT 1000.

    METHODS load_data
      IMPORTING
        it_data             TYPE zcl_etl_transformer=>tt_transformed_data
      RETURNING
        VALUE(rs_result)    TYPE ty_load_result.

    METHODS load_to_database
      IMPORTING
        it_data             TYPE zcl_etl_transformer=>tt_transformed_data
      RETURNING
        VALUE(rv_success)   TYPE abap_bool.

    METHODS load_to_file
      IMPORTING
        it_data             TYPE zcl_etl_transformer=>tt_transformed_data
        iv_filepath         TYPE string
      RETURNING
        VALUE(rv_success)   TYPE abap_bool.

  PROTECTED SECTION.
  PRIVATE SECTION.
    DATA: mv_target_type TYPE string,
          mv_batch_size  TYPE i.

    METHODS commit_batch
      IMPORTING
        it_batch            TYPE zcl_etl_transformer=>tt_transformed_data
      RETURNING
        VALUE(rv_success)   TYPE abap_bool.
ENDCLASS.

CLASS zcl_etl_loader IMPLEMENTATION.

  METHOD constructor.
    mv_target_type = COND #( WHEN iv_target_type IS NOT INITIAL
                             THEN iv_target_type
                             ELSE 'DATABASE' ).
    mv_batch_size = iv_batch_size.
  ENDMETHOD.

  METHOD load_data.
    DATA: lv_success_count TYPE i,
          lv_error_count   TYPE i,
          lt_errors        TYPE string_table,
          lt_batch         TYPE zcl_etl_transformer=>tt_transformed_data.

    " Process data in batches
    LOOP AT it_data INTO DATA(ls_data).
      APPEND ls_data TO lt_batch.

      " When batch size is reached, commit
      IF lines( lt_batch ) >= mv_batch_size.
        IF commit_batch( lt_batch ) = abap_true.
          lv_success_count = lv_success_count + lines( lt_batch ).
        ELSE.
          lv_error_count = lv_error_count + lines( lt_batch ).
          APPEND |Batch failed at row { sy-tabix }| TO lt_errors.
        ENDIF.
        CLEAR lt_batch.
      ENDIF.
    ENDLOOP.

    " Process remaining records
    IF lt_batch IS NOT INITIAL.
      IF commit_batch( lt_batch ) = abap_true.
        lv_success_count = lv_success_count + lines( lt_batch ).
      ELSE.
        lv_error_count = lv_error_count + lines( lt_batch ).
        APPEND 'Final batch failed' TO lt_errors.
      ENDIF.
    ENDIF.

    " Return results
    rs_result = VALUE #(
      success_count = lv_success_count
      error_count   = lv_error_count
      total_count   = lines( it_data )
      errors        = lt_errors
    ).
  ENDMETHOD.

  METHOD load_to_database.
    " Load data to database table
    TRY.
        " INSERT target_table FROM TABLE @it_data.
        " For demo purposes, just return success
        rv_success = abap_true.

        " In real scenario:
        " MODIFY target_table FROM TABLE @it_data.
        " IF sy-subrc = 0.
        "   COMMIT WORK.
        "   rv_success = abap_true.
        " ELSE.
        "   ROLLBACK WORK.
        "   rv_success = abap_false.
        " ENDIF.

      CATCH cx_root INTO DATA(lx_error).
        WRITE: / 'Database load error:', lx_error->get_text( ).
        rv_success = abap_false.
    ENDTRY.
  ENDMETHOD.

  METHOD load_to_file.
    " Export data to file
    TRY.
        " File writing logic would go here
        " For demo purposes
        rv_success = abap_true.
      CATCH cx_root.
        rv_success = abap_false.
    ENDTRY.
  ENDMETHOD.

  METHOD commit_batch.
    " Commit a batch of records
    CASE mv_target_type.
      WHEN 'DATABASE'.
        rv_success = load_to_database( it_batch ).
      WHEN 'FILE'.
        rv_success = load_to_file(
          it_data    = it_batch
          iv_filepath = '/tmp/output.txt'
        ).
      WHEN OTHERS.
        rv_success = abap_false.
    ENDCASE.
  ENDMETHOD.

ENDCLASS.
