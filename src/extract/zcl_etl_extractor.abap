CLASS zcl_etl_extractor DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    TYPES:
      BEGIN OF ty_source_data,
        id            TYPE char10,
        name          TYPE char100,
        value         TYPE dec15_2,
        status        TYPE char20,
        category      TYPE char50,
        source_system TYPE char20,
        created_at    TYPE timestampl,
        created_by    TYPE syuname,
        changed_at    TYPE timestampl,
        changed_by    TYPE syuname,
      END OF ty_source_data,
      tt_source_data TYPE STANDARD TABLE OF ty_source_data WITH DEFAULT KEY.

    METHODS constructor
      IMPORTING
        iv_source_type TYPE string OPTIONAL
        iv_run_id      TYPE char20.

    METHODS extract_data
      IMPORTING
        iv_filter      TYPE string OPTIONAL
        iv_max_records TYPE i DEFAULT 0
      RETURNING
        VALUE(rt_data) TYPE tt_source_data
      RAISING
        cx_sy_open_sql_db.

    METHODS extract_from_database
      IMPORTING
        iv_filter      TYPE string OPTIONAL
      RETURNING
        VALUE(rt_data) TYPE tt_source_data.

    METHODS extract_from_staging
      IMPORTING
        iv_run_id      TYPE char20
      RETURNING
        VALUE(rt_data) TYPE tt_source_data.

    METHODS extract_incremental
      IMPORTING
        iv_last_run_time TYPE timestampl
      RETURNING
        VALUE(rt_data)   TYPE tt_source_data.

  PROTECTED SECTION.
  PRIVATE SECTION.
    DATA: mv_source_type TYPE string,
          mv_run_id      TYPE char20,
          mo_logger      TYPE REF TO zcl_etl_logger.
ENDCLASS.

CLASS zcl_etl_extractor IMPLEMENTATION.

  METHOD constructor.
    mv_source_type = COND #( WHEN iv_source_type IS NOT INITIAL
                             THEN iv_source_type
                             ELSE 'DATABASE' ).
    mv_run_id = iv_run_id.
    mo_logger = zcl_etl_logger=>get_instance( ).
  ENDMETHOD.

  METHOD extract_data.
    DATA: lt_data TYPE tt_source_data.

    mo_logger->log_info(
      iv_component = 'EXTRACTOR'
      iv_message   = |Starting extraction - Source: { mv_source_type }|
    ).

    TRY.
        CASE mv_source_type.
          WHEN 'DATABASE'.
            lt_data = extract_from_database( iv_filter ).

          WHEN 'STAGING'.
            lt_data = extract_from_staging( mv_run_id ).

          WHEN 'INCREMENTAL'.
            " Get last successful run time
            SELECT SINGLE end_time FROM zetl_run_log
              WHERE status = 'SUCCESS'
              ORDER BY end_time DESCENDING
              INTO @DATA(lv_last_run).
            IF sy-subrc = 0.
              lt_data = extract_incremental( lv_last_run ).
            ELSE.
              lt_data = extract_from_database( iv_filter ).
            ENDIF.

          WHEN OTHERS.
            lt_data = extract_from_database( iv_filter ).
        ENDCASE.

        " Apply max records limit if specified
        IF iv_max_records > 0 AND lines( lt_data ) > iv_max_records.
          DELETE lt_data FROM iv_max_records + 1.
        ENDIF.

        mo_logger->log_info(
          iv_component = 'EXTRACTOR'
          iv_message   = |Extracted { lines( lt_data ) } records|
        ).

        rt_data = lt_data.

      CATCH cx_root INTO DATA(lx_error).
        mo_logger->log_error(
          iv_component = 'EXTRACTOR'
          iv_message   = 'Extraction failed'
          iv_details   = lx_error->get_text( )
        ).
        RAISE EXCEPTION lx_error.
    ENDTRY.
  ENDMETHOD.

  METHOD extract_from_database.
    DATA: lt_data       TYPE tt_source_data,
          lt_source_tab TYPE STANDARD TABLE OF zetl_source_data.

    " Extract from source table
    IF iv_filter IS INITIAL.
      SELECT * FROM zetl_source_data
        INTO CORRESPONDING FIELDS OF TABLE @lt_source_tab
        UP TO 1000 ROWS.
    ELSE.
      " Parse filter and apply
      SELECT * FROM zetl_source_data
        WHERE status = @iv_filter
        INTO CORRESPONDING FIELDS OF TABLE @lt_source_tab
        UP TO 1000 ROWS.
    ENDIF.

    " Convert to internal format
    LOOP AT lt_source_tab INTO DATA(ls_source).
      APPEND VALUE #(
        id            = ls_source-id
        name          = ls_source-name
        value         = ls_source-value
        status        = ls_source-status
        category      = ls_source-category
        source_system = ls_source-source_system
        created_at    = ls_source-created_at
        created_by    = ls_source-created_by
        changed_at    = ls_source-changed_at
        changed_by    = ls_source-changed_by
      ) TO lt_data.
    ENDLOOP.

    rt_data = lt_data.
  ENDMETHOD.

  METHOD extract_from_staging.
    DATA: lt_data        TYPE tt_source_data,
          lt_staging_tab TYPE STANDARD TABLE OF zetl_staging.

    SELECT * FROM zetl_staging
      WHERE run_id = @iv_run_id
        AND status = 'READY'
      INTO CORRESPONDING FIELDS OF TABLE @lt_staging_tab.

    " Parse staged data
    LOOP AT lt_staging_tab INTO DATA(ls_staging).
      " In real scenario, parse the raw_data/parsed_data
      " For now, create sample record
      APPEND VALUE #(
        id     = ls_staging-id
        status = 'STAGED'
      ) TO lt_data.
    ENDLOOP.

    rt_data = lt_data.
  ENDMETHOD.

  METHOD extract_incremental.
    DATA: lt_data       TYPE tt_source_data,
          lt_source_tab TYPE STANDARD TABLE OF zetl_source_data.

    " Extract only changed records since last run
    SELECT * FROM zetl_source_data
      WHERE changed_at > @iv_last_run_time
      INTO CORRESPONDING FIELDS OF TABLE @lt_source_tab.

    " Convert to internal format
    LOOP AT lt_source_tab INTO DATA(ls_source).
      APPEND VALUE #(
        id            = ls_source-id
        name          = ls_source-name
        value         = ls_source-value
        status        = ls_source-status
        category      = ls_source-category
        source_system = ls_source-source_system
        created_at    = ls_source-created_at
        created_by    = ls_source-created_by
        changed_at    = ls_source-changed_at
        changed_by    = ls_source-changed_by
      ) TO lt_data.
    ENDLOOP.

    mo_logger->log_info(
      iv_component = 'EXTRACTOR'
      iv_message   = |Incremental extraction: { lines( lt_data ) } changed records|
    ).

    rt_data = lt_data.
  ENDMETHOD.

ENDCLASS.
