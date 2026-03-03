CLASS zcl_etl_logger DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    TYPES:
      BEGIN OF ty_log_entry,
        timestamp   TYPE timestamp,
        level       TYPE string,
        component   TYPE string,
        message     TYPE string,
        details     TYPE string,
      END OF ty_log_entry,
      tt_log_entries TYPE STANDARD TABLE OF ty_log_entry WITH DEFAULT KEY.

    CLASS-METHODS get_instance
      RETURNING
        VALUE(ro_logger) TYPE REF TO zcl_etl_logger.

    METHODS log_info
      IMPORTING
        iv_component TYPE string
        iv_message   TYPE string
        iv_details   TYPE string OPTIONAL.

    METHODS log_error
      IMPORTING
        iv_component TYPE string
        iv_message   TYPE string
        iv_details   TYPE string OPTIONAL.

    METHODS log_warning
      IMPORTING
        iv_component TYPE string
        iv_message   TYPE string
        iv_details   TYPE string OPTIONAL.

    METHODS get_logs
      RETURNING
        VALUE(rt_logs) TYPE tt_log_entries.

    METHODS clear_logs.

  PROTECTED SECTION.
  PRIVATE SECTION.
    CLASS-DATA go_instance TYPE REF TO zcl_etl_logger.
    DATA mt_logs TYPE tt_log_entries.

    METHODS add_log_entry
      IMPORTING
        iv_level     TYPE string
        iv_component TYPE string
        iv_message   TYPE string
        iv_details   TYPE string OPTIONAL.
ENDCLASS.

CLASS zcl_etl_logger IMPLEMENTATION.

  METHOD get_instance.
    IF go_instance IS NOT BOUND.
      go_instance = NEW zcl_etl_logger( ).
    ENDIF.
    ro_logger = go_instance.
  ENDMETHOD.

  METHOD log_info.
    add_log_entry(
      iv_level     = 'INFO'
      iv_component = iv_component
      iv_message   = iv_message
      iv_details   = iv_details
    ).
  ENDMETHOD.

  METHOD log_error.
    add_log_entry(
      iv_level     = 'ERROR'
      iv_component = iv_component
      iv_message   = iv_message
      iv_details   = iv_details
    ).
  ENDMETHOD.

  METHOD log_warning.
    add_log_entry(
      iv_level     = 'WARNING'
      iv_component = iv_component
      iv_message   = iv_message
      iv_details   = iv_details
    ).
  ENDMETHOD.

  METHOD add_log_entry.
    DATA(ls_log) = VALUE ty_log_entry(
      timestamp = cl_abap_tstmp=>utclong2tstmp( utclong_current( ) )
      level     = iv_level
      component = iv_component
      message   = iv_message
      details   = iv_details
    ).

    APPEND ls_log TO mt_logs.

    " Also output to console
    WRITE: / |[{ ls_log-timestamp }] { ls_log-level }: { ls_log-component } - { ls_log-message }|.
    IF ls_log-details IS NOT INITIAL.
      WRITE: / |  Details: { ls_log-details }|.
    ENDIF.
  ENDMETHOD.

  METHOD get_logs.
    rt_logs = mt_logs.
  ENDMETHOD.

  METHOD clear_logs.
    CLEAR mt_logs.
  ENDMETHOD.

ENDCLASS.
