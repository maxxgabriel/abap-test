CLASS zcl_etl_transformer DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    TYPES:
      BEGIN OF ty_transformed_data,
        id              TYPE string,
        name            TYPE string,
        value           TYPE string,
        transformed_val TYPE string,
        status          TYPE string,
        processed_at    TYPE timestamp,
      END OF ty_transformed_data,
      tt_transformed_data TYPE STANDARD TABLE OF ty_transformed_data WITH DEFAULT KEY.

    METHODS transform_data
      IMPORTING
        it_source_data TYPE zcl_etl_extractor=>tt_source_data
      RETURNING
        VALUE(rt_data) TYPE tt_transformed_data.

    METHODS apply_business_rules
      CHANGING
        ct_data TYPE tt_transformed_data.

    METHODS validate_data
      IMPORTING
        it_data               TYPE tt_transformed_data
      RETURNING
        VALUE(rv_is_valid)    TYPE abap_bool
      EXPORTING
        et_validation_errors  TYPE string_table.

  PROTECTED SECTION.
  PRIVATE SECTION.
    METHODS calculate_derived_values
      IMPORTING
        iv_value              TYPE string
      RETURNING
        VALUE(rv_transformed) TYPE string.
ENDCLASS.

CLASS zcl_etl_transformer IMPLEMENTATION.

  METHOD transform_data.
    DATA: lt_transformed TYPE tt_transformed_data.

    LOOP AT it_source_data INTO DATA(ls_source).
      DATA(ls_transformed) = VALUE ty_transformed_data(
        id              = ls_source-id
        name            = to_upper( ls_source-name )  " Transform: uppercase
        value           = ls_source-value
        transformed_val = calculate_derived_values( ls_source-value )
        status          = 'PROCESSED'
        processed_at    = cl_abap_tstmp=>utclong2tstmp( utclong_current( ) )
      ).

      APPEND ls_transformed TO lt_transformed.
    ENDLOOP.

    " Apply business rules
    apply_business_rules( CHANGING ct_data = lt_transformed ).

    rt_data = lt_transformed.
  ENDMETHOD.

  METHOD apply_business_rules.
    " Apply transformation business rules
    LOOP AT ct_data ASSIGNING FIELD-SYMBOL(<fs_data>).
      " Rule 1: Set status based on value
      IF <fs_data>-value IS INITIAL.
        <fs_data>-status = 'INVALID'.
      ELSEIF <fs_data>-transformed_val > '500'.
        <fs_data>-status = 'HIGH_VALUE'.
      ELSE.
        <fs_data>-status = 'NORMAL'.
      ENDIF.

      " Rule 2: Additional transformations
      CONDENSE <fs_data>-name NO-GAPS.
    ENDLOOP.
  ENDMETHOD.

  METHOD validate_data.
    DATA: lt_errors TYPE string_table.

    rv_is_valid = abap_true.

    LOOP AT it_data INTO DATA(ls_data).
      " Validation rules
      IF ls_data-id IS INITIAL.
        APPEND |Row { sy-tabix }: ID is required| TO lt_errors.
        rv_is_valid = abap_false.
      ENDIF.

      IF ls_data-name IS INITIAL.
        APPEND |Row { sy-tabix }: Name is required| TO lt_errors.
        rv_is_valid = abap_false.
      ENDIF.
    ENDLOOP.

    et_validation_errors = lt_errors.
  ENDMETHOD.

  METHOD calculate_derived_values.
    " Calculate derived/transformed values
    DATA: lv_numeric TYPE i.

    TRY.
        lv_numeric = iv_value.
        " Apply transformation: multiply by 1.5
        lv_numeric = lv_numeric * 3 / 2.
        rv_transformed = |{ lv_numeric }|.
      CATCH cx_root.
        rv_transformed = '0'.
    ENDTRY.
  ENDMETHOD.

ENDCLASS.
