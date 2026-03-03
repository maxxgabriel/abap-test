CLASS zcl_etl_extractor DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    TYPES:
      BEGIN OF ty_source_data,
        id          TYPE string,
        name        TYPE string,
        value       TYPE string,
        created_at  TYPE timestamp,
      END OF ty_source_data,
      tt_source_data TYPE STANDARD TABLE OF ty_source_data WITH DEFAULT KEY.

    METHODS constructor
      IMPORTING
        iv_source_type TYPE string OPTIONAL.

    METHODS extract_data
      IMPORTING
        iv_filter      TYPE string OPTIONAL
      RETURNING
        VALUE(rt_data) TYPE tt_source_data
      RAISING
        cx_sy_open_sql_db.

    METHODS extract_from_file
      IMPORTING
        iv_filepath    TYPE string
      RETURNING
        VALUE(rt_data) TYPE tt_source_data.

  PROTECTED SECTION.
  PRIVATE SECTION.
    DATA mv_source_type TYPE string.
ENDCLASS.

CLASS zcl_etl_extractor IMPLEMENTATION.

  METHOD constructor.
    mv_source_type = COND #( WHEN iv_source_type IS NOT INITIAL
                             THEN iv_source_type
                             ELSE 'DATABASE' ).
  ENDMETHOD.

  METHOD extract_data.
    " Extract data from database or other sources
    DATA: lt_data TYPE tt_source_data.

    TRY.
        " Sample extraction logic
        " In real scenario, this would query actual tables
        CASE mv_source_type.
          WHEN 'DATABASE'.
            " SELECT * FROM source_table INTO TABLE @lt_data WHERE ...
            " For demo purposes, create sample data
            lt_data = VALUE #(
              ( id = '001' name = 'Sample1' value = '100' created_at = cl_abap_tstmp=>utclong2tstmp( utclong_current( ) ) )
              ( id = '002' name = 'Sample2' value = '200' created_at = cl_abap_tstmp=>utclong2tstmp( utclong_current( ) ) )
              ( id = '003' name = 'Sample3' value = '300' created_at = cl_abap_tstmp=>utclong2tstmp( utclong_current( ) ) )
            ).

          WHEN 'API'.
            " Call external API for data extraction
            " Implementation would go here

          WHEN OTHERS.
            " Default extraction
        ENDCASE.

        rt_data = lt_data.

      CATCH cx_root INTO DATA(lx_error).
        " Log error
        WRITE: / 'Error during extraction:', lx_error->get_text( ).
    ENDTRY.
  ENDMETHOD.

  METHOD extract_from_file.
    " Extract data from flat file
    DATA: lt_data TYPE tt_source_data.

    " File reading logic would go here
    " For demo, return empty table
    rt_data = lt_data.
  ENDMETHOD.

ENDCLASS.
