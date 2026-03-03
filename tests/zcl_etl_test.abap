CLASS zcl_etl_test DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC
  FOR TESTING
  DURATION SHORT
  RISK LEVEL HARMLESS.

  PRIVATE SECTION.
    DATA: mo_extractor   TYPE REF TO zcl_etl_extractor,
          mo_transformer TYPE REF TO zcl_etl_transformer,
          mo_loader      TYPE REF TO zcl_etl_loader.

    METHODS: setup,
             test_extraction FOR TESTING,
             test_transformation FOR TESTING,
             test_validation FOR TESTING,
             test_loading FOR TESTING,
             test_end_to_end FOR TESTING.
ENDCLASS.

CLASS zcl_etl_test IMPLEMENTATION.

  METHOD setup.
    " Initialize test objects
    mo_extractor   = NEW zcl_etl_extractor( 'DATABASE' ).
    mo_transformer = NEW zcl_etl_transformer( ).
    mo_loader      = NEW zcl_etl_loader(
      iv_target_type = 'DATABASE'
      iv_batch_size  = 100
    ).
  ENDMETHOD.

  METHOD test_extraction.
    " Test data extraction
    DATA(lt_data) = mo_extractor->extract_data( ).

    cl_abap_unit_assert=>assert_not_initial(
      act = lt_data
      msg = 'Extracted data should not be empty'
    ).

    cl_abap_unit_assert=>assert_equals(
      exp = 3
      act = lines( lt_data )
      msg = 'Should extract 3 sample records'
    ).
  ENDMETHOD.

  METHOD test_transformation.
    " Test data transformation
    DATA(lt_source) = mo_extractor->extract_data( ).
    DATA(lt_transformed) = mo_transformer->transform_data( lt_source ).

    cl_abap_unit_assert=>assert_not_initial(
      act = lt_transformed
      msg = 'Transformed data should not be empty'
    ).

    " Verify transformation applied
    READ TABLE lt_transformed INTO DATA(ls_transformed) INDEX 1.
    cl_abap_unit_assert=>assert_equals(
      exp = 'SAMPLE1'
      act = ls_transformed-name
      msg = 'Name should be uppercase'
    ).

    cl_abap_unit_assert=>assert_equals(
      exp = '150'
      act = ls_transformed-transformed_val
      msg = 'Value should be calculated correctly (100 * 1.5)'
    ).
  ENDMETHOD.

  METHOD test_validation.
    " Test data validation
    DATA: lt_test_data TYPE zcl_etl_transformer=>tt_transformed_data.

    " Add invalid data
    lt_test_data = VALUE #(
      ( id = '' name = 'Test1' value = '100' )
      ( id = '002' name = '' value = '200' )
    ).

    DATA(lv_valid) = mo_transformer->validate_data(
      EXPORTING
        it_data              = lt_test_data
      IMPORTING
        et_validation_errors = DATA(lt_errors)
    ).

    cl_abap_unit_assert=>assert_equals(
      exp = abap_false
      act = lv_valid
      msg = 'Validation should fail for invalid data'
    ).

    cl_abap_unit_assert=>assert_not_initial(
      act = lt_errors
      msg = 'Validation errors should be reported'
    ).
  ENDMETHOD.

  METHOD test_loading.
    " Test data loading
    DATA: lt_test_data TYPE zcl_etl_transformer=>tt_transformed_data.

    lt_test_data = VALUE #(
      ( id = '001' name = 'TEST1' value = '100' status = 'NORMAL' )
      ( id = '002' name = 'TEST2' value = '200' status = 'NORMAL' )
    ).

    DATA(ls_result) = mo_loader->load_data( lt_test_data ).

    cl_abap_unit_assert=>assert_equals(
      exp = 2
      act = ls_result-success_count
      msg = 'Should successfully load 2 records'
    ).

    cl_abap_unit_assert=>assert_equals(
      exp = 0
      act = ls_result-error_count
      msg = 'Should have no errors'
    ).
  ENDMETHOD.

  METHOD test_end_to_end.
    " Test complete ETL flow
    " Extract
    DATA(lt_source) = mo_extractor->extract_data( ).

    " Transform
    DATA(lt_transformed) = mo_transformer->transform_data( lt_source ).

    " Validate
    DATA(lv_valid) = mo_transformer->validate_data( lt_transformed ).
    cl_abap_unit_assert=>assert_equals(
      exp = abap_true
      act = lv_valid
      msg = 'Transformed data should be valid'
    ).

    " Load
    DATA(ls_result) = mo_loader->load_data( lt_transformed ).

    cl_abap_unit_assert=>assert_equals(
      exp = ls_result-total_count
      act = ls_result-success_count
      msg = 'All records should load successfully'
    ).
  ENDMETHOD.

ENDCLASS.
