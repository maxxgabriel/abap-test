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
          mo_loader      TYPE REF TO zcl_etl_loader,
          mo_orchestrator TYPE REF TO zcl_etl_orchestrator,
          mo_quality     TYPE REF TO zcl_etl_data_quality,
          mv_test_run_id TYPE char20.

    METHODS: setup,
             teardown,
             test_extraction FOR TESTING,
             test_transformation FOR TESTING,
             test_validation FOR TESTING,
             test_loading FOR TESTING,
             test_end_to_end FOR TESTING,
             test_quality_checks FOR TESTING,
             test_orchestrator FOR TESTING,
             test_error_handling FOR TESTING,
             test_batch_processing FOR TESTING,
             test_incremental_load FOR TESTING.
ENDCLASS.

CLASS zcl_etl_test IMPLEMENTATION.

  METHOD setup.
    " Initialize test objects
    mv_test_run_id = 'TESTRUN001'.

    mo_extractor = NEW zcl_etl_extractor(
      iv_source_type = 'DATABASE'
      iv_run_id      = mv_test_run_id
    ).

    mo_transformer = NEW zcl_etl_transformer( iv_run_id = mv_test_run_id ).

    mo_loader = NEW zcl_etl_loader(
      iv_target_type = 'DATABASE'
      iv_batch_size  = 100
      iv_run_id      = mv_test_run_id
    ).

    mo_orchestrator = NEW zcl_etl_orchestrator( iv_run_type = 'TEST' ).
    mo_quality = NEW zcl_etl_data_quality( iv_run_id = mv_test_run_id ).

    " Create test data
    INSERT zetl_source_data FROM TABLE @( VALUE #(
      ( client = sy-mandt id = 'TEST001' name = 'Test Product 1' value = '100' status = 'ACTIVE' category = 'PREMIUM' )
      ( client = sy-mandt id = 'TEST002' name = 'Test Product 2' value = '200' status = 'ACTIVE' category = 'STANDARD' )
      ( client = sy-mandt id = 'TEST003' name = 'Test Product 3' value = '300' status = 'ACTIVE' category = 'BASIC' )
    ) ).
    COMMIT WORK.
  ENDMETHOD.

  METHOD teardown.
    " Clean up test data
    DELETE FROM zetl_source_data WHERE id LIKE 'TEST%'.
    DELETE FROM zetl_target_data WHERE id LIKE 'TEST%'.
    DELETE FROM zetl_run_log WHERE run_id = mv_test_run_id.
    DELETE FROM zetl_error_log WHERE run_id = mv_test_run_id.
    COMMIT WORK.
  ENDMETHOD.

  METHOD test_extraction.
    " Test data extraction
    DATA(lt_data) = mo_extractor->extract_data( ).

    cl_abap_unit_assert=>assert_not_initial(
      act = lt_data
      msg = 'Extracted data should not be empty'
    ).

    cl_abap_unit_assert=>assert_bound(
      act = mo_extractor
      msg = 'Extractor should be instantiated'
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
    cl_abap_unit_assert=>assert_not_initial(
      act = ls_transformed-name
      msg = 'Name should not be empty'
    ).

    cl_abap_unit_assert=>assert_differs(
      act = ls_transformed-value
      exp = ls_transformed-transformed_value
      msg = 'Transformed value should differ from original'
    ).
  ENDMETHOD.

  METHOD test_validation.
    " Test data validation
    DATA: lt_test_data TYPE zcl_etl_transformer=>tt_transformed_data.

    " Add valid data
    lt_test_data = VALUE #(
      ( id = 'VAL001' name = 'Valid1' value = '100' transformed_value = '150' priority = 3 category = 'TEST' )
      ( id = 'VAL002' name = 'Valid2' value = '200' transformed_value = '240' priority = 2 category = 'TEST' )
    ).

    DATA(lv_valid) = mo_transformer->validate_data(
      EXPORTING
        it_data              = lt_test_data
      IMPORTING
        et_validation_errors = DATA(lt_errors)
    ).

    cl_abap_unit_assert=>assert_equals(
      exp = abap_true
      act = lv_valid
      msg = 'Validation should pass for valid data'
    ).

    " Test with invalid data
    APPEND VALUE #( id = '' name = '' value = '0' ) TO lt_test_data.

    lv_valid = mo_transformer->validate_data(
      EXPORTING
        it_data              = lt_test_data
      IMPORTING
        et_validation_errors = lt_errors
    ).

    cl_abap_unit_assert=>assert_equals(
      exp = abap_false
      act = lv_valid
      msg = 'Validation should fail for invalid data'
    ).
  ENDMETHOD.

  METHOD test_loading.
    " Test data loading
    DATA: lt_test_data TYPE zcl_etl_transformer=>tt_transformed_data.

    lt_test_data = VALUE #(
      ( id = 'LOAD001' name = 'Test1' value = '100' transformed_value = '150' status = 'NORMAL' priority = 3 category = 'TEST' etl_run_id = mv_test_run_id )
      ( id = 'LOAD002' name = 'Test2' value = '200' transformed_value = '240' status = 'NORMAL' priority = 2 category = 'TEST' etl_run_id = mv_test_run_id )
    ).

    DATA(ls_result) = mo_loader->load_data(
      it_data = lt_test_data
      iv_mode = 'INSERT'
    ).

    cl_abap_unit_assert=>assert_equals(
      exp = lines( lt_test_data )
      act = ls_result-success_count
      msg = 'All records should load successfully'
    ).

    cl_abap_unit_assert=>assert_equals(
      exp = 0
      act = ls_result-error_count
      msg = 'Should have no errors'
    ).

    " Clean up
    DELETE FROM zetl_target_data WHERE id LIKE 'LOAD%'.
    COMMIT WORK.
  ENDMETHOD.

  METHOD test_end_to_end.
    " Test complete ETL flow
    DATA(lt_source) = mo_extractor->extract_data( ).

    cl_abap_unit_assert=>assert_not_initial(
      act = lt_source
      msg = 'Should extract data'
    ).

    DATA(lt_transformed) = mo_transformer->transform_data( lt_source ).

    cl_abap_unit_assert=>assert_not_initial(
      act = lt_transformed
      msg = 'Should transform data'
    ).

    DATA(lv_valid) = mo_transformer->validate_data( lt_transformed ).

    cl_abap_unit_assert=>assert_equals(
      exp = abap_true
      act = lv_valid
      msg = 'Transformed data should be valid'
    ).

    DATA(ls_result) = mo_loader->load_data(
      it_data = lt_transformed
      iv_mode = 'UPSERT'
    ).

    cl_abap_unit_assert=>assert_not_initial(
      act = ls_result-success_count
      msg = 'Should load records'
    ).
  ENDMETHOD.

  METHOD test_quality_checks.
    " Test data quality checks
    DATA: lt_test_data TYPE zcl_etl_transformer=>tt_transformed_data.

    lt_test_data = VALUE #(
      ( id = 'QC001' name = 'Quality1' value = '100' transformed_value = '150' priority = 3 category = 'TEST' )
      ( id = 'QC002' name = 'Quality2' value = '200' transformed_value = '240' priority = 2 category = 'TEST' )
    ).

    DATA(lt_checks) = mo_quality->perform_quality_checks( lt_test_data ).

    cl_abap_unit_assert=>assert_not_initial(
      act = lt_checks
      msg = 'Quality checks should return results'
    ).

    cl_abap_unit_assert=>assert_equals(
      exp = 4
      act = lines( lt_checks )
      msg = 'Should perform 4 quality checks'
    ).

    " Test data profiling
    DATA(ls_profile) = mo_quality->profile_data( lt_test_data ).

    cl_abap_unit_assert=>assert_equals(
      exp = lines( lt_test_data )
      act = ls_profile-total_records
      msg = 'Profile should match record count'
    ).
  ENDMETHOD.

  METHOD test_orchestrator.
    " Test orchestrator execution
    DATA(ls_result) = mo_orchestrator->execute_etl(
      iv_source_type = 'DATABASE'
      iv_target_type = 'DATABASE'
      iv_batch_size  = 10
      iv_max_records = 3
    ).

    cl_abap_unit_assert=>assert_not_initial(
      act = ls_result-run_id
      msg = 'Run ID should be generated'
    ).

    cl_abap_unit_assert=>assert_not_initial(
      act = ls_result-status
      msg = 'Status should be set'
    ).
  ENDMETHOD.

  METHOD test_error_handling.
    " Test error handling with invalid data
    DATA: lt_bad_data TYPE zcl_etl_transformer=>tt_transformed_data.

    " Create data that will fail validation
    lt_bad_data = VALUE #(
      ( id = '' name = '' value = '-100' transformed_value = '-50' priority = 10 category = '' )
    ).

    DATA(lv_valid) = mo_transformer->validate_data(
      EXPORTING
        it_data              = lt_bad_data
      IMPORTING
        et_validation_errors = DATA(lt_errors)
    ).

    cl_abap_unit_assert=>assert_equals(
      exp = abap_false
      act = lv_valid
      msg = 'Should fail validation'
    ).

    cl_abap_unit_assert=>assert_not_initial(
      act = lt_errors
      msg = 'Should return error messages'
    ).
  ENDMETHOD.

  METHOD test_batch_processing.
    " Test batch processing with multiple batches
    DATA: lt_large_data TYPE zcl_etl_transformer=>tt_transformed_data.

    " Create 250 records (3 batches of 100)
    DO 250 TIMES.
      APPEND VALUE #(
        id = |BATCH{ sy-index WIDTH = 6 ALIGN = RIGHT PAD = '0' }|
        name = |Batch Test { sy-index }|
        value = sy-index * 10
        transformed_value = sy-index * 15
        status = 'TEST'
        priority = 3
        category = 'BATCH'
        etl_run_id = mv_test_run_id
      ) TO lt_large_data.
    ENDDO.

    DATA(ls_result) = mo_loader->load_data(
      it_data = lt_large_data
      iv_mode = 'INSERT'
    ).

    cl_abap_unit_assert=>assert_equals(
      exp = 250
      act = ls_result-success_count
      msg = 'All 250 records should be loaded'
    ).

    " Clean up
    DELETE FROM zetl_target_data WHERE id LIKE 'BATCH%'.
    COMMIT WORK.
  ENDMETHOD.

  METHOD test_incremental_load.
    " Test incremental loading
    DATA: lv_timestamp TYPE timestampl.

    GET TIME STAMP FIELD lv_timestamp.

    " Create records with different timestamps
    DATA(lt_incremental) = mo_extractor->extract_incremental( lv_timestamp ).

    " Should return empty for fresh timestamp
    cl_abap_unit_assert=>assert_initial(
      act = lt_incremental
      msg = 'No records should be newer than current timestamp'
    ).
  ENDMETHOD.

ENDCLASS.
