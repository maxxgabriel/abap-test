*&---------------------------------------------------------------------*
*& Report Z_ETL_MAIN
*&---------------------------------------------------------------------*
*& Main ETL execution program
*&---------------------------------------------------------------------*
REPORT z_etl_main.

PARAMETERS: p_source TYPE string DEFAULT 'DATABASE',
            p_target TYPE string DEFAULT 'DATABASE',
            p_batch  TYPE i DEFAULT 1000.

START-OF-SELECTION.

  " Initialize ETL components
  DATA(lo_extractor)   = NEW zcl_etl_extractor( iv_source_type = p_source ).
  DATA(lo_transformer) = NEW zcl_etl_transformer( ).
  DATA(lo_loader)      = NEW zcl_etl_loader(
    iv_target_type = p_target
    iv_batch_size  = p_batch
  ).

  WRITE: / '====================================',
         / 'ETL Process Started',
         / '====================================', /.

  " Step 1: Extract
  WRITE: / 'Step 1: Extracting data...'.
  DATA(lt_source_data) = lo_extractor->extract_data( ).
  WRITE: / |  Extracted { lines( lt_source_data ) } records|, /.

  " Step 2: Transform
  WRITE: / 'Step 2: Transforming data...'.
  DATA(lt_transformed_data) = lo_transformer->transform_data( lt_source_data ).

  " Validate transformed data
  DATA(lv_valid) = lo_transformer->validate_data(
    EXPORTING
      it_data              = lt_transformed_data
    IMPORTING
      et_validation_errors = DATA(lt_errors)
  ).

  IF lv_valid = abap_false.
    WRITE: / '  Validation errors found:'.
    LOOP AT lt_errors INTO DATA(lv_error).
      WRITE: / |    - { lv_error }|.
    ENDLOOP.
  ELSE.
    WRITE: / |  Transformed { lines( lt_transformed_data ) } records|, /.
  ENDIF.

  " Step 3: Load
  WRITE: / 'Step 3: Loading data...'.
  DATA(ls_load_result) = lo_loader->load_data( lt_transformed_data ).

  WRITE: / |  Success: { ls_load_result-success_count } records|.
  WRITE: / |  Errors:  { ls_load_result-error_count } records|.
  WRITE: / |  Total:   { ls_load_result-total_count } records|, /.

  IF ls_load_result-errors IS NOT INITIAL.
    WRITE: / '  Load errors:'.
    LOOP AT ls_load_result-errors INTO lv_error.
      WRITE: / |    - { lv_error }|.
    ENDLOOP.
  ENDIF.

  WRITE: / '====================================',
         / 'ETL Process Completed',
         / '===================================='.
