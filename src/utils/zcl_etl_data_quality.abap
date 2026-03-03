CLASS zcl_etl_data_quality DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    TYPES:
      BEGIN OF ty_quality_check,
        check_name    TYPE char50,
        check_type    TYPE char20,
        passed        TYPE abap_bool,
        failed_count  TYPE i,
        message       TYPE string,
      END OF ty_quality_check,
      tt_quality_checks TYPE STANDARD TABLE OF ty_quality_check WITH DEFAULT KEY,

      BEGIN OF ty_data_profile,
        total_records       TYPE i,
        null_count          TYPE i,
        duplicate_count     TYPE i,
        min_value           TYPE dec15_2,
        max_value           TYPE dec15_2,
        avg_value           TYPE dec15_2,
        std_deviation       TYPE p DECIMALS 4,
        unique_categories   TYPE i,
      END OF ty_data_profile.

    METHODS constructor
      IMPORTING
        iv_run_id TYPE char20.

    METHODS perform_quality_checks
      IMPORTING
        it_data            TYPE zcl_etl_transformer=>tt_transformed_data
      RETURNING
        VALUE(rt_checks)   TYPE tt_quality_checks.

    METHODS check_completeness
      IMPORTING
        it_data           TYPE zcl_etl_transformer=>tt_transformed_data
      RETURNING
        VALUE(rs_check)   TYPE ty_quality_check.

    METHODS check_uniqueness
      IMPORTING
        it_data           TYPE zcl_etl_transformer=>tt_transformed_data
      RETURNING
        VALUE(rs_check)   TYPE ty_quality_check.

    METHODS check_validity
      IMPORTING
        it_data           TYPE zcl_etl_transformer=>tt_transformed_data
      RETURNING
        VALUE(rs_check)   TYPE ty_quality_check.

    METHODS check_consistency
      IMPORTING
        it_data           TYPE zcl_etl_transformer=>tt_transformed_data
      RETURNING
        VALUE(rs_check)   TYPE ty_quality_check.

    METHODS profile_data
      IMPORTING
        it_data            TYPE zcl_etl_transformer=>tt_transformed_data
      RETURNING
        VALUE(rs_profile)  TYPE ty_data_profile.

    METHODS detect_anomalies
      IMPORTING
        it_data              TYPE zcl_etl_transformer=>tt_transformed_data
      RETURNING
        VALUE(rt_anomalies)  TYPE string_table.

  PROTECTED SECTION.
  PRIVATE SECTION.
    DATA: mv_run_id TYPE char20,
          mo_logger TYPE REF TO zcl_etl_logger.

    METHODS calculate_standard_deviation
      IMPORTING
        it_values        TYPE STANDARD TABLE
      RETURNING
        VALUE(rv_std_dev) TYPE p.
ENDCLASS.

CLASS zcl_etl_data_quality IMPLEMENTATION.

  METHOD constructor.
    mv_run_id = iv_run_id.
    mo_logger = zcl_etl_logger=>get_instance( ).
  ENDMETHOD.

  METHOD perform_quality_checks.
    mo_logger->log_info(
      iv_component = 'DATA_QUALITY'
      iv_message   = 'Starting data quality checks'
    ).

    " Run all quality checks
    APPEND check_completeness( it_data ) TO rt_checks.
    APPEND check_uniqueness( it_data ) TO rt_checks.
    APPEND check_validity( it_data ) TO rt_checks.
    APPEND check_consistency( it_data ) TO rt_checks.

    " Count passed/failed
    DATA(lv_passed) = 0.
    DATA(lv_failed) = 0.
    LOOP AT rt_checks INTO DATA(ls_check).
      IF ls_check-passed = abap_true.
        lv_passed = lv_passed + 1.
      ELSE.
        lv_failed = lv_failed + 1.
      ENDIF.
    ENDLOOP.

    mo_logger->log_info(
      iv_component = 'DATA_QUALITY'
      iv_message   = |Quality checks complete: { lv_passed } passed, { lv_failed } failed|
    ).
  ENDMETHOD.

  METHOD check_completeness.
    DATA: lv_null_count TYPE i.

    rs_check-check_name = 'Completeness Check'.
    rs_check-check_type = 'COMPLETENESS'.

    " Check for null/empty values in critical fields
    LOOP AT it_data INTO DATA(ls_data).
      IF ls_data-id IS INITIAL OR
         ls_data-name IS INITIAL OR
         ls_data-value IS INITIAL.
        lv_null_count = lv_null_count + 1.
      ENDIF.
    ENDLOOP.

    rs_check-failed_count = lv_null_count.

    IF lv_null_count = 0.
      rs_check-passed = abap_true.
      rs_check-message = 'All required fields are complete'.
    ELSE.
      rs_check-passed = abap_false.
      rs_check-message = |{ lv_null_count } records with incomplete data|.
    ENDIF.
  ENDMETHOD.

  METHOD check_uniqueness.
    DATA: lt_ids TYPE STANDARD TABLE OF char10,
          lv_dup_count TYPE i.

    rs_check-check_name = 'Uniqueness Check'.
    rs_check-check_type = 'UNIQUENESS'.

    " Check for duplicate IDs
    LOOP AT it_data INTO DATA(ls_data).
      APPEND ls_data-id TO lt_ids.
    ENDLOOP.

    DATA(lv_total) = lines( lt_ids ).
    SORT lt_ids.
    DELETE ADJACENT DUPLICATES FROM lt_ids.
    DATA(lv_unique) = lines( lt_ids ).

    lv_dup_count = lv_total - lv_unique.
    rs_check-failed_count = lv_dup_count.

    IF lv_dup_count = 0.
      rs_check-passed = abap_true.
      rs_check-message = 'All IDs are unique'.
    ELSE.
      rs_check-passed = abap_false.
      rs_check-message = |{ lv_dup_count } duplicate IDs found|.
    ENDIF.
  ENDMETHOD.

  METHOD check_validity.
    DATA: lv_invalid_count TYPE i.

    rs_check-check_name = 'Validity Check'.
    rs_check-check_type = 'VALIDITY'.

    " Check for invalid values
    LOOP AT it_data INTO DATA(ls_data).
      " Value must be positive
      IF ls_data-value < 0 OR ls_data-transformed_value < 0.
        lv_invalid_count = lv_invalid_count + 1.
      ENDIF.

      " Priority must be 1-5
      IF ls_data-priority < 1 OR ls_data-priority > 5.
        lv_invalid_count = lv_invalid_count + 1.
      ENDIF.

      " Category must be valid
      IF ls_data-category IS INITIAL.
        lv_invalid_count = lv_invalid_count + 1.
      ENDIF.
    ENDLOOP.

    rs_check-failed_count = lv_invalid_count.

    IF lv_invalid_count = 0.
      rs_check-passed = abap_true.
      rs_check-message = 'All values are valid'.
    ELSE.
      rs_check-passed = abap_false.
      rs_check-message = |{ lv_invalid_count } records with invalid values|.
    ENDIF.
  ENDMETHOD.

  METHOD check_consistency.
    DATA: lv_inconsistent_count TYPE i.

    rs_check-check_name = 'Consistency Check'.
    rs_check-check_type = 'CONSISTENCY'.

    " Check for inconsistencies in transformed values
    LOOP AT it_data INTO DATA(ls_data).
      " Transformed value should be greater than original
      IF ls_data-transformed_value < ls_data-value.
        lv_inconsistent_count = lv_inconsistent_count + 1.
      ENDIF.

      " High value items should have high priority
      IF ls_data-transformed_value >= 750 AND ls_data-priority > 2.
        lv_inconsistent_count = lv_inconsistent_count + 1.
      ENDIF.
    ENDLOOP.

    rs_check-failed_count = lv_inconsistent_count.

    IF lv_inconsistent_count = 0.
      rs_check-passed = abap_true.
      rs_check-message = 'Data is consistent'.
    ELSE.
      rs_check-passed = abap_false.
      rs_check-message = |{ lv_inconsistent_count } consistency issues found|.
    ENDIF.
  ENDMETHOD.

  METHOD profile_data.
    DATA: lt_values TYPE STANDARD TABLE OF dec15_2,
          lt_categories TYPE STANDARD TABLE OF char50,
          lv_sum TYPE p DECIMALS 2,
          lv_null_count TYPE i.

    rs_profile-total_records = lines( it_data ).

    " Collect values and categories
    LOOP AT it_data INTO DATA(ls_data).
      IF ls_data-value IS INITIAL.
        lv_null_count = lv_null_count + 1.
      ELSE.
        APPEND ls_data-value TO lt_values.
      ENDIF.
      APPEND ls_data-category TO lt_categories.
    ENDLOOP.

    rs_profile-null_count = lv_null_count.

    " Calculate statistics
    IF lt_values IS NOT INITIAL.
      SORT lt_values.
      rs_profile-min_value = lt_values[ 1 ].
      rs_profile-max_value = lt_values[ lines( lt_values ) ].

      LOOP AT lt_values INTO DATA(lv_value).
        lv_sum = lv_sum + lv_value.
      ENDLOOP.
      rs_profile-avg_value = lv_sum / lines( lt_values ).

      rs_profile-std_deviation = calculate_standard_deviation( lt_values ).
    ENDIF.

    " Count unique categories
    SORT lt_categories.
    DELETE ADJACENT DUPLICATES FROM lt_categories.
    rs_profile-unique_categories = lines( lt_categories ).

    mo_logger->log_info(
      iv_component = 'DATA_QUALITY'
      iv_message   = |Data profile: { rs_profile-total_records } records, Avg: { rs_profile-avg_value }|
    ).
  ENDMETHOD.

  METHOD detect_anomalies.
    DATA: ls_profile TYPE ty_data_profile,
          lv_threshold TYPE p DECIMALS 2.

    ls_profile = profile_data( it_data ).

    " Calculate threshold for outliers (mean + 2*std_dev)
    lv_threshold = ls_profile-avg_value + ( 2 * ls_profile-std_deviation ).

    " Detect anomalies
    LOOP AT it_data INTO DATA(ls_data).
      " Value outliers
      IF ls_data-value > lv_threshold.
        APPEND |ID { ls_data-id }: Value { ls_data-value } exceeds threshold { lv_threshold }|
          TO rt_anomalies.
      ENDIF.

      " Negative values
      IF ls_data-value < 0.
        APPEND |ID { ls_data-id }: Negative value { ls_data-value }|
          TO rt_anomalies.
      ENDIF.

      " Mismatched priority
      IF ls_data-value >= 500 AND ls_data-priority > 2.
        APPEND |ID { ls_data-id }: High value but low priority|
          TO rt_anomalies.
      ENDIF.
    ENDLOOP.

    IF rt_anomalies IS NOT INITIAL.
      mo_logger->log_warning(
        iv_component = 'DATA_QUALITY'
        iv_message   = |{ lines( rt_anomalies ) } anomalies detected|
      ).
    ENDIF.
  ENDMETHOD.

  METHOD calculate_standard_deviation.
    DATA: lv_mean   TYPE p DECIMALS 4,
          lv_sum    TYPE p DECIMALS 4,
          lv_sq_sum TYPE p DECIMALS 4,
          lv_count  TYPE i.

    lv_count = lines( it_values ).
    IF lv_count <= 1.
      rv_std_dev = 0.
      RETURN.
    ENDIF.

    " Calculate mean
    LOOP AT it_values INTO DATA(lv_value).
      lv_sum = lv_sum + lv_value.
    ENDLOOP.
    lv_mean = lv_sum / lv_count.

    " Calculate sum of squared differences
    LOOP AT it_values INTO lv_value.
      DATA(lv_diff) = lv_value - lv_mean.
      lv_sq_sum = lv_sq_sum + ( lv_diff * lv_diff ).
    ENDLOOP.

    " Standard deviation
    rv_std_dev = sqrt( lv_sq_sum / ( lv_count - 1 ) ).
  ENDMETHOD.

ENDCLASS.
