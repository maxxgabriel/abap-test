CLASS zcl_etl_transformer DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    TYPES:
      BEGIN OF ty_transformed_data,
        id                TYPE char10,
        name              TYPE char100,
        value             TYPE dec15_2,
        transformed_value TYPE dec15_2,
        status            TYPE char20,
        category          TYPE char50,
        priority          TYPE int1,
        etl_run_id        TYPE char20,
        processed_at      TYPE timestampl,
        processed_by      TYPE syuname,
      END OF ty_transformed_data,
      tt_transformed_data TYPE STANDARD TABLE OF ty_transformed_data WITH DEFAULT KEY.

    METHODS constructor
      IMPORTING
        iv_run_id TYPE char20.

    METHODS transform_data
      IMPORTING
        it_source_data TYPE zcl_etl_extractor=>tt_source_data
      RETURNING
        VALUE(rt_data) TYPE tt_transformed_data.

    METHODS apply_business_rules
      CHANGING
        ct_data TYPE tt_transformed_data.

    METHODS enrich_data
      CHANGING
        ct_data TYPE tt_transformed_data.

    METHODS validate_data
      IMPORTING
        it_data                  TYPE tt_transformed_data
      RETURNING
        VALUE(rv_is_valid)       TYPE abap_bool
      EXPORTING
        et_validation_errors     TYPE string_table.

    METHODS calculate_priority
      IMPORTING
        iv_value          TYPE dec15_2
        iv_category       TYPE char50
      RETURNING
        VALUE(rv_priority) TYPE int1.

  PROTECTED SECTION.
  PRIVATE SECTION.
    DATA: mv_run_id TYPE char20,
          mo_logger TYPE REF TO zcl_etl_logger.

    METHODS calculate_derived_values
      IMPORTING
        iv_value              TYPE dec15_2
        iv_category           TYPE char50
      RETURNING
        VALUE(rv_transformed) TYPE dec15_2.

    METHODS apply_category_rules
      IMPORTING
        iv_category TYPE char50
      CHANGING
        cs_data     TYPE ty_transformed_data.
ENDCLASS.

CLASS zcl_etl_transformer IMPLEMENTATION.

  METHOD constructor.
    mv_run_id = iv_run_id.
    mo_logger = zcl_etl_logger=>get_instance( ).
  ENDMETHOD.

  METHOD transform_data.
    DATA: lt_transformed TYPE tt_transformed_data,
          lv_timestamp   TYPE timestampl.

    GET TIME STAMP FIELD lv_timestamp.

    mo_logger->log_info(
      iv_component = 'TRANSFORMER'
      iv_message   = 'Starting transformation'
    ).

    LOOP AT it_source_data INTO DATA(ls_source).
      DATA(ls_transformed) = VALUE ty_transformed_data(
        id                = ls_source-id
        name              = to_upper( condense( ls_source-name ) )
        value             = ls_source-value
        transformed_value = calculate_derived_values(
                              iv_value    = ls_source-value
                              iv_category = ls_source-category
                            )
        status            = 'TRANSFORMED'
        category          = ls_source-category
        priority          = calculate_priority(
                              iv_value    = ls_source-value
                              iv_category = ls_source-category
                            )
        etl_run_id        = mv_run_id
        processed_at      = lv_timestamp
        processed_by      = sy-uname
      ).

      " Apply category-specific rules
      apply_category_rules(
        EXPORTING iv_category = ls_source-category
        CHANGING  cs_data     = ls_transformed
      ).

      APPEND ls_transformed TO lt_transformed.
    ENDLOOP.

    " Apply business rules
    apply_business_rules( CHANGING ct_data = lt_transformed ).

    " Enrich data
    enrich_data( CHANGING ct_data = lt_transformed ).

    mo_logger->log_info(
      iv_component = 'TRANSFORMER'
      iv_message   = |Transformed { lines( lt_transformed ) } records|
    ).

    rt_data = lt_transformed.
  ENDMETHOD.

  METHOD apply_business_rules.
    mo_logger->log_info(
      iv_component = 'TRANSFORMER'
      iv_message   = 'Applying business rules'
    ).

    LOOP AT ct_data ASSIGNING FIELD-SYMBOL(<fs_data>).
      " Rule 1: Set status based on value
      IF <fs_data>-value IS INITIAL.
        <fs_data>-status = 'INVALID'.
      ELSEIF <fs_data>-transformed_value >= 750.
        <fs_data>-status = 'HIGH_VALUE'.
      ELSEIF <fs_data>-transformed_value >= 300.
        <fs_data>-status = 'MEDIUM_VALUE'.
      ELSE.
        <fs_data>-status = 'LOW_VALUE'.
      ENDIF.

      " Rule 2: Priority override for high value items
      IF <fs_data>-transformed_value >= 1000.
        <fs_data>-priority = 1.  " Highest priority
      ENDIF.

      " Rule 3: Name normalization
      REPLACE ALL OCCURRENCES OF '  ' IN <fs_data>-name WITH ' '.
      CONDENSE <fs_data>-name.

      " Rule 4: Category validation
      IF <fs_data>-category IS INITIAL.
        <fs_data>-category = 'UNCATEGORIZED'.
      ENDIF.
    ENDLOOP.
  ENDMETHOD.

  METHOD enrich_data.
    DATA: lt_config TYPE STANDARD TABLE OF zetl_config.

    " Load enrichment configuration
    SELECT * FROM zetl_config
      WHERE is_active = 'X'
      INTO TABLE @lt_config.

    LOOP AT ct_data ASSIGNING FIELD-SYMBOL(<fs_data>).
      " Add enrichment logic based on config
      " For example: add region, department, etc.

      " Sample enrichment: add calculated fields
      IF <fs_data>-category = 'PREMIUM'.
        <fs_data>-transformed_value = <fs_data>-transformed_value * '1.2'.
      ENDIF.
    ENDLOOP.
  ENDMETHOD.

  METHOD validate_data.
    DATA: lt_errors TYPE string_table.

    rv_is_valid = abap_true.

    LOOP AT it_data INTO DATA(ls_data).
      " Validation rule 1: ID is required
      IF ls_data-id IS INITIAL.
        APPEND |Row { sy-tabix }: ID is required| TO lt_errors.
        rv_is_valid = abap_false.
      ENDIF.

      " Validation rule 2: Name is required
      IF ls_data-name IS INITIAL.
        APPEND |Row { sy-tabix }: Name is required| TO lt_errors.
        rv_is_valid = abap_false.
      ENDIF.

      " Validation rule 3: Value must be positive
      IF ls_data-value < 0.
        APPEND |Row { sy-tabix }: Value must be positive| TO lt_errors.
        rv_is_valid = abap_false.
      ENDIF.

      " Validation rule 4: Priority must be 1-5
      IF ls_data-priority < 1 OR ls_data-priority > 5.
        APPEND |Row { sy-tabix }: Priority must be between 1-5| TO lt_errors.
        rv_is_valid = abap_false.
      ENDIF.

      " Validation rule 5: Check for duplicates
      DATA(lv_count) = 0.
      LOOP AT it_data INTO DATA(ls_check) WHERE id = ls_data-id.
        lv_count = lv_count + 1.
      ENDLOOP.
      IF lv_count > 1.
        APPEND |Row { sy-tabix }: Duplicate ID { ls_data-id }| TO lt_errors.
        rv_is_valid = abap_false.
      ENDIF.
    ENDLOOP.

    et_validation_errors = lt_errors.

    IF rv_is_valid = abap_false.
      mo_logger->log_warning(
        iv_component = 'TRANSFORMER'
        iv_message   = |Validation failed: { lines( lt_errors ) } errors found|
      ).
    ENDIF.
  ENDMETHOD.

  METHOD calculate_derived_values.
    DATA: lv_multiplier TYPE p DECIMALS 2.

    " Calculate transformation based on category
    CASE iv_category.
      WHEN 'PREMIUM'.
        lv_multiplier = '1.5'.
      WHEN 'STANDARD'.
        lv_multiplier = '1.2'.
      WHEN 'BASIC'.
        lv_multiplier = '1.0'.
      WHEN OTHERS.
        lv_multiplier = '1.1'.
    ENDCASE.

    rv_transformed = iv_value * lv_multiplier.
  ENDMETHOD.

  METHOD calculate_priority.
    " Calculate priority (1=highest, 5=lowest)
    IF iv_value >= 500.
      rv_priority = 1.
    ELSEIF iv_value >= 300.
      rv_priority = 2.
    ELSEIF iv_value >= 100.
      rv_priority = 3.
    ELSE.
      rv_priority = 4.
    ENDIF.

    " Adjust based on category
    IF iv_category = 'PREMIUM'.
      rv_priority = rv_priority - 1.
      IF rv_priority < 1.
        rv_priority = 1.
      ENDIF.
    ENDIF.
  ENDMETHOD.

  METHOD apply_category_rules.
    CASE iv_category.
      WHEN 'PREMIUM'.
        cs_data-status = 'PREMIUM_PROCESSED'.
      WHEN 'VIP'.
        cs_data-priority = 1.
        cs_data-status = 'VIP_PROCESSED'.
      WHEN 'STANDARD'.
        " Standard processing
      WHEN OTHERS.
        " Default processing
    ENDCASE.
  ENDMETHOD.

ENDCLASS.
