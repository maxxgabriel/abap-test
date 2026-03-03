*&---------------------------------------------------------------------*
*& Report Z_ETL_INIT_DATA
*&---------------------------------------------------------------------*
*& Initialize sample data for ETL testing
*&---------------------------------------------------------------------*
REPORT z_etl_init_data.

PARAMETERS: p_rows TYPE i DEFAULT 100.

START-OF-SELECTION.
  PERFORM create_sample_data.
  PERFORM create_config_data.
  PERFORM create_schedule_data.

*&---------------------------------------------------------------------*
*& Form create_sample_data
*&---------------------------------------------------------------------*
FORM create_sample_data.
  DATA: lt_source_data TYPE STANDARD TABLE OF zetl_source_data,
        lv_timestamp   TYPE timestampl,
        lv_value       TYPE p DECIMALS 2,
        lv_category    TYPE char50.

  WRITE: / 'Creating sample source data...'.

  GET TIME STAMP FIELD lv_timestamp.

  " Generate sample records
  DO p_rows TIMES.
    DATA(lv_id) = sy-index.

    " Random value between 50 and 1000
    CALL FUNCTION 'QF05_RANDOM_INTEGER'
      EXPORTING
        ran_int_max   = 1000
        ran_int_min   = 50
      IMPORTING
        ran_int       = lv_value
      EXCEPTIONS
        invalid_input = 1
        OTHERS        = 2.

    " Assign category based on ID
    CASE lv_id MOD 5.
      WHEN 0.
        lv_category = 'PREMIUM'.
      WHEN 1.
        lv_category = 'STANDARD'.
      WHEN 2.
        lv_category = 'BASIC'.
      WHEN 3.
        lv_category = 'VIP'.
      WHEN 4.
        lv_category = 'TRIAL'.
    ENDCASE.

    APPEND VALUE #(
      client        = sy-mandt
      id            = |{ lv_id WIDTH = 10 ALIGN = RIGHT PAD = '0' }|
      name          = |Product { lv_id }|
      value         = lv_value
      status        = 'ACTIVE'
      category      = lv_category
      source_system = 'SAP_ERP'
      created_at    = lv_timestamp
      created_by    = sy-uname
      changed_at    = lv_timestamp
      changed_by    = sy-uname
    ) TO lt_source_data.
  ENDDO.

  " Insert data
  INSERT zetl_source_data FROM TABLE lt_source_data.
  IF sy-subrc = 0.
    COMMIT WORK.
    WRITE: / '✓ Created', lines( lt_source_data ), 'source records'.
  ELSE.
    ROLLBACK WORK.
    WRITE: / '✗ Failed to create source data'.
  ENDIF.
ENDFORM.

*&---------------------------------------------------------------------*
*& Form create_config_data
*&---------------------------------------------------------------------*
FORM create_config_data.
  DATA: lt_config    TYPE STANDARD TABLE OF zetl_config,
        lv_timestamp TYPE timestampl.

  WRITE: / 'Creating configuration data...'.

  GET TIME STAMP FIELD lv_timestamp.

  " Configuration entries
  lt_config = VALUE #(
    ( client      = sy-mandt
      config_key  = 'BATCH_SIZE'
      config_value = '1000'
      description = 'Default batch size for data loading'
      config_type = 'PERFORMANCE'
      is_active   = 'X'
      changed_at  = lv_timestamp
      changed_by  = sy-uname )
    ( client      = sy-mandt
      config_key  = 'MAX_RETRIES'
      config_value = '3'
      description = 'Maximum number of retries on error'
      config_type = 'ERROR_HANDLING'
      is_active   = 'X'
      changed_at  = lv_timestamp
      changed_by  = sy-uname )
    ( client      = sy-mandt
      config_key  = 'ALERT_EMAIL'
      config_value = 'admin@example.com'
      description = 'Email address for alerts'
      config_type = 'NOTIFICATION'
      is_active   = 'X'
      changed_at  = lv_timestamp
      changed_by  = sy-uname )
    ( client      = sy-mandt
      config_key  = 'LOG_RETENTION_DAYS'
      config_value = '90'
      description = 'Number of days to retain logs'
      config_type = 'MAINTENANCE'
      is_active   = 'X'
      changed_at  = lv_timestamp
      changed_by  = sy-uname )
    ( client      = sy-mandt
      config_key  = 'ENABLE_RECONCILIATION'
      config_value = 'X'
      description = 'Enable data reconciliation after load'
      config_type = 'DATA_QUALITY'
      is_active   = 'X'
      changed_at  = lv_timestamp
      changed_by  = sy-uname )
    ( client      = sy-mandt
      config_key  = 'PREMIUM_MULTIPLIER'
      config_value = '1.5'
      description = 'Value multiplier for premium category'
      config_type = 'BUSINESS_RULE'
      is_active   = 'X'
      changed_at  = lv_timestamp
      changed_by  = sy-uname )
  ).

  " Insert config
  INSERT zetl_config FROM TABLE lt_config.
  IF sy-subrc = 0.
    COMMIT WORK.
    WRITE: / '✓ Created', lines( lt_config ), 'configuration entries'.
  ELSE.
    ROLLBACK WORK.
    WRITE: / '✗ Failed to create configuration'.
  ENDIF.
ENDFORM.

*&---------------------------------------------------------------------*
*& Form create_schedule_data
*&---------------------------------------------------------------------*
FORM create_schedule_data.
  DATA: lt_schedule  TYPE STANDARD TABLE OF zetl_schedule,
        lv_timestamp TYPE timestampl.

  WRITE: / 'Creating schedule data...'.

  GET TIME STAMP FIELD lv_timestamp.

  " Schedule entries
  lt_schedule = VALUE #(
    ( client       = sy-mandt
      schedule_id  = 'SCHED001'
      schedule_name = 'Daily Full Load'
      etl_type     = 'FULL'
      frequency    = 'DAILY'
      start_date   = sy-datum
      start_time   = '020000'
      is_active    = 'X'
      created_by   = sy-uname
      created_at   = lv_timestamp )
    ( client       = sy-mandt
      schedule_id  = 'SCHED002'
      schedule_name = 'Hourly Incremental'
      etl_type     = 'INCREMENTAL'
      frequency    = 'HOURLY'
      start_date   = sy-datum
      start_time   = '000000'
      is_active    = 'X'
      created_by   = sy-uname
      created_at   = lv_timestamp )
    ( client       = sy-mandt
      schedule_id  = 'SCHED003'
      schedule_name = 'Weekly Reconciliation'
      etl_type     = 'RECONCILIATION'
      frequency    = 'WEEKLY'
      start_date   = sy-datum
      start_time   = '180000'
      is_active    = ''
      created_by   = sy-uname
      created_at   = lv_timestamp )
  ).

  " Insert schedules
  INSERT zetl_schedule FROM TABLE lt_schedule.
  IF sy-subrc = 0.
    COMMIT WORK.
    WRITE: / '✓ Created', lines( lt_schedule ), 'schedules'.
  ELSE.
    ROLLBACK WORK.
    WRITE: / '✗ Failed to create schedules'.
  ENDIF.
ENDFORM.
