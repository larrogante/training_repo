{% set v_data_batch_id = get_batch_id() %}
{% set v_unique_key_column_name = 'CallSummary_SK' %}
{% set v_business_key = ['PARTITION_COL','Id','CallRequestEvent'] %}
{% set v_incremental_compare_columns = ['CaseId'
                                       ,'DeviceName'
                                       ,'CallType'
                                       ,'CallRequestEvent'
                                       ,'StartConversationCommand'
                                       ,'ConversationStartedEvent'
                                       ,'ConversationStoppedEvent'
                                       ,'StopConversationCommand'
                                       ,'StopAfterCallWorkCommand'
                                       ,'ResponseTime'
                                       ,'HandleTime'
                                       ,'AfterCallWorkTime'
                                       ,'User'
                                       ,'Desk'
                                       ,'Reason'
                                       ,'Details'
                                       ,'Plate'
                                       ,'LogicalCase_Id'] %}

{{
    config (
      materialized = 'incremental',
      alias = 'CallSummary',
      project = 'scg-udp-etl-dev',
      schema = 'FLINQ',
      incremental_strategy = 'merge',
      partition_by = {'field': 'PARTITION_COL', 'data_type': 'date'},
      unique_key = v_unique_key_column_name,
      tags = ['DataLakeToAnon','FLINQDataLakeToAnon'],
      incremental_updated_columns = v_incremental_compare_columns + ['FILENAME_UPDATED','BATCH_ID_UPDATED','UPDATED_TIMESTAMP'],
      incremental_compare_columns = v_incremental_compare_columns
    )
}}

WITH
  /* Get all qualified records based on batch id */
   QUALIFIEDRECORDS AS
   (
    /*
       Call Summary of FLINQ Database that are for
       loading to ANON filtered by data batch id
    */
    SELECT Id
    ,      CaseId
    ,      REPLACE(REPLACE(REPLACE(DeviceName,'Bondi','Bondi Junction'),'Warringah','Warringah Mall'),'Newmarket','Westfield Newmarket') AS DeviceName
    ,      CallType
    ,      TIMESTAMP_MILLIS(CallRequestEvent) AS CallRequestEvent
    ,      TIMESTAMP_MILLIS(StartConversationCommand) AS StartConversationCommand
    ,      TIMESTAMP_MILLIS(ConversationStartedEvent) AS ConversationStartedEvent
    ,      TIMESTAMP_MILLIS(ConversationStoppedEvent) AS ConversationStoppedEvent
    ,      TIMESTAMP_MILLIS(StopConversationCommand) AS StopConversationCommand
    ,      TIMESTAMP_MILLIS(StopAfterCallWorkCommand) AS StopAfterCallWorkCommand
    ,      ResponseTime
    ,      HandleTime
    ,      AfterCallWorkTime
    ,      User
    ,      Desk
    ,      Reason
    ,      Details
    ,      Plate
    ,      LogicalCase_Id
    ,      _FILE_NAME AS FILENAME_CREATED
    ,      _FILE_NAME AS FILENAME_UPDATED
    ,      DATE(TIMESTAMP_MILLIS(ConversationStartedEvent)) AS PARTITION_COL
    ,      CAST(SPLIT(Batch_ID,'-')[ORDINAL(1)] AS INT64) AS Batch_Number
    FROM   {{ source('Datalake_FLINQ_NON_PI','CallSummary') }}
    --WHERE  Batch_ID in ({{ v_data_batch_id }})
   ),
   /* Add row number for identifying the latest record  based on batch number in batch id */
  SOURCE AS
  (
    SELECT * EXCEPT (Batch_Number)
    ,      {{ 'ROW_NUMBER() OVER (PARTITION BY ' + v_business_key | join(',') + ' ORDER BY Batch_Number DESC) AS LatestRecordInd'  }}
    FROM QUALIFIEDRECORDS
  )
/* Main query */
SELECT {{ dbt_utils.surrogate_key(v_business_key) + ' AS ' + v_unique_key_column_name }}
,      * EXCEPT (LatestRecordInd)
,      {{ get_standard_etl_columns() }}
FROM SOURCE
WHERE LatestRecordInd = 1