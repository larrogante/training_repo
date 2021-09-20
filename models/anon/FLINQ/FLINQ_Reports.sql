{% set v_data_batch_id = get_batch_id() %}
{% set v_unique_key_column_name = 'Reports_SK' %}
{% set v_business_key = ['PARTITION_COL','Id','Timestamp'] %}
{% set v_incremental_compare_columns = ['Type'
                                       ,'User'
                                       ,'Desk'
                                       ,'DeviceId'
                                       ,'DeviceName'
                                       ,'LevelId'
                                       ,'LevelName'
                                       ,'LevelActivated'
                                       ,'ConnectorId'
                                       ,'ConnectorName'
                                       ,'Command'
                                       ,'Parameters'
                                       ,'Reason'
                                       ,'Freetext'
                                       ,'CaseId'] %}
{{
    config (
        materialized = 'incremental',
        project = 'scg-udp-etl-dev',
        schema = 'FLINQ',
        alias = 'Reports',
        incremental_strategy = 'merge',
        partition_by = {'field': 'PARTITION_COL','data_type': 'date'},
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
       Reports of FLINQ Database that are for
       loading to ANON filtered by data batch id
    */
    SELECT Id
    ,      TIMESTAMP_MILLIS(Timestamp) AS Timestamp
    ,      Type
    ,      User
    ,      Desk
    ,      DeviceId
    ,      REPLACE(REPLACE(REPLACE(DeviceName,'Bondi','Bondi Junction'),'Warringah','Warringah Mall'),'Newmarket','Westfield Newmarket') AS DeviceName
    ,      LevelId
    ,      LevelName
    ,      LevelActivated
    ,      ConnectorId
    ,      ConnectorName
    ,      Command
    ,      Parameters
    ,      Reason
    ,      Freetext
    ,      CaseId
    ,      _FILE_NAME AS FILENAME_CREATED
    ,      _FILE_NAME AS FILENAME_UPDATED
    ,      DATE(TIMESTAMP_MILLIS(Timestamp)) AS PARTITION_COL
    ,      CAST(SPLIT(Batch_ID,'-')[ORDINAL(1)] AS INT64) AS Batch_Number
    FROM   {{ source('DataLake_FLINQ_NON_PI','Reports') }}
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

                                         