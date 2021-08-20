/*
The purpose of this script is to load history files to snowflake for telemetry data. Since the file size is huge, the files
have been broken down into files with record count of 500k. The final dataset is a union of all these files
Command to run this script -
dbt run --models raw_tbl_telemetry_delta --vars '{"START_TIME":"2021-01-01 00:00:00.0", "END_TIME":"2021-01-31 00:00:00.0"}'
*/
{{ config(
    materialized='incremental'
) }}
(    (
    SELECT *
    FROM demo_db.public.data_modelling_test_tbl_telemetry
    WHERE TO_TIMESTAMP(SPLIT_PART(time_updated,' UTC',1)) > '{{var("START_TIME")}}'
    AND TO_TIMESTAMP(SPLIT_PART(time_updated,' UTC',1)) <= '{{var("END_TIME")}}'
    )
