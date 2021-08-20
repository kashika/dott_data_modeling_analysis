/*
The purpose of this script is to load history files to snowflake. Since the file size is huge, the files
have been broken down into files with record count of 500k. The final dataset is a union of all these files
Command to run this script -
dbt run --models raw_tbl_telemetry
*/

{{ config(materialized='table') }}

with telemetry as(
 select *  from "DEMO_DB"."PUBLIC"."DATA_MODELLING_TEST_TBL_TELEMETRY1"
 UNION
 select *  from "DEMO_DB"."PUBLIC"."DATA_MODELLING_TEST_TBL_TELEMETRY2"
 UNION
 select *  from "DEMO_DB"."PUBLIC"."DATA_MODELLING_TEST_TBL_TELEMETRY3"
 UNION
 select *  from "DEMO_DB"."PUBLIC"."DATA_MODELLING_TEST_TBL_TELEMETRY4"
 UNION
 select *  from "DEMO_DB"."PUBLIC"."DATA_MODELLING_TEST_TBL_TELEMETRY5"
 UNION
 select *  from "DEMO_DB"."PUBLIC"."DATA_MODELLING_TEST_TBL_TELEMETRY6"
 UNION
 select *  from "DEMO_DB"."PUBLIC"."DATA_MODELLING_TEST_TBL_TELEMETRY7"
 UNION
 select *  from "DEMO_DB"."PUBLIC"."DATA_MODELLING_TEST_TBL_TELEMETRY8"
 UNION
 select *  from "DEMO_DB"."PUBLIC"."DATA_MODELLING_TEST_TBL_TELEMETRY9"
 UNION
 select *  from "DEMO_DB"."PUBLIC"."DATA_MODELLING_TEST_TBL_TELEMETRY10"
 UNION
 select *  from "DEMO_DB"."PUBLIC"."DATA_MODELLING_TEST_TBL_TELEMETRY11"
 UNION
 select *  from "DEMO_DB"."PUBLIC"."DATA_MODELLING_TEST_TBL_TELEMETRY12"
)
select * from telemetry