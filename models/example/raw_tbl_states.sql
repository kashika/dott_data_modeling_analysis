/*
The purpose of this script is to load history files to snowflake. Since the file size is huge, the files
have been broken down into files with record count of 500k. The final dataset is a union of all these files
Command to run this script -
dbt run --models raw_tbl_states
*/


{{ config(materialized='table') }}

with states as(
 select *  from "DEMO_DB"."PUBLIC"."DATA_MODELLING_TEST_TBL_STATES1"
 UNION
 select *  from "DEMO_DB"."PUBLIC"."DATA_MODELLING_TEST_TBL_STATES2"
 UNION
 select *  from "DEMO_DB"."PUBLIC"."DATA_MODELLING_TEST_TBL_STATES3"
 UNION
 select *  from "DEMO_DB"."PUBLIC"."DATA_MODELLING_TEST_TBL_STATES4"
 UNION
 select *  from "DEMO_DB"."PUBLIC"."DATA_MODELLING_TEST_TBL_STATES5"
 UNION
 select *  from "DEMO_DB"."PUBLIC"."DATA_MODELLING_TEST_TBL_STATES6"
 UNION
 select *  from "DEMO_DB"."PUBLIC"."DATA_MODELLING_TEST_TBL_STATES7"
 UNION
 select *  from "DEMO_DB"."PUBLIC"."DATA_MODELLING_TEST_TBL_STATES8"
 UNION
 select *  from "DEMO_DB"."PUBLIC"."DATA_MODELLING_TEST_TBL_STATES9"
 UNION
 select *  from "DEMO_DB"."PUBLIC"."DATA_MODELLING_TEST_TBL_STATES10"
)
select * from states