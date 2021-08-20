

      create or replace transient table DEMO_DB.PUBLIC.raw_tbl_telemetry  as
      (-- Sample command to run this script -
-- dbt run --models raw_tbl_telemetry




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
      );
    