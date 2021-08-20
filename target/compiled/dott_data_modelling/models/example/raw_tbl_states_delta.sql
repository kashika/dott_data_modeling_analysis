/*
The purpose of this script is to load history files to snowflake. Since the file size is huge, the files
have been broken down into files with record count of 500k. The final dataset is a union of all these files
Command to run this script -
dbt run --models raw_tbl_states_delta --vars '{"START_TIME":"2021-01-01 00:00:00.0", "END_TIME":"2021-01-31 00:00:00.0"}'
*/


(
    SELECT * FROM "DEMO_DB"."PUBLIC"."DATA_MODELLING_TEST_TBL_STATES"
    WHERE TO_TIMESTAMP(SPLIT_PART(time_updated,' UTC',1)) > '2021-01-01 00:00:00.0'
    AND TO_TIMESTAMP(SPLIT_PART(time_updated,' UTC',1)) <= '2021-01-31 00:00:00.0'

)