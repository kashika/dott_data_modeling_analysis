
/*
This script creates a Staging Table.
The purpose of this script is to get a list of all dates for which data is available in the states dataset. This
logic is used multiple times in all staging and final datasets, so a staging dataset is created to reduce runtime and complexity.
This is a truncate and load dataset, so full-refresh is marked as true
Command to run this script -
dbt run --models s_calender --vars '{"START_DATE":"2020-12-31", "END_DATE":"2021-01-31"}'
*/

{{ config(
    full_refresh = true,
    materialized='incremental'
) }}

(
    SELECT TO_DATE(SPLIT_PART(time_updated,' UTC',1)) AS all_dates
    FROM "DEMO_DB"."PUBLIC".TRNS_TBL_STATES
    WHERE TO_DATE(SPLIT_PART(time_updated,' UTC',1)) > '{{var("START_DATE")}}'
    AND TO_DATE(SPLIT_PART(time_updated,' UTC',1)) <= '{{var("END_DATE")}}'
    GROUP BY TO_DATE(SPLIT_PART(time_updated,' UTC',1))
)

