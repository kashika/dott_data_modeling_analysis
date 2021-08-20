 /*
This script creates the final production style aggregate table.
The purpose of this script is to create an aggregated table that displays the below KPIs per week per city/country
1. Deployed vehicle
2. Lost vehicles

Command to run this script -
dbt run --models agg_vehicle_performance_metric_weekly --vars '{"START_DATE":"2020-12-31", "END_DATE":"2021-01-31"}'
*/

{{ config(
    materialized='incremental',
    unique_key='hash_code',
    incremental_strategy='delete+insert',
) }}
(
         SELECT
         /* a unique key is created with date, city and country combination of hash code to handle duplicates */
          MD5(concat(cast(week.week_end_date as varchar), week.city_name, week.country_name, 'WEEK')) as hash_code,
          week.week_end_date, week.city_name, week.country_name, week.dep_not_broken AS vehicle_deployed_cnt,
          l_week.lost_vehicle_cnt as lost_vehicle_cnt FROM
          (SELECT
          /* logic to add week end date for every calender day */
          dateadd( day,7- dayofweek(TO_DATE(SPLIT_PART(TIME_UPDATED,' UTC',1))),TO_DATE(SPLIT_PART(TIME_UPDATED,' UTC',1))) as week_end_date,
          CITY_NAME,
          COUNTRY_NAME,
          'WEEK' as time_grain,
          COUNT(DISTINCT case when is_deployed = 'TRUE' THEN vehicle_id ELSE NULL END) AS dep,
          COUNT(DISTINCT case when is_in_warehouse = 'TRUE' THEN vehicle_id ELSE NULL END) AS ware,
          COUNT(DISTINCT case when is_broken = 'TRUE' THEN vehicle_id ELSE NULL END) AS broken,
          COUNT(DISTINCT case when (is_deployed = 'TRUE' AND is_broken = 'FALSE') THEN vehicle_id ELSE NULL END) AS dep_not_broken,
          COUNT(DISTINCT case when (is_deployed = 'TRUE' AND is_broken = 'TRUE') THEN vehicle_id ELSE NULL END) AS dep_broken,
          COUNT(DISTINCT case when (is_in_warehouse = 'TRUE' AND is_broken = 'TRUE') THEN vehicle_id ELSE NULL END) AS ware_broken

           FROM
          DEMO_DB.PUBLIC.TRNS_TBL_STATES
          WHERE TO_DATE(SPLIT_PART(time_updated,' UTC',1)) > '{{var("START_DATE")}}'
          AND TO_DATE(SPLIT_PART(time_updated,' UTC',1)) <= '{{var("END_DATE")}}'
          GROUP BY dateadd( day,7- dayofweek(TO_DATE(SPLIT_PART(TIME_UPDATED,' UTC',1))),
          TO_DATE(SPLIT_PART(TIME_UPDATED,' UTC',1))),
          CITY_NAME,
          COUNTRY_NAME,
         'WEEK')week


        LEFT JOIN
        /* joining with the weekly loss table */
        demo_db.public.s_agg_vehicle_loss_weekly l_week

on week.city_name = l_week.city_name
AND week.country_name = l_week.country_name
AND week.week_end_date = l_week.week_end_date

)


