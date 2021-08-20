/*
This script creates the final production style aggregate table.
The purpose of this script is to create an aggregated table that displays the below KPIs per day per city/country
1. Deployed vehicle
2. Lost vehicles
3. Running Loss percentage
4. Vehicles with low battery level

Command to run this script -
dbt run --models agg_vehicle_performance_metric_daily --vars '{"START_DATE":"2020-12-31", "END_DATE":"2021-01-31"}'
*/


(
SELECT
       /* a unique key is created with date, city and country combination of hash code to handle duplicates */
       MD5(concat(all_dep.date_key, all_dep.city_name, all_dep.country_name, 'DAY')) as hash_code,
       all_dep.date_key, all_dep.city_name, all_dep.country_name, all_dep.dep_not_broken AS vehicle_deployed_cnt,
       loss.lost_vehicle AS lost_vehicle_cnt, run.running_loss_pct, bat.veh_low_battery AS low_battery_vehicle_cnt

FROM

  (SELECT
    TO_DATE(SPLIT_PART(states.time_updated,' UTC',1)) as date_key,
    CITY_NAME,
    COUNTRY_NAME,
    'DAY' as time_grain,
    COUNT(DISTINCT case when is_deployed = 'TRUE' THEN vehicle_id ELSE NULL END) AS dep,
    COUNT(DISTINCT case when is_in_warehouse = 'TRUE' THEN vehicle_id ELSE NULL END) AS ware,
    COUNT(DISTINCT case when is_broken = 'TRUE' THEN vehicle_id ELSE NULL END) AS broken,
    COUNT(DISTINCT case when (is_deployed = 'TRUE' AND is_broken = 'FALSE') THEN vehicle_id ELSE NULL END) AS dep_not_broken,
    COUNT(DISTINCT case when (is_deployed = 'TRUE' AND is_broken = 'TRUE') THEN vehicle_id ELSE NULL END) AS dep_broken,
    COUNT(DISTINCT case when (is_in_warehouse = 'TRUE' AND is_broken = 'TRUE') THEN vehicle_id ELSE NULL END) AS ware_broken
    FROM  DEMO_DB.PUBLIC.trns_tbl_states states
    WHERE TO_DATE(SPLIT_PART(time_updated,' UTC',1)) > '2020-12-31'
    AND TO_DATE(SPLIT_PART(time_updated,' UTC',1)) <= '2021-01-31'

    GROUP BY TO_DATE(SPLIT_PART(states.time_updated,' UTC',1)),
    CITY_NAME,
    COUNTRY_NAME,
    'DAY'
  )all_dep

LEFT JOIN
/* joining with the daily loss table */
demo_db.public.s_agg_vehicle_loss_daily loss
ON all_dep.date_key = loss.date_key
AND all_dep.city_name = loss.city_name
AND all_dep.country_name = loss.country_name

LEFT JOIN
/* joining with the running loss percentage table */
demo_db.public.s_agg_vehicle_running_loss_daily run
ON all_dep.date_key = run.date_key
AND all_dep.city_name = run.city_name
AND all_dep.country_name = run.country_name

LEFT JOIN
/* joining with the low battery level table */
demo_db.public.s_agg_vehicle_battery_level_daily bat
ON all_dep.date_key = bat.date_key
AND all_dep.city_name = bat.city_name
AND all_dep.country_name = bat.country_name
)