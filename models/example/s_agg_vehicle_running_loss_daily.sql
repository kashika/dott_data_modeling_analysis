 /*
This script creates a Staging Table.
The purpose of this script is to display the running loss percentage from D-21 to D-7 per day per city/country
This is a truncate and load dataset, so full-refresh is marked as true
Command to run this script -
dbt run --models s_agg_vehicle_running_loss_daily --vars '{"START_DATE":"2020-12-31", "END_DATE":"2021-01-31"}'
*/

{{ config(
    full_refresh = true,
    materialized='incremental'
) }}

(
   SELECT avg_dep.all_dates as date_key,
          avg_dep.city_name,
          avg_dep.country_name,
          /* calculation for running loss percentage */
          round((rolling_loss.cnt_rolling_loss * 100.00)/avg_dep.avg_deployed,2) as running_loss_pct
          FROM
           (SELECT
             dates.all_dates,
             dep.city_name,
             dep.country_name,
             /* Average number of deployed vehicle from D-21 to D-7*/
             round(AVG(CASE WHEN date_key BETWEEN dates.all_dates - 21 AND dates.all_dates-7 THEN deployed_vehicle
             ELSE NULL END),0) AS avg_deployed
             FROM
             demo_db.public.s_calender AS dates
             LEFT JOIN
             (SELECT TO_DATE(SPLIT_PART(time_updated,' UTC',1)) as date_key,
                     city_name,
                     country_name,
                     COUNT(DISTINCT vehicle_id) AS deployed_vehicle
                     FROM DEMO_DB.PUBLIC.TRNS_TBL_STATES
                     WHERE is_deployed='TRUE'
                     AND TO_DATE(SPLIT_PART(time_updated,' UTC',1)) > '{{var("START_DATE")}}'
                     AND TO_DATE(SPLIT_PART(time_updated,' UTC',1)) <= '{{var("END_DATE")}}'
                     GROUP BY TO_DATE(SPLIT_PART(time_updated,' UTC',1)),
                         city_name,
                         country_name
             )dep
             ON dates.all_dates>=dep.date_key
             GROUP BY dates.all_dates,
                      dep.city_name,
                      dep.country_name
           )avg_dep


        LEFT JOIN

        (SELECT
          dates.all_dates,
          daily_loss.city_name,
          daily_loss.country_name,
          /* Avg lost vehicles from D-21 to D-7 */
          COUNT(DISTINCT CASE WHEN daily_loss.date_key BETWEEN dates.all_dates - 21 AND dates.all_dates-7
                             THEN daily_loss.lost_vehicle ELSE null END) AS cnt_rolling_loss
          FROM  demo_db.public.s_calender AS dates
          LEFT JOIN
          demo_db.public.s_agg_vehicle_loss_daily as daily_loss

         ON dates.all_dates>=daily_loss.date_key
         GROUP BY  dates.all_dates,
                   daily_loss.city_name,
                   daily_loss.country_name
        )rolling_loss

ON avg_dep.all_dates = rolling_loss.all_dates
AND avg_dep.city_name = rolling_loss.city_name
AND avg_dep.country_name = rolling_loss.country_name
)

