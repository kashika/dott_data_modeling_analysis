/*
This script creates a Staging Table.
The purpose of this script is to display the count of vehicles lost per week per city/country
This is a truncate and load dataset, so full-refresh is marked as true
Command to run this script -
dbt run --models s_agg_vehicle_loss_weekly --vars '{"START_DATE":"2020-12-31", "END_DATE":"2021-01-31"}'
*/

{{ config(
    full_refresh = true,
    materialized='incremental'
) }}


          (SELECT week_end_date, city_name, country_name, COUNT(DISTINCT vehicle_id) AS lost_vehicle_cnt
            FROM
            (SELECT
              dateadd( day,7- dayofweek(all_dates),all_dates) AS week_end_date ,
              city_name, country_name, c.vehicle_id,
              /* calculate the count of GPS signals in the last 7 days */
              COUNT(DISTINCT case when (TO_DATE(SPLIT_PART(time_gps,' UTC',1))>=all_dates - 7
              AND TO_DATE(SPLIT_PART(time_gps,' UTC',1))<all_dates)
              THEN TO_DATE(SPLIT_PART(time_gps,' UTC',1)) ELSE null END) AS gps_count
              FROM
                   (SELECT
                   a.all_dates, city_name, country_name, vehicle_id
                   FROM
                    demo_db.public.s_calender AS a
                   LEFT JOIN

                    (SELECT vehicle_id, city_name, country_name, min(TO_DATE(SPLIT_PART(time_updated,' UTC',1))) as deploy_date
                     FROM DEMO_DB.PUBLIC.TRNS_TBL_STATES WHERE is_in_warehouse = 'FALSE' AND is_deployed = 'TRUE' AND is_broken = 'FALSE'
                     #AND TO_DATE(SPLIT_PART(time_updated,' UTC',1)) > '{{var("START_DATE")}}'
                     #AND TO_DATE(SPLIT_PART(time_updated,' UTC',1)) <= '{{var("END_DATE")}}'
                     GROUP BY vehicle_id, city_name, country_name
                    ) b
                   ON a.all_dates>=b.deploy_date+7
                 )c
            LEFT JOIN
            DEMO_DB.PUBLIC.TRNS_TBL_TELEMETRY d
            ON c.vehicle_id=d.vehicle_id
            AND TO_DATE(SPLIT_PART(d.time_updated,' UTC',1)) > '{{var("START_DATE")}}'
            AND TO_DATE(SPLIT_PART(d.time_updated,' UTC',1)) <= '{{var("END_DATE")}}'
            GROUP BY dateadd( day,7- dayofweek(all_dates),all_dates), city_name, country_name, c.vehicle_id
            )e
            /* filter out the vehicles with no gps signals*/
            WHERE gps_count =0
            GROUP BY week_end_date, city_name, country_name
          )
