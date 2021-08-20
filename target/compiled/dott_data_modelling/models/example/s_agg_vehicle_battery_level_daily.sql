/*
This script creates a Staging Table.
The purpose of this script is to display the number of vehicles with less than 20% battery per day per city/country
This is a truncate and load dataset, so full-refresh is marked as true
Command to run this script -
dbt run --models s_agg_vehicle_battery_level_daily --vars '{"START_DATE":"2020-12-31", "END_DATE":"2021-01-31"}'
*/




(SELECT
        dates.all_dates AS date_key,
        a.city_name,
        a.country_name,
        COUNT(DISTINCT a.vehicle_id) AS veh_low_battery
        FROM DEMO_DB.PUBLIC.S_CALENDER dates
        LEFT JOIN
         (
            SELECT DISTINCT TO_DATE(SPLIT_PART(tel.time_updated,' UTC',1)) AS date_key, st.city_name, st.country_name,
            st.vehicle_id  FROM
            DEMO_DB.PUBLIC.TRNS_TBL_TELEMETRY tel
            INNER JOIN DEMO_DB.PUBLIC.TRNS_TBL_STATES st
            ON tel.vehicle_id = st.vehicle_id
            AND TO_DATE(SPLIT_PART(tel.time_updated,' UTC',1)) = TO_DATE(SPLIT_PART(st.time_updated,' UTC',1))
            /* filter for less than 20% battery level */
            WHERE battery_level < 20
            AND TO_DATE(SPLIT_PART(tel.time_updated,' UTC',1)) > '2020-12-31'
            AND TO_DATE(SPLIT_PART(tel.time_updated,' UTC',1)) <= '2021-01-31'
         )a
        ON dates.all_dates = a.date_key
        GROUP BY dates.all_dates,
        a.city_name,
        a.city_name,
        a.country_name
)