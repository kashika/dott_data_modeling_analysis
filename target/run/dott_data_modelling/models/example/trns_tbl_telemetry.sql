

      create or replace transient table DEMO_DB.PUBLIC.trns_tbl_telemetry  as
      (-- Sample command to run this script -
-- dbt run --models trns_tbl_telemetry --vars '{"START_TIME":"2020-12-31 00:00:00.0", "END_TIME":"2021-02-01 00:00:00.0"}'



    SELECT MD5(concat(trim(vehicle_id), time_updated, time_gps, is_charging, battery_level,
           IFNULL(cast(num_gps_satellites as string),''))) as hash_code,
           TRIM(vehicle_id) AS vehicle_id,
           TO_TIMESTAMP(SPLIT_PART(time_updated, ' UTC',1)) AS time_updated,
           TO_TIMESTAMP(SPLIT_PART(time_gps, ' UTC',1)) AS time_gps,
           CAST(CASE WHEN is_charging = 'TRUE' THEN 1
                     WHEN is_charging = 'FALSE' THEN 0
                     ELSE NULL END AS BOOLEAN) AS is_charging,
           CAST(battery_level AS FLOAT) AS battery_level,
           CAST(num_gps_satellites AS INTEGER) AS num_gps_satellites

    FROM "DEMO_DB"."PUBLIC"."RAW_TBL_TELEMETRY"
    WHERE TO_TIMESTAMP(SPLIT_PART(time_updated, ' UTC',1))  >  '2020-12-31 00:00:00.0'
    AND TO_DATE(SPLIT_PART(time_updated,' UTC',1)) <= '2021-02-01 00:00:00.0'
      );
    