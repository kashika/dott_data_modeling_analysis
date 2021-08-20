

      create or replace transient table DEMO_DB.PUBLIC.s_agg_vehicle_loss_weekly  as
      (-- Sample command to run this script -
-- dbt run --models s_agg_vehicle_loss_weekly --vars '{"START_DATE":"2020-12-31", "END_DATE":"2021-01-31"}'






          (SELECT week_end_date, city_name, country_name, COUNT(DISTINCT vehicle_id) AS lost_vehicle_cnt
            FROM
            (SELECT
              dateadd( day,7- dayofweek(all_dates),all_dates) AS week_end_date ,
              city_name, country_name, c.vehicle_id,COUNT(DISTINCT case when (TO_DATE(SPLIT_PART(time_gps,' UTC',1))>=all_dates - 7
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
                     AND TO_DATE(SPLIT_PART(time_updated,' UTC',1)) > '2020-12-31' AND TO_DATE(SPLIT_PART(time_updated,' UTC',1)) <= '2021-01-31'
                     GROUP BY vehicle_id, city_name, country_name
                    ) b
                   ON a.all_dates>=b.deploy_date+7
                 )c
            LEFT JOIN
            DEMO_DB.PUBLIC.TRNS_TBL_TELEMETRY d
            ON c.vehicle_id=d.vehicle_id
            AND TO_DATE(SPLIT_PART(d.time_updated,' UTC',1)) > '2020-12-31' AND TO_DATE(SPLIT_PART(d.time_updated,' UTC',1)) <= '2021-01-31'
            GROUP BY dateadd( day,7- dayofweek(all_dates),all_dates), city_name, country_name, c.vehicle_id
            )e
            WHERE gps_count =0
            GROUP BY week_end_date, city_name, country_name
          )
      );
    