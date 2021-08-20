
    delete from DEMO_DB.PUBLIC.agg_vehicle_performance_metric_weekly
    where (hash_code) in (
        select (hash_code)
        from DEMO_DB.PUBLIC.agg_vehicle_performance_metric_weekly__dbt_tmp
    );
    

    insert into DEMO_DB.PUBLIC.agg_vehicle_performance_metric_weekly ("HASH_CODE", "WEEK_END_DATE", "CITY_NAME", "COUNTRY_NAME", "VEHICLE_DEPLOYED_CNT", "LOST_VEHICLE_CNT")
    (
        select "HASH_CODE", "WEEK_END_DATE", "CITY_NAME", "COUNTRY_NAME", "VEHICLE_DEPLOYED_CNT", "LOST_VEHICLE_CNT"
        from DEMO_DB.PUBLIC.agg_vehicle_performance_metric_weekly__dbt_tmp
    );
