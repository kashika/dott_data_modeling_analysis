
    delete from DEMO_DB.PUBLIC.trns_tbl_states
    where (hash_code) in (
        select (hash_code)
        from DEMO_DB.PUBLIC.trns_tbl_states__dbt_tmp
    );
    

    insert into DEMO_DB.PUBLIC.trns_tbl_states ("HASH_CODE", "VEHICLE_ID", "TIME_UPDATED", "CITY_NAME", "COUNTRY_NAME", "IS_DEPLOYED", "IS_IN_WAREHOUSE", "IS_BROKEN")
    (
        select "HASH_CODE", "VEHICLE_ID", "TIME_UPDATED", "CITY_NAME", "COUNTRY_NAME", "IS_DEPLOYED", "IS_IN_WAREHOUSE", "IS_BROKEN"
        from DEMO_DB.PUBLIC.trns_tbl_states__dbt_tmp
    );
