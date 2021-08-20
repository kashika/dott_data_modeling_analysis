

        insert into DEMO_DB.PUBLIC.trns_tbl_states_delta ("VEHICLE_ID", "TIME_UPDATED", "CITY_NAME", "COUNTRY_NAME", "IS_DEPLOYED", "IS_IN_WAREHOUSE", "IS_BROKEN")
        (
            select "VEHICLE_ID", "TIME_UPDATED", "CITY_NAME", "COUNTRY_NAME", "IS_DEPLOYED", "IS_IN_WAREHOUSE", "IS_BROKEN"
            from DEMO_DB.PUBLIC.trns_tbl_states_delta__dbt_tmp
        );