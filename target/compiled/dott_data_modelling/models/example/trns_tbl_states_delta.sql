

    SELECT TRIM(vehicle_id) AS vehicle_id,
           TO_TIMESTAMP(SPLIT_PART(time_updated, ' UTC',1)) AS time_updated,
           TRIM(city_name) AS city_name,
           TRIM(country_name) AS country_name,
           CAST(CASE WHEN is_deployed = 'TRUE' THEN 1
                     WHEN is_deployed = 'FALSE' THEN 0
                     ELSE NULL END AS BOOLEAN) AS is_deployed,
           CAST(CASE WHEN is_in_warehouse = 'TRUE' THEN 1
                     WHEN is_in_warehouse = 'FALSE' THEN 0
                     ELSE NULL END AS BOOLEAN) AS is_in_warehouse,
           CAST(CASE WHEN is_broken = 'TRUE' THEN 1
                     WHEN is_broken = 'FALSE' THEN 0
                     ELSE NULL END AS BOOLEAN) AS is_broken
      FROM demo_db.public.tbl_states

      
      WHERE TO_TIMESTAMP(SPLIT_PART(time_updated, ' UTC',1)) > (select max(time_updated) from DEMO_DB.PUBLIC.trns_tbl_states_delta)
      