/*
The purpose of this script is to load states files to trnsformed dataset. The datatype is matched to the format provided
in the assignment. I have created a hash_code as a quinque key to make sure duplicates are not added in the dataset.
The transformed states dataset will have records loaded in an incremental fashion. If a hash code is found
in the query, it will delete and insert new hash code records in the transformed dataset
Command to run this script -
dbt run --models trns_tbl_states --vars '{"START_TIME":"2020-12-31 00:00:00.0", "END_TIME":"2021-02-01 00:00:00.0"}'
*/


    SELECT
           /* Create a unique hash code using MD5 and replace nulls by blank string*/
           MD5(concat(trim(vehicle_id), time_updated, city_name, country_name, IFNULL(cast(is_deployed as string),'')
           ,IFNULL(cast(is_in_warehouse as string),''), IFNULL(cast(is_broken as string),''))) as hash_code,
           TRIM(vehicle_id) AS vehicle_id,
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
      FROM demo_db.public.raw_tbl_states
      WHERE TO_TIMESTAMP(SPLIT_PART(time_updated,' UTC',1)) > '2020-12-31 00:00:00.0'
      AND TO_TIMESTAMP(SPLIT_PART(time_updated,' UTC',1)) <= '2021-02-01 00:00:00.0'