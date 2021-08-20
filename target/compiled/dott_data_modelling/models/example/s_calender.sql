-- Sample command to run this script -
-- dbt run --models s_calender --vars '{"START_DATE":"2020-12-31", "END_DATE":"2021-01-31"}'



(
    SELECT TO_DATE(SPLIT_PART(time_updated,' UTC',1)) AS all_dates
    FROM "DEMO_DB"."PUBLIC".TRNS_TBL_STATES
    WHERE TO_DATE(SPLIT_PART(time_updated,' UTC',1)) > '2020-12-31'
    AND TO_DATE(SPLIT_PART(time_updated,' UTC',1)) <= '2021-01-31'
    GROUP BY TO_DATE(SPLIT_PART(time_updated,' UTC',1))
)