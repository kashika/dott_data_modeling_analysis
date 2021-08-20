# This document is created to highlight the steps involved to do the dott data modelling challenge

# Requirements & Tools
- Snowflake version 0.20.0
- DBT version 0.20.0
- Pycharm (Python version 3.7, pandas)
- Github
- Draw.io( Design Doc)

# Connection to Snowflake account -
For the purpose of this project, I have created a snowflake account. The connection details to connect snowflake to dbt 
is documented in dbt_project.yml .The location where the files are located can be seen in dbt_project.yml. For 
initializing the dbt project via dbt init, i also created a profiles.yml to give details for snowflake account -


--------------------------------------
my-snowflake-db:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: bga49113.us-east-1

      # User/password auth
      user: 
      password: 

      role: ACCOUNTADMIN
      database: DEMO_DB
      warehouse: COMPUTE_WH
      schema: PUBLIC
      threads: 20
      client_session_keep_alive: False
      # query_tag: [anything]
--------------------------------------

# Project Initialization 

- dbt init dott_data_modelling
- dbt seed - This loads all the csv files present in the data folder into the snowflake account in a tabular format.

# Provided input - two data sources - tbl_states & tbl_telemetry.
The size of these files are huge, so I have segregated the input files into files with record counts of 500k. 
The logic to split these files is written in python/split_states.py and python/split_telemetry.py

COMMANDS -

python3 python/states.py
python3 python/telemetry.py


# Loading data into Raw Layer
The dbt seed command loaded all the records in 500k batches in the snowflake database. To create a common dataset I 
loaded all the records in raw_tbl_states & raw_tbl_telemetry datasets. Schema for these table is defined in models/example/schema.yml file. 
Refer - models/example/raw_tbl_states.sql & models/example/raw_tbl_telemetry.sql
These datasets are in raw format and are loaded as it is with no modifications.

Commands-
dbt run --models raw_tbl_states 
dbt run --models raw_tbl_telemetry 
 
** NOTE - The above step does one time load of raw data, the incremental logic is written in below scripts
models/example/raw_tbl_states_delta.sql & models/example/raw_tbl_telemetry_delta.sql

# Incremental Load
The process mentioned above to load files in the raw layer is effective for one time history load. Inorder to make this 
effective for daily loads, the files will have to be loaded incrementally. The logic for this is written in 
raw_tl_states_delta.sql & raw_tbl_telelmetry_delta.sql

dbt run --models raw_tbl_states_delta --vars '{"START_TIME":"2021-01-01 00:00:00.0", "END_TIME":"2021-01-31 00:00:00.0"}'
dbt run --models raw_tbl_telemetry_delta --vars '{"START_TIME":"2021-01-01 00:00:00.0", "END_TIME":"2021-01-31 00:00:00.0"}'

# Loading data into Transformation Layer 
The records are loaded into the transformation tables in incremental fashion with the column data types modified as 
per the details provided in the instructions document. Respective comments has been added in the query where applicable
for easy readability. 
Below are the two transformed tables & the commands to run these queries

trns_tbl_states 
dbt run --models trns_tbl_states --vars '{"START_TIME":"2020-12-31 00:00:00.0", "END_TIME":"2021-02-01 00:00:00.0"}'

trns_tbl_telemetry
dbt run --models trns_tbl_telemetry --vars '{"START_TIME":"2020-12-31 00:00:00.0", "END_TIME":"2021-02-01 00:00:00.0"}'


# Creating staging datasets
All staging datasets are created with naming convention 's_'. These are truncate and load tables and are fully refreshed 
every time the scripts are run to manage incremental loads. These datasets have dynamic variables with the names 
'START_DATE' & 'END_DATE'. The command to run each module is provided in the scripts as comments

1. s_calender - list of all calender dates in the incremental load
2. s_agg_vehicle_loss_daily - count of lost vehicles on a daily level in each city/country
3. s_agg_vehicle_running_loss_daily - 14 day running loss percentage on a daily level in each city/country
4. s_agg_vehicle_battery_level_daily - vehicles with battery level less that 20% in each city/country
5. s_agg_vehicle_loss_weekly - count of lost vehicles on a weekly level in each city/country



# Final datasets-
1. agg_vehicle_performance_metric_daily - Final daily dataset - loaded in incremental fashion to display the following-
      HASH_CODE    	            | VARCHAR(32)	    | Hash code to identify unique records
      DATE_KEY	                | DATE	            | Calender date
      CITY_NAME	                | VARCHAR(16777216)	| City name deployed in at that moment
      COUNTRY_NAME	            | VARCHAR(16777216)	| Country name deployed in at that moment
      VEHICLE_DEPLOYED_CNT	    | NUMBER(18,0)	    | Count of vehicles deployed
      LOST_VEHICLE_CNT	        | NUMBER(18,0)	    | Count of vehicles lost
      RUNNING_LOSS_PCT	        | NUMBER(28,2)	    | Running loss percentage from D-21 to D-7
      LOW_BATTERY_VEHICLE_CNT	| NUMBER(18,0)	    | Count of vehicles with less than 20% battery level


2. agg_vehicle_performance_metric_weekly - Final weekly dataset - loaded in incremental fashion to display the following-
      HASH_CODE    	            | VARCHAR(32)	    | Hash code to identify unique records
      WEEk_END_DATE             | DATE	            | week ending day for calender date
      CITY_NAME	                | VARCHAR(16777216)	| City name deployed in at that moment
      COUNTRY_NAME	            | VARCHAR(16777216)	| Country name deployed in at that moment
      VEHICLE_DEPLOYED_CNT	    | NUMBER(18,0)	    | Count of vehicles deployed
      LOST_VEHICLE_CNT	        | NUMBER(18,0)	    | Count of vehicles lost
      
# Modelling Approach-
PROS:
1. Data is loaded in incremental fashion - reduces memory & resource utilization
2. A unique identifier(hash code) is created in the transformed datasets to handle duplicates. 
3. DBT can handle duplicates in incremental logic by either using "MERGE" or "DELETE+INSERT". For our purpose, we have used
   "delete+insert" in incremental load to ensure if the job fails, we do not reload any data twice.
4. Easily scalable for history loads or large datasets with extended time ranges
5. Staging tables can be used standalone as views for analytics purposes
6. Final table is created for end customers(analysts, data scientists, PMs)

CONS:
1. DBT is not very user friendly to use DDL statements like - CREATE, INSERT, MERGE
2. Two separate tables are created for daily and weekly. We can also think of combining the two as per business use case

# Reason for choosing Tools-
1. Free and well documented open-source software
2. Easy to set up and use SQL-based tool.
3. Increased collaboration and re-use
4. Continuous Integration & Deployment possible to dev & prod environments
5. Logs and testing are integrated

# Future Work - 
1. A data quality check layer needs to be created to validate whether the total records, checksum and data
integrity is maintained while loading the records in raw layer and transformed layers

2. A process control logic should be added to make sure that if data is not fully loaded or job fails because of resource/
infrastructure issues, then the process works smoothly. Duplicate records cannot be added and no data can be missed 
while loading records. The best way for this is to leverage managed services like Aurora or a similar database, or
a custom logic can be created to make sure the process works smoothly

3. The process to run the dbt models should be automated.


** All screenshots of datasets are available under design folder