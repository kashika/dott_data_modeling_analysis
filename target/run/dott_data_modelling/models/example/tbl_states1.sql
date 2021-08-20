

      create or replace transient table DEMO_DB.PUBLIC.tbl_states1  as
      (

with states as(
 select *  from DEMO_DB.PUBLIC.tbl_states

  -- this filter will only be applied on an incremental run
   where to_timestamp(split_part(time_updated, ' UTC', 1)) >  timestamp '2021-01-01 00:00:00.000'
   and to_timestamp(split_part(time_updated, ' UTC', 1))  <  timestamp '2021-01-02 00:00:00.000'


)
select * from states
      );
    