

      create or replace transient table DEMO_DB.PUBLIC.drop_staging  as
      (
drop table demo_db.public.s_calender cascade
      );
    