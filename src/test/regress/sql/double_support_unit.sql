alter system set password_lock_time to '1s';
select pg_sleep(5);
show password_lock_time;
---------------------------------------------------------------------------

alter system set password_lock_time to '1.1s';
select pg_sleep(5);
show password_lock_time;
---------------------------------------------------------------------------

alter system set password_lock_time to '1min';
select pg_sleep(5);
show password_lock_time;
---------------------------------------------------------------------------

alter system set password_lock_time to '1h';
select pg_sleep(5);
show password_lock_time;
---------------------------------------------------------------------------

alter system set password_lock_time to '1d';
select pg_sleep(5);
show password_lock_time;
---------------------------------------------------------------------------

alter system set password_lock_time to '0.1h';
select pg_sleep(5);
show password_lock_time;
---------------------------------------------------------------------------

alter system set password_lock_time to '0.1d';
select pg_sleep(5);
show password_lock_time;
