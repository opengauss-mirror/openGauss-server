set resource_track_cost=1;
set resource_track_level='operator';

START TRANSACTION;
        select pg_enable_redis_proc_cancelable();
        insert into tx select v, v, v from generate_series(1, 10000000000) as v;
COMMIT;

reset resource_track_cost;
reset resource_track_level;
