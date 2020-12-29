create table leading_zero_t1 (a integer, b numeric(16,2))
WITH (orientation=column)
DISTRIBUTE BY HASH (a);
insert into leading_zero_t1 values(1, 0.02);

CREATE TABLE leading_zero_t2(id int, val1 numeric(19,2), val2 numeric(19,4)) WITH (orientation=column);
insert into leading_zero_t2 values (1,8.88,8.8888);

select cast(0.123456789 as float4), cast(0.123456789 as float8), cast(0.123456789as numeric(5,3));
select * from leading_zero_t1;
SELECT id, val1 * val1  + val1 * val1, val1 + val1 * val1, val1 * val1 /val1/val1 + 0.1 - 1 FROM leading_zero_t2 ORDER BY 1,2;
set behavior_compat_options='display_leading_zero';
select cast(0.123456789 as float4), cast(0.123456789 as float8), cast(0.123456789as numeric(5,3));
select * from leading_zero_t1;
SELECT id, val1 * val1  + val1 * val1, val1 + val1 * val1, val1 * val1 /val1/val1 + 0.1 - 1 FROM leading_zero_t2 ORDER BY 1,2;
set behavior_compat_options='';
select cast(0.123456789 as float4), cast(0.123456789 as float8), cast(0.123456789as numeric(5,3));
select * from leading_zero_t1;
SELECT id, val1 * val1  + val1 * val1, val1 + val1 * val1, val1 * val1 /val1/val1 + 0.1 - 1 FROM leading_zero_t2 ORDER BY 1,2;

drop table leading_zero_t1;
drop table leading_zero_t2;
