select pg_sleep(2);
select current_timestamp;
alter table ptx truncate partition p0;
select current_timestamp;

insert into cmpts values(2,current_timestamp);

select true
from cmpTS a, cmpTS b 
where a.c1 = 1 and b.c1 = 2 and a.ts < b.ts;
