select pg_sleep(2);

insert into cmpTS values(1,current_timestamp);
truncate table tx;
insert into cmpTS values(2,current_timestamp);

select true 
from cmpTS a, cmpTS b 
where a.c1 = 1 and b.c1 = 2 and (b.ts-a.ts)>'00:00:02';

select a.ts as ts1,b.ts as ts2 
from cmpTS a, cmpTS b 
where a.c1 = 1 and b.c1 = 2 and (b.ts-a.ts)>'00:00:02';
