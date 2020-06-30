select pg_sleep(3);
select current_timestamp;
truncate table tx;
insert into cmpts values(3,current_timestamp);
select current_timestamp;
select true from cmpts r,cmpts n, cmpts d
where r.c1 = 1 and n.c1 = 2 and d.c1 = 3
and r.ts< n.ts and n.ts < d.ts;
