-- test fastpath mechanism for index insertion
create table fastpath (a int, b text, c numeric) with (storage_type=USTORE);
create unique index fpindex1 on fastpath(a);

insert into fastpath values (1, 'b1', 100.00);
insert into fastpath values (1, 'b1', 100.00); -- unique key check

truncate fastpath;
insert into fastpath select generate_series(1,10000), 'b', 100;

-- vacuum the table so as to improve chances of index-only scans. we can't
-- guarantee if index-only scans will be picked up in all cases or not, but
-- that fuzziness actually helps the test.
vacuum fastpath;

set enable_seqscan to false;
set enable_bitmapscan to false;

select sum(a) from fastpath where a >= 5000 and a < 5700;
select sum(a) from fastpath where a >= 5000 and a < 5700;
select sum(a) from fastpath where a = 6456;
select sum(a) from fastpath where a >= 5000 and a < 5700;

-- drop the only index on the table and compute hashes for
-- a few queries which orders the results in various different ways.
drop index fpindex1;
truncate fastpath;
insert into fastpath select y.x, 'b' || (y.x/10)::text, 100 from (select generate_series(1,10000) as x) y;
select md5(string_agg(a::text, b order by a, b asc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by a desc, b desc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by b, a desc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by b, a asc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';

-- now create a multi-column index with both column asc
create index fpindex2 on fastpath(a, b);
truncate fastpath;
insert into fastpath select y.x, 'b' || (y.x/10)::text, 100 from (select generate_series(1,10000) as x) y;
-- again, vacuum here either forces index-only scans or creates fuzziness
vacuum fastpath;
select md5(string_agg(a::text, b order by a, b asc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by a desc, b desc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by b, a desc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by b, a asc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';

-- same queries with a different kind of index now. the final result must not
-- change irrespective of what kind of index we have.
drop index fpindex2;
create index fpindex3 on fastpath(a desc, b asc);
truncate fastpath;
insert into fastpath select y.x, 'b' || (y.x/10)::text, 100 from (select generate_series(1,10000) as x) y;
vacuum fastpath;
select md5(string_agg(a::text, b order by a, b asc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by a desc, b desc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by b, a desc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by b, a asc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';

-- repeat again
drop index fpindex3;
create index fpindex4 on fastpath(a asc, b desc);
truncate fastpath;
insert into fastpath select y.x, 'b' || (y.x/10)::text, 100 from (select generate_series(1,10000) as x) y;
vacuum fastpath;
select md5(string_agg(a::text, b order by a, b asc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by a desc, b desc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by b, a desc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by b, a asc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';

-- and again, this time indexing by (b, a). Note that column "b" has non-unique
-- values.
drop index fpindex4;
create index fpindex5 on fastpath(b asc, a desc);
truncate fastpath;
insert into fastpath select y.x, 'b' || (y.x/10)::text, 100 from (select generate_series(1,10000) as x) y;
vacuum fastpath;
select md5(string_agg(a::text, b order by a, b asc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by a desc, b desc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by b, a desc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by b, a asc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';

-- one last time
drop index fpindex5;
create index fpindex6 on fastpath(b desc, a desc);
truncate fastpath;
insert into fastpath select y.x, 'b' || (y.x/10)::text, 100 from (select generate_series(1,10000) as x) y;
vacuum fastpath;
select md5(string_agg(a::text, b order by a, b asc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by a desc, b desc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by b, a desc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';
select md5(string_agg(a::text, b order by b, a asc)) from fastpath
	where a >= 1000 and a < 2000 and b > 'b1' and b < 'b3';

drop table fastpath;
