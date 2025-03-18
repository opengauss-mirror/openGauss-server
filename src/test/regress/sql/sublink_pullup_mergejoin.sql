create database test_mergejoin_pullup;
\c test_mergejoin_pullup

set enable_nestloop = 0;
set enable_hashjoin = 0;

create table test_sublink(a varchar(10),b varchar(10));
insert into test_sublink values('aaa','bbb');
insert into test_sublink select * from test_sublink;
insert into test_sublink select * from test_sublink;
insert into test_sublink select * from test_sublink;
insert into test_sublink select * from test_sublink;
insert into test_sublink select * from test_sublink;
insert into test_sublink select * from test_sublink;
insert into test_sublink values('aaa','abc');
insert into test_sublink values('abc','abc');
explain(costs off) select * from test_sublink as t where t.a = (select max(d.a) as maxid from test_sublink as d where t.b=d.b) and t.a='abc';

\c regression
drop database test_mergejoin_pullup;