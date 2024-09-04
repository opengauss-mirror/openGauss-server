------------------------------
--- test various query write
--- 1. const param eval
------------------------------
create schema query_rewrite;
set current_schema = query_rewrite;

create table t1 (a int, b int);
create table t2 (a int, b int);

create index i on t2(a);

--test const param eval: const param should be removed and convert to semi-join
explain (costs off) select * from t1 where ( '1' = '0' or ( '1' = '1' and exists ( select /*+ rows(t2 #9999999) */ a from t2 where t1.a=t2.a)));

--test const param eval: const param should be removed and convert to seqscan
explain (costs off) select * from t1 where ( '1' = '1' or ( '1' = '1' and exists ( select /*+ rows(t2 #9999999) */ a from t2 where t1.a=t2.a)));

--test const param eval: const param should be removed and convert to semi-join
explain (costs off) select * from t1 where ( '1' = '1' and ( '1' = '1' and exists ( select /*+ rows(t2 #9999999) */ a from t2 where t1.a=t2.a)));

--test const param eval: const param should be removed and convert to seqscan
explain (costs off) select * from t1 where ( '1' = '0' and ( '1' = '1' and exists ( select /*+ rows(t2 #9999999) */ a from t2 where t1.a=t2.a)));

--test const param eval: const param should be removed and convert to seqscan
explain (costs off) select * from t1 where ( '1' = '0' or ( '1' = '1' or exists ( select /*+ rows(t2 #9999999) */ a from t2 where t1.a=t2.a)));

--test const param eval: const param should be removed and convert to seqscan
explain (costs off) select * from t1 where ( '1' = '1' or ( '1' = '1' or exists ( select /*+ rows(t2 #9999999) */ a from t2 where t1.a=t2.a)));

--test const param eval: const param should be removed and convert to semi-join
explain (costs off) select * from t1 where ( '1' = '1' and ( '1' = '0' or exists ( select /*+ rows(t2 #9999999) */ a from t2 where t1.a=t2.a)));

--test const param eval: const param should be removed and convert to seqscan
explain (costs off) select * from t1 where ( '1' = '0' and ( '1' = '1' or exists ( select /*+ rows(t2 #9999999) */ a from t2 where t1.a=t2.a)));

-- test for optimized join rel as sub-query
set qrw_inlist2join_optmode = 'rule_base';

CREATE TABLE t3 (
slot integer NOT NULL,
cid bigint NOT NULL,
name character varying NOT NULL
)
WITH (orientation=row);

insert into t3 (slot, cid, name) values(generate_series(1, 10), generate_series(1, 10), 'records.storage.state');

analyze t3;

explain (costs off) 
select 
  * 
from 
  t3 
where 
  slot = '5' 
  and (name) in (
    select 
      name 
    from 
      t3 
    where 
      slot = '5' 
      and cid in (
        5, 1000, 1001, 1002, 1003, 1004, 1005, 
        1006, 1007, 2000, 4000, 10781986, 10880002
      ) 
    limit 
      50
  );

select 
  * 
from 
  t3 
where 
  slot = '5' 
  and (name) in (
    select 
      name 
    from 
      t3 
    where 
      slot = '5' 
      and cid in (
        5, 1000, 1001, 1002, 1003, 1004, 1005, 
        1006, 1007, 2000, 4000, 10781986, 10880002
      ) 
    limit 
      50
  );

explain (costs off) 
select 
  * 
from 
  t3 
where 
  cid in (
    select 
      cid 
    from 
      t3 
    where 
      slot = '5' 
      and (name) in (
        select 
          name 
        from 
          t3 
        where 
          slot = '5' 
          and cid in (
            5, 1000, 1001, 1002, 1003, 1004, 1005, 
            1006, 1007, 2000, 4000, 10781986, 10880002
          ) 
        limit 
          50
      )
  );

select 
  * 
from 
  t3 
where 
  cid in (
    select 
      cid 
    from 
      t3 
    where 
      slot = '5' 
      and (name) in (
        select 
          name 
        from 
          t3 
        where 
          slot = '5' 
          and cid in (
            5, 1000, 1001, 1002, 1003, 1004, 1005, 
            1006, 1007, 2000, 4000, 10781986, 10880002
          ) 
        limit 
          50
      )
  );
-- fix bug: ERROR: no relation entry for relid 1
-- Scenario:1
set enable_hashjoin=on;
set enable_material=off;
set enable_mergejoin=off;
set enable_nestloop=off;
create table k1(id int,id1 int);
create table k2(id int,id1 int);
create table k3(id int,id1 int);
explain (costs off)select   m.*,k3.id from (select tz.* from (select k1.id from k1 WHERE exists (select k2.id from k2 where k2.id = k1.id and k2.id1 in (1,2,3,4,5,6,7,8,9,10,11))) tz limit 10) m left join k3 on m.id = k3.id;
-- Scenario:2
create table customer(c_birth_month int);
 select 
      1
    from 
      customer t1 ,
      (with tmp2 as ( select 1 as c_birth_month,   2 as c_birth_day   from   now()) 
       select c_birth_month,  c_birth_day from ( select 1 as c_birth_month,   2 as c_birth_day   from   now()) tmp2 ) t2  
    where 
      t1.c_birth_month = t2.c_birth_day  and exists (select  1 ) ;
-- Scenario:3
select 1 from  customer where c_birth_month not in (with  tmp1 as (select 1  from now()) select * from tmp1);

--fix bug: Error hint: TableScan(seq_t0), relation name "seq_t0" is not found.
drop table if exists seq_t0;
drop table if exists seq_t1;
create table seq_t0(a int, b int8 );
create table seq_t1(a int, b int8 );
explain (costs off) select /*+ tablescan(seq_t0) */ b from seq_t0 union all select /*+ tablescan(seq_t1) */ b from seq_t1;
--test pulling up sublinks: in orclause.
drop table if exists t1;
drop table if exists t2;
create table t1(c1 int, c2 int, c3 int);
create table t2(c1 int, c2 int, c3 int);
insert into t1 values(1,0),(2,0),(1,0),(2,1),(1,1),(1,0),(2,0),(1,0),(2,1),(1,1),(2,3),(2,1),(1,2);
insert into t2 values(1,0,1),(2,0,2),(1,0,1),(2,1,1),(1,1,0),(1,0,1),(2,0,2),(1,0,1),(2,1,1),(1,1,0),(0,0,1);
explain  (verbose, costs off) select * from t2 where t2.c1 in (select t1.c1 from t1 group by t1.c1, t1.c2) or t2.c2 = 1;
drop table if exists t1;
drop table if exists t2;

CREATE TABLE table1 ( column63 INT , column44 INT ) ;
SELECT 1 FROM ( SELECT 1 FROM table1 WHERE NOT EXISTS ( SELECT 1 WHERE column63 = column44 ) ) AS alias1 ;
insert into table1 values(2,3);
insert into table1 values(4,4);
SELECT 1 FROM ( SELECT 1 FROM table1 WHERE NOT EXISTS ( SELECT 1 WHERE column63 = column44 ) ) AS alias1 ;
drop table table1;

--test for update + inlist to join bug
reset all;
set current_schema = query_rewrite;
set qrw_inlist2join_optmode = 'rule_base';


create table test_forupdate(a int,b int,c text);
create table test_helper(a int, b int, c text);
insert into test_forupdate values(1,1,'test');
insert into test_helper values(1,1,'bbbtest');

explain (costs off) select * from test_forupdate where a in (1,2,3,4, 5,6,7,8,9,10,11,12);
select * from test_forupdate where a in(1,2,3,4,5,6,7,8,9,10,11,12);
--can not inlist to join
explain (costs off) select * from test_forupdate where a in (1,2,3,4,5,6,7,8,9,10,11,12) for update;
select * from test_forupdate where a in (1,2,3,4,5,6,7,8,9,10,11,12) for update;
--not target table,ok
explain (costs off) select * from test_forupdate,test_helper where test_forupdate.a in (1,2,3,4,5,6,7,8,9,10,11,12) for update of test_helper;
select * from test_forupdate,test_helper where test_forupdate.a in (1,2,3,4,5,6,7,8,9,10,11,12) for update of test_helper;
--subquery,target table,ok
explain (costs off) select * from (select * from test_forupdate limit 1) where a in (1,2,3,4,5,6,7,8,9,10,11,12) for update;
select * from (select * from test_forupdate limit 1) where a in (1,2,3,4,5,6,7,8,9,10,11,12) for update;

--test bug scene,concurrent update
\parallel on 2
begin
	set qrw_inlist2join_optmode = 'rule_base';
	perform pg_sleep(3);
	perform c,a from test_forupdate where a in (1,2,3,4,5,6,7,8,9,10,11,12) for update;
end;
/

begin
	update test_forupdate set b=10 where a=1;
	perform pg_sleep(3);
end;
/
\parallel off
drop table test_forupdate;
drop table test_helper;
reset qrw_inlist2join_optmode;

drop schema query_rewrite cascade;
reset current_schema;
