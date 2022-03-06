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

drop schema query_rewrite cascade;
reset current_schema;
