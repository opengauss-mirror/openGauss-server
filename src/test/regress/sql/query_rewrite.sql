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

drop schema query_rewrite cascade;
reset current_schema;
